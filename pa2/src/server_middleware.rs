/// This module is a dumb middleware
/// Higher functionality should be implemented on top of it
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::config::Config;
use crate::discord_stateops::SharedState;

/// if the peer fails to respond to heartbeats, or explicitly sends a disconnect,
/// then Value is None. Otherwise, the value will be a some
pub type Callback = Box<dyn FnOnce(Option<Value>) + Send>;
/// callback is called when we receive a message
pub type ReceiveCallback =
    Arc<dyn Fn(String, Value) -> Pin<Box<dyn Future<Output = Value> + Send>> + Send + Sync>;

// things to send to the other peer
// and if we get a response, then we might want to do something
// and if peer fails to respond to heartbeats, might want to know as well
struct MessageToSend {
    message: Value,
    callback: Option<Callback>,
    is_response: bool,
}
struct PeerState {
    tx: Sender<MessageToSend>,
    peer_id: String,
    sequence_num: usize,
    seq_to_callback: HashMap<usize, Callback>,
    is_alive: bool,
}
impl PeerState {
    fn new(id: String) -> (Self, Receiver<MessageToSend>) {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        (
            PeerState {
                tx,
                peer_id: id,
                sequence_num: 0,
                seq_to_callback: HashMap::new(),
                is_alive: true,
            },
            rx,
        )
    }
}
pub struct SystemState {
    discord: SharedState,
    /// id of this server
    id: String,
    /// id of the leader
    leader: String,

    // server_id -> (server, is_alive)
    server_states: HashMap<String, PeerState>,
    on_failure: Arc<Notify>,
    // server_id -> receiver
    // items should be taken from this
    receivers: HashMap<String, Receiver<MessageToSend>>,
    on_receive: Option<ReceiveCallback>,
}

impl SystemState {
    pub fn new(app_state: SharedState, config: &Config, id: String) -> Self {
        let mut server_states = HashMap::new();
        let mut receivers = HashMap::new();
        for server in config {
            let (peer_state, rx) = PeerState::new(server.id.clone());
            server_states.insert(server.id.clone(), peer_state);
            receivers.insert(server.id.clone(), rx);
        }
        Self {
            discord: app_state,
            id,
            leader: "C".to_owned(),
            server_states,
            on_failure: Arc::new(Notify::new()),
            receivers,
            on_receive: None,
        }
    }
}
pub type SharedSystemState = Arc<Mutex<SystemState>>;

pub fn set_server_state(state: SharedSystemState, server: String, is_alive: bool) {
    let mut state = state.lock().unwrap();
    state.server_states.get_mut(&server).unwrap().is_alive = is_alive;
}
pub fn get_server_state(state: SharedSystemState, server: &String) -> bool {
    let state = state.lock().unwrap();
    state.server_states.get(server).unwrap().is_alive
}
pub fn get_server_states(state: SharedSystemState) -> Vec<(String, bool)> {
    let state = state.lock().unwrap();
    let mut server_states: Vec<_> = state
        .server_states
        .values()
        .map(|server| (server.peer_id.clone(), server.is_alive))
        .collect();
    // gives us a constant ordering
    server_states.sort();
    server_states
}
pub fn set_leader(state: SharedSystemState, leader: String) {
    let mut state = state.lock().unwrap();
    state.leader = leader;
}
pub fn get_leader(state: SharedSystemState) -> String {
    let state = state.lock().unwrap();
    state.leader.clone()
}
fn get_sender(state: SharedSystemState, server: String) -> Sender<MessageToSend> {
    let state = state.lock().unwrap();
    state.server_states.get(&server).unwrap().tx.clone()
}
pub fn get_id(state: SharedSystemState) -> String {
    let state = state.lock().unwrap();
    state.id.clone()
}
pub fn get_discord(state: SharedSystemState) -> SharedState {
    let state = state.lock().unwrap();
    state.discord.clone()
}
pub fn set_receive_callback(state: SharedSystemState, callback: ReceiveCallback) {
    let mut state = state.lock().unwrap();
    state.on_receive = Some(callback);
}
pub async fn send_request(
    state: SharedSystemState,
    server: String,
    request_type: String,
    mut message: Value,
    callback: Option<Callback>,
) {
    let sender = get_sender(state.clone(), server);
    message["type"] = json!(request_type);
    sender
        .send(MessageToSend {
            message,
            callback,
            is_response: false,
        })
        .await
        .unwrap();
}

// ==============================
// Background communication tasks
// ==============================
// heartbeating
async fn send_pings_task(state: SharedSystemState, peer_id: String) {
    let id = get_id(state.clone());
    let sender = get_sender(state.clone(), peer_id.clone());
    let missed_heartbeats = Arc::new(AtomicUsize::new(0));
    let pending_heartbeats = Arc::new(Mutex::new(HashSet::new()));
    let mut key = 0;

    loop {
        // wait 2 seconds
        sleep(Duration::from_secs(2)).await;
        // set up acknowledgement machinery
        let cur_key = key;
        key += 1;
        pending_heartbeats.clone().lock().unwrap().insert(cur_key);

        // set up timeout
        {
            let pending_heartbeats = pending_heartbeats.clone();
            let missed_heartbeats = missed_heartbeats.clone();
            let app_state = state.clone();
            let peer_id = peer_id.clone();
            tokio::spawn(async move {
                sleep(Duration::from_secs(2)).await;
                let mut pending_heartbeats = pending_heartbeats.lock().unwrap();
                if pending_heartbeats.contains(&cur_key) {
                    pending_heartbeats.remove(&cur_key);
                    missed_heartbeats.fetch_add(1, Ordering::SeqCst);
                    if missed_heartbeats.load(Ordering::SeqCst) == 3 {
                        println!("Peer {peer_id} missed 3 heartbeats, disconnecting");
                        set_server_state(app_state, peer_id, false);
                    }
                }
            });
        }

        // and set up callback
        let on_ping_callback: Callback = {
            let pending_heartbeats = pending_heartbeats.clone();
            let missed_heartbeats = missed_heartbeats.clone();
            Box::new(move |_| {
                println!("Received heartbeat");
                let mut pending_heartbeats = pending_heartbeats.lock().unwrap();
                if pending_heartbeats.contains(&cur_key) {
                    pending_heartbeats.remove(&cur_key);
                    missed_heartbeats.store(0, Ordering::SeqCst);
                }
            })
        };

        println!("Sending ping to {peer_id}");
        let mut server_alive = get_server_state(state.clone(), &peer_id.clone());
        if server_alive {
            server_alive = sender
                .send(MessageToSend {
                    message: json!({"type":"PING", "from":id.clone()}),
                    callback: Some(Box::new(on_ping_callback)),
                    is_response: false,
                })
                .await
                .is_ok();
        }
        if !server_alive {
            println!("Peer {peer_id} is disconnected; stopping");
            state.lock().unwrap().on_failure.notify_waiters();
            break;
        }
    }
}
// message sending; there's a single one of these
// to prevent write conflicts
// will add a sequence number
async fn send_message_task(
    mut socket: OwnedWriteHalf,
    state: SharedSystemState,
    peer_id: String,
    mut receiver: Receiver<MessageToSend>,
) {
    loop {
        let message_str = {
            let mut message = match receiver.recv().await {
                Some(message) => message,
                None => {
                    println!("No more messages for peer {peer_id}");
                    break;
                }
            };
            let mut state = state.lock().unwrap();
            let peer_state = state.server_states.get_mut(&peer_id).unwrap();
            if !message.is_response {
                // attach a sequence number
                message.message["sequence_number"] = json!(peer_state.sequence_num + 1);
                peer_state.sequence_num += 1;
            }
            message.message["is_response"] = json!(message.is_response);
            if let Some(callback) = message.callback {
                let seq_num = peer_state.sequence_num;
                peer_state.seq_to_callback.insert(seq_num, callback);
            }
            format!("{}\n", message.message)
        };

        println!("Sending message to {peer_id}: {message_str}");

        match socket.write_all(&message_str.as_bytes()).await {
            Ok(_) => (),
            Err(e) => {
                // happens if peer disconnects
                println!("Error sending message: {}", e);
                break;
            }
        }
    }
    set_server_state(state.clone(), peer_id.clone(), false);
    state.lock().unwrap().on_failure.notify_waiters();
}
// announce self as leader if we're the leader
async fn leader_announcement_task(state: SharedSystemState, peer_id: String) {
    // at start, send leader announcement if C
    let id = get_leader(state.clone());
    let sender = get_sender(state.clone(), peer_id);
    let fail_notify = state.lock().unwrap().on_failure.clone();
    if id == "C" {
        sender
            .send(MessageToSend {
                message: json!({"type":"LEADER_ANNOUNCEMENT", "leader":id.clone()}),
                callback: None,
                is_response: false,
            })
            .await
            .unwrap();
    }
    loop {
        // on failure of a node, we might be the new leader
        // so we might want to send a leader announcement
        fail_notify.notified().await;
        let mut server_states: Vec<_> = get_server_states(state.clone())
            .into_iter()
            .filter_map(|(server, is_alive)| if is_alive { Some(server) } else { None })
            .collect();
        server_states.sort();
        server_states.reverse();
        if server_states[0] == id {
            set_leader(state.clone(), id.clone());
            let _ = sender
                .send(MessageToSend {
                    message: json!({"type":"LEADER_ANNOUNCEMENT", "leader":id.clone()}),
                    callback: None,
                    is_response: false,
                })
                .await; // if fails, we don't care since that means the peer is dead
        }
    }
}
// the main task for each peer; also handles receiving messages
async fn peer_task(mut socket: TcpStream, state: SharedSystemState, peer_id: Option<String>) {
    let id = get_id(state.clone());
    let mut buf = [0; 1024];
    let mut cur_line = String::new();

    let peer_id = if let Some(peer_id) = peer_id {
        // start by sending our ID that way receiving knows who we are
        // since the port that this connecting socket runs on on our machine isn't necessarily our peer port
        let intro_message = json!({"id":id});
        let intro_message = format!("{intro_message}\n");
        socket.write_all(&intro_message.as_bytes()).await.unwrap();
        peer_id
    } else {
        // we don't know who our peer is, but we know we can get it as the first message
        while !cur_line.contains("\n") {
            let n = socket.read(&mut buf).await.unwrap();
            cur_line.push_str(std::str::from_utf8(&buf[0..n]).unwrap());
        }
        let (line, rest) = cur_line.split_once("\n").unwrap();
        let first_message: Value = serde_json::from_str(line).unwrap();
        cur_line = String::from(rest);
        first_message
            .get("id")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    };

    println!("connected to peer {}", peer_id);

    // then now that we know who peer is, do regular connection management
    let (mut reader, writer) = socket.into_split();
    let receiver = {
        let cloned = state.clone();
        let mut state = cloned.lock().unwrap();
        state.receivers.remove(&peer_id).unwrap()
    };

    // spawn background tasks
    tokio::spawn(send_pings_task(state.clone(), peer_id.clone()));
    tokio::spawn(send_message_task(
        writer,
        state.clone(),
        peer_id.clone(),
        receiver,
    ));
    tokio::spawn(leader_announcement_task(state.clone(), peer_id.clone()));

    let on_failure = state.lock().unwrap().on_failure.clone();
    let sender = get_sender(state.clone(), peer_id.clone());

    loop {
        // then get response
        while !cur_line.contains("\n") {
            let n = reader.read(&mut buf).await.unwrap();
            if n == 0 {
                // peer disconnected
                set_server_state(state.clone(), peer_id.clone(), false);
                on_failure.notify_waiters();
                return;
            }
            cur_line.push_str(std::str::from_utf8(&buf[0..n]).unwrap());
        }
        let (line, rest) = cur_line.split_once("\n").unwrap();
        let value: Value = serde_json::from_str(line).unwrap();
        println!("Received {}", value);
        cur_line = String::from(rest);

        // then dispatch
        let seq_num = value.get("sequence_number").unwrap().as_u64().unwrap();
        let seq_num = seq_num as usize;
        if value.get("is_response").unwrap().as_bool().unwrap() {
            // it's a response
            let mut state = state.lock().unwrap();
            let peer_state = state.server_states.get_mut(&peer_id).unwrap();
            let callback = peer_state.seq_to_callback.remove(&seq_num);
            if let Some(callback) = callback {
                callback(Some(value));
            }
        } else {
            // it's a request
            let request_type = value.get("type").unwrap().as_str().unwrap().to_owned();
            if request_type == "PING" {
                // respond with PONG
                let _ = sender.send(
                    MessageToSend{
                        message: json!({"type":"PONG", "from":id.clone(), "sequence_number":value.get("sequence_number")}),
                    callback: None,
                    is_response: true,
                    }
                ).await; // if fails, we don't care since that means the peer is dead
            } else if request_type == "LEADER_ANNOUNCEMENT" {
                let leader = value.get("leader").unwrap().as_str().unwrap().to_string();
                println!("received leader announcement from {leader}");
                set_leader(state.clone(), leader);
            } else {
                // higher level request; answer in separate thread
                // to avoid deadlock
                let callback = state.lock().unwrap().on_receive.clone();
                {
                    let value = value.clone();
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        let response = match callback {
                            Some(callback) => {
                                let mut response_value =
                                    callback(request_type.to_owned(), value.clone()).await;
                                response_value["sequence_number"] = json!(seq_num);
                                response_value["request_type"] = json!(request_type);
                                Some(response_value)
                            }
                            None => None,
                        };
                        if let Some(response) = response {
                            let _ = sender
                                .send(MessageToSend {
                                    message: response,
                                    callback: None,
                                    is_response: true,
                                })
                                .await;
                        }
                    });
                }
            }
        }
    }
}

// main task
pub async fn main_task(
    state: SharedSystemState,
    config: Config,
    id: String,
    // technically, these 2 are redundant, but it makes our life easier
    host: String,
    port: u16,
) -> JoinHandle<()> {
    // connect to all servers < my id (prevents us from launching multiple connections)
    for other_server in &config {
        if other_server.id < id {
            let socket =
                TcpStream::connect(format!("{}:{}", other_server.host, other_server.peer_port))
                    .await
                    .unwrap();
            tokio::spawn(peer_task(
                socket,
                state.clone(),
                Some(other_server.id.clone()),
            ));
        }
    }
    // listener
    tokio::spawn(async move {
        println!("starting tcp server");
        let listener = TcpListener::bind(format!("{}:{}", host, port))
            .await
            .unwrap();

        loop {
            println!("listening for connection");
            let (socket, _) = listener.accept().await.unwrap();

            println!("received connection");
            tokio::spawn(peer_task(socket, state.clone(), None));
        }
    })
}
