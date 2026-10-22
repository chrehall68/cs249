use std::collections::{HashMap, HashSet};
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
use crate::stateops::{
    SharedState, get_server_state, get_server_states, set_leader, set_server_state,
};

/// if the peer fails to respond to heartbeats, or explicitly sends a disconnect,
/// then Value is None. Otherwise, the value will be a some
type Callback = Box<dyn FnMut(Option<Value>) + Send>;

// things to send to the other peer
// and if we get a response, then we might want to do something
// and if peer fails to respond to heartbeats, might want to know as well
struct MessageToSend {
    message: Value,
    callback: Option<Callback>,
    is_response: bool,
}
struct PeerState {
    sequence_num: usize,
    seq_to_callback: HashMap<usize, Callback>,
}
impl PeerState {
    fn new() -> Self {
        PeerState {
            sequence_num: 0,
            seq_to_callback: HashMap::new(),
        }
    }
}
type SharedPeerState = Arc<Mutex<PeerState>>;

async fn send_pings_task(
    app_state: SharedState,
    id: String,
    peer_id: String,
    sender: Sender<MessageToSend>,
    fail_notify: Arc<Notify>,
) {
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
            let app_state = app_state.clone();
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
        let mut server_alive = get_server_state(app_state.clone(), &peer_id.clone());
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
            fail_notify.notify_waiters();
            break;
        }
    }
}
async fn send_message_task(
    mut socket: OwnedWriteHalf,
    app_state: SharedState,
    peer_id: String,
    peer_state: SharedPeerState,
    mut receiver: Receiver<MessageToSend>,
    fail_notify: Arc<Notify>,
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
            let mut peer_state = peer_state.lock().unwrap();
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

        match socket.write_all(&message_str.as_bytes()).await {
            Ok(_) => (),
            Err(e) => {
                // happens if peer disconnects
                println!("Error sending message: {}", e);
                break;
            }
        }
    }
    set_server_state(app_state.clone(), peer_id.clone(), false);
    fail_notify.notify_waiters();
}

async fn leader_announcement(
    app_state: SharedState,
    id: String,
    fail_notify: Arc<Notify>,
    sender: Sender<MessageToSend>,
) {
    // at start, send leader announcement if C
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
        let mut server_states: Vec<_> = get_server_states(app_state.clone())
            .into_iter()
            .filter(|server| server.is_alive)
            .collect();
        server_states.sort_by(|a, b| a.server.id.cmp(&b.server.id));
        server_states.reverse();
        if server_states[0].server.id == id {
            set_leader(app_state.clone(), id.clone());
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
async fn peer_task(
    mut socket: TcpStream,
    state: SharedState,
    id: String,
    peer_id: Option<String>,
    fail_notify: Arc<Notify>,
) {
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
    let (sender, receiver) = tokio::sync::mpsc::channel(100);
    let peer_state = Arc::new(Mutex::new(PeerState::new()));
    let (mut reader, writer) = socket.into_split();

    // spawn background tasks
    tokio::spawn(send_pings_task(
        state.clone(),
        id.clone(),
        peer_id.clone(),
        sender.clone(),
        fail_notify.clone(),
    ));
    tokio::spawn(send_message_task(
        writer,
        state.clone(),
        peer_id.clone(),
        peer_state.clone(),
        receiver,
        fail_notify.clone(),
    ));
    tokio::spawn(leader_announcement(
        state.clone(),
        id.clone(),
        fail_notify.clone(),
        sender.clone(),
    ));
    loop {
        // then get response
        while !cur_line.contains("\n") {
            let n = reader.read(&mut buf).await.unwrap();
            if n == 0 {
                // peer disconnected
                set_server_state(state.clone(), peer_id.clone(), false);
                fail_notify.notify_waiters();
                return;
            }
            cur_line.push_str(std::str::from_utf8(&buf[0..n]).unwrap());
        }
        let (line, rest) = cur_line.split_once("\n").unwrap();
        let value: Value = serde_json::from_str(line).unwrap();
        println!("Received {}", value);
        cur_line = String::from(rest);

        // then dispatch
        if value.get("is_response").unwrap().as_bool().unwrap() {
            // it's a response
            let seq_num = value.get("sequence_number").unwrap().as_u64().unwrap();
            let seq_num = seq_num as usize;
            let callback = peer_state.lock().unwrap().seq_to_callback.remove(&seq_num);
            if let Some(mut callback) = callback {
                callback(Some(value));
            }
        } else {
            // it's a request
            let request_type = value.get("type").unwrap().as_str().unwrap();
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
            }
        }
    }
}

pub async fn create_task(
    state: SharedState,
    config: Config,
    host: String,
    port: u16,
    id: String,
) -> JoinHandle<()> {
    let fail_notify = Arc::new(Notify::new());
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
                id.clone(),
                Some(other_server.id.clone()),
                fail_notify.clone(),
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
            tokio::spawn(peer_task(
                socket,
                state.clone(),
                id.clone(),
                None,
                fail_notify.clone(),
            ));
        }
    })
}
