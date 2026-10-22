use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::config::Config;
use crate::stateops::{SharedState, get_server_state, set_server_state};

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
        if get_server_state(app_state.clone(), &peer_id.clone()) {
            sender
                .send(MessageToSend {
                    message: json!({"type":"PING", "from":id.clone()}),
                    callback: Some(Box::new(on_ping_callback)),
                    is_response: false,
                })
                .await
                .unwrap();
        } else {
            println!("Peer {peer_id} is disconnected; stopping");
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
) {
    loop {
        let message_str = {
            let mut message = match receiver.recv().await {
                Some(message) => message,
                None => break,
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
                set_server_state(app_state.clone(), peer_id.clone(), false);
                break;
            }
        }
    }
}

async fn peer_task(mut socket: TcpStream, state: SharedState, id: String, peer_id: Option<String>) {
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

    // spawn background tasks
    tokio::spawn(send_pings_task(
        state.clone(),
        id.clone(),
        peer_id.clone(),
        sender.clone(),
    ));
    let (mut reader, writer) = socket.into_split();
    tokio::spawn(send_message_task(
        writer,
        state.clone(),
        peer_id.clone(),
        peer_state.clone(),
        receiver,
    ));
    loop {
        // then get response
        while !cur_line.contains("\n") {
            let n = reader.read(&mut buf).await.unwrap();
            if n == 0 {
                // peer disconnected
                set_server_state(state.clone(), peer_id.clone(), false);
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
                sender.send(
                    MessageToSend{
                        message: json!({"type":"PONG", "from":id.clone(), "sequence_number":value.get("sequence_number")}),
                    callback: None,
                    is_response: true,
                    }
                ).await.unwrap();
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
            tokio::spawn(peer_task(socket, state.clone(), id.clone(), None));
        }
    })
}
