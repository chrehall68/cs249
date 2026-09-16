use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::stateops::{self, SharedState, StateResult};

struct TcpServerStateInner {
    shared: SharedState,
    user: Option<String>,
}
type TcpServerState = Arc<Mutex<TcpServerStateInner>>;

fn handle_register(state: TcpServerState, line: &str) -> StateResult<Value> {
    let parts = line.split(" ").collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid register command; expected `REGISTER <username>`".to_string(),
        ));
    }
    let state = state.lock().unwrap();
    let username = parts[1].to_owned();
    stateops::create_user(state.shared.clone(), username.clone())?;
    Ok(json! ({"status": "ok", "operation":"register", "username":username}))
}

fn handle_login(state: TcpServerState, line: &str) -> StateResult<Value> {
    let mut state = state.lock().unwrap();
    if state.user.is_some() {
        return Err((StatusCode::BAD_REQUEST, "Already logged in".to_string()));
    }
    // not already logged in, check whether request is valid
    let parts = line.split(" ").collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid login command; expected `LOGIN <username>`".to_string(),
        ));
    }

    let username = parts[1].to_owned();
    if !stateops::user_exists(state.shared.clone(), &username) {
        return Err((StatusCode::BAD_REQUEST, "User does not exist".to_string()));
    }

    // user exists, so we can log in as them
    state.user = Some(username.clone());
    Ok(json! ({"status": "ok", "operation":"login", "username":username}))
}
fn handle_logout(state: TcpServerState, line: &str) -> StateResult<Value> {
    let parts = line.split(" ").collect::<Vec<_>>();
    if parts.len() != 1 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid logout command; expected `LOGOUT`".to_string(),
        ));
    }
    let mut state = state.lock().unwrap();
    let Some(username) = state.user.clone() else {
        return Err((StatusCode::BAD_REQUEST, "User is not logged in".to_string()));
    };
    // log out the user
    state.user = None;
    Ok(json! ({"status": "ok", "operation":"logout", "username":username}))
}
fn handle_join(state: TcpServerState, line: &str) -> StateResult<Value> {
    let parts = line.split(" ").collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid join command; expected `JOIN <channel>`".to_string(),
        ));
    }
    let channel = parts[1].to_owned();
    let state = state.lock().unwrap();
    let Some(username) = state.user.clone() else {
        return Err((StatusCode::BAD_REQUEST, "User is not logged in".to_string()));
    };
    stateops::join_channel(state.shared.clone(), username.clone(), channel.clone())?;
    Ok(json! ({"status": "ok", "operation":"join", "channel":channel, "username": username}))
}
fn handle_leave(state: TcpServerState, line: &str) -> StateResult<Value> {
    let parts = line.split(" ").collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid leave command; expected `LEAVE <channel>`".to_string(),
        ));
    }
    let channel = parts[1].to_owned();
    let state = state.lock().unwrap();
    let Some(username) = state.user.clone() else {
        return Err((StatusCode::BAD_REQUEST, "User is not logged in".to_string()));
    };
    stateops::leave_channel(state.shared.clone(), username.clone(), channel.clone())?;
    Ok(json! ({"status": "ok", "operation":"leave", "channel":channel, "username": username}))
}
fn handle_list_channels(state: TcpServerState, line: &str) -> StateResult<Value> {
    let parts = line.split(" ").collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid list channels command; expected `LIST CHANNELS`".to_string(),
        ));
    }
    let state = state.lock().unwrap();
    let channels = stateops::get_channels(state.shared.clone());
    Ok(json! ({"status": "ok", "operation":"list_channels", "channels":channels}))
}
fn handle_list_users(state: TcpServerState, line: &str) -> StateResult<Value> {
    let parts = line.split(" ").collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid list users command; expected `LIST USERS <channel>`".to_string(),
        ));
    }
    let channel = parts[2].to_owned();
    let state = state.lock().unwrap();
    let users = stateops::list_channel_users(state.shared.clone(), channel.clone())?;
    Ok(json! ({"status": "ok", "operation":"list_users", "channel":channel, "users":users}))
}
fn handle_send(state: TcpServerState, line: &str) -> StateResult<Value> {
    let (_, parts) = line.split_once(" ").unwrap();
    let Some((channel, message)) = parts.split_once(" ") else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid send command; expected `SEND <channel> <message>`".to_string(),
        ));
    };
    let state = state.lock().unwrap();
    let Some(username) = state.user.clone() else {
        return Err((StatusCode::BAD_REQUEST, "User is not logged in".to_string()));
    };
    let message_id = stateops::send_message(
        state.shared.clone(),
        channel.to_owned(),
        username.clone(),
        message.to_owned(),
    )?;
    Ok(json! ({"status": "ok", "operation":"send", "message_id": message_id}))
}
fn handle_history(state: TcpServerState, line: &str) -> StateResult<Value> {
    let (_, parts) = line.split_once(" ").unwrap();
    let (channel, limit) = match parts.split_once(" ") {
        Some((channel, limit)) => {
            let Ok(limit) = limit.parse::<usize>() else {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Invalid limit! Limit must be a nonnegative integer".to_string(),
                ));
            };
            (channel, Some(limit))
        }
        None => (parts.trim(), None),
    };
    let state = state.lock().unwrap();
    let messages = stateops::get_messages(state.shared.clone(), channel.to_owned(), limit, None)?;
    Ok(json! ({"status": "ok", "operation":"history", "messages":messages}))
}
enum SocketOperation {
    Close,
    Send(StateResult<Value>),
}
async fn handle_unexpected_close(state: TcpServerState) {
    // must remove user from all channels
    let state = state.lock().unwrap();
    let Some(username) = state.user.clone() else {
        return;
    };
    stateops::leave_all_channels(state.shared.clone(), username.clone());
}
async fn process_socket(state: SharedState, mut socket: TcpStream, addr: SocketAddr) {
    use SocketOperation::*;
    // do work with socket here
    println!("socket: {:?}", addr);
    // TODO - maybe deal with unlimited strings later
    let mut cur_line = String::new();
    let mut buf = [0; 1024];
    // state variables
    let state = Arc::new(Mutex::new(TcpServerStateInner {
        shared: state,
        user: None,
    }));
    let mut open = true;
    while open {
        let n = match socket.read(&mut buf).await {
            Ok(n) => n,
            Err(e) => {
                // would happen if tcp keepalive fails
                println!("error: {}", e);
                handle_unexpected_close(state.clone()).await;
                break;
            }
        };
        println!("n: {}", n);
        if n == 0 {
            // unexpected close
            handle_unexpected_close(state.clone()).await;
            break;
        }
        cur_line.push_str(std::str::from_utf8(&buf[0..n]).unwrap());
        while let Some((line, rest)) = cur_line.split_once("\n") {
            println!("line: {}", line);
            let line = line.trim();
            // process the case-sensitive command
            let command: SocketOperation = if line.starts_with("REGISTER") {
                Send(handle_register(state.clone(), line))
            } else if line.starts_with("LOGIN") {
                Send(handle_login(state.clone(), line))
            } else if line.starts_with("LOGOUT") {
                Send(handle_logout(state.clone(), line))
            } else if line.starts_with("JOIN") {
                Send(handle_join(state.clone(), line))
            } else if line.starts_with("LEAVE") {
                Send(handle_leave(state.clone(), line))
            } else if line.starts_with("LIST CHANNELS") {
                Send(handle_list_channels(state.clone(), line))
            } else if line.starts_with("LIST USERS") {
                Send(handle_list_users(state.clone(), line))
            } else if line.starts_with("SEND") {
                Send(handle_send(state.clone(), line))
            } else if line.starts_with("HISTORY") {
                Send(handle_history(state.clone(), line))
            } else if line.starts_with("QUIT") {
                Close
            } else {
                // unknown command
                Send(Err((StatusCode::BAD_REQUEST, "Unknown command".to_owned())))
            };

            // handle socket operations
            match command {
                Close => {
                    println!("closing socket");
                    open = false;
                    break; // don't process any more messages
                }
                Send(val) => {
                    println!("sending: {:?}", val);
                    let to_send = match val {
                        Ok(val) => val,
                        Err(e) => {
                            json!({"status":"error", "code": e.0.as_u16(), "reason": format!("{}", e.1)})
                        }
                    };
                    socket
                        .write_all(format!("{}\n", to_send.to_string()).as_bytes())
                        .await
                        .unwrap();
                }
            }

            cur_line = rest.to_owned();
        }
    }
    socket.shutdown().await.unwrap();
    println!("Exiting for socket: {}", addr);
}

pub fn create_task(state: SharedState, host: String, port: u16) -> JoinHandle<()> {
    tokio::spawn(async move {
        println!("starting tcp server");
        let listener = TcpListener::bind(format!("{}:{}", host, port))
            .await
            .unwrap();

        loop {
            println!("listening for connection");
            let (socket, addr) = listener.accept().await.unwrap();

            // set keepalive
            // since we only care about client failure detection, this acts as a ping-ack
            // mechanism where, after 5 seconds of inactivity, the server will
            // send a keepalive packet to the client to ensure the connection is still alive
            let keepalive = TcpKeepalive::new()
                .with_time(Duration::from_secs(5))
                .with_interval(Duration::from_secs(1))
                .with_retries(2);

            let socket_ref = SockRef::from(&socket);
            socket_ref.set_tcp_keepalive(&keepalive).unwrap();

            println!("received connection");
            tokio::spawn(process_socket(state.clone(), socket, addr));
        }
    })
}
