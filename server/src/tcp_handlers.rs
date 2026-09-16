use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use serde_json::{Value, json};
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
    let () = stateops::create_user(state.shared.clone(), username.clone())?;
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
enum SocketOperation {
    Close,
    Send(StateResult<Value>),
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
        let n = socket.read(&mut buf).await.unwrap();
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
                    socket.shutdown().await.unwrap();
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
                        .write_all(to_send.to_string().as_bytes())
                        .await
                        .unwrap();
                }
            }

            cur_line = rest.to_owned();
        }
    }
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
            println!("received connection");
            tokio::spawn(process_socket(state.clone(), socket, addr));
        }
    })
}
