use std::net::SocketAddr;

use axum::Json;
use axum::http::StatusCode;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::stateops::{self, SharedState, StateResult};

fn handle_register(state: SharedState, line: &str) -> StateResult<String> {
    let parts = line.split(" ").collect::<Vec<_>>();
    if parts.len() != 2 {
        println!("invalid register command");
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid register command".to_string(),
        ));
    }
    let username = parts[1].to_owned();
    let () = stateops::create_user(state.clone(), username.clone())?;
    Ok(username)
}

async fn process_socket(state: SharedState, mut socket: TcpStream, addr: SocketAddr) {
    // do work with socket here
    println!("socket: {:?}", addr);
    // TODO - maybe deal with unlimited strings later
    let mut cur_line = String::new();
    let mut buf = [0; 1024];
    loop {
        let n = socket.read(&mut buf).await.unwrap();
        cur_line.push_str(std::str::from_utf8(&buf[0..n]).unwrap());
        while let Some((to_process, rest)) = cur_line.split_once("\n") {
            println!("line: {}", to_process);
            let to_process = to_process.trim();
            // process the case-sensitive command
            if to_process.starts_with("REGISTER") {
                match handle_register(state.clone(), to_process) {
                    Ok(username) => socket
                        .write_all(
                            json!({"status": "ok", "operation":"register", "username":username})
                                .to_string()
                                .as_bytes(),
                        )
                        .await
                        .unwrap(),
                    Err(e) => socket
                        .write_all(
                            json!({"status":"error", "reason": format!("{}", e.1)})
                                .to_string()
                                .as_bytes(),
                        )
                        .await
                        .unwrap(),
                }
            }
            cur_line = rest.to_owned();
        }
    }
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
