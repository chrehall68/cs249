use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::stateops::{self, SharedState, StateResult};

async fn peer_task(mut socket: TcpStream, state: SharedState, id: String, peer_id: Option<String>) {
    let mut buf = [0; 1024];
    let mut cur_line = String::new();

    let peer_id = if let Some(peer_id) = peer_id {
        // start by sending our ID that way receiving knows who we are
        // since the port that this connecting socket runs on on our machine isn't necessarily our peer port
        let intro_message = json!({"id":id});
        socket
            .write_all(&intro_message.to_string().as_bytes())
            .await
            .unwrap();
        peer_id
    } else {
        // we don't know who our peer is, but we know we can get it as the first message
        while !cur_line.contains("\n") {
            let n = socket.read(&mut buf).await.unwrap();
            cur_line.push_str(std::str::from_utf8(&buf[0..n]).unwrap());
        }
        let (line, rest) = cur_line.split_once("\n").unwrap();
        let first_message: Value = line.into();
        cur_line = String::from(rest);
        first_message.get("id").unwrap().to_string()
    };

    // then now that we know who peer is, do regular connection management
    loop {
        // then get response
        while !cur_line.contains("\n") {
            let n = socket.read(&mut buf).await.unwrap();
            cur_line.push_str(std::str::from_utf8(&buf[0..n]).unwrap());
        }
        let (line, rest) = cur_line.split_once("\n").unwrap();
        println!("{}", line);
        cur_line = String::from(rest);
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
