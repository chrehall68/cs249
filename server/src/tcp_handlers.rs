use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::stateops::SharedState;

async fn process_socket(_socket: TcpStream, addr: SocketAddr) {
    // do work with socket here
    println!("socket: {:?}", addr);
}

pub fn create_task(_state: SharedState, host: String, port: u16) -> JoinHandle<()> {
    tokio::spawn(async move {
        println!("starting tcp server");
        let listener = TcpListener::bind(format!("{}:{}", host, port))
            .await
            .unwrap();

        loop {
            println!("listening for connection");
            let (socket, addr) = listener.accept().await.unwrap();
            println!("received connection");
            tokio::spawn(process_socket(socket, addr));
        }
    })
}
