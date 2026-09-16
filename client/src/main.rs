use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct TcpClientCli {
    #[arg(short, long)]
    verbose: bool,
    /// the port to connect to for tcp connections
    #[arg(long, default_value_t = discord_common::DEFAULT_TCP_PORT)]
    tcp_port: u16,
    /// the host to connect to
    #[arg(long, default_value_t = discord_common::DEFAULT_HOST.to_string())]
    host: String,
}

#[tokio::main]
async fn main() {
    let cli = TcpClientCli::parse();
    println!("connecting to server {}:{}", cli.host, cli.tcp_port);
    let mut socket = TcpStream::connect(format!("{}:{}", cli.host, cli.tcp_port))
        .await
        .unwrap();
    println!("connected");
    let mut buf = [0; 1024];
    let mut cur_line = String::new();
    loop {
        // read in input
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        socket.write_all(input.as_bytes()).await.unwrap();
        if input.trim() == "QUIT" {
            break;
        }

        // then get response
        while !cur_line.contains("\n") {
            let n = socket.read(&mut buf).await.unwrap();
            cur_line.push_str(std::str::from_utf8(&buf[0..n]).unwrap());
        }
        let (line, rest) = cur_line.split_once("\n").unwrap();
        println!("{}", line);
        cur_line = String::from(rest);
    }
    socket.shutdown().await.unwrap();
    println!("disconnected");
}
