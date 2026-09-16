mod http_handlers;
mod stateops;
mod tcp_handlers;
use clap::Parser;
use stateops::{AppState, SharedState};
use std::sync::{Arc, Mutex};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct ServerCli {
    #[arg(short, long)]
    verbose: bool,
    /// the port to listen for http connections on
    #[arg(long, default_value_t = discord_common::DEFAULT_HTTP_PORT)]
    http_port: u16,
    /// the port to listen for regular tcp connections on
    #[arg(long, default_value_t = discord_common::DEFAULT_TCP_PORT)]
    tcp_port: u16,
    /// the host to listen on
    #[arg(long, default_value_t = discord_common::DEFAULT_HOST.to_string())]
    host: String,
}

#[tokio::main]
async fn main() {
    let cli = ServerCli::parse();
    println!(
        "verbose: {} http port: {} tcp_port: {} host: {}",
        cli.verbose, cli.http_port, cli.tcp_port, cli.host
    );
    // build our application
    let state: SharedState = Arc::new(Mutex::new(AppState::new()));
    let t1 = http_handlers::create_task(state.clone(), cli.host.clone(), cli.http_port);
    let t2 = tcp_handlers::create_task(state, cli.host, cli.tcp_port);
    t1.await.unwrap();
    t2.await.unwrap();
}
