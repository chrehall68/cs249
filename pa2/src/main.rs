mod config;
mod http_handlers;
mod stateops;
mod tcp_handlers;
use clap::Parser;
use stateops::{AppState, SharedState};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::config::{get_config_for, read_config};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct ServerCli {
    #[arg(short, long)]
    verbose: bool,
    /// the id of this server
    #[arg(long)]
    id: String,
    /// the file containing the locations of the other servers
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli = ServerCli::parse();
    println!(
        "verbose: {} id: {} config: {:?}",
        cli.verbose, cli.id, cli.config
    );
    // read in the config file
    let config = read_config(&cli.config);
    let server_config = get_config_for(&config, &cli.id);

    // build our application
    println!("server config: {:?}", server_config);
    println!("peers: {:?}", config);
    let state: SharedState = Arc::new(Mutex::new(AppState::from(cli.id.clone(), &config)));
    let t1 = http_handlers::create_task(
        state.clone(),
        server_config.host.clone(),
        server_config.http_port,
    );
    let t2 = tcp_handlers::create_task(
        state.clone(),
        config.clone(),
        server_config.host,
        server_config.peer_port,
        cli.id.clone(),
    )
    .await;
    t1.await.unwrap();
    t2.await.unwrap();
}
