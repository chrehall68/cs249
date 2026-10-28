mod config;
mod discord_stateops;
mod distributed_stateops;
mod http_handlers;
mod server_middleware;
use clap::Parser;
use discord_stateops::AppState;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    config::{get_config_for, read_config},
    distributed_stateops::init_state,
    server_middleware::SystemState,
};

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
    let state = Arc::new(Mutex::new(AppState::new()));
    let state = Arc::new(Mutex::new(SystemState::new(
        state.clone(),
        &config,
        cli.id.clone(),
    )));
    init_state(state.clone());
    let t1 = http_handlers::create_task(
        state.clone(),
        server_config.host.clone(),
        server_config.http_port,
    );
    let t2 = server_middleware::main_task(
        state.clone(),
        config.clone(),
        cli.id.clone(),
        server_config.host,
        server_config.peer_port,
    )
    .await;
    t1.await.unwrap();
    t2.await.unwrap();
}
