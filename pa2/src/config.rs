use std::{collections::HashMap, path::PathBuf};

use serde::{Deserialize, Serialize};

pub type ServerId = String;
#[derive(Serialize, Deserialize, Clone, Hash, Eq, PartialEq, Debug)]
pub struct Server {
    pub id: ServerId,
    pub host: String,
    pub http_port: u16,
    pub peer_port: u16,
    pub priority: u8,
}

pub type Config = Vec<Server>;

pub fn read_config(path: &PathBuf) -> Config {
    let config = std::fs::read_to_string(&path).unwrap();
    let mut peers: HashMap<String, Vec<Server>> = serde_json::from_str(&config).unwrap();
    peers.remove("servers").unwrap()
}
pub fn get_config_for(config: &Config, id: &ServerId) -> Server {
    for server in config {
        if server.id == *id {
            return server.clone();
        }
    }
    panic!("No config for {id}");
}
