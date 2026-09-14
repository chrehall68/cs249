use std::{
    collections::{HashMap, HashSet},
    os::linux::raw::stat,
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
};
use clap::Parser;
use serde::Deserialize;
use serde_json::{Value, json};

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

struct Channel {
    users: HashSet<String>,
    // messages: Vec<String>,
}
struct AppState {
    users: HashSet<String>,
    channels: HashMap<String, Channel>,
}
impl AppState {
    pub fn new() -> Self {
        Self {
            users: HashSet::new(),
            channels: HashMap::new(),
        }
    }
}

// Handlers get a clone of this `Arc`. The `Mutex` is what actually lets
// multiple handlers mutate the same `AppState` safely.
type SharedState = Arc<Mutex<AppState>>;

// ==================================================
// HTTP endpoints
// ==================================================
async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}
#[derive(Deserialize)]
struct PostUserRequest {
    username: String,
}
async fn post_user(
    State(state): State<SharedState>,
    Json(body): Json<PostUserRequest>,
) -> impl IntoResponse {
    let mut state = state.lock().unwrap();
    if state.users.contains(&body.username) {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "code": 400, "message":"User already exists"})),
        )
    } else {
        state.users.insert(body.username.clone());
        (StatusCode::OK, Json(json!({"username": body.username})))
    }
}
async fn get_users(State(state): State<SharedState>) -> Json<Value> {
    let state = state.lock().unwrap();
    Json(json!({"users": state.users.iter().collect::<Vec<_>>()}))
}
async fn get_channels(State(state): State<SharedState>) -> Json<Value> {
    let state = state.lock().unwrap();
    Json(json!({"channels": state.channels.iter().map(|(k, _)| k).collect::<Vec<_>>()}))
}
async fn join_chanel(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(channel_name): Path<String>,
) -> impl IntoResponse {
    let mut state = state.lock().unwrap();
    if !channel_name.starts_with('#') {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "code": 400, "message":"Channel must start with #"})),
        );
    }
    // TODO - might want to validate that user exists too
    // we have a valid channel, now look it up
    let channel = state
        .channels
        .entry(channel_name.clone())
        .or_insert(Channel {
            users: HashSet::new(),
        });
    let username = headers
        .get("username")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    if channel.users.contains(&username) {
        // duplicate
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "code": 400, "message":"User already in channel"})),
        )
    }
    // otherwise, add the user
    else {
        channel.users.insert(username.clone());
        (
            StatusCode::OK,
            Json(json!({"channel": channel_name, "username": username, "joined": true})),
        )
    }
}
async fn list_channel_users(
    State(state): State<SharedState>,
    Path(channel_name): Path<String>,
) -> impl IntoResponse {
    let state = state.lock().unwrap();
    if !channel_name.starts_with('#') {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "code": 400, "message":"Channel must start with #"})),
        );
    }
    // we have a valid channel, now look it up
    if !state.channels.contains_key(&channel_name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "code": 400, "message":"Channel does not exist"})),
        );
    }
    let channel = state.channels.get(&channel_name).unwrap();
    (
        StatusCode::OK,
        Json(json!({"channel": channel_name, "users": channel.users.iter().collect::<Vec<_>>()})),
    )
}

async fn leave_channel(
    State(state): State<SharedState>,
    Path((channel_name, username)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut state = state.lock().unwrap();
    if !channel_name.starts_with('#') {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "code": 400, "message":"Channel must start with #"})),
        );
    }
    // we have a valid channel, now look it up
    if !state.channels.contains_key(&channel_name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "code": 400, "message":"Channel does not exist"})),
        );
    }
    let channel = state.channels.get_mut(&channel_name).unwrap();
    if !channel.users.contains(&username) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "code": 400, "message":"User not in channel"})),
        );
    }
    channel.users.remove(&username);
    (
        StatusCode::OK,
        Json(json!({"channel": channel_name, "username": username, "left": true})),
    )
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
    let app = Router::new()
        .route("/health", get(health))
        .route("/users", post(post_user).get(get_users))
        .route("/channels", get(get_channels))
        .route("/channels/{channel}/members", post(join_chanel))
        .route(
            "/channels/{channel}/members/{username}",
            delete(leave_channel),
        )
        .route("/channels/{channel}/users", get(list_channel_users))
        .with_state(state);

    // run our app
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", cli.host, cli.http_port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}
