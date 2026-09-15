mod stateops;
use stateops::{AppState, SharedState};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::{FromRequestParts, Path, Query, State, rejection::JsonRejection},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
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
// convert errors from stateops to http errors
struct HttpServerError {
    status: String,
    message: String,
    code: StatusCode,
}
impl From<stateops::StateError> for HttpServerError {
    fn from(e: stateops::StateError) -> Self {
        HttpServerError {
            status: "error".to_string(),
            message: e.1,
            code: e.0,
        }
    }
}
impl IntoResponse for HttpServerError {
    fn into_response(self) -> Response {
        (
            self.code,
            Json(
                json!({"status": self.status, "code": self.code.as_u16(), "message": self.message}),
            ),
        )
            .into_response()
    }
}
// header extraction
struct ExistingUser {
    username: String,
}
impl<S> FromRequestParts<S> for ExistingUser
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _s: &S) -> Result<Self, Self::Rejection> {
        let Some(username_value) = parts.headers.get("username") else {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "code": 400, "message":"Missing username header"})),
            )
                .into_response());
        };
        let Ok(username) = username_value.to_str() else {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "code": 400, "message":"Invalid username header"})),
            )
                .into_response());
        };
        Ok(ExistingUser {
            username: username.to_owned(),
        })
    }
}

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
    body: Result<Json<PostUserRequest>, JsonRejection>,
) -> Result<Json<Value>, HttpServerError> {
    let Ok(body) = body else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid request body; must be valid json with a username field".to_string(),
        )
            .into());
    };
    let () = stateops::create_user(state, body.username.clone())?;
    Ok(Json(json!({"username": body.username.clone()})))
}
async fn get_users(State(state): State<SharedState>) -> Json<Value> {
    let users = stateops::get_users(state);
    Json(json!({"users":users}))
}
async fn get_channels(State(state): State<SharedState>) -> Json<Value> {
    let channels = stateops::get_channels(state);
    Json(json!({"channels": channels}))
}
async fn join_chanel(
    State(state): State<SharedState>,
    existing_user: ExistingUser,
    Path(channel_name): Path<String>,
) -> Result<Json<Value>, HttpServerError> {
    let () = stateops::join_channel(state, existing_user.username.clone(), channel_name.clone())?;
    Ok(Json(
        json!({"username": existing_user.username, "channel": channel_name, "joined": true}),
    ))
}
async fn list_channel_users(
    State(state): State<SharedState>,
    Path(channel_name): Path<String>,
) -> Result<impl IntoResponse, HttpServerError> {
    let users = stateops::list_channel_users(state, channel_name.clone())?;
    Ok(Json(json!({"users": users, "channel": channel_name})))
}

async fn leave_channel(
    State(state): State<SharedState>,
    Path((channel_name, username)): Path<(String, String)>,
) -> Result<impl IntoResponse, HttpServerError> {
    let () = stateops::leave_channel(state, username.clone(), channel_name.clone())?;
    Ok(Json(
        json!({"channel": channel_name, "username": username, "left": true}),
    ))
}
#[derive(Deserialize)]
struct SendMessageBody {
    text: String,
}
async fn send_message(
    State(state): State<SharedState>,
    existing_user: ExistingUser,
    Path(channel_name): Path<String>,
    body: Result<Json<SendMessageBody>, JsonRejection>,
) -> Result<impl IntoResponse, HttpServerError> {
    let Ok(body) = body else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid request body; must be valid json with a text field".to_string(),
        )
            .into());
    };
    let message_id = stateops::send_message(
        state,
        channel_name.clone(),
        existing_user.username.clone(),
        body.text.clone(),
    )?;
    Ok(Json(
        json!({"channel": channel_name, "message_id": message_id, "username": existing_user.username, "text": body.text}),
    ))
}
async fn get_messages(
    State(state): State<SharedState>,
    Path(channel_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, HttpServerError> {
    let mut limit = None;
    if let Some(lim) = params.get("limit")
        && !lim.is_empty()
    {
        let Ok(lim) = lim.parse::<usize>() else {
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid limit! Limit must be a nonnegative integer".to_string(),
            )
                .into());
        };
        limit = Some(lim);
    }
    let mut offset = None;
    if let Some(off) = params.get("offset")
        && !off.is_empty()
    {
        let Ok(off) = off.parse::<usize>() else {
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid offset! Offset must be a nonnegative integer".to_string(),
            )
                .into());
        };
        offset = Some(off);
    }
    let messages = stateops::get_messages(state, channel_name.clone(), limit, offset)?;
    Ok(Json(json!({"channel": channel_name, "messages": messages})))
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
        .route(
            "/channels/{channel}/messages",
            post(send_message).get(get_messages),
        )
        .with_state(state);

    // run our app
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", cli.host, cli.http_port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}
