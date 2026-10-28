use crate::discord_stateops::state_error;
use crate::server_middleware::{
    SharedSystemState, get_id, get_leader, get_server_state, get_server_states,
};
use crate::{discord_stateops, distributed_stateops};
use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{FromRequestParts, Path, Query, State, rejection::JsonRejection},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

// convert errors from stateops to http errors
struct HttpServerError {
    status: StatusCode,
    code: String,
    message: String,
}
impl From<discord_stateops::StateError> for HttpServerError {
    fn from(e: discord_stateops::StateError) -> Self {
        HttpServerError {
            status: e.0,
            code: e.1,
            message: e.2,
        }
    }
}
impl IntoResponse for HttpServerError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"status": "error", "code": self.code, "message": self.message})),
        )
            .into_response()
    }
}
// invalid json syntax vs valid json that is missing/has wrongly typed fields
fn json_rejection_error(rejection: JsonRejection, expected: &str) -> HttpServerError {
    let (code, message) = match rejection {
        JsonRejection::JsonDataError(_) => (
            "MISSING_FIELD",
            format!("Request body must be a json object with a {expected} field"),
        ),
        _ => ("INVALID_JSON", format!("Invalid json body: {rejection}")),
    };
    state_error(StatusCode::BAD_REQUEST, code, &message).into()
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
        let Some(username_value) = parts.headers.get("X-User") else {
            let error = state_error(
                StatusCode::BAD_REQUEST,
                "MISSING_HEADER",
                "Missing X-User header",
            );
            return Err(HttpServerError::from(error).into_response());
        };
        let Ok(username) = username_value.to_str() else {
            let error = state_error(
                StatusCode::BAD_REQUEST,
                "INVALID_USERNAME",
                "Invalid X-User header",
            );
            return Err(HttpServerError::from(error).into_response());
        };
        Ok(ExistingUser {
            username: username.to_owned(),
        })
    }
}

// ==================================================
// System-related endpoints
// ==================================================
async fn health(State(state): State<SharedSystemState>) -> Json<Value> {
    Json(json!({"status": "ok", "server_id": get_id(state)}))
}
async fn servers(State(state): State<SharedSystemState>) -> Json<Value> {
    let server_states = get_server_states(state);
    let formatted: Vec<_> = server_states
        .into_iter()
        .map(|(server, is_alive)| json!({"server_id":server, "alive":is_alive}))
        .collect();
    Json(json!({"servers":formatted}))
}
async fn leader(State(state): State<SharedSystemState>) -> Json<Value> {
    let leader = get_leader(state.clone());
    let alive = get_server_state(state.clone(), &leader);
    Json(json!({"leader":leader, "alive":alive}))
}

// ==================================================
// User endpoints
// ==================================================
#[derive(Deserialize)]
struct PostUserRequest {
    username: String,
}
async fn post_user(
    State(state): State<SharedSystemState>,
    body: Result<Json<PostUserRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpServerError> {
    let body = body.map_err(|rejection| json_rejection_error(rejection, "username"))?;
    // forward to correct owner, or if we're the owner, handle it
    let () = distributed_stateops::create_user(state, body.username.clone()).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"username": body.username.clone()})),
    ))
}
async fn get_users(State(state): State<SharedSystemState>) -> Json<Value> {
    let users = distributed_stateops::get_users(state).await;
    Json(json!({"users":users}))
}
async fn debug_get_users(State(state): State<SharedSystemState>) -> Json<Value> {
    let users = distributed_stateops::get_my_users(state).await;
    Json(json!({"users":users}))
}

// ==================================================
// Channel endpoints
// ==================================================

async fn debug_get_channels(State(state): State<SharedSystemState>) -> Json<Value> {
    let channels = distributed_stateops::get_my_channels(state).await;
    Json(json!({"channels": channels}))
}
async fn get_channels(State(state): State<SharedSystemState>) -> Json<Value> {
    let channels = distributed_stateops::get_channels(state).await;
    Json(json!({"channels": channels}))
}
async fn join_channel(
    State(state): State<SharedSystemState>,
    existing_user: ExistingUser,
    Path(channel_name): Path<String>,
) -> Result<Json<Value>, HttpServerError> {
    let () = distributed_stateops::join_channel(
        state,
        existing_user.username.clone(),
        channel_name.clone(),
    )
    .await?;
    Ok(Json(
        json!({"username": existing_user.username, "channel": channel_name, "joined": true}),
    ))
}
async fn list_channel_users(
    State(state): State<SharedSystemState>,
    Path(channel_name): Path<String>,
) -> Result<impl IntoResponse, HttpServerError> {
    let users = distributed_stateops::list_channel_users(state, channel_name.clone()).await?;
    Ok(Json(json!({"users": users, "channel": channel_name})))
}
async fn debug_list_channel_users(
    State(state): State<SharedSystemState>,
    Path(channel_name): Path<String>,
) -> Result<impl IntoResponse, HttpServerError> {
    let users = distributed_stateops::debug_list_channel_users(state, channel_name.clone()).await?;
    Ok(Json(json!({"users": users, "channel": channel_name})))
}
async fn leave_channel(
    State(state): State<SharedSystemState>,
    Path((channel_name, username)): Path<(String, String)>,
) -> Result<impl IntoResponse, HttpServerError> {
    let () =
        distributed_stateops::leave_channel(state, username.clone(), channel_name.clone()).await?;
    Ok(Json(
        json!({"channel": channel_name, "username": username, "left": true}),
    ))
}
#[derive(Deserialize)]
struct SendMessageBody {
    text: String,
}
async fn send_message(
    State(state): State<SharedSystemState>,
    existing_user: ExistingUser,
    Path(channel_name): Path<String>,
    body: Result<Json<SendMessageBody>, JsonRejection>,
) -> Result<impl IntoResponse, HttpServerError> {
    let body = body.map_err(|rejection| json_rejection_error(rejection, "text"))?;
    let message_id = distributed_stateops::send_message(
        state,
        channel_name.clone(),
        existing_user.username.clone(),
        body.text.clone(),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"channel": channel_name, "message_id": message_id, "username": existing_user.username, "text": body.text}),
        ),
    ))
}
async fn debug_get_messages(
    State(state): State<SharedSystemState>,
    Path(channel_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, HttpServerError> {
    let mut limit = None;
    if let Some(lim) = params.get("limit")
        && !lim.is_empty()
    {
        let Ok(lim) = lim.parse::<usize>() else {
            return Err(state_error(
                StatusCode::BAD_REQUEST,
                "INVALID_QUERY",
                "Invalid limit! Limit must be a nonnegative integer",
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
            return Err(state_error(
                StatusCode::BAD_REQUEST,
                "INVALID_QUERY",
                "Invalid offset! Offset must be a nonnegative integer",
            )
            .into());
        };
        offset = Some(off);
    }
    let messages =
        distributed_stateops::debug_get_messages(state, channel_name.clone(), limit, offset)
            .await?;
    Ok(Json(json!({"channel": channel_name, "messages": messages})))
}
async fn get_messages(
    State(state): State<SharedSystemState>,
    Path(channel_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, HttpServerError> {
    let mut limit = None;
    if let Some(lim) = params.get("limit")
        && !lim.is_empty()
    {
        let Ok(lim) = lim.parse::<usize>() else {
            return Err(state_error(
                StatusCode::BAD_REQUEST,
                "INVALID_QUERY",
                "Invalid limit! Limit must be a nonnegative integer",
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
            return Err(state_error(
                StatusCode::BAD_REQUEST,
                "INVALID_QUERY",
                "Invalid offset! Offset must be a nonnegative integer",
            )
            .into());
        };
        offset = Some(off);
    }
    let messages =
        distributed_stateops::get_messages(state, channel_name.clone(), limit, offset).await?;
    Ok(Json(json!({"channel": channel_name, "messages": messages})))
}

async fn not_found() -> Result<impl IntoResponse, HttpServerError> {
    Err::<Json<Value>, HttpServerError>(
        state_error(StatusCode::NOT_FOUND, "UNKNOWN_PATH", "Route not found").into(),
    )
}

async fn method_not_allowed() -> Result<impl IntoResponse, HttpServerError> {
    Err::<Json<Value>, HttpServerError>(
        state_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "METHOD_NOT_ALLOWED",
            "method not allowed",
        )
        .into(),
    )
}
fn build_app(state: SharedSystemState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/servers", get(servers))
        .route("/leader", get(leader))
        .route("/users", post(post_user).get(get_users))
        .route("/debug/users", get(debug_get_users))
        .route("/channels", get(get_channels))
        .route("/debug/channels", get(debug_get_channels))
        .route("/channels/{channel}/members", post(join_channel))
        .route(
            "/channels/{channel}/members/{username}",
            delete(leave_channel),
        )
        .route("/channels/{channel}/users", get(list_channel_users))
        .route(
            "/debug/channels/{channel}/users",
            get(debug_list_channel_users),
        )
        .route(
            "/channels/{channel}/messages",
            post(send_message).get(get_messages),
        )
        .route(
            "/debug/channels/{channel}/messages",
            get(debug_get_messages),
        )
        // handling so that we return json, as requested, on these errors
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state)
}
pub fn create_task(state: SharedSystemState, host: String, port: u16) -> JoinHandle<()> {
    tokio::spawn(async move {
        let app = build_app(state);

        // run our app
        let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port))
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    })
}
