use crate::discord_stateops::SharedState;
use crate::server_middleware::{SharedSystemState, get_leader, get_server_states};
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
    status: String,
    message: String,
    code: StatusCode,
}
impl From<discord_stateops::StateError> for HttpServerError {
    fn from(e: discord_stateops::StateError) -> Self {
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
        let Some(username_value) = parts.headers.get("X-User") else {
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
// System-related endpoints
// ==================================================
async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}
async fn servers(State(state): State<SharedSystemState>) -> Json<Value> {
    let server_states = get_server_states(state);
    let formatted: Vec<_> = server_states
        .into_iter()
        .map(|(server, is_alive)| json!({"server_id":server, "is_alive":is_alive}))
        .collect();
    Json(json!({"servers":formatted}))
}
async fn leader(State(state): State<SharedSystemState>) -> Json<Value> {
    let leader = get_leader(state);
    Json(json!({"leader":leader, "alive":true}))
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
) -> Result<Json<Value>, HttpServerError> {
    let Ok(body) = body else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid request body; must be valid json with a username field".to_string(),
        )
            .into());
    };
    // forward to correct owner, or if we're the owner, handle it
    let () = distributed_stateops::create_user(state, body.username.clone()).await?;
    Ok(Json(json!({"username": body.username.clone()})))
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
// async fn list_channel_users(
//     State(state): State<SharedState>,
//     Path(channel_name): Path<String>,
// ) -> Result<impl IntoResponse, HttpServerError> {
//     let users = discord_stateops::list_channel_users(state, channel_name.clone())?;
//     Ok(Json(json!({"users": users, "channel": channel_name})))
// }

// async fn leave_channel(
//     State(state): State<SharedState>,
//     Path((channel_name, username)): Path<(String, String)>,
// ) -> Result<impl IntoResponse, HttpServerError> {
//     let () = discord_stateops::leave_channel(state, username.clone(), channel_name.clone())?;
//     Ok(Json(
//         json!({"channel": channel_name, "username": username, "left": true}),
//     ))
// }
// #[derive(Deserialize)]
// struct SendMessageBody {
//     text: String,
// }
// async fn send_message(
//     State(state): State<SharedState>,
//     existing_user: ExistingUser,
//     Path(channel_name): Path<String>,
//     body: Result<Json<SendMessageBody>, JsonRejection>,
// ) -> Result<impl IntoResponse, HttpServerError> {
//     let Ok(body) = body else {
//         return Err((
//             StatusCode::BAD_REQUEST,
//             "Invalid request body; must be valid json with a text field".to_string(),
//         )
//             .into());
//     };
//     let message_id = discord_stateops::send_message(
//         state,
//         channel_name.clone(),
//         existing_user.username.clone(),
//         body.text.clone(),
//     )?;
//     Ok(Json(
//         json!({"channel": channel_name, "message_id": message_id, "username": existing_user.username, "text": body.text}),
//     ))
// }
// async fn get_messages(
//     State(state): State<SharedState>,
//     Path(channel_name): Path<String>,
//     Query(params): Query<HashMap<String, String>>,
// ) -> Result<impl IntoResponse, HttpServerError> {
//     let mut limit = None;
//     if let Some(lim) = params.get("limit")
//         && !lim.is_empty()
//     {
//         let Ok(lim) = lim.parse::<usize>() else {
//             return Err((
//                 StatusCode::BAD_REQUEST,
//                 "Invalid limit! Limit must be a nonnegative integer".to_string(),
//             )
//                 .into());
//         };
//         limit = Some(lim);
//     }
//     let mut offset = None;
//     if let Some(off) = params.get("offset")
//         && !off.is_empty()
//     {
//         let Ok(off) = off.parse::<usize>() else {
//             return Err((
//                 StatusCode::BAD_REQUEST,
//                 "Invalid offset! Offset must be a nonnegative integer".to_string(),
//             )
//                 .into());
//         };
//         offset = Some(off);
//     }
//     let messages = discord_stateops::get_messages(state, channel_name.clone(), limit, offset)?;
//     Ok(Json(json!({"channel": channel_name, "messages": messages})))
// }

async fn not_found() -> Result<impl IntoResponse, HttpServerError> {
    Err::<Json<Value>, HttpServerError>(
        (StatusCode::NOT_FOUND, "Route not found".to_string()).into(),
    )
}

async fn method_not_allowed() -> Result<impl IntoResponse, HttpServerError> {
    Err::<Json<Value>, HttpServerError>(
        (
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed".to_string(),
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
        // .route(
        //     "/channels/{channel}/members/{username}",
        //     delete(leave_channel),
        // )
        // .route("/channels/{channel}/users", get(list_channel_users))
        // .route(
        //     "/channels/{channel}/messages",
        //     post(send_message).get(get_messages),
        // )
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
