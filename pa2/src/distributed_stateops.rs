use std::sync::Arc;

use axum::http::StatusCode;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::Notify;

use crate::{
    config::ServerId,
    discord_stateops::{self, StateResult, create_channel},
    server_middleware::{
        ReceiveCallback, SharedSystemState, get_discord, get_id, get_server_state, send_request,
        set_receive_callback,
    },
};

struct LocationConfig {
    primary: ServerId,
    backup: ServerId,
}

/// wire format for a StateResult:
/// {"ok": <value>} on success, {"error": {"code": <u16>, "message": <string>}} on failure
fn state_result_to_value<T: Serialize>(result: StateResult<T>) -> Value {
    match result {
        Ok(v) => json!({"ok": v}),
        Err((code, message)) => json!({"error": {"code": code.as_u16(), "message": message}}),
    }
}
fn state_result_from_value<T: DeserializeOwned>(value: &Value) -> StateResult<T> {
    if let Some(error) = value.get("error") {
        let code = StatusCode::from_u16(error["code"].as_u64().unwrap() as u16).unwrap();
        Err((code, error["message"].as_str().unwrap().to_string()))
    } else {
        Ok(serde_json::from_value(value["ok"].clone()).unwrap())
    }
}

fn receive_callback(state: SharedSystemState) -> ReceiveCallback {
    Arc::new(move |request_type, request: Value| {
        let state = state.clone();
        Box::pin(async move {
            if request_type == "user_exists" {
                let username = request["username"].as_str().unwrap();
                let exists = user_exists(state.clone(), username).await;
                return json!({"user_exists": exists});
            } else if request_type == "create_user" {
                let username = request["username"].as_str().unwrap();
                let result = create_user(state.clone(), username.to_string()).await;
                return state_result_to_value(result);
            }

            panic!("Unknown request type: {}", request_type);
        })
    })
}

pub fn init_state(state: SharedSystemState) {
    // create channels that must exist on startup
    let id = get_id(state.clone());
    if id == "A" {
        let _ = create_channel(get_discord(state.clone()), "#systems".to_string()).unwrap();
        let _ = create_channel(get_discord(state.clone()), "#cs249".to_string()).unwrap();
    } else if id == "B" {
        let _ = create_channel(get_discord(state.clone()), "#cs249".to_string()).unwrap();
        let _ = create_channel(get_discord(state.clone()), "#distributed".to_string()).unwrap();
    } else if id == "C" {
        let _ = create_channel(get_discord(state.clone()), "#distributed".to_string()).unwrap();
        let _ = create_channel(get_discord(state.clone()), "#systems".to_string()).unwrap();
    } else {
        panic!("Invalid server ID");
    }

    // and register our hook
    set_receive_callback(state.clone(), receive_callback(state));
}

fn user_owner(username: &str) -> LocationConfig {
    // partition users by first letter
    assert!(username.len() >= 1);
    let first_char = username.chars().next().unwrap();
    let first_char_mod = (first_char as u8) % 3;
    if first_char_mod == 0 {
        return LocationConfig {
            primary: "A".to_string(),
            backup: "B".to_string(),
        };
    } else if first_char_mod == 1 {
        return LocationConfig {
            primary: "B".to_string(),
            backup: "C".to_string(),
        };
    } else if first_char_mod == 2 {
        return LocationConfig {
            primary: "C".to_string(),
            backup: "A".to_string(),
        };
    }
    panic!("Invalid server ID");
}

// if None is returned, then that means the
// server is down. Trying again should succeed
async fn forward_request(
    state: SharedSystemState,
    request: Value,
    request_type: String,
    location_config: LocationConfig,
) -> Option<Value> {
    let id = get_id(state.clone());
    assert!(id != location_config.primary);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let callback = {
        Box::new(move |result| {
            tx.send(result).unwrap();
        })
    };

    let location = if get_server_state(state.clone(), &location_config.primary) {
        // forward to primary since primary is alive
        location_config.primary.clone()
    } else {
        location_config.backup.clone()
    };
    send_request(
        state.clone(),
        location,
        request_type.clone(),
        request.clone(),
        Some(callback),
    )
    .await;
    rx.await.unwrap()
}

pub async fn user_exists(state: SharedSystemState, username: &str) -> bool {
    let config = user_owner(username);
    let id = get_id(state.clone());
    // either we are the primary server, or the primary server is down
    // means we are the authoritative server
    if config.primary == id
        || (!get_server_state(state.clone(), &config.primary) && id == config.backup)
    {
        discord_stateops::user_exists(get_discord(state), username)
    } else {
        // send a request to whatever is up out of the two
        let request = json!({"username": username});
        let request_type = "user_exists".to_string();
        let result = forward_request(state.clone(), request, request_type, config).await;
        if let Some(result) = result {
            result["user_exists"].as_bool().unwrap()
        } else {
            // retry
            Box::pin(user_exists(state, username)).await
        }
    }
}
pub async fn create_user(state: SharedSystemState, username: String) -> StateResult<()> {
    let config = user_owner(&username);
    let id = get_id(state.clone());
    // either we are the primary server, or the primary server is down
    // means we are the authoritative server
    if config.primary == id
        || (!get_server_state(state.clone(), &config.primary) && id == config.backup)
    {
        discord_stateops::create_user(get_discord(state), username)
    } else {
        // send a request to whatever is up out of the two
        let request = json!({"username": username});
        let request_type = "create_user".to_string();
        let result = forward_request(state.clone(), request, request_type, config).await;
        if let Some(result) = result {
            return state_result_from_value(&result);
        }
        // retry
        Box::pin(create_user(state, username)).await
    }
}
