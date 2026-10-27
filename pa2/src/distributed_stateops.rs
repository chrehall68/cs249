use std::{
    hash::{self, Hasher},
    sync::Arc,
};

use crate::{
    config::ServerId,
    discord_stateops::{self, StateResult, UserExistsFn, create_channel},
    server_middleware::{
        ReceiveCallback, SharedSystemState, get_discord, get_id, get_server_state,
        get_server_states, send_request, set_receive_callback,
    },
};
use axum::http::StatusCode;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

struct LocationConfig {
    primary: ServerId,
    backup: ServerId,
}

// ==============================
// Request Forwarding
// ==============================

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

async fn forward_request_to(
    state: SharedSystemState,
    request: Value,
    request_type: String,
    to: ServerId,
) -> Option<Value> {
    let id = get_id(state.clone());
    assert!(id != to);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let callback = {
        Box::new(move |result| {
            tx.send(result).unwrap();
        })
    };
    send_request(
        state.clone(),
        to,
        request_type.clone(),
        request.clone(),
        Some(callback),
    )
    .await;
    rx.await.unwrap()
}
/// request should contain any necessary parameters
async fn replicate_op(
    state: SharedSystemState,
    mut request: Value,
    request_type: String,
    to: ServerId,
) -> Option<Value> {
    // nest the request before sending
    request["type"] = request_type.into();
    let request = json!({"request": request});

    forward_request_to(state, request, "replicate".to_owned(), to).await
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
    let location = if get_server_state(state.clone(), &location_config.primary) {
        // forward to primary since primary is alive
        location_config.primary.clone()
    } else {
        location_config.backup.clone()
    };
    forward_request_to(state, request, request_type, location).await
}

fn receive_callback(state: SharedSystemState) -> ReceiveCallback {
    Arc::new(move |request_type, request: Value| {
        let state = state.clone();
        Box::pin(async move {
            if request_type == "user_exists" {
                // if forwarded here, don't need to look elsewhere
                let username = request["username"].as_str().unwrap();
                let exists = discord_stateops::user_exists(get_discord(state), username);
                return json!({"user_exists": exists});
            } else if request_type == "create_user" {
                let username = request["username"].as_str().unwrap();
                let result = create_user(state, username.to_string()).await;
                return state_result_to_value(result);
            } else if request_type == "get_users" {
                // get users is a special case where we don't need to do any communication
                // since get users caller will already call all servers
                let users = discord_stateops::get_users(get_discord(state));
                return state_result_to_value(Ok(users));
            } else if request_type == "get_channels" {
                let channels = discord_stateops::get_channels(get_discord(state));
                return state_result_to_value(Ok(channels));
            } else if request_type == "join_channel" {
                let username = request["username"].as_str().unwrap();
                let channel_name = request["channel"].as_str().unwrap();
                let result =
                    join_channel(state, username.to_string(), channel_name.to_string()).await;
                return state_result_to_value(result);
            } else if request_type == "list_channel_users" {
                let channel_name = request["channel"].as_str().unwrap();
                let users = list_channel_users(state, channel_name.to_string()).await;
                return state_result_to_value(users);
            } else if request_type == "replicate" {
                let request = request["request"].clone();
                let request_type = request["type"].as_str().unwrap();
                if request_type == "create_user" {
                    let username = request["username"].as_str().unwrap();
                    let result =
                        discord_stateops::create_user(get_discord(state), username.to_string());
                    return state_result_to_value(result);
                } else if request_type == "join_channel" {
                    let username = request["username"].as_str().unwrap();
                    let channel_name = request["channel"].as_str().unwrap();
                    // if the original request succeeded, then the user exists somewhere in the system
                    // so we don't need to check
                    let user_exists: UserExistsFn =
                        Box::new(move |_username: String| Box::pin(async move { true }));
                    let result = discord_stateops::join_channel(
                        get_discord(state),
                        username.to_owned(),
                        channel_name.to_owned(),
                        user_exists,
                    )
                    .await;
                    return state_result_to_value(result);
                }
                panic!(
                    "Unknown replicate type: {}",
                    request["type"].as_str().unwrap()
                );
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

// ==============================
// Users
// ==============================

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
        let () = discord_stateops::create_user(get_discord(state.clone()), username.clone())?;
        // and also replicate operation
        if config.backup == id || !get_server_state(state.clone(), &config.backup) {
            return Ok(());
        }
        let message = json!({"username": username});
        let forward_result = replicate_op(
            state.clone(),
            message,
            "create_user".to_owned(),
            config.backup,
        )
        .await
        .unwrap();
        state_result_from_value(&forward_result)
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
pub async fn get_my_users(state: SharedSystemState) -> Vec<String> {
    discord_stateops::get_users(get_discord(state))
}
pub async fn get_users(state: SharedSystemState) -> Vec<String> {
    // and ask everyone else
    let id = get_id(state.clone());
    let other_servers = get_server_states(state.clone());

    let mut all_users = get_my_users(state.clone()).await;
    for (peer_id, is_alive) in other_servers {
        if peer_id != id && is_alive {
            let mut other_users = state_result_from_value(
                &forward_request_to(state.clone(), json!({}), "get_users".to_owned(), peer_id)
                    .await
                    .unwrap(),
            )
            .unwrap();
            all_users.append(&mut other_users);
        }
    }
    all_users.sort();
    all_users.dedup();
    all_users
}

// ==============================
// Channels
// ==============================
fn channel_owner(channel_name: &str) -> LocationConfig {
    // partition channels by first letter
    if channel_name == "#cs249" {
        return LocationConfig {
            primary: "A".to_string(),
            backup: "B".to_string(),
        };
    } else if channel_name == "#distributed" {
        return LocationConfig {
            primary: "B".to_string(),
            backup: "C".to_string(),
        };
    } else if channel_name == "#systems" {
        return LocationConfig {
            primary: "C".to_string(),
            backup: "A".to_string(),
        };
    }

    // if it's not a special channel, hash it
    let mut h = hash::DefaultHasher::new();
    h.write(channel_name.as_bytes());
    let h = h.finish();

    let hash_mod = h % 3;
    if hash_mod == 0 {
        return LocationConfig {
            primary: "A".to_string(),
            backup: "B".to_string(),
        };
    } else if hash_mod == 1 {
        return LocationConfig {
            primary: "B".to_string(),
            backup: "C".to_string(),
        };
    } else if hash_mod == 2 {
        return LocationConfig {
            primary: "C".to_string(),
            backup: "A".to_string(),
        };
    }
    panic!("Invalid channel name");
}

pub async fn get_my_channels(state: SharedSystemState) -> Vec<String> {
    discord_stateops::get_channels(get_discord(state))
}
pub async fn get_channels(state: SharedSystemState) -> Vec<String> {
    // and ask everyone else
    let id = get_id(state.clone());
    let other_servers = get_server_states(state.clone());

    let mut all_channels = get_my_channels(state.clone()).await;
    for (peer_id, is_alive) in other_servers {
        if peer_id != id && is_alive {
            let mut other_users = state_result_from_value(
                &forward_request_to(state.clone(), json!({}), "get_channels".to_owned(), peer_id)
                    .await
                    .unwrap(),
            )
            .unwrap();
            all_channels.append(&mut other_users);
        }
    }
    all_channels.sort();
    all_channels.dedup();
    all_channels
}
pub async fn debug_list_channel_users(
    state: SharedSystemState,
    channel_name: String,
) -> StateResult<Vec<String>> {
    discord_stateops::list_channel_users(get_discord(state), channel_name)
}
pub async fn list_channel_users(
    state: SharedSystemState,
    channel_name: String,
) -> StateResult<Vec<String>> {
    let id = get_id(state.clone());
    let config = channel_owner(&channel_name);
    // either we are the primary server, or the primary server is down
    // means we are the authoritative server
    if config.primary == id
        || (!get_server_state(state.clone(), &config.primary) && id == config.backup)
    {
        discord_stateops::list_channel_users(get_discord(state), channel_name)
    } else {
        // send a request to whatever is up out of the two
        let request = json!({"channel": channel_name});
        let request_type = "list_channel_users".to_string();
        let result = forward_request(state.clone(), request, request_type, config).await;
        if let Some(result) = result {
            return state_result_from_value(&result);
        }
        // retry
        Box::pin(list_channel_users(state, channel_name)).await
    }
}
pub async fn join_channel(
    state: SharedSystemState,
    username: String,
    channel_name: String,
) -> StateResult<()> {
    let id = get_id(state.clone());
    let config = channel_owner(&channel_name);
    // either we are the primary server, or the primary server is down
    // means we are the authoritative server
    if config.primary == id
        || (!get_server_state(state.clone(), &config.primary) && id == config.backup)
    {
        let user_exists: UserExistsFn = {
            let state = state.clone();
            Box::new(move |username: String| {
                Box::pin(async move { user_exists(state, &username).await })
            })
        };
        let () = discord_stateops::join_channel(
            get_discord(state.clone()),
            username.clone(),
            channel_name.clone(),
            user_exists,
        )
        .await?;
        // and also replicate operation
        if config.backup == id || !get_server_state(state.clone(), &config.backup) {
            return Ok(());
        }
        let message = json!({"username": username, "channel": channel_name});
        let forward_result = replicate_op(
            state.clone(),
            message,
            "join_channel".to_owned(),
            config.backup,
        )
        .await
        .unwrap();
        state_result_from_value(&forward_result)
    } else {
        // send a request to whatever is up out of the two
        let request = json!({"username": username, "channel": channel_name});
        let request_type = "join_channel".to_string();
        let result = forward_request(state.clone(), request, request_type, config).await;
        if let Some(result) = result {
            return state_result_from_value(&result);
        }
        // retry
        Box::pin(join_channel(state, username, channel_name)).await
    }
}
