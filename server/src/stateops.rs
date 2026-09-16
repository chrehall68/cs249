use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use axum::http::StatusCode;
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct Message {
    pub username: String,
    pub text: String,
    pub message_id: usize,
}
impl Message {
    pub fn new(username: String, text: String, message_id: usize) -> Self {
        Self {
            username,
            text,
            message_id,
        }
    }
}
pub struct Channel {
    users: HashSet<String>,
    messages: Vec<Message>,
}
pub struct AppState {
    users: HashSet<String>,
    channels: HashMap<String, Channel>,
    next_message_id: usize,
}
impl AppState {
    pub fn new() -> Self {
        Self {
            users: HashSet::new(),
            channels: HashMap::new(),
            next_message_id: 0,
        }
    }
}

// Handlers get a clone of this `Arc`. The `Mutex` is what actually lets
// multiple handlers mutate the same `AppState` safely.
// TODO - maybe worth using a RwLock here
pub type SharedState = Arc<Mutex<AppState>>;
/// (error code, error message)
pub type StateError = (StatusCode, String);
/// Return type for handlers
/// on error, we have (error code, error message)
/// on success, we have the value
pub type StateResult<T> = Result<T, StateError>;

// ==============================
// Users
// ==============================
pub fn get_users(state: SharedState) -> Vec<String> {
    let state = state.lock().unwrap();
    state.users.iter().map(|k| k.clone()).collect::<Vec<_>>()
}
pub fn user_exists(state: SharedState, username: &str) -> bool {
    let state = state.lock().unwrap();
    state.users.contains(username)
}
/// Returns the username, if the user was successfully created
pub fn create_user(state: SharedState, username: String) -> StateResult<()> {
    if user_exists(state.clone(), &username) {
        Err((StatusCode::BAD_REQUEST, "User already exists".to_string()))
    } else {
        let mut state = state.lock().unwrap();
        state.users.insert(username.clone());
        Ok(())
    }
}

// ==============================
// Channels
// ==============================
pub fn get_channels(state: SharedState) -> Vec<String> {
    let state = state.lock().unwrap();
    state
        .channels
        .iter()
        .map(|(k, _)| k.clone())
        .collect::<Vec<_>>()
}
fn channel_name_is_valid(channel_name: &str) -> bool {
    channel_name.starts_with('#')
}
fn channel_exists(state: SharedState, channel_name: &str) -> bool {
    let state = state.lock().unwrap();
    state.channels.contains_key(channel_name)
}
/// if the channel doesn't exist yet, it will be created
pub fn join_channel(state: SharedState, username: String, channel_name: String) -> StateResult<()> {
    // validate that user exists
    if !user_exists(state.clone(), &username) {
        return Err((StatusCode::BAD_REQUEST, "User does not exist".to_string()));
    }
    if !channel_name_is_valid(&channel_name) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Channel must start with #".to_owned(),
        ));
    }
    // user exists, and channel is valid
    let mut state = state.lock().unwrap();
    let channel = state
        .channels
        .entry(channel_name.clone())
        .or_insert(Channel {
            users: HashSet::new(),
            messages: Vec::new(),
        });
    if channel.users.contains(&username) {
        // duplicate
        Err((
            StatusCode::BAD_REQUEST,
            "User already in channel".to_owned(),
        ))
    } else {
        // otherwise, add the user to the channel
        channel.users.insert(username.clone());
        Ok(())
    }
}
pub fn list_channel_users(state: SharedState, channel_name: String) -> StateResult<Vec<String>> {
    if !channel_name_is_valid(&channel_name) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Channel must start with #".to_owned(),
        ));
    }
    if !channel_exists(state.clone(), &channel_name) {
        return Err((StatusCode::NOT_FOUND, "Channel does not exist".to_owned()));
    }
    let state = state.lock().unwrap();
    let channel = state.channels.get(&channel_name).unwrap();
    Ok(channel.users.iter().map(|k| k.clone()).collect::<Vec<_>>())
}
pub fn leave_channel(
    state: SharedState,
    username: String,
    channel_name: String,
) -> StateResult<()> {
    if !channel_name_is_valid(&channel_name) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Channel must start with #".to_owned(),
        ));
    }
    // we have a valid channel, now look it up
    if !channel_exists(state.clone(), &channel_name) {
        return Err((StatusCode::NOT_FOUND, "Channel does not exist".to_owned()));
    }
    if !user_exists(state.clone(), &username) {
        return Err((StatusCode::NOT_FOUND, "User does not exist".to_owned()));
    }
    let mut state = state.lock().unwrap();
    let channel = state.channels.get_mut(&channel_name).unwrap();
    if !channel.users.contains(&username) {
        return Err((StatusCode::BAD_REQUEST, "User not in channel".to_owned()));
    }
    channel.users.remove(&username);
    Ok(())
}
// ==============================
// Messages
// ==============================
pub fn send_message(
    state: SharedState,
    channel_name: String,
    username: String,
    text: String,
) -> StateResult<usize> {
    if !channel_name_is_valid(&channel_name) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Channel must start with #".to_owned(),
        ));
    }
    // we have a valid channel, now look it up
    if !channel_exists(state.clone(), &channel_name) {
        return Err((StatusCode::NOT_FOUND, "Channel does not exist".to_owned()));
    }
    if !user_exists(state.clone(), &username) {
        return Err((StatusCode::NOT_FOUND, "User does not exist".to_owned()));
    }

    let mut state = state.lock().unwrap();
    let channel = state.channels.get(&channel_name).unwrap();
    if !channel.users.contains(&username) {
        return Err((StatusCode::BAD_REQUEST, "User not in channel".to_owned()));
    }
    let message_id = state.next_message_id;
    state.next_message_id += 1;
    // get channel mutably now
    let channel = state.channels.get_mut(&channel_name).unwrap();
    let message = Message::new(username, text, message_id);
    channel.messages.push(message);
    Ok(message_id)
}
/// Returns a list of messages
/// default limit is 20
/// default offset is 0
pub fn get_messages(
    state: SharedState,
    channel_name: String,
    limit: Option<usize>,
    offset: Option<usize>,
) -> StateResult<Vec<Message>> {
    if !channel_name_is_valid(&channel_name) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Channel must start with #".to_owned(),
        ));
    }
    // we have a valid channel, now look it up
    if !channel_exists(state.clone(), &channel_name) {
        return Err((StatusCode::NOT_FOUND, "Channel does not exist".to_owned()));
    }
    let mut state = state.lock().unwrap();
    let channel = state.channels.get_mut(&channel_name).unwrap();
    let limit = limit.unwrap_or(20);
    let offset = offset.unwrap_or(0);
    let messages = channel
        .messages
        .iter()
        .rev()
        .skip(offset)
        .take(limit)
        .map(|m| m.clone())
        .collect::<Vec<_>>();
    Ok(messages)
}
