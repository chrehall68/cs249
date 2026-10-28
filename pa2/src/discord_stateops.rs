use std::{
    collections::{BTreeSet, HashMap, HashSet},
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Message {
    pub username: String,
    pub text: String,
    /// Every message has a unique id of the form "{channel_name}-{numeric_message_id}"
    pub message_id: String,
}
impl Ord for Message {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get_numeric_message_id()
            .cmp(&other.get_numeric_message_id())
    }
}
impl PartialOrd for Message {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Message {
    pub fn new(username: String, text: String, message_id: String) -> Self {
        Self {
            username,
            text,
            message_id,
        }
    }
    pub fn from(username: String, text: String, channel_name: &str, message_id: usize) -> Self {
        Self {
            username,
            text,
            message_id: format!("{}-{}", channel_name, message_id),
        }
    }
    pub fn get_numeric_message_id(&self) -> usize {
        self.message_id.split('-').last().unwrap().parse().unwrap()
    }
}
pub struct Channel {
    users: HashSet<String>,
    messages: BTreeSet<Message>,
    next_message_id: usize,
}
impl Channel {
    pub fn new() -> Self {
        Channel {
            users: HashSet::new(),
            messages: BTreeSet::new(),
            next_message_id: 0,
        }
    }
}
pub struct AppState {
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
// TODO - maybe worth using a RwLock here
pub type SharedState = Arc<Mutex<AppState>>;
/// (http status, error code like "UNKNOWN_USER", error message)
pub type StateError = (StatusCode, String, String);
/// Return type for handlers
/// on error, we have (http status, error code, error message)
/// on success, we have the value
pub type StateResult<T> = Result<T, StateError>;
pub fn state_error(status: StatusCode, code: &str, message: &str) -> StateError {
    (status, code.to_owned(), message.to_owned())
}

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
    if username.is_empty() {
        return Err(state_error(
            StatusCode::BAD_REQUEST,
            "INVALID_USERNAME",
            "Empty username",
        ));
    }
    let mut state = state.lock().unwrap();
    let inserted = state.users.insert(username.clone());
    if !inserted {
        Err(state_error(
            StatusCode::CONFLICT,
            "DUPLICATE_USER",
            "User already exists",
        ))
    } else {
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
pub fn create_channel(state: SharedState, channel_name: String) -> StateResult<()> {
    if channel_exists(state.clone(), &channel_name) {
        Err(state_error(
            StatusCode::CONFLICT,
            "DUPLICATE_CHANNEL",
            "Channel already exists",
        ))
    } else {
        let mut state = state.lock().unwrap();
        state.channels.insert(channel_name.clone(), Channel::new());
        Ok(())
    }
}
pub type UserExistsFn =
    Box<dyn FnOnce(String) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;
/// if the channel doesn't exist yet, it will be created
pub async fn join_channel(
    state: SharedState,
    username: String,
    channel_name: String,
    user_exists: UserExistsFn,
) -> StateResult<()> {
    // validate that user exists somewhere in the system
    if !user_exists(username.clone()).await {
        return Err(state_error(
            StatusCode::NOT_FOUND,
            "UNKNOWN_USER",
            "User does not exist",
        ));
    }
    if !channel_name_is_valid(&channel_name) {
        return Err(state_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CHANNEL_NAME",
            "Channel must start with #",
        ));
    }
    // user exists somewhere in the system, and channel is valid
    let mut state = state.lock().unwrap();
    let channel = state
        .channels
        .entry(channel_name.clone())
        .or_insert(Channel::new());
    if channel.users.contains(&username) {
        // duplicate
        Err(state_error(
            StatusCode::CONFLICT,
            "ALREADY_MEMBER",
            "User already in channel",
        ))
    } else {
        // otherwise, add the user to the channel
        channel.users.insert(username.clone());
        Ok(())
    }
}
pub fn list_channel_users(state: SharedState, channel_name: String) -> StateResult<Vec<String>> {
    if !channel_name_is_valid(&channel_name) {
        return Err(state_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CHANNEL_NAME",
            "Channel must start with #",
        ));
    }
    if !channel_exists(state.clone(), &channel_name) {
        return Err(state_error(
            StatusCode::NOT_FOUND,
            "UNKNOWN_CHANNEL",
            "Channel does not exist",
        ));
    }
    let state = state.lock().unwrap();
    let channel = state.channels.get(&channel_name).unwrap();
    Ok(channel.users.iter().map(|k| k.clone()).collect::<Vec<_>>())
}
pub async fn leave_channel(
    state: SharedState,
    username: String,
    channel_name: String,
    user_exists: UserExistsFn,
) -> StateResult<()> {
    if !channel_name_is_valid(&channel_name) {
        return Err(state_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CHANNEL_NAME",
            "Channel must start with #",
        ));
    }
    // we have a valid channel, now look it up
    if !channel_exists(state.clone(), &channel_name) {
        return Err(state_error(
            StatusCode::NOT_FOUND,
            "UNKNOWN_CHANNEL",
            "Channel does not exist",
        ));
    }
    if !user_exists(username.clone()).await {
        return Err(state_error(
            StatusCode::NOT_FOUND,
            "UNKNOWN_USER",
            "User does not exist",
        ));
    }
    let mut state = state.lock().unwrap();
    let channel = state.channels.get_mut(&channel_name).unwrap();
    if !channel.users.contains(&username) {
        return Err(state_error(
            StatusCode::NOT_FOUND,
            "NOT_A_MEMBER",
            "User not in channel",
        ));
    }
    channel.users.remove(&username);
    Ok(())
}

// ==============================
// Messages
// ==============================
// unlike other functions, this requires its own special replication because of the fact
// that we assign id's
pub async fn replicate_send_message(
    state: SharedState,
    channel_name: String,
    username: String,
    text: String,
    message_id: String,
) -> StateResult<String> {
    let mut state = state.lock().unwrap();
    let channel = state.channels.get_mut(&channel_name).unwrap();
    let did_insert = channel
        .messages
        .insert(Message::new(username, text, message_id.clone()));
    // after all replications, then the replica's next_message_id will match the primary's
    channel.next_message_id += did_insert as usize;
    Ok(message_id)
}
pub async fn send_message(
    state: SharedState,
    channel_name: String,
    username: String,
    text: String,
    user_exists: UserExistsFn,
) -> StateResult<String> {
    if !channel_name_is_valid(&channel_name) {
        return Err(state_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CHANNEL_NAME",
            "Channel must start with #",
        ));
    }
    // we have a valid channel, now look it up
    if !channel_exists(state.clone(), &channel_name) {
        return Err(state_error(
            StatusCode::NOT_FOUND,
            "UNKNOWN_CHANNEL",
            "Channel does not exist",
        ));
    }
    if !user_exists(username.clone()).await {
        return Err(state_error(
            StatusCode::NOT_FOUND,
            "UNKNOWN_USER",
            "User does not exist",
        ));
    }

    let mut state = state.lock().unwrap();
    let channel = state.channels.get_mut(&channel_name).unwrap();
    if !channel.users.contains(&username) {
        // sending to a channel you haven't joined is an unauthorized channel operation
        return Err(state_error(
            StatusCode::FORBIDDEN,
            "NOT_A_MEMBER",
            "User not in channel",
        ));
    }
    let numeric_message_id = channel.next_message_id;
    channel.next_message_id += 1;
    // then insert
    let channel = state.channels.get_mut(&channel_name).unwrap();
    let message = Message::from(username, text, &channel_name, numeric_message_id);
    channel.messages.insert(message.clone());
    Ok(message.message_id)
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
        return Err(state_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CHANNEL_NAME",
            "Channel must start with #",
        ));
    }
    // we have a valid channel, now look it up
    if !channel_exists(state.clone(), &channel_name) {
        return Err(state_error(
            StatusCode::NOT_FOUND,
            "UNKNOWN_CHANNEL",
            "Channel does not exist",
        ));
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
