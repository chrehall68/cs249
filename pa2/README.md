# CS 249 - Programming Assignment 2: Distributed Mini Discord

Name: Eliot Hall

TODO - add video / timestamps

Video timestamps :

| Step                              | Time |
| --------------------------------- | ---- |
| Start all three servers           | TODO |
| Verify health                     | TODO |
| Initial process and leader status | TODO |
| User sharing                      | TODO |
| Request forwarding                | TODO |
| Message replication               | TODO |
| Failure detection                 | TODO |
| Leader selection                  | TODO |

1. Architecture and process configuration

---

The service is three independent processes running the same Rust binary
(target/release/server), each started with its own id, ie:

```
server --id A --config servers.json
```

To find its peers, every process reads the given config for the host,
HTTP port, peer TCP port and leader priority of all three servers.

Source layout (src/):

```
├── main.rs (argument parsing, startup)
├── config.rs (reads and validates servers.json)
├── http_handlers.rs (HTTP endpoints)
├── distributed_stateops.rs (ownership rules, forwarding, replication)
├── discord_stateops.rs (local users/channels/messages state)
├── server_middleware.rs (peer connections, heartbeats, leaders)
```

2. Dependencies and setup

---

To get set up, you need to have the rust toolchain installed. Then,
you can simply run `cargo build --release` to build the project since
all the dependencies are listed in our Cargo.toml and Cargo.lock

Also, the tests need python3 (standard library only, no packages to install).

3. Startup

---

```
./run.sh --config servers.json
```

run.sh builds the release binary, then starts servers A,B,and C.
Each server's output goes to logs/<id>.log. Pressing Ctrl-C stops all three processes.

4. User sharing

---

Each username has an owner pair (primary, backup), chosen by hashing the
username. This enables us to have very similar
code for user replication as we do for channel replication.

- `POST /users` is forwarded to the owner's primary, which replicates successful requests to the backup. This allows us to easily reject duplicates.
- Checks such as "is user X registered?" (for joining or sending) also
  get forwarded to the primary
- `GET /users` works differently. Since every server has only a shard
  of all users, we query all alive servers for their users and take the union.

If an owner's primary is marked failed, its backup answers for those users.

5. Channel ownership

---

Channels `#cs249`, `#distributed`, and `#systems` exist on startup with
hardcoded owners as specified by the assignment.

For other channels (which get created upon a user joining it), we
determine their owner by hashing the channel name (just like with users).

For every channel request, just like with users, we forward it to the
primary (if we're not the primary) who answers the request, replicates
anything successful to the backup, and then forwards the response back.

Also similar to with `GET /users`, `GET /channels` returns the union of the
channels known to all alive servers.

Lastly, just like with users, if a channel's primary fails, the request goes
to the channel's backup instead, which then serves reads and writes for that channel.

6. Request Forwarding

---

Every forwarded request has the following fields:

- `"type"` what the message is
- `"sequence_number"` per-connection counter assigned by the sender of a request. This allows us to set up callbacks for responses
- `"is_response"` false for requests, true for responses

A response echoes the sequence_number of its request; this is basically my
implementation of the `"request_id"` in the assignment examples.

Forwarded requests look like:

`{"type":"<request type>","<request fields>":<request field values>}`

| type               | request fields                |
| ------------------ | ----------------------------- |
| user_exists        | `username`                    |
| create_user        | `username`                    |
| get_users          |                               |
| get_channels       |                               |
| join_channel       | `username`, `channel`         |
| leave_channel      | `username`, `channel`         |
| list_channel_users | `channel`                     |
| send_message       | `username`, `channel`, `text` |
| get_messages       | `channel`, `limit`, `offset`  |

Responses hold either `{"ok": <result>}` or
`{"error":{"status":<http status>,"code":"<ERROR_CODE>","message":"..."}}`
The exception is `user_exists`, which responds with `{"user_exists": true|false}`.

Example (Server C forwarding a send_message for #cs249 to Server A):

```
(the request from C to A)
{"type":"send_message","channel":"#cs249","username":"alice",
"text":"Hello","sequence_number":3,"is_response":false}

(the response from A to C)
{"ok":"#cs249-0","request_type":"send_message","sequence_number":3,
"is_response":true}
```

7. Replication design

---

Replication is synchronous. Whenever an operation succeeds on the primary
and needs to be replicated (ie after user creation, join channel, send
message, etc), the primary sends a replicate request to the
channel's backup (if it's alive) and waits for an acknowledgement before
returning a response to the client:

Example replication message

```
{"type":"replicate","request":{"type":"send_message","channel":"#cs249",
"username":"alice","text":"Hello","message_id":"#cs249-0"},
"sequence_number":9,"is_response":false}

(the acknowledgement)
{"ok":"#cs249-0","request_type":"replicate","sequence_number":9,
"is_response":true}
```

> Note: `send_message` is a special case where the request contains an
> additional field `message_id`, which is the message id returned by the
> primary. This is to ensure consistent ids between primary and backup.
> Other requests don't have this field since they don't need it, and their
> `request` value matches the specs in [Request Forwarding](#request-forwarding).

To show that a backup holds its own copy rather than forwarding the read,
use the debug endpoints, which only read local state:

```
GET /debug/users
GET /debug/channels
GET /debug/channels/{channel}/users
GET /debug/channels/{channel}/messages
```

8. Heartbeat failure detection

---

As specified in the assignment, we detect failures by ping-ack
"heartbeats". Each server runs one heartbeat task per peer, which does
the following:

- every 2 seconds it sends a PING;
- if the matching PONG has not arrived within 2 seconds, that counts as one
  missed heartbeat;
- a PONG resets the missed count to 0;
- after 3 consecutive misses the peer is marked unavailable and the log shows
  "Peer C missed 3 heartbeats, disconnecting".

When a peer is marked failed, every request still waiting on it is completed
with "peer unavailable", and the request is retried against the backup, so
clients do not hang.

Example:

```
(ping from A to C)
{"from":"A","is_response":false,"sequence_number":1,"type":"PING"}

(pong from C to A)
{"from":"C","is_response":true,"sequence_number":1,"type":"PONG"}
```

9. Leader selection

---

Priorities come from servers.json. At startup every server sets the leader to
the highest-priority server, and said server also sends a LEADER_ANNOUNCEMENT.

When heartbeats mark a peer failed, each surviving server:

1. takes the servers it still considers alive, itself included,
2. picks the one with the highest priority,
3. updates its own leader if that changed, logging e.g.
   "Because of failure, leader changed from C to B",
4. if it picked itself, sends LEADER_ANNOUNCEMENT to every peer.
   A server receiving LEADER_ANNOUNCEMENT sets its leader to the announced
   server and logs "received leader announcement from B".

`GET /leader` returns the leader and whether this server considers it alive:
{"leader":"B","alive":true}

> Because of how I designed user/channel replication, the leader doesn't
> do anything other than send a LEADER_ANNOUNCEMENT to all peers.

10. Concurrency and shared state

---

I made sure to use mutexes and atomic counters to prevent race
conditions. Plus, since it's written in Rust, we already have
built-in fearless concurrency.

11. Testing

---

I have some automated tests in tests/. Each one starts its own servers on the
ports in servers.json, so stop run.sh first. Run them from this directory, e.g.
`python3 tests/test_replication.py`:

> Note: The exception is `tests/cluster.py`, which is just helpers (not a test).
