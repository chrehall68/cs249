# CS 249 - Programming Assignment 1

Author: Eliot Hall

This is the first programming assignment for CS 249. In this directory,
I implemented a mini Discord clone. The program allows users to create
accounts, join channels, send messages, and leave channels.

## Structure

The project is structured as follows:

```bash
.
├── README.md
├── client
|   ├── (contains client code)
|   ├── ...
├── server
|   ├── (contains server code)
|   ├── ...
└── ...
```

I didn't write any automated tests, so there is no `test` directory.

## Usage

To run the server, you can run `run.sh` and pass in the host, http port,
and tcp port. This takes care of building the server and starting it.

```bash
./run.sh --host 0.0.0.0 --http-port 8000 --tcp-port 8001
```

Alternatively, if you want to build the server yourself, you can also run

```bash
cargo run --release --bin server -- --http-port 8000 --tcp-port 8001 --host 0.0.0.0
```

To run the TCP client, you can run `runclient.sh` and pass in the host
and tcp port. This takes care of building the client and starting it.

```bash
./runclient.sh --host 0.0.0.0 --tcp-port 8001
```

Alternatively, if you want to build the client yourself, you can also run

```bash
cargo run --release --bin client -- --host 0.0.0.0 --tcp-port 8001
```

## Protocol Decisions

There wasn't really much protocol to decide. We already were told that
the TCP client/server should treat a single line as a single message,
so I went with that for the TCP protocol. As for the HTTP protocol,
that was already pretty well specified from the project description.

The only interesting thing protocol-wise is that the server
uses TCP keepalive to detect client failure. I chose this over having
the client send a heartbeat every `T` seconds because the TCP keepalive
is basically the same thing, and doesn't require the extra work of
having a separate thread/callback for heartbeats and either:

- having a separate socket for heartbeats
- OR reusing the existing one and editing our application-level TCP protocol to account for custom
