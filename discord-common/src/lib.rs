//! Shared utilities used by both the `server` and `client` crates.

/// Default host to bind (server) or connect to (client).
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// Default port for HTTP connections.
pub const DEFAULT_HTTP_PORT: u16 = 8000;

/// Default port for raw TCP connections.
pub const DEFAULT_TCP_PORT: u16 = 8001;
