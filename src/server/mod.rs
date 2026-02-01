//! MCP server implementation.

pub mod handler;
pub mod state;

pub use handler::McpHandler;
pub use state::{ServerState, ServerStateBuilder};
