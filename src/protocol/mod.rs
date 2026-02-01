//! MCP protocol implementation over JSON-RPC 2.0.

pub mod handler;
pub mod server;
pub mod transport;
pub mod types;

pub use handler::{Dispatcher, Handler, RequestContext};
pub use server::{McpServer, McpServerBuilder, ServerState};
pub use transport::{MessageReader, MessageWriter, StdioTransport, Transport};
pub use types::*;
