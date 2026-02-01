//! MCP server for database analysis and schema exploration.
//!
//! Supports MSSQL and PostgreSQL with read-only query execution,
//! schema introspection, and performance diagnostics.
//!
//! # Example
//!
//! ```no_run
//! use r3nz_database_mcp::{
//!     config::ServerConfig,
//!     database::ConnectionManager,
//!     protocol::McpServerBuilder,
//!     server::{McpHandler, ServerStateBuilder},
//! };
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create connection manager (starts disconnected)
//!     let connection_manager = Arc::new(ConnectionManager::new());
//!
//!     // Build server config
//!     let config = ServerConfig::default();
//!
//!     // Create server state
//!     let state = Arc::new(
//!         ServerStateBuilder::new()
//!             .config(config)
//!             .connection_manager(connection_manager)
//!             .build()
//!             .map_err(|e| anyhow::anyhow!(e))?
//!     );
//!
//!     // Create and run server
//!     let handler = McpHandler::new(state);
//!     let server = McpServerBuilder::new()
//!         .handler(handler)
//!         .with_tools()
//!         .build()?;
//!
//!     // Use the 'connect' tool to establish a database connection
//!     server.run().await?;
//!     Ok(())
//! }
//! ```

// Warnings are kept minimal for production quality
#![allow(dead_code, reason = "Some fields reserved for future use")]

pub mod cache;
pub mod config;
pub mod database;
pub mod error;
pub mod protocol;
pub mod security;
pub mod server;
pub mod tools;

pub use config::{DatabaseConfig, DatabaseConfigBuilder, DatabaseType, ServerConfig};
pub use database::{
    ConnectionConfig, ConnectionManager, ConnectionMetadata, ConnectionState,
    ConnectionStringParser, DatabaseDriver, create_driver,
};
pub use error::{McpError, Result};
pub use protocol::{McpServer, McpServerBuilder};
pub use security::{RateLimiter, SqlValidator};
pub use server::{McpHandler, ServerState, ServerStateBuilder};
