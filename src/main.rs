//! MCP server binary entry point.

use anyhow::Result;
use r3nz_database_mcp::{
    config::{DatabaseConfigBuilder, ServerConfig},
    database::ConnectionManager,
    protocol::McpServerBuilder,
    server::{McpHandler, ServerStateBuilder},
};
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    info!(
        "Starting {} v{}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    let connection_manager = Arc::new(ConnectionManager::new());
    let auto_connected = try_auto_connect(&connection_manager).await;

    let config = if auto_connected {
        let db_config = DatabaseConfigBuilder::new()
            .from_env()
            .ok()
            .and_then(|b| b.build().ok())
            .unwrap_or_default();
        ServerConfig::builder().database(db_config).build()
    } else {
        ServerConfig::default()
    };

    let state = Arc::new(
        ServerStateBuilder::new()
            .config(config)
            .connection_manager(Arc::clone(&connection_manager))
            .build()
            .map_err(|e| anyhow::anyhow!(e))?,
    );

    info!("Server state initialized with {} tools", state.tools.len());

    if !auto_connected {
        info!("No database connection. Use the 'connect' tool to establish a connection.");
    }

    let handler = McpHandler::new(state);
    let server = McpServerBuilder::new()
        .handler(handler)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .with_tools()
        .build()?;

    info!("MCP server ready, waiting for connections...");

    server.run().await?;

    info!("Server shutdown complete");
    Ok(())
}

async fn try_auto_connect(connection_manager: &ConnectionManager) -> bool {
    let has_db_config =
        std::env::var("DATABASE_TYPE").is_ok() || std::env::var("DATABASE_HOST").is_ok();

    if !has_db_config {
        return false;
    }

    info!("Found database environment variables, attempting auto-connect...");

    let db_config = match DatabaseConfigBuilder::new().from_env() {
        Ok(builder) => match builder.build() {
            Ok(config) => config,
            Err(e) => {
                warn!("Invalid database configuration: {}", e);
                return false;
            }
        },
        Err(e) => {
            warn!("Failed to read database configuration: {}", e);
            return false;
        }
    };

    info!(
        "Connecting to {} database at {}:{}",
        match db_config.database_type {
            r3nz_database_mcp::config::DatabaseType::Postgres => "PostgreSQL",
            r3nz_database_mcp::config::DatabaseType::Mssql => "MSSQL",
        },
        db_config.host,
        db_config.port
    );

    match connection_manager.connect(db_config).await {
        Ok(metadata) => {
            info!(
                "Connected to {} on {}",
                metadata.database.as_deref().unwrap_or("unknown"),
                metadata.host.as_deref().unwrap_or("unknown")
            );
            true
        }
        Err(e) => {
            error!("Failed to connect to database: {}", e);
            warn!("Server will start without database connection. Use 'connect' tool to retry.");
            false
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("r3nz_database_mcp=info,warn"));

    // Use JSON format for structured logging to stderr (stdout is for MCP protocol)
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .json()
        .init();
}
