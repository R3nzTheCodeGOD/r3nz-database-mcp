//! Tools: connect, disconnect, list_databases, switch_database, connection_info

use crate::config::{DatabaseConfig, DatabaseConfigBuilder, DatabaseType};
use crate::database::{ConnectionManager, ConnectionStringParser};
use crate::error::{McpError, Result, ToolError};
use crate::protocol::{CallToolResult, Tool};
use crate::tools::registry::ToolHandler;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tracing::instrument;

#[derive(Debug, Deserialize)]
pub struct ConnectArgs {
    #[serde(default)]
    pub connection_string: Option<String>,
    #[serde(default, rename = "type")]
    pub database_type: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default, alias = "user")]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

pub struct ConnectTool {
    connection_manager: Arc<ConnectionManager>,
}

impl ConnectTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    fn build_config_from_params(&self, args: &ConnectArgs) -> Result<DatabaseConfig> {
        let db_type = args
            .database_type
            .as_ref()
            .and_then(|t| DatabaseType::parse(t))
            .ok_or_else(|| {
                ToolError::MissingArgument("type (database type: 'mssql' or 'postgres')".into())
            })?;

        let host = args
            .host
            .as_ref()
            .ok_or_else(|| ToolError::MissingArgument("host".into()))?;

        let database = args
            .database
            .as_ref()
            .ok_or_else(|| ToolError::MissingArgument("database".into()))?;

        let username = args
            .username
            .as_ref()
            .ok_or_else(|| ToolError::MissingArgument("username".into()))?;

        let default_port = match db_type {
            DatabaseType::Mssql => 1433,
            DatabaseType::Postgres => 5432,
        };

        DatabaseConfigBuilder::new()
            .database_type(db_type)
            .host(host.clone())
            .port(args.port.unwrap_or(default_port))
            .database(database.clone())
            .username(username.clone())
            .password(args.password.as_deref().unwrap_or(""))
            .build()
    }
}

#[async_trait]
impl ToolHandler for ConnectTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "connect".into(),
            description: Some(
                "Connect to a database. Accepts either a connection string or individual parameters. \
                Connection string formats: \
                ADO.NET: 'Server=host,port;Database=db;User Id=user;Password=pass' \
                URL: 'mssql://user:pass@host:port/database' or 'postgres://user:pass@host:port/database'".into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_string": {
                        "type": "string",
                        "description": "Full connection string (ADO.NET or URL format)"
                    },
                    "type": {
                        "type": "string",
                        "description": "Database type: 'mssql' or 'postgres'",
                        "enum": ["mssql", "postgres", "postgresql", "sqlserver"]
                    },
                    "host": {
                        "type": "string",
                        "description": "Database server hostname or IP address"
                    },
                    "port": {
                        "type": "integer",
                        "description": "Database server port (default: 1433 for MSSQL, 5432 for PostgreSQL)"
                    },
                    "database": {
                        "type": "string",
                        "description": "Database name to connect to"
                    },
                    "username": {
                        "type": "string",
                        "description": "Database username"
                    },
                    "password": {
                        "type": "string",
                        "description": "Database password"
                    }
                }
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "connect"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let args: ConnectArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        // Try connection string first, then individual parameters
        let config = if let Some(conn_str) = &args.connection_string {
            ConnectionStringParser::parse(conn_str).map_err(McpError::Database)?
        } else {
            self.build_config_from_params(&args)?
        };

        let metadata = self
            .connection_manager
            .connect(config)
            .await
            .map_err(McpError::from)?;

        Ok(CallToolResult::json(&serde_json::json!({
            "status": "connected",
            "connection": metadata
        })))
    }
}

pub struct DisconnectTool {
    connection_manager: Arc<ConnectionManager>,
}

impl DisconnectTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for DisconnectTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "disconnect".into(),
            description: Some("Disconnect from the current database connection.".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    #[instrument(skip(self, _arguments), fields(tool = "disconnect"))]
    async fn execute(&self, _arguments: Value) -> Result<CallToolResult> {
        let metadata = self
            .connection_manager
            .disconnect()
            .map_err(McpError::from)?;

        if metadata.connected {
            Ok(CallToolResult::json(&serde_json::json!({
                "status": "disconnected",
                "previous_connection": metadata
            })))
        } else {
            Ok(CallToolResult::text("No active connection to disconnect."))
        }
    }
}

pub struct ConnectionInfoTool {
    connection_manager: Arc<ConnectionManager>,
}

impl ConnectionInfoTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for ConnectionInfoTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "connection_info".into(),
            description: Some("Get information about the current database connection.".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    #[instrument(skip(self, _arguments), fields(tool = "connection_info"))]
    async fn execute(&self, _arguments: Value) -> Result<CallToolResult> {
        let metadata = self.connection_manager.metadata();
        Ok(CallToolResult::json(&metadata))
    }
}

pub struct ListDatabasesTool {
    connection_manager: Arc<ConnectionManager>,
}

impl ListDatabasesTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for ListDatabasesTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "list_databases".into(),
            description: Some("List all databases on the connected server.".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    #[instrument(skip(self, _arguments), fields(tool = "list_databases"))]
    async fn execute(&self, _arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let databases = driver.list_databases().await.map_err(McpError::from)?;

        Ok(CallToolResult::json(&serde_json::json!({
            "databases": databases,
            "count": databases.len()
        })))
    }
}

#[derive(Debug, Deserialize)]
pub struct SwitchDatabaseArgs {
    pub database: String,
    pub password: String,
}

pub struct SwitchDatabaseTool {
    connection_manager: Arc<ConnectionManager>,
}

impl SwitchDatabaseTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for SwitchDatabaseTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "switch_database".into(),
            description: Some(
                "Switch to a different database on the same server. \
                Requires the password to establish a new connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Name of the database to switch to"
                    },
                    "password": {
                        "type": "string",
                        "description": "Password for the new connection"
                    }
                },
                "required": ["database", "password"]
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "switch_database"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let args: SwitchDatabaseArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        let metadata = self
            .connection_manager
            .switch_database(&args.database, &args.password)
            .await
            .map_err(McpError::from)?;

        Ok(CallToolResult::json(&serde_json::json!({
            "status": "switched",
            "connection": metadata
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_args_deserialize() {
        let json = serde_json::json!({
            "connection_string": "Server=localhost;Database=test;User Id=sa;Password=secret"
        });
        let args: ConnectArgs = serde_json::from_value(json).unwrap();
        assert!(args.connection_string.is_some());
    }

    #[test]
    fn test_connect_args_with_params() {
        let json = serde_json::json!({
            "type": "mssql",
            "host": "localhost",
            "database": "test",
            "username": "sa",
            "password": "secret"
        });
        let args: ConnectArgs = serde_json::from_value(json).unwrap();
        assert_eq!(args.database_type.as_deref(), Some("mssql"));
        assert_eq!(args.host.as_deref(), Some("localhost"));
    }

    #[test]
    fn test_switch_database_args() {
        let json = serde_json::json!({
            "database": "newdb",
            "password": "secret"
        });
        let args: SwitchDatabaseArgs = serde_json::from_value(json).unwrap();
        assert_eq!(args.database, "newdb");
    }
}
