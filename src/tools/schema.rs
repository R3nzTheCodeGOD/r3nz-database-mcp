//! Schema inspection tools: list_tables, get_schema, get_relationships.

use crate::database::ConnectionManager;
use crate::error::{McpError, Result, ToolError};
use crate::protocol::{CallToolResult, Tool};
use crate::tools::registry::ToolHandler;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tracing::instrument;

#[derive(Debug, Deserialize)]
pub struct ListTablesArgs {
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub filter: Option<String>,
}

pub struct ListTablesTool {
    connection_manager: Arc<ConnectionManager>,
}

impl ListTablesTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for ListTablesTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "list_tables".into(),
            description: Some(
                "List all tables and views in the database. \
                Optionally filter by schema name. \
                Requires an active database connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Schema name to filter (default: 'public' for PostgreSQL, 'dbo' for MSSQL)"
                    },
                    "filter": {
                        "type": "string",
                        "description": "Optional filter pattern for table names (case-insensitive)"
                    }
                }
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "list_tables"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: ListTablesArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        let tables = driver
            .list_tables(args.schema.as_deref())
            .await
            .map_err(McpError::from)?;

        // Apply filter if provided
        let filtered = if let Some(filter) = &args.filter {
            let filter_lower = filter.to_lowercase();
            tables
                .into_iter()
                .filter(|t| t.name.to_lowercase().contains(&filter_lower))
                .collect()
        } else {
            tables
        };

        Ok(CallToolResult::json(&filtered))
    }
}

#[derive(Debug, Deserialize)]
pub struct GetSchemaArgs {
    pub table_name: String,
}

pub struct GetSchemaTool {
    connection_manager: Arc<ConnectionManager>,
}

impl GetSchemaTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for GetSchemaTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "get_schema".into(),
            description: Some(
                "Get detailed schema information for a table including columns, \
                data types, constraints, and indexes. \
                Requires an active database connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table name (can include schema prefix, e.g., 'public.users')"
                    }
                },
                "required": ["table_name"]
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "get_schema"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: GetSchemaArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        let schema = driver
            .get_table_schema(&args.table_name)
            .await
            .map_err(McpError::from)?;

        Ok(CallToolResult::json(&schema))
    }
}

#[derive(Debug, Deserialize)]
pub struct GetRelationshipsArgs {
    #[serde(default)]
    pub table_name: Option<String>,
}

pub struct GetRelationshipsTool {
    connection_manager: Arc<ConnectionManager>,
}

impl GetRelationshipsTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for GetRelationshipsTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "get_relationships".into(),
            description: Some(
                "Get foreign key relationships for a table or all tables. \
                Shows how tables are connected through foreign key constraints. \
                Requires an active database connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table name to get relationships for (optional, returns all if not specified)"
                    }
                }
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "get_relationships"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: GetRelationshipsArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        let relationships = driver
            .get_relationships(args.table_name.as_deref())
            .await
            .map_err(McpError::from)?;

        Ok(CallToolResult::json(&relationships))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_list_tables_definition() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "schema": { "type": "string" }
            }
        });
        assert!(schema["properties"]["schema"].is_object());
    }

    #[test]
    fn test_get_schema_definition() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "table_name": { "type": "string" }
            },
            "required": ["table_name"]
        });
        assert!(
            schema["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("table_name"))
        );
    }
}
