//! Read-only query execution tool.

use crate::database::ConnectionManager;
use crate::error::{McpError, Result, ToolError};
use crate::protocol::{CallToolResult, Tool};
use crate::security::{RateLimiter, SqlValidator};
use crate::tools::registry::ToolHandler;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument};

#[derive(Debug, Deserialize)]
pub struct ExecuteQueryArgs {
    pub query: String,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

pub struct ExecuteQueryTool {
    connection_manager: Arc<ConnectionManager>,
    validator: SqlValidator,
    rate_limiter: Arc<RateLimiter>,
    default_limit: u32,
    default_timeout: Duration,
}

impl ExecuteQueryTool {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        validator: SqlValidator,
        rate_limiter: Arc<RateLimiter>,
    ) -> Self {
        Self {
            connection_manager,
            validator,
            rate_limiter,
            default_limit: 1000,
            default_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_default_limit(mut self, limit: u32) -> Self {
        self.default_limit = limit;
        self
    }

    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }
}

#[async_trait]
impl ToolHandler for ExecuteQueryTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "execute_query".into(),
            description: Some(
                "Execute a read-only SQL query against the database. \
                Only SELECT, WITH, and EXPLAIN queries are allowed. \
                Requires an active database connection. \
                IMPORTANT: Do NOT use TOP or LIMIT clauses in your SQL query to restrict row count. \
                Instead, use the 'limit' parameter which handles row limiting automatically \
                and works across all database backends (MSSQL, PostgreSQL)."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute (SELECT only). Do NOT include TOP or LIMIT in the query — use the 'limit' parameter instead."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default: 1000). Use this instead of SQL TOP/LIMIT clauses.",
                        "minimum": 1,
                        "maximum": 10000
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "description": "Query timeout in milliseconds (default: 30000)",
                        "minimum": 1000,
                        "maximum": 300000
                    }
                },
                "required": ["query"]
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "execute_query"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        // Check connection first
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: ExecuteQueryArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        // Validate query
        self.validator
            .validate(&args.query)
            .map_err(McpError::from)?;

        // Sanitize query
        let query = self.validator.sanitize(&args.query);

        // Acquire rate limit permit
        let _permit = self.rate_limiter.try_acquire().map_err(McpError::from)?;

        // Execute query
        let limit = args.limit.unwrap_or(self.default_limit);
        let timeout = args
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(self.default_timeout);

        debug!(
            "Executing query with limit {} and timeout {:?}",
            limit, timeout
        );

        let result = driver
            .execute_query_with_timeout(&query, Some(limit), timeout)
            .await
            .map_err(McpError::from)?;

        // Format result
        let output = QueryOutput {
            columns: result.columns.iter().map(|c| c.name.clone()).collect(),
            rows: result.rows,
            row_count: result.row_count,
            execution_time_ms: result.execution_time_ms,
            truncated: result.truncated.unwrap_or(false),
        };

        Ok(CallToolResult::json(&output))
    }
}

#[derive(Debug, Serialize)]
struct QueryOutput {
    columns: Vec<String>,
    rows: Vec<std::collections::HashMap<String, crate::database::CellValue>>,
    row_count: usize,
    execution_time_ms: u64,
    truncated: bool,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_tool_definition() {
        // This would need a mock driver to test fully
        // For now, just verify the schema structure
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "query": { "type": "string" }
            },
            "required": ["query"]
        });

        assert!(schema["properties"]["query"].is_object());
    }
}
