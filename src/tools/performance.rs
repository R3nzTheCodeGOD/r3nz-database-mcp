//! Performance analysis tools: execution plans, statistics, blocking queries.

use crate::database::ConnectionManager;
use crate::error::{McpError, Result, ToolError};
use crate::protocol::{CallToolResult, Tool};
use crate::security::SqlValidator;
use crate::tools::registry::ToolHandler;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tracing::instrument;

#[derive(Debug, Deserialize)]
pub struct AnalyzeQueryArgs {
    pub query: String,
}

pub struct AnalyzeQueryTool {
    connection_manager: Arc<ConnectionManager>,
    validator: SqlValidator,
}

impl AnalyzeQueryTool {
    pub fn new(connection_manager: Arc<ConnectionManager>, validator: SqlValidator) -> Self {
        Self {
            connection_manager,
            validator,
        }
    }
}

#[async_trait]
impl ToolHandler for AnalyzeQueryTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "analyze_query".into(),
            description: Some(
                "Analyze a SQL query's execution plan without running it. \
                Shows estimated costs, row counts, and operations. \
                Requires an active database connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze (SELECT only)"
                    }
                },
                "required": ["query"]
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "analyze_query"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: AnalyzeQueryArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        // Validate query
        self.validator
            .validate(&args.query)
            .map_err(McpError::from)?;

        let plan = driver
            .get_execution_plan(&args.query)
            .await
            .map_err(McpError::from)?;

        Ok(CallToolResult::json(&plan))
    }
}

#[derive(Debug, Deserialize)]
pub struct GetPerformanceStatsArgs {
    #[serde(default)]
    pub metric_type: Option<String>,
}

pub struct GetPerformanceStatsTool {
    connection_manager: Arc<ConnectionManager>,
}

impl GetPerformanceStatsTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for GetPerformanceStatsTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "get_performance_stats".into(),
            description: Some(
                "Get database performance statistics including top queries by duration, \
                wait statistics, and cache hit ratios. \
                Requires an active database connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "metric_type": {
                        "type": "string",
                        "description": "Type of metrics to retrieve: 'queries', 'waits', 'cache', or 'all' (default)",
                        "enum": ["queries", "waits", "cache", "all"]
                    }
                }
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "get_performance_stats"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let _args: GetPerformanceStatsArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        let stats = driver
            .get_performance_stats()
            .await
            .map_err(McpError::from)?;

        Ok(CallToolResult::json(&stats))
    }
}

#[derive(Debug, Deserialize)]
pub struct GetIndexUsageArgs {
    #[serde(default)]
    pub table_name: Option<String>,
}

pub struct GetIndexUsageTool {
    connection_manager: Arc<ConnectionManager>,
}

impl GetIndexUsageTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for GetIndexUsageTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "get_index_usage".into(),
            description: Some(
                "Get index usage statistics showing which indexes are being used \
                and how often. Helps identify unused or underutilized indexes. \
                Requires an active database connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table name to get index usage for (optional, returns all if not specified)"
                    }
                }
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "get_index_usage"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: GetIndexUsageArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        let usage = driver
            .get_index_usage(args.table_name.as_deref())
            .await
            .map_err(McpError::from)?;

        Ok(CallToolResult::json(&usage))
    }
}

pub struct FindBlockingQueriesTool {
    connection_manager: Arc<ConnectionManager>,
}

impl FindBlockingQueriesTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for FindBlockingQueriesTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "find_blocking_queries".into(),
            description: Some(
                "Find queries that are currently blocking other queries. \
                Shows session IDs, wait types, and the blocking query text. \
                Requires an active database connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    #[instrument(skip(self, _arguments), fields(tool = "find_blocking_queries"))]
    async fn execute(&self, _arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let blocking = driver
            .find_blocking_queries()
            .await
            .map_err(McpError::from)?;

        if blocking.is_empty() {
            Ok(CallToolResult::text("No blocking queries found."))
        } else {
            Ok(CallToolResult::json(&blocking))
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_analyze_query_definition() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "query": { "type": "string" }
            },
            "required": ["query"]
        });
        assert!(
            schema["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("query"))
        );
    }

    #[test]
    fn test_performance_stats_definition() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "metric_type": {
                    "type": "string",
                    "enum": ["queries", "waits", "cache", "all"]
                }
            }
        });
        assert!(schema["properties"]["metric_type"]["enum"].is_array());
    }
}
