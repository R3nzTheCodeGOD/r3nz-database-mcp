//! MCP request handler implementation.

use crate::error::ProtocolResult;
use crate::protocol::{
    CallToolParams, CallToolResult, Handler, InitializeParams, InitializeResult, ListToolsResult,
    MCP_VERSION, ServerCapabilities, ServerInfo, ToolsCapability,
};
use crate::server::state::ServerState;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

/// MCP request handler that processes protocol messages.
pub struct McpHandler {
    state: Arc<ServerState>,
}

impl McpHandler {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    pub fn state(&self) -> &Arc<ServerState> {
        &self.state
    }
}

#[async_trait]
impl Handler for McpHandler {
    async fn initialize(&self, params: InitializeParams) -> ProtocolResult<InitializeResult> {
        info!(
            "Initialize request from {} v{}",
            params.client_info.name, params.client_info.version
        );
        debug!("Client capabilities: {:?}", params.capabilities);

        // Store client info
        self.state.set_initialized(params.client_info);

        let capabilities = ServerCapabilities {
            tools: Some(ToolsCapability {
                list_changed: Some(false),
            }),
            resources: None,
            prompts: None,
            logging: None,
        };

        // Build instructions based on connection state
        let instructions = if self.state.is_connected() {
            let metadata = self.state.connection_manager.metadata();
            format!(
                "Database MCP Server connected to {} ({} on {}). \
                Available tools: connect, disconnect, connection_info, list_databases, switch_database, \
                execute_query, list_tables, get_schema, get_relationships, analyze_query, \
                get_performance_stats, get_index_usage, find_blocking_queries.",
                metadata.database.as_deref().unwrap_or("unknown"),
                metadata.database_type.as_deref().unwrap_or("unknown"),
                metadata.host.as_deref().unwrap_or("unknown")
            )
        } else {
            "Database MCP Server (not connected). \
            Use the 'connect' tool to establish a database connection. \
            Available connection tools: connect, disconnect, connection_info. \
            Once connected: execute_query, list_tables, get_schema, get_relationships, \
            list_databases, switch_database, analyze_query, get_performance_stats, \
            get_index_usage, find_blocking_queries."
                .to_string()
        };

        let result = InitializeResult {
            protocol_version: MCP_VERSION.into(),
            capabilities,
            server_info: ServerInfo {
                name: self.state.config.name.to_string(),
                version: self.state.config.version.to_string(),
            },
            instructions: Some(instructions),
        };

        Ok(result)
    }

    async fn initialized(&self) -> ProtocolResult<()> {
        info!("Server initialized successfully");
        Ok(())
    }

    async fn shutdown(&self) -> ProtocolResult<()> {
        info!("Shutdown request received");
        Ok(())
    }

    async fn list_tools(&self) -> ProtocolResult<ListToolsResult> {
        let tools = self.state.tools.list();
        debug!("Listing {} tools", tools.len());

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
        })
    }

    async fn call_tool(&self, params: CallToolParams) -> ProtocolResult<CallToolResult> {
        debug!("Tool call: {}", params.name);

        match self.state.tools.execute(params).await {
            Ok(result) => Ok(result),
            Err(e) => {
                tracing::error!("Tool execution error: {}", e);
                Ok(CallToolResult::error(e.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests would require setting up mock state
}
