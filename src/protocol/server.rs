//! MCP server with lifecycle management.

use crate::error::{McpError, ProtocolError, Result};
use crate::protocol::handler::{Dispatcher, Handler};
use crate::protocol::transport::{StdioTransport, Transport};
use crate::protocol::types::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

/// Server state enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    /// Server created but not initialized.
    Created,
    /// Initialize request received, awaiting initialized notification.
    Initializing,
    /// Server is fully operational.
    Running,
    /// Shutdown requested.
    ShuttingDown,
    /// Server has stopped.
    Stopped,
}

/// MCP Server.
pub struct McpServer<H: Handler> {
    info: ServerInfo,
    capabilities: ServerCapabilities,
    handler: Arc<H>,
    state: Arc<RwLock<ServerState>>,
    running: AtomicBool,
}

impl<H: Handler> McpServer<H> {
    /// Create a new MCP server.
    pub fn new(handler: H, info: ServerInfo, capabilities: ServerCapabilities) -> Self {
        Self {
            info,
            capabilities,
            handler: Arc::new(handler),
            state: Arc::new(RwLock::new(ServerState::Created)),
            running: AtomicBool::new(false),
        }
    }

    /// Get current server state.
    pub async fn state(&self) -> ServerState {
        *self.state.read().await
    }

    /// Check if server is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Run the server with stdio transport.
    #[instrument(skip(self), fields(server = %self.info.name))]
    pub async fn run(self) -> Result<()> {
        let transport = Arc::new(StdioTransport::new());
        self.run_with_transport(transport).await
    }

    /// Run the server with a custom transport.
    pub async fn run_with_transport<T: Transport + 'static>(self, transport: Arc<T>) -> Result<()> {
        info!(
            "Starting MCP server: {} v{}",
            self.info.name, self.info.version
        );
        self.running.store(true, Ordering::SeqCst);

        let dispatcher = Dispatcher::new(Arc::clone(&self.handler));
        let server = Arc::new(self);

        loop {
            // Check if we should stop
            if !server.running.load(Ordering::SeqCst) {
                info!("Server stopping...");
                break;
            }

            // Read next message
            let message = match transport.read_message().await {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    debug!("EOF received, shutting down");
                    break;
                }
                Err(McpError::Protocol(ProtocolError::ParseError)) => {
                    // Send parse error response
                    let response = JsonRpcResponse::error(None, JsonRpcError::parse_error());
                    if let Err(e) = transport.write_response(&response).await {
                        error!("Failed to send error response: {}", e);
                    }
                    continue;
                }
                Err(e) => {
                    error!("Transport error: {}", e);
                    break;
                }
            };

            // Process message
            match message {
                Message::Request(request) => {
                    let is_notification = request.is_notification();
                    let method = request.method.clone();

                    // Update state based on method
                    server.update_state_for_method(&method).await;

                    // Dispatch request
                    let response = dispatcher.dispatch(request).await;

                    // Don't send response for notifications
                    if !is_notification && let Err(e) = transport.write_response(&response).await {
                        error!("Failed to send response: {}", e);
                    }

                    // Check for shutdown
                    if method == "shutdown" {
                        info!("Shutdown request received");
                        server.running.store(false, Ordering::SeqCst);
                    }
                }
                Message::Response(response) => {
                    // We don't expect responses in server mode, but log them
                    warn!("Unexpected response received: {:?}", response.id);
                }
            }
        }

        *server.state.write().await = ServerState::Stopped;
        info!("Server stopped");
        Ok(())
    }

    /// Update server state based on the method being processed.
    async fn update_state_for_method(&self, method: &str) {
        let mut state = self.state.write().await;
        match method {
            "initialize" => {
                if *state == ServerState::Created {
                    *state = ServerState::Initializing;
                }
            }
            "initialized" => {
                if *state == ServerState::Initializing {
                    *state = ServerState::Running;
                    info!("Server initialized and running");
                }
            }
            "shutdown" => {
                *state = ServerState::ShuttingDown;
            }
            _ => {}
        }
    }

    /// Stop the server.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

/// Builder for MCP Server.
pub struct McpServerBuilder<H: Handler> {
    handler: Option<H>,
    name: String,
    version: String,
    capabilities: ServerCapabilities,
}

impl<H: Handler> McpServerBuilder<H> {
    pub fn new() -> Self {
        Self {
            handler: None,
            name: env!("CARGO_PKG_NAME").into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: ServerCapabilities::default(),
        }
    }

    pub fn handler(mut self, handler: H) -> Self {
        self.handler = Some(handler);
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn capabilities(mut self, capabilities: ServerCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn with_tools(mut self) -> Self {
        self.capabilities.tools = Some(ToolsCapability {
            list_changed: Some(true),
        });
        self
    }

    pub fn build(self) -> Result<McpServer<H>> {
        let handler = self.handler.ok_or_else(|| McpError::Internal {
            message: "Handler is required".into(),
        })?;

        Ok(McpServer::new(
            handler,
            ServerInfo {
                name: self.name,
                version: self.version,
            },
            self.capabilities,
        ))
    }
}

impl<H: Handler> Default for McpServerBuilder<H> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::handler::Handler;
    use async_trait::async_trait;

    struct TestHandler;

    #[async_trait]
    impl Handler for TestHandler {
        async fn initialize(
            &self,
            _params: InitializeParams,
        ) -> crate::error::ProtocolResult<InitializeResult> {
            Ok(InitializeResult {
                protocol_version: MCP_VERSION.into(),
                capabilities: ServerCapabilities::default(),
                server_info: ServerInfo {
                    name: "test".into(),
                    version: "1.0".into(),
                },
                instructions: None,
            })
        }

        async fn initialized(&self) -> crate::error::ProtocolResult<()> {
            Ok(())
        }

        async fn shutdown(&self) -> crate::error::ProtocolResult<()> {
            Ok(())
        }

        async fn list_tools(&self) -> crate::error::ProtocolResult<ListToolsResult> {
            Ok(ListToolsResult {
                tools: vec![],
                next_cursor: None,
            })
        }

        async fn call_tool(
            &self,
            _params: CallToolParams,
        ) -> crate::error::ProtocolResult<CallToolResult> {
            Ok(CallToolResult::text("test"))
        }
    }

    #[test]
    fn test_server_builder() {
        let server = McpServerBuilder::new()
            .handler(TestHandler)
            .name("test-server")
            .version("0.1.0")
            .with_tools()
            .build()
            .unwrap();

        assert_eq!(server.info.name, "test-server");
        assert_eq!(server.info.version, "0.1.0");
        assert!(server.capabilities.tools.is_some());
    }

    #[tokio::test]
    async fn test_server_state() {
        let server = McpServerBuilder::new()
            .handler(TestHandler)
            .build()
            .unwrap();

        assert_eq!(server.state().await, ServerState::Created);
    }
}
