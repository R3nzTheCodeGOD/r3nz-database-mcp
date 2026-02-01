//! Request handler and method dispatcher.

use crate::error::{ProtocolError, ProtocolResult};
use crate::protocol::types::*;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, instrument, warn};

/// Handler trait for processing MCP requests.
#[async_trait]
pub trait Handler: Send + Sync {
    /// Handle initialize request.
    async fn initialize(&self, params: InitializeParams) -> ProtocolResult<InitializeResult>;

    /// Handle initialized notification.
    async fn initialized(&self) -> ProtocolResult<()>;

    /// Handle shutdown request.
    async fn shutdown(&self) -> ProtocolResult<()>;

    /// List available tools.
    async fn list_tools(&self) -> ProtocolResult<ListToolsResult>;

    /// Call a tool.
    async fn call_tool(&self, params: CallToolParams) -> ProtocolResult<CallToolResult>;

    /// Handle ping request.
    async fn ping(&self) -> ProtocolResult<Value> {
        Ok(serde_json::json!({}))
    }
}

/// Method dispatcher that routes requests to appropriate handlers.
pub struct Dispatcher<H: Handler> {
    handler: Arc<H>,
}

impl<H: Handler> Dispatcher<H> {
    pub fn new(handler: Arc<H>) -> Self {
        Self { handler }
    }

    /// Dispatch a request to the appropriate handler method.
    #[instrument(skip(self, request), fields(method = %request.method))]
    pub async fn dispatch(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        debug!("Dispatching request: {}", request.method);

        let result = match request.method.as_str() {
            "initialize" => self.handle_initialize(request.params).await,
            "initialized" => self.handle_initialized().await,
            "shutdown" => self.handle_shutdown().await,
            "ping" => self.handle_ping().await,
            "tools/list" => self.handle_list_tools().await,
            "tools/call" => self.handle_call_tool(request.params).await,
            method => {
                warn!("Unknown method: {}", method);
                Err(ProtocolError::MethodNotFound(method.to_string()))
            }
        };

        match result {
            Ok(value) => JsonRpcResponse::success(request.id, value),
            Err(e) => {
                error!("Request failed: {}", e);
                JsonRpcResponse::error(request.id, JsonRpcError::new(e.code(), e.to_string()))
            }
        }
    }

    async fn handle_initialize(&self, params: Option<Value>) -> ProtocolResult<Value> {
        let params: InitializeParams = params
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| ProtocolError::InvalidParams(e.to_string().into()))?
            .ok_or_else(|| ProtocolError::InvalidParams("Missing params".into()))?;

        let result = self.handler.initialize(params).await?;
        serde_json::to_value(result).map_err(|e| ProtocolError::InternalError(e.to_string().into()))
    }

    async fn handle_initialized(&self) -> ProtocolResult<Value> {
        self.handler.initialized().await?;
        Ok(Value::Null)
    }

    async fn handle_shutdown(&self) -> ProtocolResult<Value> {
        self.handler.shutdown().await?;
        Ok(Value::Null)
    }

    async fn handle_ping(&self) -> ProtocolResult<Value> {
        self.handler.ping().await
    }

    async fn handle_list_tools(&self) -> ProtocolResult<Value> {
        let result = self.handler.list_tools().await?;
        serde_json::to_value(result).map_err(|e| ProtocolError::InternalError(e.to_string().into()))
    }

    async fn handle_call_tool(&self, params: Option<Value>) -> ProtocolResult<Value> {
        let params: CallToolParams = params
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| ProtocolError::InvalidParams(e.to_string().into()))?
            .ok_or_else(|| ProtocolError::InvalidParams("Missing params".into()))?;

        let result = self.handler.call_tool(params).await?;
        serde_json::to_value(result).map_err(|e| ProtocolError::InternalError(e.to_string().into()))
    }
}

/// Request context for handler methods.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: Option<RequestId>,
    pub method: String,
    pub metadata: HashMap<String, String>,
}

impl RequestContext {
    pub fn new(request: &JsonRpcRequest) -> Self {
        Self {
            request_id: request.id.clone(),
            method: request.method.clone(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct MockHandler {
        initialized: AtomicBool,
    }

    impl MockHandler {
        fn new() -> Self {
            Self {
                initialized: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl Handler for MockHandler {
        async fn initialize(&self, _params: InitializeParams) -> ProtocolResult<InitializeResult> {
            self.initialized.store(true, Ordering::SeqCst);
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

        async fn initialized(&self) -> ProtocolResult<()> {
            Ok(())
        }

        async fn shutdown(&self) -> ProtocolResult<()> {
            Ok(())
        }

        async fn list_tools(&self) -> ProtocolResult<ListToolsResult> {
            Ok(ListToolsResult {
                tools: vec![],
                next_cursor: None,
            })
        }

        async fn call_tool(&self, _params: CallToolParams) -> ProtocolResult<CallToolResult> {
            Ok(CallToolResult::text("test"))
        }
    }

    #[tokio::test]
    async fn test_dispatcher_initialize() {
        let handler = Arc::new(MockHandler::new());
        let dispatcher = Dispatcher::new(handler.clone());

        let request = JsonRpcRequest::new("initialize")
            .with_id(1)
            .with_params(serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0"
                }
            }));

        let response = dispatcher.dispatch(request).await;
        assert!(response.result.is_some());
        assert!(response.error.is_none());
        assert!(handler.initialized.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_dispatcher_unknown_method() {
        let handler = Arc::new(MockHandler::new());
        let dispatcher = Dispatcher::new(handler);

        let request = JsonRpcRequest::new("unknown/method").with_id(1);
        let response = dispatcher.dispatch(request).await;

        assert!(response.result.is_none());
        assert!(response.error.is_some());
        assert_eq!(response.error.unwrap().code, -32601);
    }
}
