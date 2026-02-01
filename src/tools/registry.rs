//! Tool registry for dynamic tool registration.

use crate::error::{Result, ToolError};
use crate::protocol::{CallToolParams, CallToolResult, Tool};
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;
use tracing::debug;

#[async_trait]
pub trait ToolHandler: Send + Sync {
    fn definition(&self) -> Tool;
    async fn execute(&self, arguments: Value) -> Result<CallToolResult>;
}

pub struct ToolRegistry {
    tools: DashMap<String, Arc<dyn ToolHandler>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: DashMap::new(),
        }
    }

    pub fn register<T: ToolHandler + 'static>(&self, tool: T) {
        let definition = tool.definition();
        let name = definition.name.clone();
        debug!("Registering tool: {}", name);
        self.tools.insert(name, Arc::new(tool));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ToolHandler>> {
        self.tools.get(name).map(|r| Arc::clone(&*r))
    }

    pub fn list(&self) -> Vec<Tool> {
        self.tools.iter().map(|r| r.value().definition()).collect()
    }

    pub async fn execute(&self, params: CallToolParams) -> Result<CallToolResult> {
        let tool = self
            .get(&params.name)
            .ok_or_else(|| ToolError::NotFound(params.name.clone()))?;

        tool.execute(params.arguments).await
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[macro_export]
macro_rules! define_tool {
    (
        name: $name:expr,
        description: $desc:expr,
        schema: $schema:tt
    ) => {
        $crate::protocol::Tool {
            name: $name.into(),
            description: Some($desc.into()),
            input_schema: serde_json::json!($schema),
        }
    };
}

pub fn text_result(text: impl Into<String>) -> CallToolResult {
    CallToolResult::text(text)
}

pub fn json_result<T: serde::Serialize>(data: &T) -> CallToolResult {
    CallToolResult::json(data)
}

pub fn error_result(message: impl Into<String>) -> CallToolResult {
    CallToolResult::error(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTool;

    #[async_trait]
    impl ToolHandler for TestTool {
        fn definition(&self) -> Tool {
            Tool {
                name: "test_tool".into(),
                description: Some("A test tool".into()),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {}
                }),
            }
        }

        async fn execute(&self, _arguments: Value) -> Result<CallToolResult> {
            Ok(CallToolResult::text("test result"))
        }
    }

    #[test]
    fn test_registry() {
        let registry = ToolRegistry::new();
        registry.register(TestTool);

        assert_eq!(registry.len(), 1);
        assert!(registry.get("test_tool").is_some());
        assert!(registry.get("unknown").is_none());

        let tools = registry.list();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "test_tool");
    }

    #[tokio::test]
    async fn test_execute() {
        let registry = ToolRegistry::new();
        registry.register(TestTool);

        let params = CallToolParams {
            name: "test_tool".into(),
            arguments: serde_json::json!({}),
        };

        let result = registry.execute(params).await.unwrap();
        assert!(result.is_error.is_none());
    }
}
