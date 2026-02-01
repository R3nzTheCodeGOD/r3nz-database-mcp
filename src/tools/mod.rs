//! MCP tool definitions and registry.

pub mod connection;
pub mod performance;
pub mod query;
pub mod registry;
pub mod schema;

pub use connection::{
    ConnectTool, ConnectionInfoTool, DisconnectTool, ListDatabasesTool, SwitchDatabaseTool,
};
pub use performance::{
    AnalyzeQueryTool, FindBlockingQueriesTool, GetIndexUsageTool, GetPerformanceStatsTool,
};
pub use query::ExecuteQueryTool;
pub use registry::{ToolHandler, ToolRegistry, error_result, json_result, text_result};
pub use schema::{GetRelationshipsTool, GetSchemaTool, ListTablesTool};

use crate::database::ConnectionManager;
use crate::security::{RateLimiter, SqlValidator};
use std::sync::Arc;

/// Create and register all tools.
pub fn create_registry(
    connection_manager: Arc<ConnectionManager>,
    validator: SqlValidator,
    rate_limiter: Arc<RateLimiter>,
) -> ToolRegistry {
    let registry = ToolRegistry::new();

    // Connection management tools (always available)
    registry.register(ConnectTool::new(Arc::clone(&connection_manager)));
    registry.register(DisconnectTool::new(Arc::clone(&connection_manager)));
    registry.register(ConnectionInfoTool::new(Arc::clone(&connection_manager)));
    registry.register(ListDatabasesTool::new(Arc::clone(&connection_manager)));
    registry.register(SwitchDatabaseTool::new(Arc::clone(&connection_manager)));

    // Query tools (require connection)
    registry.register(ExecuteQueryTool::new(
        Arc::clone(&connection_manager),
        validator.clone(),
        Arc::clone(&rate_limiter),
    ));

    // Schema tools (require connection)
    registry.register(ListTablesTool::new(Arc::clone(&connection_manager)));
    registry.register(GetSchemaTool::new(Arc::clone(&connection_manager)));
    registry.register(GetRelationshipsTool::new(Arc::clone(&connection_manager)));

    // Performance tools (require connection)
    registry.register(AnalyzeQueryTool::new(
        Arc::clone(&connection_manager),
        validator,
    ));
    registry.register(GetPerformanceStatsTool::new(Arc::clone(
        &connection_manager,
    )));
    registry.register(GetIndexUsageTool::new(Arc::clone(&connection_manager)));
    registry.register(FindBlockingQueriesTool::new(connection_manager));

    registry
}
