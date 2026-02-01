//! Error types for the MCP server.
//!
//! Uses `thiserror` for ergonomic error definitions with automatic `From` conversions.

use std::borrow::Cow;
use thiserror::Error;

/// Main error type for the MCP database server.
#[derive(Debug, Error)]
pub enum McpError {
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Database error: {0}")]
    Database(#[from] DatabaseError),

    #[error("Security error: {0}")]
    Security(#[from] SecurityError),

    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("Tool error: {0}")]
    Tool(#[from] ToolError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Internal error: {message}")]
    Internal { message: Cow<'static, str> },
}

/// JSON-RPC 2.0 and MCP protocol errors.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Parse error: invalid JSON")]
    ParseError,

    #[error("Invalid request: {0}")]
    InvalidRequest(Cow<'static, str>),

    #[error("Method not found: {0}")]
    MethodNotFound(String),

    #[error("Invalid params: {0}")]
    InvalidParams(Cow<'static, str>),

    #[error("Internal error: {0}")]
    InternalError(Cow<'static, str>),

    #[error("Server not initialized")]
    NotInitialized,

    #[error("Server already initialized")]
    AlreadyInitialized,

    #[error("Transport error: {0}")]
    Transport(Cow<'static, str>),
}

impl ProtocolError {
    /// Returns the JSON-RPC 2.0 error code.
    pub fn code(&self) -> i32 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest(_) => -32600,
            Self::MethodNotFound(_) => -32601,
            Self::InvalidParams(_) => -32602,
            Self::InternalError(_) => -32603,
            Self::NotInitialized => -32002,
            Self::AlreadyInitialized => -32002,
            Self::Transport(_) => -32000,
        }
    }
}

/// Database-related errors.
#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("No active database connection. Use the 'connect' tool to establish a connection.")]
    NotConnected,

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Connection pool exhausted")]
    PoolExhausted,

    #[error("Query execution failed: {0}")]
    QueryFailed(String),

    #[error("Query timeout after {0}ms")]
    Timeout(u64),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Unsupported database type: {0}")]
    UnsupportedType(String),

    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("MSSQL error: {0}")]
    Mssql(String),

    #[error("PostgreSQL error: {0}")]
    Postgres(String),
}

/// Security-related errors.
#[derive(Debug, Error)]
pub enum SecurityError {
    #[error("SQL injection detected: {0}")]
    SqlInjection(String),

    #[error("Forbidden keyword: {0}")]
    ForbiddenKeyword(String),

    #[error("Query not allowed: {0}")]
    QueryNotAllowed(String),

    #[error("Rate limit exceeded: {0} requests per second")]
    RateLimitExceeded(u32),

    #[error("Concurrent query limit exceeded: {0}")]
    ConcurrentLimitExceeded(u32),

    #[error("Query too complex: {0}")]
    QueryTooComplex(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),
}

/// Configuration errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Missing required field: {0}")]
    MissingField(Cow<'static, str>),

    #[error("Invalid value for {field}: {message}")]
    InvalidValue {
        field: Cow<'static, str>,
        message: Cow<'static, str>,
    },

    #[error("Environment variable not found: {0}")]
    EnvNotFound(String),

    #[error("Invalid database URL: {0}")]
    InvalidDatabaseUrl(String),
}

/// Tool execution errors.
#[derive(Debug, Error)]
pub enum ToolError {
    #[error("Tool not found: {0}")]
    NotFound(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("Execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Missing required argument: {0}")]
    MissingArgument(Cow<'static, str>),
}

/// Result type alias for McpError.
pub type Result<T> = std::result::Result<T, McpError>;

/// Result type alias for DatabaseError.
pub type DbResult<T> = std::result::Result<T, DatabaseError>;

/// Result type alias for ProtocolError.
pub type ProtocolResult<T> = std::result::Result<T, ProtocolError>;

/// Result type alias for SecurityError.
pub type SecurityResult<T> = std::result::Result<T, SecurityError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_error_codes() {
        assert_eq!(ProtocolError::ParseError.code(), -32700);
        assert_eq!(ProtocolError::InvalidRequest("test".into()).code(), -32600);
        assert_eq!(ProtocolError::MethodNotFound("test".into()).code(), -32601);
        assert_eq!(ProtocolError::InvalidParams("test".into()).code(), -32602);
        assert_eq!(ProtocolError::InternalError("test".into()).code(), -32603);
    }

    #[test]
    fn test_error_conversion() {
        let db_error = DatabaseError::ConnectionFailed("test".into());
        let mcp_error: McpError = db_error.into();
        assert!(matches!(mcp_error, McpError::Database(_)));
    }
}
