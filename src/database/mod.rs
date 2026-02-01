//! Database abstraction layer.
//!
//! Provides a unified interface for different database backends (MSSQL, PostgreSQL).
//!
//! # Feature Flags
//!
//! - `mssql` - Enable Microsoft SQL Server support (enabled by default)
//! - `postgres` - Enable PostgreSQL support (enabled by default)
//!
//! # Example
//!
//! ```toml
//! # Cargo.toml - Use only MSSQL
//! [dependencies]
//! r3nz-database-mcp = { version = "0.1", default-features = false, features = ["mssql"] }
//! ```

pub mod connection;
pub mod connection_string;
#[cfg(feature = "mssql")]
pub mod mssql;
pub mod pool;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod result;
pub mod traits;

pub use connection::{ConnectionConfig, ConnectionManager, ConnectionMetadata, ConnectionState};
pub use connection_string::ConnectionStringParser;
#[cfg(feature = "mssql")]
pub use mssql::MssqlDriver;
pub use pool::{ConnectionGuard, HealthCheck, PoolConfig, PoolMetrics, PoolMetricsSnapshot};
#[cfg(feature = "postgres")]
pub use postgres::PostgresDriver;
pub use result::*;
pub use traits::{ConnectionPool, DatabaseDriver, PoolStatus, QueryOptions};

use crate::config::{DatabaseConfig, DatabaseType};
#[cfg(not(all(feature = "mssql", feature = "postgres")))]
use crate::error::DatabaseError;
use crate::error::DbResult;
use std::sync::Arc;

/// Create a database driver based on configuration.
///
/// # Feature Requirements
///
/// - For `DatabaseType::Mssql`, the `mssql` feature must be enabled
/// - For `DatabaseType::Postgres`, the `postgres` feature must be enabled
///
/// # Errors
///
/// Returns [`DatabaseError::UnsupportedType`] if the requested database type
/// is not enabled via feature flags.
pub async fn create_driver(config: DatabaseConfig) -> DbResult<Arc<dyn DatabaseDriver>> {
    match config.database_type {
        #[cfg(feature = "mssql")]
        DatabaseType::Mssql => {
            let driver = MssqlDriver::new(config).await?;
            Ok(Arc::new(driver))
        }
        #[cfg(not(feature = "mssql"))]
        DatabaseType::Mssql => Err(DatabaseError::UnsupportedType(
            "MSSQL support not enabled. Enable the 'mssql' feature.".to_string(),
        )),

        #[cfg(feature = "postgres")]
        DatabaseType::Postgres => {
            let driver = PostgresDriver::new(config).await?;
            Ok(Arc::new(driver))
        }
        #[cfg(not(feature = "postgres"))]
        DatabaseType::Postgres => Err(DatabaseError::UnsupportedType(
            "PostgreSQL support not enabled. Enable the 'postgres' feature.".to_string(),
        )),
    }
}
