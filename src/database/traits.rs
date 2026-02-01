//! Database driver trait.

use crate::database::result::{
    BlockingQuery, ExecutionPlan, IndexUsage, PerformanceStats, QueryResult, TableInfo,
    TableRelationship, TableSchema,
};
use crate::error::DbResult;
use async_trait::async_trait;
use std::time::Duration;

/// Async database driver trait.
///
/// Implementations: [`MssqlDriver`](crate::database::MssqlDriver),
/// [`PostgresDriver`](crate::database::PostgresDriver).
#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    /// Returns the driver name (e.g., "mssql", "postgres").
    fn name(&self) -> &'static str;

    /// Checks if the driver has active connections in the pool.
    ///
    /// Note: This only checks pool state, not actual database connectivity.
    async fn is_connected(&self) -> bool;

    /// Executes a read-only SQL query and returns the results.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query to execute (must be SELECT, WITH, or EXPLAIN)
    /// * `limit` - Optional maximum number of rows to return
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::QueryFailed`] if the query execution fails.
    /// Returns [`DatabaseError::PoolExhausted`] if no connections are available.
    async fn execute_query(&self, query: &str, limit: Option<u32>) -> DbResult<QueryResult>;

    /// Executes a query with a timeout.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::Timeout`] if the query exceeds the timeout duration.
    async fn execute_query_with_timeout(
        &self,
        query: &str,
        limit: Option<u32>,
        timeout: Duration,
    ) -> DbResult<QueryResult>;

    /// Lists all tables and views in the specified schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - Optional schema name filter (defaults to "dbo" for MSSQL, "public" for PostgreSQL)
    async fn list_tables(&self, schema: Option<&str>) -> DbResult<Vec<TableInfo>>;

    /// Gets detailed schema information for a table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Table name, optionally prefixed with schema (e.g., "dbo.users")
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::TableNotFound`] if the table does not exist.
    async fn get_table_schema(&self, table_name: &str) -> DbResult<TableSchema>;

    /// Gets foreign key relationships.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Optional table name filter. If None, returns all relationships.
    async fn get_relationships(&self, table_name: Option<&str>)
    -> DbResult<Vec<TableRelationship>>;

    /// Gets the execution plan for a query without executing it.
    ///
    /// Useful for query optimization and performance analysis.
    async fn get_execution_plan(&self, query: &str) -> DbResult<ExecutionPlan>;

    /// Gets database-wide performance statistics.
    ///
    /// Returns top queries by duration, wait statistics, and cache information.
    async fn get_performance_stats(&self) -> DbResult<PerformanceStats>;

    /// Gets index usage statistics.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Optional table name filter
    async fn get_index_usage(&self, table_name: Option<&str>) -> DbResult<Vec<IndexUsage>>;

    /// Finds currently blocking and blocked queries.
    ///
    /// Useful for diagnosing lock contention issues.
    async fn find_blocking_queries(&self) -> DbResult<Vec<BlockingQuery>>;

    /// Lists all databases on the connected server.
    ///
    /// Returns a list of database names that the connected user has access to.
    async fn list_databases(&self) -> DbResult<Vec<String>>;
}

/// Connection pool trait for managing database connections.
#[async_trait]
pub trait ConnectionPool: Send + Sync {
    type Connection: Send;

    /// Get a connection from the pool.
    async fn get(&self) -> DbResult<Self::Connection>;

    /// Get the current pool status.
    fn status(&self) -> PoolStatus;
}

/// Pool status information.
#[derive(Debug, Clone)]
pub struct PoolStatus {
    pub size: u32,
    pub available: u32,
    pub in_use: u32,
}

/// Query options for customizing query execution.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub limit: Option<u32>,
    pub timeout: Option<Duration>,
    pub read_only: bool,
}

impl QueryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_options_builder() {
        let options = QueryOptions::new()
            .with_limit(100)
            .with_timeout(Duration::from_secs(30))
            .read_only();

        assert_eq!(options.limit, Some(100));
        assert_eq!(options.timeout, Some(Duration::from_secs(30)));
        assert!(options.read_only);
    }
}
