//! Connection pool utilities and metrics.

use crate::config::DatabaseConfig;
use crate::error::DbResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

/// Generic connection pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_size: u32,
    pub min_idle: u32,
    pub connection_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            min_idle: 1,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(600)),
            max_lifetime: Some(Duration::from_secs(1800)),
        }
    }
}

impl From<&DatabaseConfig> for PoolConfig {
    fn from(config: &DatabaseConfig) -> Self {
        Self {
            max_size: config.pool_size,
            connection_timeout: config.connection_timeout,
            ..Default::default()
        }
    }
}

/// Pool metrics for monitoring.
#[derive(Debug, Default)]
pub struct PoolMetrics {
    pub connections_created: AtomicU32,
    pub connections_closed: AtomicU32,
    pub connection_errors: AtomicU32,
    pub queries_executed: AtomicU32,
    pub query_errors: AtomicU32,
}

impl PoolMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_connection_created(&self) {
        self.connections_created.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_closed(&self) {
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_executed(&self) {
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_error(&self) {
        self.query_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            connection_errors: self.connection_errors.load(Ordering::Relaxed),
            queries_executed: self.queries_executed.load(Ordering::Relaxed),
            query_errors: self.query_errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of pool metrics.
#[derive(Debug, Clone)]
pub struct PoolMetricsSnapshot {
    pub connections_created: u32,
    pub connections_closed: u32,
    pub connection_errors: u32,
    pub queries_executed: u32,
    pub query_errors: u32,
}

/// Connection guard that tracks usage.
pub struct ConnectionGuard<C> {
    pub connection: C,
    metrics: Arc<PoolMetrics>,
}

impl<C> ConnectionGuard<C> {
    pub fn new(connection: C, metrics: Arc<PoolMetrics>) -> Self {
        Self {
            connection,
            metrics,
        }
    }

    pub fn record_query(&self) {
        self.metrics.record_query_executed();
    }

    pub fn record_error(&self) {
        self.metrics.record_query_error();
    }
}

impl<C> std::ops::Deref for ConnectionGuard<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<C> std::ops::DerefMut for ConnectionGuard<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

/// Health check trait for connection pools.
#[async_trait::async_trait]
pub trait HealthCheck: Send + Sync {
    async fn is_healthy(&self) -> bool;
    async fn check_connection(&self) -> DbResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_size, 10);
        assert_eq!(config.min_idle, 1);
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_pool_metrics() {
        let metrics = PoolMetrics::new();
        metrics.record_query_executed();
        metrics.record_query_executed();
        metrics.record_query_error();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.queries_executed, 2);
        assert_eq!(snapshot.query_errors, 1);
    }
}
