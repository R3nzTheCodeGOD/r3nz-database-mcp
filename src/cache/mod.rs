//! Schema cache with TTL-based expiration.

use crate::database::{TableInfo, TableSchema};
use dashmap::DashMap;
use std::time::{Duration, Instant};
use tracing::debug;

/// Cache entry with TTL.
#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    created_at: Instant,
    ttl: Duration,
}

impl<T> CacheEntry<T> {
    fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            created_at: Instant::now(),
            ttl,
        }
    }

    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

/// Schema cache for storing table metadata.
pub struct SchemaCache {
    /// Table list cache (by schema name).
    tables: DashMap<String, CacheEntry<Vec<TableInfo>>>,
    /// Table schema cache (by full table name).
    schemas: DashMap<String, CacheEntry<TableSchema>>,
    /// Default TTL for cache entries.
    default_ttl: Duration,
}

impl SchemaCache {
    /// Create a new schema cache with the specified TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            tables: DashMap::new(),
            schemas: DashMap::new(),
            default_ttl: ttl,
        }
    }

    /// Get cached table list.
    pub fn get_tables(&self, schema: &str) -> Option<Vec<TableInfo>> {
        if let Some(entry) = self.tables.get(schema) {
            if !entry.is_expired() {
                debug!("Cache hit for tables in schema: {}", schema);
                return Some(entry.value.clone());
            }
            // Entry expired, remove it
            drop(entry);
            self.tables.remove(schema);
        }
        None
    }

    /// Cache table list.
    pub fn set_tables(&self, schema: &str, tables: Vec<TableInfo>) {
        debug!("Caching {} tables for schema: {}", tables.len(), schema);
        self.tables.insert(
            schema.to_string(),
            CacheEntry::new(tables, self.default_ttl),
        );
    }

    /// Get cached table schema.
    pub fn get_schema(&self, table_name: &str) -> Option<TableSchema> {
        if let Some(entry) = self.schemas.get(table_name) {
            if !entry.is_expired() {
                debug!("Cache hit for schema: {}", table_name);
                return Some(entry.value.clone());
            }
            drop(entry);
            self.schemas.remove(table_name);
        }
        None
    }

    /// Cache table schema.
    pub fn set_schema(&self, table_name: &str, schema: TableSchema) {
        debug!("Caching schema for table: {}", table_name);
        self.schemas.insert(
            table_name.to_string(),
            CacheEntry::new(schema, self.default_ttl),
        );
    }

    /// Invalidate all cache entries for a schema.
    pub fn invalidate_schema(&self, schema: &str) {
        debug!("Invalidating cache for schema: {}", schema);
        self.tables.remove(schema);
        // Remove all schemas that start with this schema prefix
        self.schemas
            .retain(|k, _| !k.starts_with(&format!("{}.", schema)));
    }

    /// Invalidate a specific table's schema.
    pub fn invalidate_table(&self, table_name: &str) {
        debug!("Invalidating cache for table: {}", table_name);
        self.schemas.remove(table_name);
    }

    /// Clear all cache entries.
    pub fn clear(&self) {
        debug!("Clearing all cache entries");
        self.tables.clear();
        self.schemas.clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            tables_cached: self.tables.len(),
            schemas_cached: self.schemas.len(),
        }
    }

    /// Remove expired entries.
    pub fn cleanup(&self) {
        self.tables.retain(|_, v| !v.is_expired());
        self.schemas.retain(|_, v| !v.is_expired());
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(300)) // 5 minutes default TTL
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub tables_cached: usize,
    pub schemas_cached: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::TableType;

    #[test]
    fn test_cache_basic() {
        let cache = SchemaCache::new(Duration::from_secs(60));

        let tables = vec![TableInfo {
            schema: "public".into(),
            name: "users".into(),
            table_type: TableType::Table,
            row_count: Some(100),
        }];

        cache.set_tables("public", tables.clone());
        let cached = cache.get_tables("public");
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().len(), 1);
    }

    #[test]
    fn test_cache_expiry() {
        let cache = SchemaCache::new(Duration::from_millis(1));

        let tables = vec![TableInfo {
            schema: "public".into(),
            name: "users".into(),
            table_type: TableType::Table,
            row_count: None,
        }];

        cache.set_tables("public", tables);

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(10));

        let cached = cache.get_tables("public");
        assert!(cached.is_none());
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = SchemaCache::new(Duration::from_secs(60));

        let tables = vec![TableInfo {
            schema: "public".into(),
            name: "users".into(),
            table_type: TableType::Table,
            row_count: None,
        }];

        cache.set_tables("public", tables);
        assert!(cache.get_tables("public").is_some());

        cache.invalidate_schema("public");
        assert!(cache.get_tables("public").is_none());
    }
}
