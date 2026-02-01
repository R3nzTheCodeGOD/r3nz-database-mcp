//! PostgreSQL driver using `tokio-postgres` and `deadpool`.

use crate::config::DatabaseConfig;
use crate::database::pool::PoolMetrics;
use crate::database::result::*;
use crate::database::traits::DatabaseDriver;
use crate::error::{DatabaseError, DbResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool_postgres::{Config as DeadpoolConfig, Pool, Runtime};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tokio_postgres::NoTls;
use tokio_postgres::types::Type;
use tracing::{debug, info, instrument};

/// PostgreSQL database driver.
pub struct PostgresDriver {
    pool: Pool,
    metrics: Arc<PoolMetrics>,
    config: DatabaseConfig,
}

impl PostgresDriver {
    /// Create a new PostgreSQL driver with the given configuration.
    pub async fn new(config: DatabaseConfig) -> DbResult<Self> {
        info!(
            "Connecting to PostgreSQL: {}:{}/{}",
            config.host, config.port, config.database
        );

        let mut deadpool_config = DeadpoolConfig::new();
        deadpool_config.host = Some(config.host.clone());
        deadpool_config.port = Some(config.port);
        deadpool_config.dbname = Some(config.database.clone());
        deadpool_config.user = Some(config.username.clone());
        deadpool_config.password = Some(config.password.clone());

        let pool = deadpool_config
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| DatabaseError::ConnectionFailed(e.to_string()))?;

        // Test connection
        let _conn = pool
            .get()
            .await
            .map_err(|e| DatabaseError::ConnectionFailed(e.to_string()))?;

        info!(
            "PostgreSQL connection pool created with max size {}",
            config.pool_size
        );

        Ok(Self {
            pool,
            metrics: Arc::new(PoolMetrics::new()),
            config,
        })
    }

    /// Convert a PostgreSQL row to our Row type.
    fn convert_row(pg_row: &tokio_postgres::Row, columns: &[Column]) -> Row {
        let mut row = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            let value = Self::get_cell_value(pg_row, i, &col.data_type);
            row.insert(col.name.clone(), value);
        }
        row
    }

    /// Extract cell value from PostgreSQL row.
    fn get_cell_value(row: &tokio_postgres::Row, index: usize, data_type: &str) -> CellValue {
        // Handle NULL values
        let is_null: Option<bool> = row.try_get(index).ok();
        if is_null.is_none() {
            // Try to get as different types
            if let Ok(Some(val)) = row.try_get::<_, Option<i32>>(index) {
                return CellValue::Int(val as i64);
            }
            if let Ok(Some(val)) = row.try_get::<_, Option<i64>>(index) {
                return CellValue::Int(val);
            }
            if let Ok(Some(val)) = row.try_get::<_, Option<f64>>(index) {
                return CellValue::Float(val);
            }
            if let Ok(Some(val)) = row.try_get::<_, Option<f32>>(index) {
                return CellValue::Float(val as f64);
            }
            if let Ok(Some(val)) = row.try_get::<_, Option<bool>>(index) {
                return CellValue::Bool(val);
            }
            if let Ok(Some(val)) = row.try_get::<_, Option<String>>(index) {
                return CellValue::String(val);
            }
            // Note: rust_decimal requires additional feature in tokio-postgres
            // Decimals are handled as strings for now
            if let Ok(Some(val)) = row.try_get::<_, Option<chrono::NaiveDateTime>>(index) {
                return CellValue::DateTime(DateTime::from_naive_utc_and_offset(val, Utc));
            }
            if let Ok(Some(val)) = row.try_get::<_, Option<DateTime<Utc>>>(index) {
                return CellValue::DateTime(val);
            }
            return CellValue::Null;
        }

        // Try different types based on data_type hint
        match data_type.to_lowercase().as_str() {
            "int2" | "int4" | "int8" | "smallint" | "integer" | "bigint" => {
                if let Ok(val) = row.try_get::<_, i64>(index) {
                    return CellValue::Int(val);
                }
                if let Ok(val) = row.try_get::<_, i32>(index) {
                    return CellValue::Int(val as i64);
                }
            }
            "float4" | "float8" | "real" | "double precision" => {
                if let Ok(val) = row.try_get::<_, f64>(index) {
                    return CellValue::Float(val);
                }
            }
            "bool" | "boolean" => {
                if let Ok(val) = row.try_get::<_, bool>(index) {
                    return CellValue::Bool(val);
                }
            }
            "numeric" | "decimal" => {
                // Decimal values are retrieved as f64 or string
                if let Ok(val) = row.try_get::<_, f64>(index) {
                    return CellValue::Float(val);
                }
            }
            "timestamp" | "timestamptz" => {
                if let Ok(val) = row.try_get::<_, DateTime<Utc>>(index) {
                    return CellValue::DateTime(val);
                }
                if let Ok(val) = row.try_get::<_, chrono::NaiveDateTime>(index) {
                    return CellValue::DateTime(DateTime::from_naive_utc_and_offset(val, Utc));
                }
            }
            _ => {}
        }

        // Fallback: try as string
        if let Ok(val) = row.try_get::<_, String>(index) {
            return CellValue::String(val);
        }

        CellValue::Null
    }

    /// Get column type name from OID.
    fn type_name(ty: &Type) -> String {
        ty.name().to_string()
    }
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn is_connected(&self) -> bool {
        self.pool.status().available > 0 || self.pool.get().await.is_ok()
    }

    #[instrument(skip(self), fields(db = "postgres"))]
    async fn execute_query(&self, query: &str, limit: Option<u32>) -> DbResult<QueryResult> {
        let start = Instant::now();
        let conn = self
            .pool
            .get()
            .await
            .map_err(|_| DatabaseError::PoolExhausted)?;

        // Add LIMIT clause if specified and query is SELECT
        let modified_query = if let Some(limit) = limit {
            if query.trim().to_uppercase().starts_with("SELECT") {
                format!("{} LIMIT {}", query.trim_end_matches(';'), limit)
            } else {
                query.to_string()
            }
        } else {
            query.to_string()
        };

        debug!("Executing query: {}", modified_query);

        let stmt = conn
            .prepare(&modified_query)
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let pg_rows = conn
            .query(&stmt, &[])
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let elapsed = start.elapsed().as_millis() as u64;
        self.metrics.record_query_executed();

        if pg_rows.is_empty() {
            return Ok(QueryResult::empty());
        }

        // Get column info
        let columns: Vec<Column> = stmt
            .columns()
            .iter()
            .map(|c| Column {
                name: c.name().to_string(),
                data_type: Self::type_name(c.type_()),
                nullable: None,
            })
            .collect();

        let rows: Vec<Row> = pg_rows
            .iter()
            .map(|r| Self::convert_row(r, &columns))
            .collect();

        let truncated = limit.map(|l| rows.len() >= l as usize);

        Ok(QueryResult {
            columns,
            row_count: rows.len(),
            rows,
            execution_time_ms: elapsed,
            truncated,
        })
    }

    async fn execute_query_with_timeout(
        &self,
        query: &str,
        limit: Option<u32>,
        query_timeout: Duration,
    ) -> DbResult<QueryResult> {
        timeout(query_timeout, self.execute_query(query, limit))
            .await
            .map_err(|_| DatabaseError::Timeout(query_timeout.as_millis() as u64))?
    }

    #[instrument(skip(self))]
    async fn list_tables(&self, schema: Option<&str>) -> DbResult<Vec<TableInfo>> {
        let schema_filter = schema.unwrap_or("public");
        let query = format!(
            r#"
            SELECT
                schemaname AS schema_name,
                tablename AS table_name,
                'TABLE' AS table_type,
                n_live_tup AS row_count
            FROM pg_stat_user_tables
            WHERE schemaname = '{}'
            UNION ALL
            SELECT
                schemaname AS schema_name,
                viewname AS table_name,
                'VIEW' AS table_type,
                NULL AS row_count
            FROM pg_views
            WHERE schemaname = '{}'
            ORDER BY schema_name, table_name
            "#,
            schema_filter, schema_filter
        );

        let result = self.execute_query(&query, None).await?;

        let tables = result
            .rows
            .iter()
            .map(|row| {
                let table_type_str = row
                    .get("table_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("TABLE");

                TableInfo {
                    schema: row
                        .get("schema_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("public")
                        .to_string(),
                    name: row
                        .get("table_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    table_type: match table_type_str {
                        "VIEW" => TableType::View,
                        _ => TableType::Table,
                    },
                    row_count: row.get("row_count").and_then(|v| v.as_i64()),
                }
            })
            .collect();

        Ok(tables)
    }

    #[instrument(skip(self))]
    async fn get_table_schema(&self, table_name: &str) -> DbResult<TableSchema> {
        let parts: Vec<&str> = table_name.split('.').collect();
        let (schema, table) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("public", parts[0])
        };

        let query = format!(
            r#"
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key,
                CASE WHEN c.column_default LIKE 'nextval%' THEN true ELSE false END AS is_identity
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.table_schema, kcu.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.table_schema = pk.table_schema
                AND c.table_name = pk.table_name
                AND c.column_name = pk.column_name
            WHERE c.table_schema = '{}' AND c.table_name = '{}'
            ORDER BY c.ordinal_position
            "#,
            schema, table
        );

        let result = self.execute_query(&query, None).await?;

        if result.rows.is_empty() {
            return Err(DatabaseError::TableNotFound(table_name.to_string()));
        }

        let columns: Vec<ColumnSchema> = result
            .rows
            .iter()
            .map(|row| ColumnSchema {
                name: row
                    .get("column_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                data_type: row
                    .get("data_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                nullable: row
                    .get("is_nullable")
                    .and_then(|v| v.as_str())
                    .map(|v| v == "YES")
                    .unwrap_or(true),
                default_value: row
                    .get("column_default")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                max_length: row
                    .get("character_maximum_length")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                precision: row
                    .get("numeric_precision")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                scale: row
                    .get("numeric_scale")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                is_primary_key: row
                    .get("is_primary_key")
                    .and_then(|v| match v {
                        CellValue::Bool(b) => Some(*b),
                        _ => None,
                    })
                    .unwrap_or(false),
                is_identity: row
                    .get("is_identity")
                    .and_then(|v| match v {
                        CellValue::Bool(b) => Some(*b),
                        _ => None,
                    })
                    .unwrap_or(false),
            })
            .collect();

        // Get primary key info
        let pk_columns: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        let primary_key = if !pk_columns.is_empty() {
            Some(PrimaryKey {
                name: format!("{}_pkey", table),
                columns: pk_columns,
            })
        } else {
            None
        };

        // Get indexes
        let index_query = format!(
            r#"
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                am.amname AS index_type,
                array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = '{}' AND t.relname = '{}'
            GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname
            "#,
            schema, table
        );

        let index_result = self.execute_query(&index_query, None).await?;

        let indexes: Vec<IndexInfo> = index_result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get("index_name").and_then(|v| v.as_str())?;
                Some(IndexInfo {
                    name: name.to_string(),
                    columns: row
                        .get("columns")
                        .and_then(|v| v.as_str())
                        .map(|s| {
                            s.trim_matches(|c| c == '{' || c == '}')
                                .split(',')
                                .map(String::from)
                                .collect()
                        })
                        .unwrap_or_default(),
                    is_unique: row
                        .get("is_unique")
                        .and_then(|v| match v {
                            CellValue::Bool(b) => Some(*b),
                            _ => None,
                        })
                        .unwrap_or(false),
                    is_primary: row
                        .get("is_primary")
                        .and_then(|v| match v {
                            CellValue::Bool(b) => Some(*b),
                            _ => None,
                        })
                        .unwrap_or(false),
                    index_type: row
                        .get("index_type")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                })
            })
            .collect();

        Ok(TableSchema {
            schema: schema.to_string(),
            name: table.to_string(),
            columns,
            primary_key,
            indexes,
        })
    }

    #[instrument(skip(self))]
    async fn get_relationships(
        &self,
        table_name: Option<&str>,
    ) -> DbResult<Vec<TableRelationship>> {
        let where_clause = table_name
            .map(|t| {
                let parts: Vec<&str> = t.split('.').collect();
                if parts.len() == 2 {
                    format!(
                        "AND fk_nsp.nspname = '{}' AND fk_tbl.relname = '{}'",
                        parts[0], parts[1]
                    )
                } else {
                    format!("AND fk_tbl.relname = '{}'", parts[0])
                }
            })
            .unwrap_or_default();

        let query = format!(
            r#"
            SELECT
                conname AS constraint_name,
                fk_nsp.nspname AS from_schema,
                fk_tbl.relname AS from_table,
                fk_col.attname AS from_column,
                pk_nsp.nspname AS to_schema,
                pk_tbl.relname AS to_table,
                pk_col.attname AS to_column
            FROM pg_constraint c
            JOIN pg_class fk_tbl ON c.conrelid = fk_tbl.oid
            JOIN pg_namespace fk_nsp ON fk_tbl.relnamespace = fk_nsp.oid
            JOIN pg_class pk_tbl ON c.confrelid = pk_tbl.oid
            JOIN pg_namespace pk_nsp ON pk_tbl.relnamespace = pk_nsp.oid
            JOIN pg_attribute fk_col ON fk_col.attrelid = c.conrelid AND fk_col.attnum = ANY(c.conkey)
            JOIN pg_attribute pk_col ON pk_col.attrelid = c.confrelid AND pk_col.attnum = ANY(c.confkey)
            WHERE c.contype = 'f' {}
            ORDER BY conname
            "#,
            where_clause
        );

        let result = self.execute_query(&query, None).await?;

        let relationships = result
            .rows
            .iter()
            .map(|row| TableRelationship {
                constraint_name: row
                    .get("constraint_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                from_schema: row
                    .get("from_schema")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                from_table: row
                    .get("from_table")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                from_column: row
                    .get("from_column")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                to_schema: row
                    .get("to_schema")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                to_table: row
                    .get("to_table")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                to_column: row
                    .get("to_column")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            })
            .collect();

        Ok(relationships)
    }

    #[instrument(skip(self))]
    async fn get_execution_plan(&self, query: &str) -> DbResult<ExecutionPlan> {
        let explain_query = format!("EXPLAIN (FORMAT TEXT, ANALYZE false, COSTS true) {}", query);
        let result = self.execute_query(&explain_query, None).await?;

        let mut plan_text = String::new();
        let mut operations = Vec::new();
        let mut estimated_cost: Option<f64> = None;
        let mut estimated_rows: Option<i64> = None;

        for row in &result.rows {
            if let Some(CellValue::String(line)) = row.values().next() {
                plan_text.push_str(line);
                plan_text.push('\n');

                // Parse basic info from first line
                if estimated_cost.is_none() {
                    if let Some(cost_start) = line.find("cost=") {
                        let cost_str = &line[cost_start + 5..];
                        if let Some(cost_end) = cost_str.find("..")
                            && let Ok(cost) = cost_str[..cost_end].parse::<f64>()
                        {
                            estimated_cost = Some(cost);
                        }
                    }
                    if let Some(rows_start) = line.find("rows=") {
                        let rows_str = &line[rows_start + 5..];
                        if let Some(rows_end) = rows_str.find(' ')
                            && let Ok(rows) = rows_str[..rows_end].parse::<i64>()
                        {
                            estimated_rows = Some(rows);
                        }
                    }
                }

                // Extract operation
                let trimmed = line.trim();
                if !trimmed.is_empty() && !trimmed.starts_with("->") {
                    operations.push(PlanOperation {
                        operation: trimmed.to_string(),
                        object: None,
                        estimated_cost: None,
                        estimated_rows: None,
                        details: None,
                    });
                } else if let Some(stripped) = trimmed.strip_prefix("->") {
                    operations.push(PlanOperation {
                        operation: stripped.trim().to_string(),
                        object: None,
                        estimated_cost: None,
                        estimated_rows: None,
                        details: None,
                    });
                }
            }
        }

        Ok(ExecutionPlan {
            plan_text,
            estimated_cost,
            estimated_rows,
            operations,
            warnings: None,
        })
    }

    #[instrument(skip(self))]
    async fn get_performance_stats(&self) -> DbResult<PerformanceStats> {
        // Check if pg_stat_statements is available
        let check_query = r#"
            SELECT EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
            ) AS has_extension
        "#;

        let check_result = self.execute_query(check_query, None).await?;
        let has_extension = check_result
            .rows
            .first()
            .and_then(|r| r.get("has_extension"))
            .and_then(|v| match v {
                CellValue::Bool(b) => Some(*b),
                _ => None,
            })
            .unwrap_or(false);

        let top_queries = if has_extension {
            let query = r#"
                SELECT
                    queryid::text AS query_hash,
                    query AS query_text,
                    calls AS execution_count,
                    total_exec_time AS total_duration_ms,
                    mean_exec_time AS avg_duration_ms,
                    total_exec_time AS total_cpu_ms,
                    (shared_blks_hit + shared_blks_read)::float / NULLIF(calls, 0) AS avg_logical_reads
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC
                LIMIT 10
            "#;

            let result = self.execute_query(query, None).await?;

            result
                .rows
                .iter()
                .map(|row| QueryStats {
                    query_hash: row
                        .get("query_hash")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    query_text: row
                        .get("query_text")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    execution_count: row
                        .get("execution_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    total_duration_ms: row
                        .get("total_duration_ms")
                        .and_then(|v| match v {
                            CellValue::Float(f) => Some(*f),
                            _ => None,
                        })
                        .unwrap_or(0.0),
                    avg_duration_ms: row
                        .get("avg_duration_ms")
                        .and_then(|v| match v {
                            CellValue::Float(f) => Some(*f),
                            _ => None,
                        })
                        .unwrap_or(0.0),
                    total_cpu_ms: row.get("total_cpu_ms").and_then(|v| match v {
                        CellValue::Float(f) => Some(*f),
                        _ => None,
                    }),
                    avg_logical_reads: row.get("avg_logical_reads").and_then(|v| match v {
                        CellValue::Float(f) => Some(*f),
                        _ => None,
                    }),
                    last_execution: None,
                })
                .collect()
        } else {
            // Fallback: get from pg_stat_user_tables
            vec![]
        };

        // Get cache stats
        let cache_query = r#"
            SELECT
                CASE WHEN blks_hit + blks_read = 0 THEN 0
                     ELSE blks_hit::float / (blks_hit + blks_read) * 100
                END AS cache_hit_ratio,
                pg_size_pretty(pg_database_size(current_database())) AS db_size
            FROM pg_stat_database
            WHERE datname = current_database()
        "#;

        let cache_result = self.execute_query(cache_query, None).await?;
        let cache_stats = cache_result.rows.first().map(|row| CacheStats {
            cache_hit_ratio: row
                .get("cache_hit_ratio")
                .and_then(|v| match v {
                    CellValue::Float(f) => Some(*f),
                    _ => None,
                })
                .unwrap_or(0.0),
            buffer_pool_size_mb: 0.0, // Would need additional query
        });

        Ok(PerformanceStats {
            top_queries,
            wait_stats: None,
            cache_stats,
        })
    }

    #[instrument(skip(self))]
    async fn get_index_usage(&self, table_name: Option<&str>) -> DbResult<Vec<IndexUsage>> {
        let where_clause = table_name
            .map(|t| format!("AND relname = '{}'", t))
            .unwrap_or_default();

        let query = format!(
            r#"
            SELECT
                schemaname AS schema_name,
                relname AS table_name,
                indexrelname AS index_name,
                idx_scan AS user_seeks,
                idx_tup_read AS user_scans,
                idx_tup_fetch AS user_lookups,
                0 AS user_updates,
                last_idx_scan AS last_user_seek
            FROM pg_stat_user_indexes
            WHERE 1=1 {}
            ORDER BY idx_scan DESC
            LIMIT 50
            "#,
            where_clause
        );

        let result = self.execute_query(&query, None).await?;

        let usage: Vec<IndexUsage> = result
            .rows
            .iter()
            .map(|row| IndexUsage {
                schema: row
                    .get("schema_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                table_name: row
                    .get("table_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                index_name: row
                    .get("index_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                user_seeks: row.get("user_seeks").and_then(|v| v.as_i64()).unwrap_or(0),
                user_scans: row.get("user_scans").and_then(|v| v.as_i64()).unwrap_or(0),
                user_lookups: row
                    .get("user_lookups")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0),
                user_updates: row
                    .get("user_updates")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0),
                last_user_seek: None,
                last_user_scan: None,
            })
            .collect();

        Ok(usage)
    }

    #[instrument(skip(self))]
    async fn find_blocking_queries(&self) -> DbResult<Vec<BlockingQuery>> {
        let query = r#"
            SELECT
                blocked.pid AS session_id,
                blocking.pid AS blocking_session_id,
                blocked.wait_event_type AS wait_type,
                EXTRACT(EPOCH FROM (now() - blocked.query_start)) * 1000 AS wait_time_ms,
                blocked.wait_event AS wait_resource,
                blocked.query AS query_text,
                blocked.datname AS database_name
            FROM pg_stat_activity blocked
            JOIN pg_stat_activity blocking ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
            WHERE blocked.pid != blocking.pid
            ORDER BY wait_time_ms DESC
        "#;

        let result = self.execute_query(query, None).await?;

        let blocking: Vec<BlockingQuery> = result
            .rows
            .iter()
            .map(|row| BlockingQuery {
                session_id: row.get("session_id").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
                blocking_session_id: row
                    .get("blocking_session_id")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                wait_type: row
                    .get("wait_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                wait_time_ms: row
                    .get("wait_time_ms")
                    .and_then(|v| match v {
                        CellValue::Float(f) => Some(*f as i64),
                        CellValue::Int(i) => Some(*i),
                        _ => None,
                    })
                    .unwrap_or(0),
                wait_resource: row
                    .get("wait_resource")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                query_text: row
                    .get("query_text")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                database_name: row
                    .get("database_name")
                    .and_then(|v| v.as_str())
                    .map(String::from),
            })
            .collect();

        Ok(blocking)
    }

    async fn list_databases(&self) -> DbResult<Vec<String>> {
        let query = r#"
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
            AND datname NOT IN ('postgres')
            ORDER BY datname
        "#;

        let result = self.execute_query(query, None).await?;

        let databases: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| {
                row.get("datname")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .collect();

        Ok(databases)
    }
}
