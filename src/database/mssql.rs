//! MSSQL driver using `tiberius` and `bb8` connection pool.

use crate::config::DatabaseConfig;
use crate::database::pool::PoolMetrics;
use crate::database::result::*;
use crate::database::traits::DatabaseDriver;
use crate::error::{DatabaseError, DbResult};
use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tiberius::{AuthMethod, Config, EncryptionLevel, Row as TiberiusRow};
use tokio::time::timeout;
use tracing::{debug, info, instrument};

/// Validate a SQL identifier (schema name, table name, column name) to prevent SQL injection.
///
/// Only allows alphanumeric characters and underscores. This is a whitelist approach
/// that prevents SQL injection by rejecting any potentially dangerous input.
///
/// # Arguments
/// * `name` - The identifier to validate
///
/// # Returns
/// * `Ok(&str)` - The validated identifier if it passes validation
/// * `Err(DatabaseError)` - If the identifier contains invalid characters
fn validate_identifier(name: &str) -> DbResult<&str> {
    if name.is_empty() {
        return Err(DatabaseError::QueryFailed(
            "Identifier cannot be empty".to_string(),
        ));
    }

    // Only allow alphanumeric characters and underscores
    // This is a strict whitelist approach for SQL injection prevention
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(name)
    } else {
        Err(DatabaseError::QueryFailed(format!(
            "Invalid identifier '{}': only alphanumeric characters and underscores are allowed",
            name
        )))
    }
}

/// MSSQL database driver.
pub struct MssqlDriver {
    pool: Pool<ConnectionManager>,
    metrics: Arc<PoolMetrics>,
    config: DatabaseConfig,
}

impl MssqlDriver {
    /// Creates a new MSSQL driver.
    ///
    /// Establishes a connection pool and verifies connectivity with a test query.
    /// Certificates are trusted by default; encryption level is server-decided.
    pub async fn new(config: DatabaseConfig) -> DbResult<Self> {
        info!(
            "Connecting to MSSQL: {}:{}/{}",
            config.host, config.port, config.database
        );

        let mut tiberius_config = Config::new();
        tiberius_config.host(&config.host);
        tiberius_config.port(config.port);
        tiberius_config.database(&config.database);
        tiberius_config.authentication(AuthMethod::sql_server(&config.username, &config.password));

        // TLS configuration - trust all certs and let server decide encryption
        tiberius_config.trust_cert();
        tiberius_config.encryption(EncryptionLevel::On);

        let manager = ConnectionManager::new(tiberius_config);
        let pool = Pool::builder()
            .max_size(config.pool_size)
            .connection_timeout(Duration::from_secs(30)) // 30 sec connection timeout for remote
            .build(manager)
            .await
            .map_err(|e| DatabaseError::ConnectionFailed(e.to_string()))?;

        info!(
            "MSSQL connection pool created with size {}",
            config.pool_size
        );

        // Warm up the pool with a test connection
        info!("Testing database connection...");
        {
            let mut conn = pool.get().await.map_err(|e| {
                DatabaseError::ConnectionFailed(format!("Failed to get test connection: {}", e))
            })?;

            // Run a simple query to verify connection works
            conn.query("SELECT 1", &[]).await.map_err(|e| {
                DatabaseError::ConnectionFailed(format!("Test query failed: {}", e))
            })?;
        }
        info!("Database connection verified successfully");

        Ok(Self {
            pool,
            metrics: Arc::new(PoolMetrics::new()),
            config,
        })
    }

    /// Get a connection from the pool with retry logic.
    async fn get_connection(&self) -> DbResult<bb8::PooledConnection<'_, ConnectionManager>> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 200;

        for attempt in 1..=MAX_RETRIES {
            match tokio::time::timeout(Duration::from_secs(10), self.pool.get()).await {
                Ok(Ok(conn)) => return Ok(conn),
                Ok(Err(e)) => {
                    if attempt == MAX_RETRIES {
                        return Err(DatabaseError::PoolExhausted);
                    }
                    debug!(
                        "Connection pool attempt {} failed: {}, retrying...",
                        attempt, e
                    );
                }
                Err(_) => {
                    if attempt == MAX_RETRIES {
                        return Err(DatabaseError::Timeout(10000));
                    }
                    debug!("Connection pool attempt {} timed out, retrying...", attempt);
                }
            }
            tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS * attempt as u64)).await;
        }
        Err(DatabaseError::PoolExhausted)
    }

    /// Convert Tiberius row to our Row type.
    fn convert_row(tiberius_row: &TiberiusRow, columns: &[Column]) -> Row {
        let mut row = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            let value = Self::get_cell_value(tiberius_row, i);
            row.insert(col.name.clone(), value);
        }
        row
    }

    /// Extract cell value from Tiberius row.
    fn get_cell_value(row: &TiberiusRow, index: usize) -> CellValue {
        // Try different types
        if let Some(val) = row.try_get::<i32, _>(index).ok().flatten() {
            return CellValue::Int(val as i64);
        }
        if let Some(val) = row.try_get::<i64, _>(index).ok().flatten() {
            return CellValue::Int(val);
        }
        if let Some(val) = row.try_get::<f64, _>(index).ok().flatten() {
            return CellValue::Float(val);
        }
        if let Some(val) = row.try_get::<f32, _>(index).ok().flatten() {
            return CellValue::Float(val as f64);
        }
        if let Some(val) = row.try_get::<bool, _>(index).ok().flatten() {
            return CellValue::Bool(val);
        }
        if let Some(val) = row.try_get::<&str, _>(index).ok().flatten() {
            return CellValue::String(val.to_string());
        }
        if let Some(val) = row
            .try_get::<rust_decimal::Decimal, _>(index)
            .ok()
            .flatten()
        {
            return CellValue::Decimal(val);
        }
        if let Some(val) = row
            .try_get::<chrono::NaiveDateTime, _>(index)
            .ok()
            .flatten()
        {
            return CellValue::DateTime(DateTime::from_naive_utc_and_offset(val, Utc));
        }

        CellValue::Null
    }
}

#[async_trait]
impl DatabaseDriver for MssqlDriver {
    fn name(&self) -> &'static str {
        "mssql"
    }

    async fn is_connected(&self) -> bool {
        self.pool.state().connections > 0
    }

    #[instrument(skip(self), fields(db = "mssql"))]
    async fn execute_query(&self, query: &str, limit: Option<u32>) -> DbResult<QueryResult> {
        let start = Instant::now();
        let mut conn = self.get_connection().await?;

        // Add TOP clause if limit is specified and query is SELECT
        let modified_query = if let Some(limit) = limit {
            if query.trim().to_uppercase().starts_with("SELECT") {
                query.replacen("SELECT", &format!("SELECT TOP {}", limit), 1)
            } else {
                query.to_string()
            }
        } else {
            query.to_string()
        };

        debug!("Executing query: {}", modified_query);

        let stream = conn
            .query(&modified_query, &[])
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let rows_result = stream
            .into_results()
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let elapsed = start.elapsed().as_millis() as u64;
        self.metrics.record_query_executed();

        // Process first result set
        if let Some(rows) = rows_result.first() {
            if rows.is_empty() {
                return Ok(QueryResult::empty());
            }

            // Get column info from first row
            let columns: Vec<Column> = rows
                .first()
                .map(|r| {
                    r.columns()
                        .iter()
                        .map(|c| Column {
                            name: c.name().to_string(),
                            data_type: format!("{:?}", c.column_type()),
                            nullable: None, // Tiberius doesn't expose nullable info directly
                        })
                        .collect()
                })
                .unwrap_or_default();

            let result_rows: Vec<Row> = rows
                .iter()
                .map(|r| Self::convert_row(r, &columns))
                .collect();

            let truncated = limit.map(|l| result_rows.len() >= l as usize);

            return Ok(QueryResult {
                columns,
                row_count: result_rows.len(),
                rows: result_rows,
                execution_time_ms: elapsed,
                truncated,
            });
        }

        Ok(QueryResult::empty())
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
        let schema_filter = schema.unwrap_or("dbo");
        // Validate schema name to prevent SQL injection
        let schema_filter = validate_identifier(schema_filter)?;

        let query = format!(
            r#"
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                CASE WHEN t.type = 'U' THEN 'TABLE'
                     WHEN t.type = 'V' THEN 'VIEW'
                     ELSE 'OTHER' END AS table_type,
                p.rows AS row_count
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
            WHERE s.name = '{}'
            UNION ALL
            SELECT
                s.name AS schema_name,
                v.name AS table_name,
                'VIEW' AS table_type,
                NULL AS row_count
            FROM sys.views v
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = '{}'
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
                        .unwrap_or("dbo")
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
            ("dbo", parts[0])
        };

        // Validate identifiers to prevent SQL injection
        let schema = validate_identifier(schema)?;
        let table = validate_identifier(table)?;

        let query = format!(
            r#"
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
                COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS is_identity
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                AND c.TABLE_NAME = pk.TABLE_NAME
                AND c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = '{}' AND c.TABLE_NAME = '{}'
            ORDER BY c.ORDINAL_POSITION
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
                    .get("COLUMN_NAME")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                data_type: row
                    .get("DATA_TYPE")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                nullable: row
                    .get("IS_NULLABLE")
                    .and_then(|v| v.as_str())
                    .map(|v| v == "YES")
                    .unwrap_or(true),
                default_value: row
                    .get("COLUMN_DEFAULT")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                max_length: row
                    .get("CHARACTER_MAXIMUM_LENGTH")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                precision: row
                    .get("NUMERIC_PRECISION")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                scale: row
                    .get("NUMERIC_SCALE")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                is_primary_key: row
                    .get("is_primary_key")
                    .and_then(|v| v.as_i64())
                    .map(|v| v == 1)
                    .unwrap_or(false),
                is_identity: row
                    .get("is_identity")
                    .and_then(|v| v.as_i64())
                    .map(|v| v == 1)
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
                name: format!("PK_{}", table),
                columns: pk_columns,
            })
        } else {
            None
        };

        // Get indexes
        let index_query = format!(
            r#"
            SELECT
                i.name AS index_name,
                i.is_unique,
                i.is_primary_key,
                i.type_desc,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = OBJECT_ID('{}.{}')
            GROUP BY i.name, i.is_unique, i.is_primary_key, i.type_desc
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
                        .map(|s| s.split(", ").map(String::from).collect())
                        .unwrap_or_default(),
                    is_unique: row
                        .get("is_unique")
                        .and_then(|v| v.as_i64())
                        .map(|v| v == 1)
                        .unwrap_or(false),
                    is_primary: row
                        .get("is_primary_key")
                        .and_then(|v| v.as_i64())
                        .map(|v| v == 1)
                        .unwrap_or(false),
                    index_type: row
                        .get("type_desc")
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
        let where_clause = match table_name {
            Some(t) => {
                let parts: Vec<&str> = t.split('.').collect();
                if parts.len() == 2 {
                    // Validate both schema and table name
                    let schema = validate_identifier(parts[0])?;
                    let table = validate_identifier(parts[1])?;
                    format!(
                        "AND (fk_schema.name = '{}' AND fk_tab.name = '{}')",
                        schema, table
                    )
                } else {
                    // Validate table name
                    let table = validate_identifier(parts[0])?;
                    format!("AND fk_tab.name = '{}'", table)
                }
            }
            None => String::new(),
        };

        let query = format!(
            r#"
            SELECT
                fk.name AS constraint_name,
                fk_schema.name AS from_schema,
                fk_tab.name AS from_table,
                fk_col.name AS from_column,
                pk_schema.name AS to_schema,
                pk_tab.name AS to_table,
                pk_col.name AS to_column
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables fk_tab ON fkc.parent_object_id = fk_tab.object_id
            INNER JOIN sys.schemas fk_schema ON fk_tab.schema_id = fk_schema.schema_id
            INNER JOIN sys.columns fk_col ON fkc.parent_object_id = fk_col.object_id AND fkc.parent_column_id = fk_col.column_id
            INNER JOIN sys.tables pk_tab ON fkc.referenced_object_id = pk_tab.object_id
            INNER JOIN sys.schemas pk_schema ON pk_tab.schema_id = pk_schema.schema_id
            INNER JOIN sys.columns pk_col ON fkc.referenced_object_id = pk_col.object_id AND fkc.referenced_column_id = pk_col.column_id
            WHERE 1=1 {}
            ORDER BY fk.name
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
        let mut conn = self.get_connection().await?;

        // Enable showplan
        conn.execute("SET SHOWPLAN_TEXT ON", &[])
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let stream = conn
            .query(query, &[])
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let rows = stream
            .into_results()
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        // Disable showplan
        conn.execute("SET SHOWPLAN_TEXT OFF", &[])
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let mut plan_text = String::new();
        let mut operations = Vec::new();

        for result_set in rows {
            for row in result_set {
                if let Some(text) = row.try_get::<&str, _>(0).ok().flatten() {
                    plan_text.push_str(text);
                    plan_text.push('\n');

                    // Parse basic operation
                    if text.contains("--") {
                        operations.push(PlanOperation {
                            operation: text.trim().to_string(),
                            object: None,
                            estimated_cost: None,
                            estimated_rows: None,
                            details: None,
                        });
                    }
                }
            }
        }

        Ok(ExecutionPlan {
            plan_text,
            estimated_cost: None,
            estimated_rows: None,
            operations,
            warnings: None,
        })
    }

    #[instrument(skip(self))]
    async fn get_performance_stats(&self) -> DbResult<PerformanceStats> {
        // Use a single connection for all queries to avoid pool exhaustion
        let mut conn = self.get_connection().await?;
        let start = Instant::now();

        // Get top queries by total duration
        let query = r#"
            SELECT TOP 10
                qs.execution_count,
                qs.total_elapsed_time / 1000.0 AS total_duration_ms,
                (qs.total_elapsed_time / NULLIF(qs.execution_count, 0)) / 1000.0 AS avg_duration_ms,
                qs.total_worker_time / 1000.0 AS total_cpu_ms,
                qs.total_logical_reads / NULLIF(qs.execution_count, 0) AS avg_logical_reads,
                LEFT(qt.text, 200) AS query_text
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
            WHERE qt.text IS NOT NULL
            ORDER BY qs.total_elapsed_time DESC
        "#;

        let stream = conn
            .query(query, &[])
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let rows_result = stream
            .into_results()
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let mut top_queries: Vec<QueryStats> = Vec::new();

        if let Some(rows) = rows_result.first() {
            for row in rows {
                top_queries.push(QueryStats {
                    query_hash: None,
                    query_text: row
                        .try_get::<&str, _>("query_text")
                        .ok()
                        .flatten()
                        .unwrap_or("")
                        .to_string(),
                    execution_count: row
                        .try_get::<i64, _>("execution_count")
                        .ok()
                        .flatten()
                        .or_else(|| {
                            row.try_get::<i32, _>("execution_count")
                                .ok()
                                .flatten()
                                .map(|v| v as i64)
                        })
                        .unwrap_or(0),
                    total_duration_ms: row
                        .try_get::<f64, _>("total_duration_ms")
                        .ok()
                        .flatten()
                        .or_else(|| {
                            row.try_get::<rust_decimal::Decimal, _>("total_duration_ms")
                                .ok()
                                .flatten()
                                .and_then(|d| d.to_string().parse().ok())
                        })
                        .unwrap_or(0.0),
                    avg_duration_ms: row
                        .try_get::<f64, _>("avg_duration_ms")
                        .ok()
                        .flatten()
                        .or_else(|| {
                            row.try_get::<rust_decimal::Decimal, _>("avg_duration_ms")
                                .ok()
                                .flatten()
                                .and_then(|d| d.to_string().parse().ok())
                        })
                        .unwrap_or(0.0),
                    total_cpu_ms: row
                        .try_get::<f64, _>("total_cpu_ms")
                        .ok()
                        .flatten()
                        .or_else(|| {
                            row.try_get::<rust_decimal::Decimal, _>("total_cpu_ms")
                                .ok()
                                .flatten()
                                .and_then(|d| d.to_string().parse().ok())
                        }),
                    avg_logical_reads: row
                        .try_get::<i64, _>("avg_logical_reads")
                        .ok()
                        .flatten()
                        .map(|v| v as f64),
                    last_execution: None,
                });
            }
        }

        // Get wait stats using the same connection
        let wait_query = r#"
            SELECT TOP 10
                wait_type,
                wait_time_ms,
                waiting_tasks_count
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT LIKE '%SLEEP%'
            AND wait_type NOT LIKE '%IDLE%'
            AND wait_type NOT LIKE '%QUEUE%'
            AND wait_time_ms > 0
            ORDER BY wait_time_ms DESC
        "#;

        let wait_stream = conn
            .query(wait_query, &[])
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let wait_rows_result = wait_stream
            .into_results()
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?;

        let mut wait_stats: Vec<WaitStats> = Vec::new();

        if let Some(rows) = wait_rows_result.first() {
            for row in rows {
                wait_stats.push(WaitStats {
                    wait_type: row
                        .try_get::<&str, _>("wait_type")
                        .ok()
                        .flatten()
                        .unwrap_or("")
                        .to_string(),
                    wait_time_ms: row
                        .try_get::<i64, _>("wait_time_ms")
                        .ok()
                        .flatten()
                        .unwrap_or(0) as f64,
                    wait_count: row
                        .try_get::<i64, _>("waiting_tasks_count")
                        .ok()
                        .flatten()
                        .unwrap_or(0),
                });
            }
        }

        debug!("Performance stats collected in {:?}", start.elapsed());

        Ok(PerformanceStats {
            top_queries,
            wait_stats: Some(wait_stats),
            cache_stats: None,
        })
    }

    #[instrument(skip(self))]
    async fn get_index_usage(&self, table_name: Option<&str>) -> DbResult<Vec<IndexUsage>> {
        let where_clause = match table_name {
            Some(t) => {
                // Validate table name to prevent SQL injection
                let table = validate_identifier(t)?;
                format!("AND OBJECT_NAME(i.object_id) = '{}'", table)
            }
            None => String::new(),
        };

        let query = format!(
            r#"
            SELECT
                SCHEMA_NAME(t.schema_id) AS schema_name,
                OBJECT_NAME(i.object_id) AS table_name,
                i.name AS index_name,
                ISNULL(us.user_seeks, 0) AS user_seeks,
                ISNULL(us.user_scans, 0) AS user_scans,
                ISNULL(us.user_lookups, 0) AS user_lookups,
                ISNULL(us.user_updates, 0) AS user_updates,
                us.last_user_seek,
                us.last_user_scan
            FROM sys.indexes i
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            LEFT JOIN sys.dm_db_index_usage_stats us
                ON i.object_id = us.object_id AND i.index_id = us.index_id
            WHERE i.name IS NOT NULL {}
            ORDER BY ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) DESC
            "#,
            where_clause
        );

        let result = self.execute_query(&query, Some(50)).await?;

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
                r.session_id,
                r.blocking_session_id,
                r.wait_type,
                r.wait_time AS wait_time_ms,
                r.wait_resource,
                t.text AS query_text,
                DB_NAME(r.database_id) AS database_name
            FROM sys.dm_exec_requests r
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
            WHERE r.blocking_session_id <> 0
            OR r.session_id IN (SELECT DISTINCT blocking_session_id FROM sys.dm_exec_requests WHERE blocking_session_id <> 0)
            ORDER BY r.wait_time DESC
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
                    .and_then(|v| v.as_i64())
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
            SELECT name
            FROM sys.databases
            WHERE state_desc = 'ONLINE'
            AND name NOT IN ('master', 'tempdb', 'model', 'msdb')
            ORDER BY name
        "#;

        let result = self.execute_query(query, None).await?;

        let databases: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| row.get("name").and_then(|v| v.as_str()).map(String::from))
            .collect();

        Ok(databases)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_valid() {
        // Valid identifiers
        assert!(validate_identifier("users").is_ok());
        assert!(validate_identifier("dbo").is_ok());
        assert!(validate_identifier("my_table").is_ok());
        assert!(validate_identifier("Table1").is_ok());
        assert!(validate_identifier("_underscore").is_ok());
        assert!(validate_identifier("UPPERCASE").is_ok());
        assert!(validate_identifier("MixedCase123").is_ok());
    }

    #[test]
    fn test_validate_identifier_invalid() {
        // Invalid identifiers - SQL injection attempts
        assert!(validate_identifier("users; DROP TABLE users").is_err());
        assert!(validate_identifier("users'--").is_err());
        assert!(validate_identifier("users OR 1=1").is_err());
        assert!(validate_identifier("table.name").is_err());
        assert!(validate_identifier("name with spaces").is_err());
        assert!(validate_identifier("special@char").is_err());
        assert!(validate_identifier("quote'test").is_err());
        assert!(validate_identifier("double\"quote").is_err());
        assert!(validate_identifier("").is_err()); // Empty string
    }

    #[test]
    fn test_validate_identifier_unicode() {
        // Unicode characters should be rejected (only ASCII alphanumeric + underscore)
        assert!(validate_identifier("tablo_adı").is_err());
        assert!(validate_identifier("表").is_err());
        assert!(validate_identifier("таблица").is_err());
    }

    #[test]
    fn test_validate_identifier_boundary() {
        // Boundary cases
        assert!(validate_identifier("a").is_ok()); // Single char
        assert!(validate_identifier("_").is_ok()); // Just underscore
        assert!(validate_identifier("1").is_ok()); // Just number
        assert!(validate_identifier("a_b_c_d_e").is_ok()); // Multiple underscores
    }
}
