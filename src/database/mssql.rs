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

/// Map a TDS wire type to the SQL Server type name users know.
///
/// Nullable numeric columns arrive as variable-width wire types (`Intn`,
/// `Floatn`, ...), so the exact declared width is not always recoverable.
fn column_type_name(t: tiberius::ColumnType) -> &'static str {
    use tiberius::ColumnType as T;

    match t {
        T::Null => "null",
        T::Bit | T::Bitn => "bit",
        T::Int1 => "tinyint",
        T::Int2 => "smallint",
        T::Int4 | T::Intn => "int",
        T::Int8 => "bigint",
        T::Float4 => "real",
        T::Float8 | T::Floatn => "float",
        T::Money => "money",
        T::Money4 => "smallmoney",
        T::Decimaln => "decimal",
        T::Numericn => "numeric",
        T::Guid => "uniqueidentifier",
        T::Datetime | T::Datetime4 | T::Datetimen => "datetime",
        T::Datetime2 => "datetime2",
        T::DatetimeOffsetn => "datetimeoffset",
        T::Daten => "date",
        T::Timen => "time",
        T::BigChar => "char",
        T::BigVarChar => "varchar",
        T::NChar => "nchar",
        T::NVarchar => "nvarchar",
        T::Text => "text",
        T::NText => "ntext",
        T::BigBinary => "binary",
        T::BigVarBin => "varbinary",
        T::Image => "image",
        T::Xml => "xml",
        T::Udt => "udt",
        T::SSVariant => "sql_variant",
    }
}

/// Format raw bytes as an uppercase SQL hex literal (`0x4D65...`).
fn to_hex_literal(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        let _ = write!(s, "{b:02X}");
    }
    s
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

    /// Inject `TOP (n)` into a SELECT statement.
    ///
    /// Case-insensitive, honors an optional DISTINCT/ALL qualifier (TOP must
    /// come after it in T-SQL) and leaves the query untouched when it already
    /// has TOP or does not start with SELECT (e.g. CTEs); those cases are
    /// limited client-side in `execute_query` instead.
    fn apply_top(query: &str, n: u32) -> String {
        let trimmed = query.trim_start();
        if !Self::starts_with_keyword(trimmed, "SELECT") {
            return query.to_string();
        }

        // Skip an optional DISTINCT/ALL qualifier; TOP must come after it.
        let mut insert_at = 6;
        let rest = trimmed[6..].trim_start();
        for kw in ["DISTINCT", "ALL"] {
            if Self::starts_with_keyword(rest, kw) {
                insert_at = trimmed.len() - rest.len() + kw.len();
                break;
            }
        }

        // Respect an existing TOP instead of injecting a second one.
        if Self::starts_with_keyword(trimmed[insert_at..].trim_start(), "TOP") {
            return query.to_string();
        }

        format!(
            "{} TOP ({}) {}",
            &trimmed[..insert_at],
            n,
            trimmed[insert_at..].trim_start()
        )
    }

    /// True when `s` starts with the ASCII keyword `kw` (case-insensitive)
    /// followed by a word boundary.
    fn starts_with_keyword(s: &str, kw: &str) -> bool {
        s.get(..kw.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(kw))
            && s[kw.len()..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_alphanumeric() && c != '_')
    }

    /// Read a float column from a SHOWPLAN_ALL row, tolerating f32 and f64.
    fn get_plan_float(row: &TiberiusRow, col: &str) -> Option<f64> {
        row.try_get::<f64, _>(col)
            .ok()
            .flatten()
            .or_else(|| row.try_get::<f32, _>(col).ok().flatten().map(f64::from))
    }

    /// Run `query` between a `SET <option> ON`/`OFF` pair on one connection.
    ///
    /// The statements go through `simple_query`: tiberius runs parameterized
    /// queries through sp_executesql, where SET options are scoped to that
    /// call and reset immediately after it returns. OFF is attempted on every
    /// path so a plan-mode connection can never travel back into the pool.
    async fn run_with_session_option(
        &self,
        option: &str,
        query: &str,
    ) -> DbResult<Vec<Vec<TiberiusRow>>> {
        let mut conn = self.get_connection().await?;

        Self::collect_simple_query(&mut conn, format!("SET {option} ON")).await?;

        // OFF must run even when the query fails, so the outcome is inspected
        // only after the session is restored.
        let plan_result = Self::collect_simple_query(&mut conn, query.to_string()).await;
        let off_result = Self::collect_simple_query(&mut conn, format!("SET {option} OFF")).await;

        let result_sets = plan_result?;
        off_result?;
        Ok(result_sets)
    }

    /// Run one raw batch on the connection and collect every result set.
    async fn collect_simple_query(
        conn: &mut bb8::PooledConnection<'_, ConnectionManager>,
        sql: String,
    ) -> DbResult<Vec<Vec<TiberiusRow>>> {
        conn.simple_query(sql)
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))?
            .into_results()
            .await
            .map_err(|e| DatabaseError::QueryFailed(e.to_string()))
    }

    /// Build an `ExecutionPlan` from SHOWPLAN_ALL or STATISTICS PROFILE rows.
    ///
    /// Both modes share the same tabular layout; STATISTICS PROFILE prefixes
    /// actual `Rows`/`Executes` columns and interleaves the query's own result
    /// sets, which are skipped here because they carry no `StmtText` column.
    fn parse_plan_rows(result_sets: &[Vec<TiberiusRow>], actual: bool) -> ExecutionPlan {
        let mut plan_text = String::new();
        let mut operations = Vec::new();
        let mut estimated_cost = None;
        let mut estimated_rows = None;
        let mut actual_rows = None;
        let mut warnings: Vec<String> = Vec::new();
        let mut statement_seen = false;

        for result_set in result_sets {
            for row in result_set {
                let Some(stmt) = row.try_get::<&str, _>("StmtText").ok().flatten() else {
                    continue;
                };
                plan_text.push_str(stmt);
                plan_text.push('\n');

                let cost = Self::get_plan_float(row, "TotalSubtreeCost");
                let rows_est = Self::get_plan_float(row, "EstimateRows").map(|r| r.round() as i64);
                let rows_act = if actual {
                    row.try_get::<i64, _>("Rows").ok().flatten()
                } else {
                    None
                };

                // The first row is the statement itself and carries the whole
                // plan's cost; the remaining rows are operator nodes.
                if !statement_seen {
                    statement_seen = true;
                    estimated_cost = cost;
                    estimated_rows = rows_est;
                    actual_rows = rows_act;
                    continue;
                }

                let operation = row
                    .try_get::<&str, _>("PhysicalOp")
                    .ok()
                    .flatten()
                    .unwrap_or(stmt)
                    .trim();
                operations.push(PlanOperation {
                    operation: operation.to_string(),
                    object: None,
                    estimated_cost: cost,
                    estimated_rows: rows_est,
                    actual_rows: rows_act,
                    details: row
                        .try_get::<&str, _>("Argument")
                        .ok()
                        .flatten()
                        .map(|s| s.trim().to_string()),
                });

                if let Some(w) = row.try_get::<&str, _>("Warnings").ok().flatten()
                    && !w.trim().is_empty()
                {
                    warnings.push(format!("{}: {}", operation, w.trim()));
                }
            }
        }

        ExecutionPlan {
            plan_text,
            estimated_cost,
            estimated_rows,
            actual_rows,
            operations,
            warnings: if warnings.is_empty() {
                None
            } else {
                Some(warnings)
            },
        }
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
    ///
    /// Tiberius maps every SQL Server type to exactly one Rust type, so each
    /// type the server can send must be tried here explicitly; an unmatched
    /// type would silently come back as NULL.
    fn get_cell_value(row: &TiberiusRow, index: usize) -> CellValue {
        // Probes run in rough frequency order: every miss costs one enum
        // variant check per cell, so the common types come first.
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
        if let Some(val) = row.try_get::<u8, _>(index).ok().flatten() {
            // tinyint
            return CellValue::Int(val as i64);
        }
        if let Some(val) = row.try_get::<i16, _>(index).ok().flatten() {
            // smallint
            return CellValue::Int(val as i64);
        }
        if let Some(val) = row.try_get::<uuid::Uuid, _>(index).ok().flatten() {
            // uniqueidentifier
            return CellValue::String(val.to_string());
        }
        // NaiveDateTime must be tried before DateTime<Utc>: both match
        // datetime2, but only DateTime<Utc> matches datetimeoffset.
        if let Some(val) = row
            .try_get::<chrono::NaiveDateTime, _>(index)
            .ok()
            .flatten()
        {
            return CellValue::DateTime(DateTime::from_naive_utc_and_offset(val, Utc));
        }
        if let Some(val) = row.try_get::<DateTime<Utc>, _>(index).ok().flatten() {
            // datetimeoffset (normalized to UTC)
            return CellValue::DateTime(val);
        }
        if let Some(val) = row.try_get::<chrono::NaiveDate, _>(index).ok().flatten() {
            // date
            return CellValue::String(val.format("%Y-%m-%d").to_string());
        }
        if let Some(val) = row.try_get::<chrono::NaiveTime, _>(index).ok().flatten() {
            // time
            return CellValue::String(val.format("%H:%M:%S%.f").to_string());
        }
        if let Some(val) = row.try_get::<&[u8], _>(index).ok().flatten() {
            // binary / varbinary, rendered as a SQL hex literal
            return CellValue::String(to_hex_literal(val));
        }
        if let Some(val) = row
            .try_get::<&tiberius::xml::XmlData, _>(index)
            .ok()
            .flatten()
        {
            return CellValue::String(val.as_ref().to_string());
        }

        CellValue::Null
    }
}

#[async_trait]
impl DatabaseDriver for MssqlDriver {
    fn name(&self) -> &'static str {
        "mssql"
    }

    fn quote_qualified(&self, schema: &str, table: &str) -> String {
        format!("[{schema}].[{table}]")
    }

    async fn is_connected(&self) -> bool {
        self.pool.state().connections > 0
    }

    #[instrument(skip(self), fields(db = "mssql"))]
    async fn execute_query(&self, query: &str, limit: Option<u32>) -> DbResult<QueryResult> {
        let start = Instant::now();
        let mut conn = self.get_connection().await?;

        // Fetch one row beyond the limit so truncation can be detected reliably;
        // the extra row is dropped below before returning.
        let modified_query = match limit {
            Some(l) => Self::apply_top(query, l.saturating_add(1)),
            None => query.to_string(),
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
                            data_type: column_type_name(c.column_type()).to_string(),
                            nullable: None, // Tiberius doesn't expose nullable info directly
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut result_rows: Vec<Row> = rows
                .iter()
                .map(|r| Self::convert_row(r, &columns))
                .collect();

            let truncated = limit.map(|l| {
                let l = l as usize;
                let over = result_rows.len() > l;
                result_rows.truncate(l);
                over
            });

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

        // Get indexes. Column names are aggregated with FOR XML PATH instead of
        // STRING_AGG so this also works on databases with compatibility level
        // below 140 (SQL Server 2017), still common in the field.
        let index_query = format!(
            r#"
            SELECT
                i.name AS index_name,
                i.is_unique,
                i.is_primary_key,
                i.type_desc,
                STUFF((
                    SELECT ', ' + c.name
                    FROM sys.index_columns ic
                    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                    ORDER BY ic.key_ordinal
                    FOR XML PATH(''), TYPE
                ).value('.', 'nvarchar(max)'), 1, 2, '') AS columns
            FROM sys.indexes i
            WHERE i.object_id = OBJECT_ID('{}.{}')
              AND i.name IS NOT NULL
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
        let result_sets = self.run_with_session_option("SHOWPLAN_ALL", query).await?;
        Ok(Self::parse_plan_rows(&result_sets, false))
    }

    async fn get_execution_plan_actual(&self, query: &str) -> DbResult<ExecutionPlan> {
        let result_sets = self
            .run_with_session_option("STATISTICS PROFILE", query)
            .await?;
        Ok(Self::parse_plan_rows(&result_sets, true))
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
              AND qt.text NOT LIKE '%sys.dm[_]%'
              AND qt.text NOT LIKE '%INFORMATION[_]SCHEMA%'
              AND qt.text NOT LIKE '%sys.indexes%'
              AND qt.text NOT LIKE '%sys.tables%'
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
            AND wait_type NOT LIKE 'XE[_]%'
            AND wait_type NOT LIKE 'BROKER[_]%'
            AND wait_type NOT LIKE 'HADR[_]%'
            AND wait_type NOT LIKE 'QDS[_]%'
            AND wait_type NOT LIKE 'CLR[_]%'
            AND wait_type NOT LIKE 'WAIT[_]XTP[_]%'
            AND wait_type NOT IN (
                'SOS_WORK_DISPATCHER', 'DIRTY_PAGE_POLL', 'REQUEST_FOR_DEADLOCK_SEARCH',
                'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'SP_SERVER_DIAGNOSTICS_SLEEP',
                'FT_IFTS_SCHEDULER_IDLE_WAIT', 'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE'
            )
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

        // Buffer cache metrics; tolerated on failure so a missing permission
        // does not take down the whole stats call.
        let cache_query = r#"
            SELECT
                CAST(a.cntr_value AS float) / NULLIF(CAST(b.cntr_value AS float), 0) * 100.0 AS cache_hit_ratio,
                CAST(c.cntr_value AS float) * 8.0 / 1024.0 AS buffer_pool_size_mb
            FROM sys.dm_os_performance_counters a
            CROSS JOIN sys.dm_os_performance_counters b
            CROSS JOIN sys.dm_os_performance_counters c
            WHERE a.counter_name = 'Buffer cache hit ratio'
              AND a.object_name LIKE '%Buffer Manager%'
              AND b.counter_name = 'Buffer cache hit ratio base'
              AND b.object_name LIKE '%Buffer Manager%'
              AND c.counter_name = 'Database pages'
              AND c.object_name LIKE '%Buffer Manager%'
        "#;

        let cache_stats = if let Ok(stream) = conn.query(cache_query, &[]).await
            && let Ok(rows_result) = stream.into_results().await
            && let Some(row) = rows_result.first().and_then(|rows| rows.first())
            && let Some(hit_ratio) = Self::get_plan_float(row, "cache_hit_ratio")
        {
            Some(CacheStats {
                cache_hit_ratio: hit_ratio,
                buffer_pool_size_mb: Self::get_plan_float(row, "buffer_pool_size_mb")
                    .unwrap_or(0.0),
            })
        } else {
            None
        };

        debug!("Performance stats collected in {:?}", start.elapsed());

        Ok(PerformanceStats {
            top_queries,
            wait_stats: Some(wait_stats),
            cache_stats,
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
    fn test_apply_top_basic() {
        assert_eq!(
            MssqlDriver::apply_top("SELECT * FROM Tickets", 11),
            "SELECT TOP (11) * FROM Tickets"
        );
        // Case-insensitive: lowercase select must also get a limit
        assert_eq!(
            MssqlDriver::apply_top("select Id from Tickets", 5),
            "select TOP (5) Id from Tickets"
        );
    }

    #[test]
    fn test_apply_top_distinct_ordering() {
        // TOP must come after DISTINCT in T-SQL
        assert_eq!(
            MssqlDriver::apply_top("SELECT DISTINCT Name FROM Tickets", 3),
            "SELECT DISTINCT TOP (3) Name FROM Tickets"
        );
        // DISTINCTROW-like identifiers must not be treated as the keyword
        assert_eq!(
            MssqlDriver::apply_top("SELECT DistinctCount FROM T", 3),
            "SELECT TOP (3) DistinctCount FROM T"
        );
    }

    #[test]
    fn test_apply_top_untouched_cases() {
        // Existing TOP is respected
        assert_eq!(
            MssqlDriver::apply_top("SELECT TOP 5 * FROM T", 99),
            "SELECT TOP 5 * FROM T"
        );
        assert_eq!(
            MssqlDriver::apply_top("SELECT TOP(5) * FROM T", 99),
            "SELECT TOP(5) * FROM T"
        );
        // Non-SELECT statements (CTEs) are limited client-side instead
        assert_eq!(
            MssqlDriver::apply_top("WITH x AS (SELECT 1 AS a) SELECT a FROM x", 5),
            "WITH x AS (SELECT 1 AS a) SELECT a FROM x"
        );
        // "SELECTX" is not SELECT
        assert_eq!(
            MssqlDriver::apply_top("SELECTX FROM T", 5),
            "SELECTX FROM T"
        );
    }

    #[test]
    fn test_to_hex_literal() {
        assert_eq!(to_hex_literal(b"Hello"), "0x48656C6C6F");
        assert_eq!(to_hex_literal(&[]), "0x");
        assert_eq!(to_hex_literal(&[0x00, 0xFF]), "0x00FF");
    }

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
