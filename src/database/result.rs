//! Query result types and schema structures.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Generic query result containing rows and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time_ms: 0,
            truncated: None,
        }
    }

    pub fn new(columns: Vec<Column>, rows: Vec<Row>, execution_time_ms: u64) -> Self {
        let row_count = rows.len();
        Self {
            columns,
            rows,
            row_count,
            execution_time_ms,
            truncated: None,
        }
    }

    pub fn with_truncated(mut self, truncated: bool) -> Self {
        self.truncated = Some(truncated);
        self
    }
}

/// Column metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable: None,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = Some(nullable);
        self
    }
}

/// Row data as a map of column name to value.
pub type Row = HashMap<String, CellValue>;

/// Cell value that can hold different SQL types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CellValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(Decimal),
    String(String),
    DateTime(DateTime<Utc>),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl CellValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(n) => Some(*n),
            _ => None,
        }
    }
}

impl From<()> for CellValue {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

impl From<bool> for CellValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i64> for CellValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<i32> for CellValue {
    fn from(v: i32) -> Self {
        Self::Int(v as i64)
    }
}

impl From<f64> for CellValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<Decimal> for CellValue {
    fn from(v: Decimal) -> Self {
        Self::Decimal(v)
    }
}

impl From<String> for CellValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for CellValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_string())
    }
}

impl From<Option<String>> for CellValue {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(s) => Self::String(s),
            None => Self::Null,
        }
    }
}

/// Table information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub table_type: TableType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<i64>,
}

/// Table type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TableType {
    Table,
    View,
    MaterializedView,
    SystemTable,
}

/// Table schema with columns and constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Option<PrimaryKey>,
    pub indexes: Vec<IndexInfo>,
}

/// Column schema information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,
    pub is_primary_key: bool,
    pub is_identity: bool,
}

/// Primary key information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKey {
    pub name: String,
    pub columns: Vec<String>,
}

/// Index information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
}

/// Table relationship (foreign key).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRelationship {
    pub constraint_name: String,
    pub from_schema: String,
    pub from_table: String,
    pub from_column: String,
    pub to_schema: String,
    pub to_table: String,
    pub to_column: String,
}

/// Query execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub plan_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<i64>,
    pub operations: Vec<PlanOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

/// Plan operation node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanOperation {
    pub operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

/// Database performance statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub top_queries: Vec<QueryStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_stats: Option<Vec<WaitStats>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_stats: Option<CacheStats>,
}

/// Query statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_hash: Option<String>,
    pub query_text: String,
    pub execution_count: i64,
    pub total_duration_ms: f64,
    pub avg_duration_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cpu_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_logical_reads: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_execution: Option<DateTime<Utc>>,
}

/// Wait statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitStats {
    pub wait_type: String,
    pub wait_time_ms: f64,
    pub wait_count: i64,
}

/// Cache statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub cache_hit_ratio: f64,
    pub buffer_pool_size_mb: f64,
}

/// Index usage statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexUsage {
    pub schema: String,
    pub table_name: String,
    pub index_name: String,
    pub user_seeks: i64,
    pub user_scans: i64,
    pub user_lookups: i64,
    pub user_updates: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_user_seek: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_user_scan: Option<DateTime<Utc>>,
}

/// Blocking query information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockingQuery {
    pub session_id: i32,
    pub blocking_session_id: Option<i32>,
    pub wait_type: String,
    pub wait_time_ms: i64,
    pub wait_resource: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_result_creation() {
        let columns = vec![Column::new("id", "int"), Column::new("name", "varchar")];
        let mut row = Row::new();
        row.insert("id".into(), CellValue::Int(1));
        row.insert("name".into(), CellValue::String("test".into()));

        let result = QueryResult::new(columns, vec![row], 100);
        assert_eq!(result.row_count, 1);
        assert_eq!(result.columns.len(), 2);
    }

    #[test]
    fn test_cell_value_conversions() {
        let null: CellValue = ().into();
        assert!(null.is_null());

        let int: CellValue = 42i64.into();
        assert_eq!(int.as_i64(), Some(42));

        let string: CellValue = "hello".into();
        assert_eq!(string.as_str(), Some("hello"));
    }
}
