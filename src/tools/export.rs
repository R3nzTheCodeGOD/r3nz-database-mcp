//! Export tools: export query results and database schema to JSON/Excel/CSV files.

use crate::database::ConnectionManager;
use crate::database::connection::ConnectionMetadata;
use crate::database::result::{CellValue, QueryResult, TableInfo, TableRelationship, TableSchema};
use crate::error::{McpError, Result, ToolError};
use crate::protocol::{CallToolResult, Tool};
use crate::security::{RateLimiter, SqlValidator};
use crate::tools::registry::ToolHandler;
use async_trait::async_trait;
use rust_xlsxwriter::{Color, Format, FormatAlign, FormatBorder, Workbook, Worksheet};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;
use tracing::{debug, instrument};

/// Supported export formats.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Json,
    Excel,
    Csv,
}

/// Professional Excel styling with orange theme and zebra striping.
struct ExcelStyles {
    header: Format,
    text_even: Format,
    text_odd: Format,
    number_even: Format,
    number_odd: Format,
    decimal_even: Format,
    decimal_odd: Format,
    date_even: Format,
    date_odd: Format,
    info_header: Format,
    info_value: Format,
}

const PRIMARY: u32 = 0xFF961F;
const PRIMARY_DARK: u32 = 0xE0861A;
const ROW_EVEN: u32 = 0xFFF4E6;
const BORDER: u32 = 0xE8E0D8;

impl ExcelStyles {
    fn new() -> Self {
        let header = Format::new()
            .set_bold()
            .set_font_color(Color::White)
            .set_background_color(Color::RGB(PRIMARY))
            .set_border(FormatBorder::Thin)
            .set_border_color(Color::RGB(PRIMARY_DARK))
            .set_font_size(11)
            .set_align(FormatAlign::Center);

        let base_even = Format::new()
            .set_background_color(Color::RGB(ROW_EVEN))
            .set_border(FormatBorder::Thin)
            .set_border_color(Color::RGB(BORDER))
            .set_font_size(10);

        let base_odd = Format::new()
            .set_background_color(Color::White)
            .set_border(FormatBorder::Thin)
            .set_border_color(Color::RGB(BORDER))
            .set_font_size(10);

        let number_even = base_even.clone().set_num_format("#,##0");
        let number_odd = base_odd.clone().set_num_format("#,##0");
        let decimal_even = base_even.clone().set_num_format("#,##0.00");
        let decimal_odd = base_odd.clone().set_num_format("#,##0.00");
        let date_even = base_even.clone().set_num_format("yyyy-mm-dd hh:mm:ss");
        let date_odd = base_odd.clone().set_num_format("yyyy-mm-dd hh:mm:ss");

        let info_header = Format::new()
            .set_bold()
            .set_font_size(11)
            .set_background_color(Color::RGB(PRIMARY))
            .set_font_color(Color::White)
            .set_border(FormatBorder::Thin)
            .set_border_color(Color::RGB(PRIMARY_DARK));

        let info_value = Format::new()
            .set_font_size(10)
            .set_border(FormatBorder::Thin)
            .set_border_color(Color::RGB(BORDER));

        Self {
            header,
            text_even: base_even,
            text_odd: base_odd,
            number_even,
            number_odd,
            decimal_even,
            decimal_odd,
            date_even,
            date_odd,
            info_header,
            info_value,
        }
    }

    fn text_format(&self, row_index: usize) -> &Format {
        if row_index.is_multiple_of(2) {
            &self.text_even
        } else {
            &self.text_odd
        }
    }

    fn number_format(&self, row_index: usize) -> &Format {
        if row_index.is_multiple_of(2) {
            &self.number_even
        } else {
            &self.number_odd
        }
    }

    fn decimal_format(&self, row_index: usize) -> &Format {
        if row_index.is_multiple_of(2) {
            &self.decimal_even
        } else {
            &self.decimal_odd
        }
    }

    fn date_format(&self, row_index: usize) -> &Format {
        if row_index.is_multiple_of(2) {
            &self.date_even
        } else {
            &self.date_odd
        }
    }
}

/// Convert a `CellValue` to a display string.
fn cell_value_to_string(value: &CellValue) -> String {
    match value {
        CellValue::Null => String::new(),
        CellValue::Bool(b) => b.to_string(),
        CellValue::Int(i) => i.to_string(),
        CellValue::Float(f) => f.to_string(),
        CellValue::Decimal(d) => d.to_string(),
        CellValue::String(s) => s.clone(),
        CellValue::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        CellValue::Bytes(b) => format!("[{} bytes]", b.len()),
        CellValue::Json(v) => serde_json::to_string(v).unwrap_or_default(),
    }
}

/// Generate a default file path with timestamp.
fn default_file_path(prefix: &str, format: ExportFormat) -> String {
    let now = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let ext = match format {
        ExportFormat::Json => "json",
        ExportFormat::Excel => "xlsx",
        ExportFormat::Csv => "csv",
    };
    format!("{prefix}_{now}.{ext}")
}

/// Calculate column widths by sampling header + first 100 rows.
fn calculate_column_widths(
    columns: &[String],
    rows: &[crate::database::result::Row],
    max_width: f64,
) -> Vec<f64> {
    let mut widths: Vec<f64> = columns.iter().map(|c| c.len() as f64 + 4.0).collect();

    for row in rows.iter().take(100) {
        for (i, col_name) in columns.iter().enumerate() {
            if let Some(value) = row.get(col_name) {
                let len = cell_value_to_string(value).len() as f64 + 2.0;
                if len > widths[i] {
                    widths[i] = len;
                }
            }
        }
    }

    widths.iter().map(|w| w.min(max_width)).collect()
}

/// Write a single `CellValue` to an Excel worksheet cell with appropriate formatting.
fn write_cell_value(
    worksheet: &mut Worksheet,
    row: u32,
    col: u16,
    value: &CellValue,
    styles: &ExcelStyles,
    row_index: usize,
) -> std::result::Result<(), rust_xlsxwriter::XlsxError> {
    match value {
        CellValue::Null => {
            worksheet.write_blank(row, col, styles.text_format(row_index))?;
        }
        CellValue::Bool(b) => {
            worksheet.write_boolean_with_format(row, col, *b, styles.text_format(row_index))?;
        }
        CellValue::Int(i) => {
            worksheet.write_number_with_format(
                row,
                col,
                *i as f64,
                styles.number_format(row_index),
            )?;
        }
        CellValue::Float(f) => {
            worksheet.write_number_with_format(row, col, *f, styles.decimal_format(row_index))?;
        }
        CellValue::Decimal(d) => {
            let f: f64 = d.to_string().parse().unwrap_or(0.0);
            worksheet.write_number_with_format(row, col, f, styles.decimal_format(row_index))?;
        }
        CellValue::String(s) => {
            worksheet.write_string_with_format(row, col, s, styles.text_format(row_index))?;
        }
        CellValue::DateTime(dt) => {
            let formatted = dt.format("%Y-%m-%d %H:%M:%S").to_string();
            worksheet.write_string_with_format(
                row,
                col,
                &formatted,
                styles.date_format(row_index),
            )?;
        }
        CellValue::Bytes(b) => {
            worksheet.write_string_with_format(
                row,
                col,
                format!("[{} bytes]", b.len()),
                styles.text_format(row_index),
            )?;
        }
        CellValue::Json(v) => {
            let text = serde_json::to_string(v).unwrap_or_default();
            worksheet.write_string_with_format(row, col, &text, styles.text_format(row_index))?;
        }
    }
    Ok(())
}

/// Write an Info/Metadata sheet to the workbook.
fn write_info_sheet(
    workbook: &mut Workbook,
    styles: &ExcelStyles,
    metadata: &[(&str, String)],
) -> std::result::Result<(), rust_xlsxwriter::XlsxError> {
    let sheet = workbook.add_worksheet();
    sheet.set_name("Info")?;

    sheet.write_string_with_format(0, 0, "Property", &styles.info_header)?;
    sheet.write_string_with_format(0, 1, "Value", &styles.info_header)?;

    sheet.set_column_width(0, 25)?;
    sheet.set_column_width(1, 60)?;
    sheet.set_freeze_panes(1, 0)?;

    for (i, (key, value)) in metadata.iter().enumerate() {
        let row = (i + 1) as u32;
        sheet.write_string_with_format(row, 0, *key, &styles.info_header)?;
        sheet.write_string_with_format(row, 1, value, &styles.info_value)?;
    }

    Ok(())
}

/// Map `XlsxError` to `McpError::Internal`.
fn xlsx_error(e: rust_xlsxwriter::XlsxError) -> McpError {
    McpError::Internal {
        message: format!("Excel error: {e}").into(),
    }
}

/// Write a data sheet with query results into an existing workbook.
fn write_data_sheet(
    workbook: &mut Workbook,
    styles: &ExcelStyles,
    result: &QueryResult,
    sheet_name: &str,
) -> Result<()> {
    let columns: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();
    let sheet = workbook.add_worksheet();
    sheet.set_name(sheet_name).map_err(xlsx_error)?;
    sheet.set_tab_color(Color::RGB(PRIMARY));
    sheet.set_row_height(0, 24).map_err(xlsx_error)?;

    // Column widths
    let widths = calculate_column_widths(&columns, &result.rows, 50.0);
    for (i, width) in widths.iter().enumerate() {
        sheet
            .set_column_width(i as u16, *width)
            .map_err(xlsx_error)?;
    }

    // Header row
    for (col_idx, col_name) in columns.iter().enumerate() {
        sheet
            .write_string_with_format(0, col_idx as u16, col_name, &styles.header)
            .map_err(xlsx_error)?;
    }

    // Freeze header
    sheet.set_freeze_panes(1, 0).map_err(xlsx_error)?;

    // Data rows with zebra striping
    for (row_idx, row) in result.rows.iter().enumerate() {
        let excel_row = (row_idx + 1) as u32;
        for (col_idx, col_name) in columns.iter().enumerate() {
            if let Some(value) = row.get(col_name) {
                write_cell_value(sheet, excel_row, col_idx as u16, value, styles, row_idx)
                    .map_err(xlsx_error)?;
            } else {
                sheet
                    .write_blank(excel_row, col_idx as u16, styles.text_format(row_idx))
                    .map_err(xlsx_error)?;
            }
        }
    }

    // Autofilter
    if !result.rows.is_empty() && !columns.is_empty() {
        sheet
            .autofilter(0, 0, result.row_count as u32, (columns.len() - 1) as u16)
            .map_err(xlsx_error)?;
    }

    Ok(())
}

/// Escape a field value according to RFC 4180.
fn escape_csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_string()
    }
}

/// Export query results to a CSV file (RFC 4180 with UTF-8 BOM for Excel compatibility).
fn export_query_to_csv(result: &QueryResult, file_path: &str) -> Result<()> {
    let mut file = std::fs::File::create(file_path).map_err(McpError::Io)?;

    // UTF-8 BOM for encoding detection
    file.write_all(&[0xEF, 0xBB, 0xBF]).map_err(McpError::Io)?;

    let columns: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();

    // Header row
    let header_line: Vec<String> = columns.iter().map(|c| escape_csv_field(c)).collect();
    file.write_all(header_line.join(",").as_bytes())
        .map_err(McpError::Io)?;
    file.write_all(b"\r\n").map_err(McpError::Io)?;

    // Data rows
    for row in &result.rows {
        let fields: Vec<String> = columns
            .iter()
            .map(|col_name| {
                row.get(col_name)
                    .map(|v| escape_csv_field(&cell_value_to_string(v)))
                    .unwrap_or_default()
            })
            .collect();
        file.write_all(fields.join(",").as_bytes())
            .map_err(McpError::Io)?;
        file.write_all(b"\r\n").map_err(McpError::Io)?;
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct ExportQueryArgs {
    pub query: String,
    pub format: ExportFormat,
    #[serde(default)]
    pub file_path: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub sheet_name: Option<String>,
}

pub struct ExportQueryTool {
    connection_manager: Arc<ConnectionManager>,
    validator: SqlValidator,
    rate_limiter: Arc<RateLimiter>,
}

impl ExportQueryTool {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        validator: SqlValidator,
        rate_limiter: Arc<RateLimiter>,
    ) -> Self {
        Self {
            connection_manager,
            validator,
            rate_limiter,
        }
    }
}

#[async_trait]
impl ToolHandler for ExportQueryTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "export_query".into(),
            description: Some(
                "Export SQL query results to a file. Supports JSON, Excel and CSV formats. \
                Excel exports include professional styling with headers, zebra striping, \
                and a metadata sheet. Requires an active database connection. \
                IMPORTANT: Do NOT use TOP or LIMIT clauses in your SQL query to restrict row count. \
                Instead, use the 'limit' parameter which handles row limiting automatically \
                and works across all database backends (MSSQL, PostgreSQL)."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL SELECT query to execute and export. Do NOT include TOP or LIMIT in the query — use the 'limit' parameter instead."
                    },
                    "format": {
                        "type": "string",
                        "description": "Export format: 'json', 'excel' or 'csv'",
                        "enum": ["json", "excel", "csv"]
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Output file path (auto-generated if not specified)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to export (default: 10000). Use this instead of SQL TOP/LIMIT clauses.",
                        "minimum": 1,
                        "maximum": 100000
                    },
                    "sheet_name": {
                        "type": "string",
                        "description": "Excel sheet name for query results (default: 'Query Results')"
                    }
                },
                "required": ["query", "format"]
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "export_query"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: ExportQueryArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        // Validate and sanitize query
        self.validator
            .validate(&args.query)
            .map_err(McpError::from)?;
        let query = self.validator.sanitize(&args.query);

        // Rate limit
        let _permit = self.rate_limiter.try_acquire().map_err(McpError::from)?;

        // Execute query
        let limit = args.limit.unwrap_or(10_000);
        debug!("Exporting query with limit {}", limit);

        let result = driver
            .execute_query(&query, Some(limit))
            .await
            .map_err(McpError::from)?;

        // Determine output path
        let file_path = args
            .file_path
            .unwrap_or_else(|| default_file_path("export_query", args.format));

        // Get connection metadata for info sheet
        let metadata = self.connection_manager.metadata();

        // Export
        match args.format {
            ExportFormat::Json => export_query_to_json(&result, &query, &file_path)?,
            ExportFormat::Excel => export_query_to_excel(
                &result,
                &query,
                &file_path,
                args.sheet_name.as_deref().unwrap_or("Query Results"),
                &metadata,
            )?,
            ExportFormat::Csv => export_query_to_csv(&result, &file_path)?,
        }

        let format_str = match args.format {
            ExportFormat::Json => "json",
            ExportFormat::Excel => "excel",
            ExportFormat::Csv => "csv",
        };

        Ok(CallToolResult::json(&serde_json::json!({
            "status": "success",
            "file_path": file_path,
            "format": format_str,
            "row_count": result.row_count,
            "column_count": result.columns.len(),
            "execution_time_ms": result.execution_time_ms,
            "truncated": result.truncated.unwrap_or(false)
        })))
    }
}

fn export_query_to_json(result: &QueryResult, query: &str, file_path: &str) -> Result<()> {
    let export = serde_json::json!({
        "metadata": {
            "query": query,
            "exported_at": chrono::Utc::now().to_rfc3339(),
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
            "truncated": result.truncated.unwrap_or(false),
            "format_version": "1.0"
        },
        "columns": result.columns.iter().map(|c| {
            serde_json::json!({
                "name": c.name,
                "data_type": c.data_type
            })
        }).collect::<Vec<_>>(),
        "data": result.rows
    });

    let json_string = serde_json::to_string_pretty(&export).map_err(McpError::Json)?;
    std::fs::write(file_path, json_string).map_err(McpError::Io)?;

    Ok(())
}

fn export_query_to_excel(
    result: &QueryResult,
    query: &str,
    file_path: &str,
    sheet_name: &str,
    conn_metadata: &ConnectionMetadata,
) -> Result<()> {
    let mut workbook = Workbook::new();
    let styles = ExcelStyles::new();

    // --- Data Sheet ---
    write_data_sheet(&mut workbook, &styles, result, sheet_name)?;

    // --- Info Sheet ---
    let info_data = vec![
        ("Query", query.to_string()),
        ("Exported At", chrono::Utc::now().to_rfc3339()),
        ("Row Count", result.row_count.to_string()),
        ("Column Count", result.columns.len().to_string()),
        ("Execution Time (ms)", result.execution_time_ms.to_string()),
        ("Truncated", result.truncated.unwrap_or(false).to_string()),
        (
            "Database Type",
            conn_metadata.database_type.clone().unwrap_or_default(),
        ),
        ("Host", conn_metadata.host.clone().unwrap_or_default()),
        (
            "Database",
            conn_metadata.database.clone().unwrap_or_default(),
        ),
        (
            "Username",
            conn_metadata.username.clone().unwrap_or_default(),
        ),
    ];
    write_info_sheet(&mut workbook, &styles, &info_data).map_err(xlsx_error)?;

    workbook.save(file_path).map_err(xlsx_error)?;

    Ok(())
}

// ─── Export Schema Tool ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ExportSchemaArgs {
    pub format: ExportFormat,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub file_path: Option<String>,
    #[serde(default = "default_true")]
    pub include_relationships: bool,
}

fn default_true() -> bool {
    true
}

pub struct ExportSchemaTool {
    connection_manager: Arc<ConnectionManager>,
}

impl ExportSchemaTool {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

#[async_trait]
impl ToolHandler for ExportSchemaTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "export_schema".into(),
            description: Some(
                "Export the full database schema to a file. Supports JSON and Excel formats. \
                Excel exports include separate sheets for tables, columns, and relationships. \
                Requires an active database connection."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "format": {
                        "type": "string",
                        "description": "Export format: 'json' or 'excel'",
                        "enum": ["json", "excel"]
                    },
                    "schema": {
                        "type": "string",
                        "description": "Optional schema filter (e.g., 'public', 'dbo')"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Output file path (auto-generated if not specified)"
                    },
                    "include_relationships": {
                        "type": "boolean",
                        "description": "Whether to include foreign key relationships (default: true)"
                    }
                },
                "required": ["format"]
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "export_schema"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: ExportSchemaArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        // List tables
        let tables = driver
            .list_tables(args.schema.as_deref())
            .await
            .map_err(McpError::from)?;

        // Fetch schema for each table
        let mut table_schemas = Vec::new();
        for table in &tables {
            let full_name = format!("{}.{}", table.schema, table.name);
            match driver.get_table_schema(&full_name).await {
                Ok(schema) => table_schemas.push(schema),
                Err(e) => {
                    tracing::warn!("Failed to get schema for {}: {}", full_name, e);
                }
            }
        }

        // Fetch relationships
        let relationships = if args.include_relationships {
            driver
                .get_relationships(None)
                .await
                .map_err(McpError::from)?
        } else {
            Vec::new()
        };

        // Determine output path
        let file_path = args
            .file_path
            .unwrap_or_else(|| default_file_path("export_schema", args.format));

        // Get connection metadata
        let conn_metadata = self.connection_manager.metadata();

        // Export (CSV not supported for schema - multi-sheet structure)
        match args.format {
            ExportFormat::Json => {
                export_schema_to_json(&tables, &table_schemas, &relationships, &file_path)?;
            }
            ExportFormat::Excel => {
                export_schema_to_excel(
                    &tables,
                    &table_schemas,
                    &relationships,
                    &file_path,
                    &conn_metadata,
                )?;
            }
            ExportFormat::Csv => {
                return Err(ToolError::InvalidArguments(
                    "CSV format is not supported for schema export. Use 'json' or 'excel'.".into(),
                )
                .into());
            }
        }

        let format_str = match args.format {
            ExportFormat::Json => "json",
            ExportFormat::Excel => "excel",
            ExportFormat::Csv => unreachable!(),
        };

        Ok(CallToolResult::json(&serde_json::json!({
            "status": "success",
            "file_path": file_path,
            "format": format_str,
            "table_count": tables.len(),
            "relationship_count": relationships.len()
        })))
    }
}

fn export_schema_to_json(
    tables: &[TableInfo],
    schemas: &[TableSchema],
    relationships: &[TableRelationship],
    file_path: &str,
) -> Result<()> {
    let export = serde_json::json!({
        "metadata": {
            "exported_at": chrono::Utc::now().to_rfc3339(),
            "table_count": tables.len(),
            "relationship_count": relationships.len(),
            "format_version": "1.0"
        },
        "tables": tables,
        "schemas": schemas,
        "relationships": relationships
    });

    let json_string = serde_json::to_string_pretty(&export).map_err(McpError::Json)?;
    std::fs::write(file_path, json_string).map_err(McpError::Io)?;

    Ok(())
}

fn export_schema_to_excel(
    tables: &[TableInfo],
    schemas: &[TableSchema],
    relationships: &[TableRelationship],
    file_path: &str,
    conn_metadata: &ConnectionMetadata,
) -> Result<()> {
    let mut workbook = Workbook::new();
    let styles = ExcelStyles::new();

    // --- Sheet 1: Tables ---
    write_tables_sheet(&mut workbook, &styles, tables)?;

    // --- Sheet 2: Columns ---
    write_columns_sheet(&mut workbook, &styles, schemas)?;

    // --- Sheet 3: Relationships ---
    if !relationships.is_empty() {
        write_relationships_sheet(&mut workbook, &styles, relationships)?;
    }

    // --- Sheet 4: Info ---
    let info_data = vec![
        ("Exported At", chrono::Utc::now().to_rfc3339()),
        ("Table Count", tables.len().to_string()),
        ("Relationship Count", relationships.len().to_string()),
        (
            "Database Type",
            conn_metadata.database_type.clone().unwrap_or_default(),
        ),
        ("Host", conn_metadata.host.clone().unwrap_or_default()),
        (
            "Database",
            conn_metadata.database.clone().unwrap_or_default(),
        ),
    ];
    write_info_sheet(&mut workbook, &styles, &info_data).map_err(xlsx_error)?;

    workbook.save(file_path).map_err(xlsx_error)?;

    Ok(())
}

fn write_tables_sheet(
    workbook: &mut Workbook,
    styles: &ExcelStyles,
    tables: &[TableInfo],
) -> Result<()> {
    let sheet = workbook.add_worksheet();
    sheet.set_name("Tables").map_err(xlsx_error)?;

    let headers = ["Schema", "Table Name", "Type", "Row Count"];
    for (i, h) in headers.iter().enumerate() {
        sheet
            .write_string_with_format(0, i as u16, *h, &styles.header)
            .map_err(xlsx_error)?;
    }

    sheet.set_freeze_panes(1, 0).map_err(xlsx_error)?;
    let col_widths = [20.0, 40.0, 20.0, 15.0];
    for (i, w) in col_widths.iter().enumerate() {
        sheet.set_column_width(i as u16, *w).map_err(xlsx_error)?;
    }

    for (idx, table) in tables.iter().enumerate() {
        let row = (idx + 1) as u32;
        let fmt = styles.text_format(idx);
        sheet
            .write_string_with_format(row, 0, &table.schema, fmt)
            .map_err(xlsx_error)?;
        sheet
            .write_string_with_format(row, 1, &table.name, fmt)
            .map_err(xlsx_error)?;
        sheet
            .write_string_with_format(row, 2, format!("{:?}", table.table_type), fmt)
            .map_err(xlsx_error)?;
        if let Some(count) = table.row_count {
            sheet
                .write_number_with_format(row, 3, count as f64, styles.number_format(idx))
                .map_err(xlsx_error)?;
        } else {
            sheet.write_blank(row, 3, fmt).map_err(xlsx_error)?;
        }
    }

    if !tables.is_empty() {
        sheet
            .autofilter(0, 0, tables.len() as u32, 3)
            .map_err(xlsx_error)?;
    }

    Ok(())
}

fn write_columns_sheet(
    workbook: &mut Workbook,
    styles: &ExcelStyles,
    schemas: &[TableSchema],
) -> Result<()> {
    let sheet = workbook.add_worksheet();
    sheet.set_name("Columns").map_err(xlsx_error)?;

    let headers = [
        "Schema",
        "Table",
        "Column",
        "Data Type",
        "Nullable",
        "Primary Key",
        "Identity",
        "Max Length",
        "Default",
    ];
    for (i, h) in headers.iter().enumerate() {
        sheet
            .write_string_with_format(0, i as u16, *h, &styles.header)
            .map_err(xlsx_error)?;
    }

    sheet.set_freeze_panes(1, 0).map_err(xlsx_error)?;
    let col_widths = [15.0, 30.0, 30.0, 20.0, 10.0, 12.0, 10.0, 12.0, 25.0];
    for (i, w) in col_widths.iter().enumerate() {
        sheet.set_column_width(i as u16, *w).map_err(xlsx_error)?;
    }

    let mut global_row = 0usize;
    for ts in schemas {
        for col in &ts.columns {
            let row = (global_row + 1) as u32;
            let fmt = styles.text_format(global_row);
            sheet
                .write_string_with_format(row, 0, &ts.schema, fmt)
                .map_err(xlsx_error)?;
            sheet
                .write_string_with_format(row, 1, &ts.name, fmt)
                .map_err(xlsx_error)?;
            sheet
                .write_string_with_format(row, 2, &col.name, fmt)
                .map_err(xlsx_error)?;
            sheet
                .write_string_with_format(row, 3, &col.data_type, fmt)
                .map_err(xlsx_error)?;
            sheet
                .write_string_with_format(row, 4, if col.nullable { "Yes" } else { "No" }, fmt)
                .map_err(xlsx_error)?;
            sheet
                .write_string_with_format(
                    row,
                    5,
                    if col.is_primary_key { "Yes" } else { "No" },
                    fmt,
                )
                .map_err(xlsx_error)?;
            sheet
                .write_string_with_format(row, 6, if col.is_identity { "Yes" } else { "No" }, fmt)
                .map_err(xlsx_error)?;
            if let Some(ml) = col.max_length {
                sheet
                    .write_number_with_format(
                        row,
                        7,
                        f64::from(ml),
                        styles.number_format(global_row),
                    )
                    .map_err(xlsx_error)?;
            } else {
                sheet.write_blank(row, 7, fmt).map_err(xlsx_error)?;
            }
            sheet
                .write_string_with_format(row, 8, col.default_value.as_deref().unwrap_or(""), fmt)
                .map_err(xlsx_error)?;
            global_row += 1;
        }
    }

    if global_row > 0 {
        sheet
            .autofilter(0, 0, global_row as u32, (headers.len() - 1) as u16)
            .map_err(xlsx_error)?;
    }

    Ok(())
}

fn write_relationships_sheet(
    workbook: &mut Workbook,
    styles: &ExcelStyles,
    relationships: &[TableRelationship],
) -> Result<()> {
    let sheet = workbook.add_worksheet();
    sheet.set_name("Relationships").map_err(xlsx_error)?;

    let headers = [
        "Constraint",
        "From Schema",
        "From Table",
        "From Column",
        "To Schema",
        "To Table",
        "To Column",
    ];
    for (i, h) in headers.iter().enumerate() {
        sheet
            .write_string_with_format(0, i as u16, *h, &styles.header)
            .map_err(xlsx_error)?;
    }

    sheet.set_freeze_panes(1, 0).map_err(xlsx_error)?;
    let col_widths = [35.0, 15.0, 30.0, 25.0, 15.0, 30.0, 25.0];
    for (i, w) in col_widths.iter().enumerate() {
        sheet.set_column_width(i as u16, *w).map_err(xlsx_error)?;
    }

    for (idx, rel) in relationships.iter().enumerate() {
        let row = (idx + 1) as u32;
        let fmt = styles.text_format(idx);
        sheet
            .write_string_with_format(row, 0, &rel.constraint_name, fmt)
            .map_err(xlsx_error)?;
        sheet
            .write_string_with_format(row, 1, &rel.from_schema, fmt)
            .map_err(xlsx_error)?;
        sheet
            .write_string_with_format(row, 2, &rel.from_table, fmt)
            .map_err(xlsx_error)?;
        sheet
            .write_string_with_format(row, 3, &rel.from_column, fmt)
            .map_err(xlsx_error)?;
        sheet
            .write_string_with_format(row, 4, &rel.to_schema, fmt)
            .map_err(xlsx_error)?;
        sheet
            .write_string_with_format(row, 5, &rel.to_table, fmt)
            .map_err(xlsx_error)?;
        sheet
            .write_string_with_format(row, 6, &rel.to_column, fmt)
            .map_err(xlsx_error)?;
    }

    if !relationships.is_empty() {
        sheet
            .autofilter(0, 0, relationships.len() as u32, 6)
            .map_err(xlsx_error)?;
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct SheetQuery {
    query: String,
    sheet_name: String,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ExportMultiQueryArgs {
    queries: Vec<SheetQuery>,
    #[serde(default)]
    file_path: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

pub struct ExportMultiQueryTool {
    connection_manager: Arc<ConnectionManager>,
    validator: SqlValidator,
    rate_limiter: Arc<RateLimiter>,
}

impl ExportMultiQueryTool {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        validator: SqlValidator,
        rate_limiter: Arc<RateLimiter>,
    ) -> Self {
        Self {
            connection_manager,
            validator,
            rate_limiter,
        }
    }
}

#[async_trait]
impl ToolHandler for ExportMultiQueryTool {
    fn definition(&self) -> Tool {
        Tool {
            name: "export_multi_query".into(),
            description: Some(
                "Export multiple SQL query results to a single Excel file with separate sheets. \
                Each query gets its own professionally styled sheet. Includes an Info sheet \
                summarizing all queries. Maximum 20 sheets. Requires an active database connection. \
                IMPORTANT: Do NOT use TOP or LIMIT clauses in your SQL queries to restrict row count. \
                Instead, use the 'limit' parameter (per-query or global) which handles row limiting \
                automatically and works across all database backends (MSSQL, PostgreSQL)."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "queries": {
                        "type": "array",
                        "description": "Array of queries, each with a SQL query and sheet name",
                        "items": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The SQL SELECT query to execute. Do NOT include TOP or LIMIT in the query — use the 'limit' parameter instead."
                                },
                                "sheet_name": {
                                    "type": "string",
                                    "description": "Sheet name for this query's results"
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Per-query row limit (overrides global limit). Use this instead of SQL TOP/LIMIT clauses.",
                                    "minimum": 1,
                                    "maximum": 100000
                                }
                            },
                            "required": ["query", "sheet_name"]
                        },
                        "minItems": 1,
                        "maxItems": 20
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Output file path (auto-generated if not specified)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Global default row limit per query (default: 10000). Use this instead of SQL TOP/LIMIT clauses.",
                        "minimum": 1,
                        "maximum": 100000
                    }
                },
                "required": ["queries"]
            }),
        }
    }

    #[instrument(skip(self, arguments), fields(tool = "export_multi_query"))]
    async fn execute(&self, arguments: Value) -> Result<CallToolResult> {
        let driver = self
            .connection_manager
            .get_driver()
            .map_err(McpError::from)?;

        let args: ExportMultiQueryArgs = serde_json::from_value(arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        // --- Input validation ---
        if args.queries.is_empty() {
            return Err(
                ToolError::InvalidArguments("At least one query is required".into()).into(),
            );
        }
        if args.queries.len() > 20 {
            return Err(ToolError::InvalidArguments("Maximum 20 sheets allowed".into()).into());
        }

        let mut seen_names = HashSet::new();
        for sq in &args.queries {
            if !seen_names.insert(&sq.sheet_name) {
                return Err(ToolError::InvalidArguments(format!(
                    "Duplicate sheet name: {}",
                    sq.sheet_name
                ))
                .into());
            }
        }

        let global_limit = args.limit.unwrap_or(10_000);

        // --- Execute all queries ---
        let mut results: Vec<(String, QueryResult)> = Vec::with_capacity(args.queries.len());

        for sq in &args.queries {
            self.validator.validate(&sq.query).map_err(McpError::from)?;
            let query = self.validator.sanitize(&sq.query);
            let _permit = self.rate_limiter.try_acquire().map_err(McpError::from)?;

            let limit = sq.limit.unwrap_or(global_limit);
            let result = driver
                .execute_query(&query, Some(limit))
                .await
                .map_err(McpError::from)?;

            results.push((sq.sheet_name.clone(), result));
        }

        // --- Build workbook ---
        let file_path = args
            .file_path
            .unwrap_or_else(|| default_file_path("export_multi", ExportFormat::Excel));

        let mut workbook = Workbook::new();
        let styles = ExcelStyles::new();

        for (sheet_name, result) in &results {
            write_data_sheet(&mut workbook, &styles, result, sheet_name)?;
        }

        // --- Info sheet ---
        let conn_metadata = self.connection_manager.metadata();

        // Build sheet labels as owned Strings so we can borrow them for info_data
        let sheet_labels: Vec<(String, String)> = results
            .iter()
            .enumerate()
            .map(|(i, (sheet_name, result))| {
                (
                    format!("Sheet {} ({})", i + 1, sheet_name),
                    format!("{} rows", result.row_count),
                )
            })
            .collect();

        let mut info_data: Vec<(&str, String)> = vec![
            ("Exported At", chrono::Utc::now().to_rfc3339()),
            ("Sheet Count", results.len().to_string()),
            (
                "Database Type",
                conn_metadata.database_type.clone().unwrap_or_default(),
            ),
            ("Host", conn_metadata.host.clone().unwrap_or_default()),
            (
                "Database",
                conn_metadata.database.clone().unwrap_or_default(),
            ),
        ];

        for (label, value) in &sheet_labels {
            info_data.push((label.as_str(), value.clone()));
        }

        write_info_sheet(&mut workbook, &styles, &info_data).map_err(xlsx_error)?;
        workbook.save(&file_path).map_err(xlsx_error)?;

        let sheet_summary: Vec<Value> = results
            .iter()
            .map(|(name, r)| {
                serde_json::json!({
                    "sheet_name": name,
                    "row_count": r.row_count,
                    "column_count": r.columns.len(),
                })
            })
            .collect();

        Ok(CallToolResult::json(&serde_json::json!({
            "status": "success",
            "file_path": file_path,
            "format": "excel",
            "sheet_count": results.len(),
            "sheets": sheet_summary
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::result::{CellValue, Column, QueryResult, Row};
    use std::collections::HashMap;

    #[test]
    fn test_export_format_deserialize() {
        let json: ExportFormat = serde_json::from_str("\"json\"").unwrap();
        assert_eq!(json, ExportFormat::Json);

        let excel: ExportFormat = serde_json::from_str("\"excel\"").unwrap();
        assert_eq!(excel, ExportFormat::Excel);

        let csv: ExportFormat = serde_json::from_str("\"csv\"").unwrap();
        assert_eq!(csv, ExportFormat::Csv);
    }

    #[test]
    fn test_cell_value_to_string() {
        assert_eq!(cell_value_to_string(&CellValue::Null), "");
        assert_eq!(cell_value_to_string(&CellValue::Bool(true)), "true");
        assert_eq!(cell_value_to_string(&CellValue::Int(42)), "42");
        assert_eq!(cell_value_to_string(&CellValue::Float(3.14)), "3.14");
        assert_eq!(
            cell_value_to_string(&CellValue::String("hello".into())),
            "hello"
        );
        assert_eq!(
            cell_value_to_string(&CellValue::Bytes(vec![1, 2, 3])),
            "[3 bytes]"
        );
    }

    #[test]
    fn test_default_file_path() {
        let json_path = default_file_path("export_query", ExportFormat::Json);
        assert!(json_path.starts_with("export_query_"));
        assert!(json_path.ends_with(".json"));

        let excel_path = default_file_path("export_schema", ExportFormat::Excel);
        assert!(excel_path.starts_with("export_schema_"));
        assert!(excel_path.ends_with(".xlsx"));

        let csv_path = default_file_path("export_query", ExportFormat::Csv);
        assert!(csv_path.starts_with("export_query_"));
        assert!(csv_path.ends_with(".csv"));
    }

    #[test]
    fn test_calculate_column_widths() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let mut row: Row = HashMap::new();
        row.insert("id".into(), CellValue::Int(1));
        row.insert(
            "name".into(),
            CellValue::String("A very long name value".into()),
        );

        let widths = calculate_column_widths(&columns, &[row], 50.0);
        assert_eq!(widths.len(), 2);
        // "name" column should be wider due to long value
        assert!(widths[1] > widths[0]);
    }

    #[test]
    fn test_export_query_to_json() {
        let columns = vec![Column::new("id", "int"), Column::new("name", "varchar")];
        let mut row: Row = HashMap::new();
        row.insert("id".into(), CellValue::Int(1));
        row.insert("name".into(), CellValue::String("test".into()));

        let result = QueryResult::new(columns, vec![row], 10);
        let dir = std::env::temp_dir();
        let file_path = dir.join("test_export.json");
        let path_str = file_path.to_string_lossy().to_string();

        export_query_to_json(&result, "SELECT * FROM test", &path_str).unwrap();

        let content = std::fs::read_to_string(&file_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(parsed["metadata"]["row_count"], 1);
        assert_eq!(parsed["metadata"]["query"], "SELECT * FROM test");
        assert_eq!(parsed["metadata"]["format_version"], "1.0");
        assert_eq!(parsed["columns"][0]["name"], "id");
        assert!(parsed["data"].is_array());

        // Cleanup
        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_export_query_to_excel() {
        let columns = vec![Column::new("id", "int"), Column::new("name", "varchar")];
        let mut row: Row = HashMap::new();
        row.insert("id".into(), CellValue::Int(1));
        row.insert("name".into(), CellValue::String("test".into()));

        let result = QueryResult::new(columns, vec![row], 10);
        let dir = std::env::temp_dir();
        let file_path = dir.join("test_export.xlsx");
        let path_str = file_path.to_string_lossy().to_string();

        let metadata = ConnectionMetadata {
            connected: true,
            database_type: Some("postgres".into()),
            host: Some("localhost".into()),
            port: Some(5432),
            database: Some("testdb".into()),
            username: Some("user".into()),
        };

        export_query_to_excel(
            &result,
            "SELECT * FROM test",
            &path_str,
            "Results",
            &metadata,
        )
        .unwrap();

        assert!(file_path.exists());
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        assert!(file_size > 0);

        // Cleanup
        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_export_query_tool_definition() {
        let tool = ExportQueryTool::new(
            Arc::new(crate::database::ConnectionManager::new()),
            crate::security::SqlValidator::default(),
            Arc::new(crate::security::RateLimiter::new(100, 10)),
        );
        let def = tool.definition();
        assert_eq!(def.name, "export_query");
        assert!(
            def.input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("query"))
        );
        assert!(
            def.input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("format"))
        );
    }

    #[test]
    fn test_export_schema_tool_definition() {
        let tool = ExportSchemaTool::new(Arc::new(crate::database::ConnectionManager::new()));
        let def = tool.definition();
        assert_eq!(def.name, "export_schema");
        assert!(
            def.input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("format"))
        );
    }

    #[test]
    fn test_escape_csv_field() {
        assert_eq!(escape_csv_field("hello"), "hello");
        assert_eq!(escape_csv_field("hello,world"), "\"hello,world\"");
        assert_eq!(escape_csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(escape_csv_field("line1\nline2"), "\"line1\nline2\"");
        assert_eq!(escape_csv_field("line1\r\nline2"), "\"line1\r\nline2\"");
        assert_eq!(escape_csv_field(""), "");
    }

    #[test]
    fn test_export_query_to_csv() {
        let columns = vec![Column::new("id", "int"), Column::new("name", "varchar")];
        let mut row1: Row = HashMap::new();
        row1.insert("id".into(), CellValue::Int(1));
        row1.insert("name".into(), CellValue::String("Alice".into()));
        let mut row2: Row = HashMap::new();
        row2.insert("id".into(), CellValue::Int(2));
        row2.insert("name".into(), CellValue::String("Bob, Jr.".into()));

        let result = QueryResult::new(columns, vec![row1, row2], 10);
        let dir = std::env::temp_dir();
        let file_path = dir.join("test_export.csv");
        let path_str = file_path.to_string_lossy().to_string();

        export_query_to_csv(&result, &path_str).unwrap();

        let bytes = std::fs::read(&file_path).unwrap();
        // Check UTF-8 BOM
        assert_eq!(&bytes[..3], &[0xEF, 0xBB, 0xBF]);

        let content = String::from_utf8_lossy(&bytes[3..]);
        let lines: Vec<&str> = content.split("\r\n").collect();
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines[1], "1,Alice");
        assert_eq!(lines[2], "2,\"Bob, Jr.\"");

        // Cleanup
        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_export_multi_query_tool_definition() {
        let tool = ExportMultiQueryTool::new(
            Arc::new(crate::database::ConnectionManager::new()),
            crate::security::SqlValidator::default(),
            Arc::new(crate::security::RateLimiter::new(100, 10)),
        );
        let def = tool.definition();
        assert_eq!(def.name, "export_multi_query");
        assert!(
            def.input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("queries"))
        );
        // Verify queries items schema
        let items = &def.input_schema["properties"]["queries"]["items"];
        assert!(
            items["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("query"))
        );
        assert!(
            items["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("sheet_name"))
        );
    }

    #[test]
    fn test_multi_query_validation() {
        // Empty queries array should fail deserialization requirement
        let args: std::result::Result<ExportMultiQueryArgs, _> =
            serde_json::from_value(serde_json::json!({ "queries": [] }));
        assert!(args.is_ok()); // Deserialization succeeds, validation is in execute

        // Duplicate sheet name detection
        let args: ExportMultiQueryArgs = serde_json::from_value(serde_json::json!({
            "queries": [
                { "query": "SELECT 1", "sheet_name": "Sheet1" },
                { "query": "SELECT 2", "sheet_name": "Sheet1" }
            ]
        }))
        .unwrap();

        let mut seen = HashSet::new();
        let has_dup = args.queries.iter().any(|sq| !seen.insert(&sq.sheet_name));
        assert!(has_dup);

        // Per-query limit override
        let args: ExportMultiQueryArgs = serde_json::from_value(serde_json::json!({
            "queries": [
                { "query": "SELECT 1", "sheet_name": "S1", "limit": 500 },
                { "query": "SELECT 2", "sheet_name": "S2" }
            ],
            "limit": 1000
        }))
        .unwrap();

        let global = args.limit.unwrap_or(10_000);
        assert_eq!(args.queries[0].limit.unwrap_or(global), 500);
        assert_eq!(args.queries[1].limit.unwrap_or(global), 1000);
    }
}
