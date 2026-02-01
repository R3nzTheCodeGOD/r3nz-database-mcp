//! SQL query validator.
//!
//! Validates queries against forbidden keywords and injection patterns,
//! enforces read-only operations.

use crate::error::{SecurityError, SecurityResult};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashSet;
use tracing::{debug, warn};

/// Forbidden SQL keywords that indicate write operations.
static FORBIDDEN_KEYWORDS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "EXEC",
        "EXECUTE",
        "MERGE",
        "BULK",
        "OPENROWSET",
        "OPENQUERY",
        "OPENDATASOURCE",
        "XP_",
        "SP_",
        "SHUTDOWN",
        "BACKUP",
        "RESTORE",
        "DBCC",
        "KILL",
        "RECONFIGURE",
        "LOAD",
        "COPY",
    ]
    .into_iter()
    .collect()
});

/// Regex for sanitizing line comments (--).
static COMMENT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"--.*$").expect("Invalid regex: line comment sanitize pattern"));

/// Regex for sanitizing block comments (/* */).
static BLOCK_COMMENT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"/\*.*?\*/").expect("Invalid regex: block comment sanitize pattern"));

/// Regex for normalizing whitespace.
static WHITESPACE_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\s+").expect("Invalid regex: whitespace pattern"));

/// Suspicious patterns that might indicate SQL injection.
/// All patterns are compile-time constants, so expect() is safe here.
static INJECTION_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"(?i);\s*(DROP|DELETE|UPDATE|INSERT|TRUNCATE|ALTER|CREATE)")
            .expect("Invalid regex: statement injection pattern"),
        Regex::new(r"(?i)--\s*$").expect("Invalid regex: line comment pattern"),
        Regex::new(r"(?i)/\*.*\*/").expect("Invalid regex: block comment pattern"),
        Regex::new(r"(?i)UNION\s+ALL\s+SELECT").expect("Invalid regex: UNION ALL SELECT pattern"),
        Regex::new(r"(?i)UNION\s+SELECT").expect("Invalid regex: UNION SELECT pattern"),
        Regex::new(r"(?i)INTO\s+OUTFILE").expect("Invalid regex: INTO OUTFILE pattern"),
        Regex::new(r"(?i)INTO\s+DUMPFILE").expect("Invalid regex: INTO DUMPFILE pattern"),
        Regex::new(r"(?i)LOAD_FILE\s*\(").expect("Invalid regex: LOAD_FILE pattern"),
        Regex::new(r"(?i)BENCHMARK\s*\(").expect("Invalid regex: BENCHMARK pattern"),
        Regex::new(r"(?i)SLEEP\s*\(").expect("Invalid regex: SLEEP pattern"),
        Regex::new(r"(?i)WAITFOR\s+DELAY").expect("Invalid regex: WAITFOR DELAY pattern"),
        Regex::new(r"(?i)0x[0-9a-fA-F]+").expect("Invalid regex: hex string pattern"),
        Regex::new(r"(?i)CHAR\s*\(\s*\d+\s*\)").expect("Invalid regex: CHAR function pattern"),
    ]
});

/// SQL query validator.
#[derive(Debug, Clone)]
pub struct SqlValidator {
    max_query_length: usize,
    allow_subqueries: bool,
    allowed_schemas: Vec<String>,
    strict_mode: bool,
}

impl Default for SqlValidator {
    fn default() -> Self {
        Self {
            max_query_length: 10000,
            allow_subqueries: true,
            allowed_schemas: vec!["public".into(), "dbo".into()],
            strict_mode: true,
        }
    }
}

impl SqlValidator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_query_length(mut self, length: usize) -> Self {
        self.max_query_length = length;
        self
    }

    pub fn allow_subqueries(mut self, allow: bool) -> Self {
        self.allow_subqueries = allow;
        self
    }

    pub fn allowed_schemas(mut self, schemas: Vec<String>) -> Self {
        self.allowed_schemas = schemas;
        self
    }

    pub fn strict_mode(mut self, strict: bool) -> Self {
        self.strict_mode = strict;
        self
    }

    /// Validate a SQL query.
    pub fn validate(&self, query: &str) -> SecurityResult<()> {
        debug!("Validating query: {}", &query[..query.len().min(100)]);

        // Check length
        self.check_length(query)?;

        // Check for forbidden keywords
        self.check_forbidden_keywords(query)?;

        // Check for injection patterns
        self.check_injection_patterns(query)?;

        // Validate query structure
        self.validate_structure(query)?;

        debug!("Query validation passed");
        Ok(())
    }

    /// Check query length.
    fn check_length(&self, query: &str) -> SecurityResult<()> {
        if query.len() > self.max_query_length {
            return Err(SecurityError::QueryTooComplex(format!(
                "Query exceeds maximum length of {} characters",
                self.max_query_length
            )));
        }
        Ok(())
    }

    /// Check for forbidden SQL keywords.
    fn check_forbidden_keywords(&self, query: &str) -> SecurityResult<()> {
        let upper_query = query.to_uppercase();
        let words: Vec<&str> = upper_query
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .filter(|s| !s.is_empty())
            .collect();

        for word in words {
            if FORBIDDEN_KEYWORDS.contains(word) {
                warn!("Forbidden keyword detected: {}", word);
                return Err(SecurityError::ForbiddenKeyword(word.to_string()));
            }
            // Check for prefixes like XP_, SP_
            if word.starts_with("XP_") || word.starts_with("SP_") {
                warn!("Forbidden system procedure detected: {}", word);
                return Err(SecurityError::ForbiddenKeyword(word.to_string()));
            }
        }
        Ok(())
    }

    /// Check for SQL injection patterns.
    fn check_injection_patterns(&self, query: &str) -> SecurityResult<()> {
        for pattern in INJECTION_PATTERNS.iter() {
            if pattern.is_match(query) {
                warn!("SQL injection pattern detected: {}", pattern.as_str());
                return Err(SecurityError::SqlInjection(format!(
                    "Suspicious pattern detected: {}",
                    pattern.as_str()
                )));
            }
        }
        Ok(())
    }

    /// Validate query structure.
    fn validate_structure(&self, query: &str) -> SecurityResult<()> {
        let trimmed = query.trim();

        // Must start with SELECT, WITH, or EXPLAIN
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT")
            && !upper.starts_with("WITH")
            && !upper.starts_with("EXPLAIN")
            && !upper.starts_with("SHOW")
            && !upper.starts_with("DESCRIBE")
            && !upper.starts_with("SET SHOWPLAN")
        {
            return Err(SecurityError::QueryNotAllowed(
                "Only SELECT, WITH, EXPLAIN, SHOW, and DESCRIBE queries are allowed".into(),
            ));
        }

        // Check for multiple statements
        if self.strict_mode {
            let semicolon_count = query.matches(';').count();
            // Allow one trailing semicolon
            if semicolon_count > 1 {
                return Err(SecurityError::QueryNotAllowed(
                    "Multiple statements not allowed".into(),
                ));
            }
        }

        // Check subqueries if not allowed
        if !self.allow_subqueries {
            let paren_count = query.matches('(').count();
            if paren_count > 2 {
                // Allow simple function calls
                return Err(SecurityError::QueryTooComplex(
                    "Subqueries not allowed".into(),
                ));
            }
        }

        Ok(())
    }

    /// Sanitize a query (basic cleanup).
    ///
    /// Removes comments, trailing semicolons, and normalizes whitespace.
    pub fn sanitize(&self, query: &str) -> String {
        let mut sanitized = query.trim().to_string();

        // Remove comments first (before removing semicolons)
        sanitized = COMMENT_REGEX.replace_all(&sanitized, "").to_string();
        sanitized = BLOCK_COMMENT_REGEX.replace_all(&sanitized, "").to_string();

        // Trim again after removing comments
        sanitized = sanitized.trim().to_string();

        // Remove trailing semicolons
        while sanitized.ends_with(';') {
            sanitized.pop();
        }

        // Normalize whitespace
        sanitized = WHITESPACE_REGEX.replace_all(&sanitized, " ").to_string();

        sanitized.trim().to_string()
    }
}

/// Validation result with details.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationResult {
    pub fn valid() -> Self {
        Self {
            is_valid: true,
            errors: vec![],
            warnings: vec![],
        }
    }

    pub fn invalid(error: impl Into<String>) -> Self {
        Self {
            is_valid: false,
            errors: vec![error.into()],
            warnings: vec![],
        }
    }

    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_select() {
        let validator = SqlValidator::new();
        assert!(validator.validate("SELECT * FROM users").is_ok());
        assert!(
            validator
                .validate("SELECT id, name FROM users WHERE id = 1")
                .is_ok()
        );
        assert!(
            validator
                .validate("WITH cte AS (SELECT 1) SELECT * FROM cte")
                .is_ok()
        );
    }

    #[test]
    fn test_forbidden_keywords() {
        let validator = SqlValidator::new();
        assert!(validator.validate("DROP TABLE users").is_err());
        assert!(validator.validate("DELETE FROM users").is_err());
        assert!(validator.validate("INSERT INTO users VALUES (1)").is_err());
        assert!(
            validator
                .validate("UPDATE users SET name = 'test'")
                .is_err()
        );
    }

    #[test]
    fn test_injection_patterns() {
        let validator = SqlValidator::new();
        assert!(
            validator
                .validate("SELECT * FROM users; DROP TABLE users")
                .is_err()
        );
        assert!(
            validator
                .validate("SELECT * FROM users UNION SELECT * FROM passwords")
                .is_err()
        );
        assert!(
            validator
                .validate("SELECT * FROM users WHERE 1=1 --")
                .is_err()
        );
    }

    #[test]
    fn test_sanitize() {
        let validator = SqlValidator::new();
        let sanitized = validator.sanitize("SELECT * FROM users;  -- comment\n");
        assert_eq!(sanitized, "SELECT * FROM users");

        let sanitized2 = validator.sanitize("SELECT  *   FROM   users /* block */");
        assert_eq!(sanitized2, "SELECT * FROM users");
    }

    #[test]
    fn test_query_length() {
        let validator = SqlValidator::new().max_query_length(50);
        let long_query =
            "SELECT * FROM users WHERE name = 'a very long name that exceeds the limit'";
        assert!(validator.validate(long_query).is_err());
    }
}
