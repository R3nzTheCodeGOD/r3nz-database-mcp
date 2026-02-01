//! Connection string parser supporting multiple formats.
//!
//! Supports:
//! - MSSQL ADO.NET format: `Server=host,port;Database=db;User Id=user;Password=pass`
//! - URL format: `mssql://user:pass@host:port/database`
//! - PostgreSQL format: `postgres://user:pass@host:port/database`

use crate::config::{DatabaseConfig, DatabaseConfigBuilder, DatabaseType};
use crate::error::DatabaseError;
use std::collections::HashMap;

pub type ParseResult<T> = Result<T, DatabaseError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionStringFormat {
    AdoNet,
    Url,
}

pub struct ConnectionStringParser;

impl ConnectionStringParser {
    /// Auto-detects format (ADO.NET vs URL) and parses accordingly.
    pub fn parse(connection_string: &str) -> ParseResult<DatabaseConfig> {
        let trimmed = connection_string.trim();

        if trimmed.is_empty() {
            return Err(DatabaseError::ConnectionFailed(
                "Connection string cannot be empty".into(),
            ));
        }

        match Self::detect_format(trimmed) {
            ConnectionStringFormat::AdoNet => Self::parse_adonet(trimmed),
            ConnectionStringFormat::Url => Self::parse_url(trimmed),
        }
    }

    fn detect_format(s: &str) -> ConnectionStringFormat {
        if s.contains("://") {
            ConnectionStringFormat::Url
        } else {
            ConnectionStringFormat::AdoNet
        }
    }

    /// Parses ADO.NET format: `Server=host,port;Database=db;User Id=user;Password=pass`
    ///
    /// Key aliases: Server/Data Source/Address, Database/Initial Catalog,
    /// User Id/User/UID, Password/Pwd
    fn parse_adonet(s: &str) -> ParseResult<DatabaseConfig> {
        let pairs = Self::parse_key_value_pairs(s);

        // Extract server (host and optional port)
        let server =
            Self::get_value(&pairs, &["server", "data source", "address"]).ok_or_else(|| {
                DatabaseError::ConnectionFailed("Missing 'Server' in connection string".into())
            })?;

        let (host, port) = Self::parse_server_string(&server)?;

        // Extract database
        let database =
            Self::get_value(&pairs, &["database", "initial catalog"]).ok_or_else(|| {
                DatabaseError::ConnectionFailed("Missing 'Database' in connection string".into())
            })?;

        // Extract credentials
        let username = Self::get_value(&pairs, &["user id", "user", "uid"]).ok_or_else(|| {
            DatabaseError::ConnectionFailed("Missing 'User Id' in connection string".into())
        })?;

        let password = Self::get_value(&pairs, &["password", "pwd"]).unwrap_or_default();

        // Extract optional settings
        let trust_cert = Self::get_value(&pairs, &["trustservercertificate", "trust_cert"])
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(true);

        let encrypt = Self::get_value(&pairs, &["encrypt"])
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(true);

        // Build config - default to MSSQL for ADO.NET format
        DatabaseConfigBuilder::new()
            .database_type(DatabaseType::Mssql)
            .host(host)
            .port(port)
            .database(database)
            .username(username)
            .password(password)
            .trust_cert(trust_cert)
            .use_tls(encrypt)
            .build()
            .map_err(|e| DatabaseError::ConnectionFailed(e.to_string()))
    }

    fn parse_url(s: &str) -> ParseResult<DatabaseConfig> {
        DatabaseConfigBuilder::new()
            .from_url(s)
            .map_err(|e| DatabaseError::ConnectionFailed(e.to_string()))?
            .build()
            .map_err(|e| DatabaseError::ConnectionFailed(e.to_string()))
    }

    fn parse_key_value_pairs(s: &str) -> HashMap<String, String> {
        let mut pairs = HashMap::new();

        for part in s.split(';') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim().to_lowercase();
                let value = value.trim().to_string();
                pairs.insert(key, value);
            }
        }

        pairs
    }

    fn get_value(pairs: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
        for key in keys {
            if let Some(value) = pairs.get(*key)
                && !value.is_empty()
            {
                return Some(value.clone());
            }
        }
        None
    }

    /// Parses server string: `hostname`, `hostname,port`, or `hostname:port`
    fn parse_server_string(server: &str) -> ParseResult<(String, u16)> {
        let server = server.trim();

        // Check for comma-separated port (MSSQL style)
        if let Some((host, port_str)) = server.split_once(',') {
            let port = port_str.trim().parse::<u16>().map_err(|_| {
                DatabaseError::ConnectionFailed(format!("Invalid port number: {}", port_str))
            })?;
            return Ok((host.trim().to_string(), port));
        }

        // Check for colon-separated port
        if let Some((host, port_str)) = server.rsplit_once(':')
            // Make sure it's not an IPv6 address without port
            && !host.contains(':')
            && let Ok(port) = port_str.parse::<u16>()
        {
            return Ok((host.to_string(), port));
        }

        // No port specified, use default MSSQL port
        Ok((server.to_string(), 1433))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_format_adonet() {
        assert_eq!(
            ConnectionStringParser::detect_format("Server=localhost;Database=test"),
            ConnectionStringFormat::AdoNet
        );
    }

    #[test]
    fn test_detect_format_url() {
        assert_eq!(
            ConnectionStringParser::detect_format("mssql://user:pass@localhost/db"),
            ConnectionStringFormat::Url
        );
        assert_eq!(
            ConnectionStringParser::detect_format("postgres://user@host/db"),
            ConnectionStringFormat::Url
        );
    }

    #[test]
    fn test_parse_adonet_full() {
        let conn_str =
            "Server=192.168.1.100,1433;Initial Catalog=MyDb;User Id=admin;Password=secret123";
        let config = ConnectionStringParser::parse(conn_str).unwrap();

        assert_eq!(config.database_type, DatabaseType::Mssql);
        assert_eq!(config.host, "192.168.1.100");
        assert_eq!(config.port, 1433);
        assert_eq!(config.database, "MyDb");
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "secret123");
    }

    #[test]
    fn test_parse_adonet_with_comma_port() {
        let conn_str = "Server=192.168.1.2,50872;Initial Catalog=testReposetDb;User Id=testreportsqluser;pwd=secret";
        let config = ConnectionStringParser::parse(conn_str).unwrap();

        assert_eq!(config.host, "192.168.1.2");
        assert_eq!(config.port, 50872);
        assert_eq!(config.database, "testReposetDb");
        assert_eq!(config.username, "testreportsqluser");
    }

    #[test]
    fn test_parse_adonet_alternative_keys() {
        let conn_str = "Data Source=localhost;Database=test;UID=user;Pwd=pass";
        let config = ConnectionStringParser::parse(conn_str).unwrap();

        assert_eq!(config.host, "localhost");
        assert_eq!(config.database, "test");
        assert_eq!(config.username, "user");
        assert_eq!(config.password, "pass");
    }

    #[test]
    fn test_parse_adonet_case_insensitive() {
        let conn_str = "SERVER=localhost;DATABASE=test;USER ID=admin;PASSWORD=secret";
        let config = ConnectionStringParser::parse(conn_str).unwrap();

        assert_eq!(config.host, "localhost");
        assert_eq!(config.database, "test");
        assert_eq!(config.username, "admin");
    }

    #[test]
    fn test_parse_adonet_default_port() {
        let conn_str = "Server=myserver;Database=mydb;User Id=sa;Password=pass";
        let config = ConnectionStringParser::parse(conn_str).unwrap();

        assert_eq!(config.port, 1433);
    }

    #[test]
    fn test_parse_url_mssql() {
        let conn_str = "mssql://admin:secret@localhost:1433/testdb";
        let config = ConnectionStringParser::parse(conn_str).unwrap();

        assert_eq!(config.database_type, DatabaseType::Mssql);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1433);
        assert_eq!(config.database, "testdb");
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "secret");
    }

    #[test]
    fn test_parse_url_postgres() {
        let conn_str = "postgres://user:pass@db.example.com:5432/mydb";
        let config = ConnectionStringParser::parse(conn_str).unwrap();

        assert_eq!(config.database_type, DatabaseType::Postgres);
        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 5432);
    }

    #[test]
    fn test_parse_empty_string() {
        let result = ConnectionStringParser::parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_adonet_missing_server() {
        let conn_str = "Database=test;User Id=user";
        let result = ConnectionStringParser::parse(conn_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_adonet_trailing_semicolon() {
        let conn_str = "Server=localhost;Database=test;User Id=user;Password=pass;";
        let config = ConnectionStringParser::parse(conn_str).unwrap();
        assert_eq!(config.host, "localhost");
    }
}
