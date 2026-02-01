//! Configuration types and builders.

use crate::error::{ConfigError, McpError, Result};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::env;
use std::time::Duration;

/// Database type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    #[default]
    Postgres,
    Mssql,
}

impl DatabaseType {
    /// Parse a database type from a string.
    ///
    /// Accepts various common aliases for each database type.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Some(Self::Postgres),
            "mssql" | "sqlserver" | "sql_server" => Some(Self::Mssql),
            _ => None,
        }
    }
}

impl TryFrom<&str> for DatabaseType {
    type Error = ConfigError;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        Self::parse(s).ok_or_else(|| ConfigError::InvalidValue {
            field: "database_type".into(),
            message: format!("Unknown database type: '{}'. Valid types: postgres, postgresql, pg, mssql, sqlserver, sql_server", s).into(),
        })
    }
}

impl TryFrom<String> for DatabaseType {
    type Error = ConfigError;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

/// Database connection configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub database_type: DatabaseType,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    #[serde(skip_serializing)]
    pub password: String,
    pub use_tls: bool,
    pub trust_cert: bool,
    pub pool_size: u32,
    pub connection_timeout: Duration,
    pub query_timeout: Duration,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            database_type: DatabaseType::default(),
            host: "localhost".into(),
            port: 5432,
            database: "postgres".into(),
            username: "postgres".into(),
            password: String::new(),
            use_tls: true,
            trust_cert: true,
            pool_size: 20,
            connection_timeout: Duration::from_secs(30),
            query_timeout: Duration::from_secs(60),
        }
    }
}

/// Builder for DatabaseConfig with fluent API.
#[derive(Default)]
pub struct DatabaseConfigBuilder {
    config: DatabaseConfig,
}

impl DatabaseConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn database_type(mut self, db_type: DatabaseType) -> Self {
        self.config.database_type = db_type;
        // Set default port based on database type
        self.config.port = match db_type {
            DatabaseType::Postgres => 5432,
            DatabaseType::Mssql => 1433,
        };
        self
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.config.host = host.into();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.config.database = database.into();
        self
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.config.username = username.into();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.config.password = password.into();
        self
    }

    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.config.use_tls = use_tls;
        self
    }

    pub fn trust_cert(mut self, trust_cert: bool) -> Self {
        self.config.trust_cert = trust_cert;
        self
    }

    pub fn pool_size(mut self, size: u32) -> Self {
        self.config.pool_size = size;
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.config.query_timeout = timeout;
        self
    }

    /// Build from environment variables.
    pub fn from_env(mut self) -> Result<Self> {
        if let Ok(db_type) = env::var("DATABASE_TYPE") {
            self.config.database_type = DatabaseType::parse(&db_type).ok_or_else(|| {
                McpError::Config(ConfigError::InvalidValue {
                    field: "DATABASE_TYPE".into(),
                    message: format!("Unknown database type: {}", db_type).into(),
                })
            })?;
        }

        if let Ok(host) = env::var("DATABASE_HOST") {
            self.config.host = host;
        }

        if let Ok(port) = env::var("DATABASE_PORT") {
            self.config.port = port.parse().map_err(|_| {
                McpError::Config(ConfigError::InvalidValue {
                    field: "DATABASE_PORT".into(),
                    message: "Invalid port number".into(),
                })
            })?;
        }

        if let Ok(database) = env::var("DATABASE_NAME") {
            self.config.database = database;
        }

        if let Ok(username) = env::var("DATABASE_USER") {
            self.config.username = username;
        }

        if let Ok(password) = env::var("DATABASE_PASSWORD") {
            self.config.password = password;
        }

        if let Ok(use_tls) = env::var("DATABASE_USE_TLS") {
            self.config.use_tls = use_tls.parse().unwrap_or(false);
        }

        if let Ok(trust_cert) = env::var("DATABASE_TRUST_CERT") {
            self.config.trust_cert = trust_cert.parse().unwrap_or(false);
        }

        if let Ok(pool_size) = env::var("DATABASE_POOL_SIZE") {
            self.config.pool_size = pool_size.parse().unwrap_or(10);
        }

        Ok(self)
    }

    /// Build from a connection URL.
    pub fn from_url(mut self, url: &str) -> Result<Self> {
        // Parse URL format: type://user:pass@host:port/database
        let url = url.trim();

        let (db_type, rest) = url
            .split_once("://")
            .ok_or_else(|| ConfigError::InvalidDatabaseUrl("Missing protocol".into()))?;

        self.config.database_type = DatabaseType::parse(db_type)
            .ok_or_else(|| ConfigError::InvalidDatabaseUrl(format!("Unknown type: {}", db_type)))?;

        // Parse credentials and host
        let (creds_host, database) = rest
            .rsplit_once('/')
            .ok_or_else(|| ConfigError::InvalidDatabaseUrl("Missing database name".into()))?;

        self.config.database = database.into();

        let (creds, host_port) = if creds_host.contains('@') {
            creds_host
                .split_once('@')
                .ok_or_else(|| ConfigError::InvalidDatabaseUrl("Invalid format".into()))?
        } else {
            ("", creds_host)
        };

        // Parse credentials
        if !creds.is_empty() {
            let (username, password) = creds.split_once(':').unwrap_or((creds, ""));
            self.config.username = username.into();
            self.config.password = password.into();
        }

        // Parse host and port
        let (host, port) = if host_port.contains(':') {
            host_port
                .split_once(':')
                .ok_or_else(|| ConfigError::InvalidDatabaseUrl("Invalid host:port".into()))?
        } else {
            (
                host_port,
                match self.config.database_type {
                    DatabaseType::Postgres => "5432",
                    DatabaseType::Mssql => "1433",
                },
            )
        };

        self.config.host = host.into();
        self.config.port = port
            .parse()
            .map_err(|_| ConfigError::InvalidDatabaseUrl("Invalid port".into()))?;

        Ok(self)
    }

    pub fn build(self) -> Result<DatabaseConfig> {
        self.validate()?;
        Ok(self.config)
    }

    fn validate(&self) -> Result<()> {
        if self.config.host.is_empty() {
            return Err(ConfigError::MissingField("host".into()).into());
        }
        if self.config.database.is_empty() {
            return Err(ConfigError::MissingField("database".into()).into());
        }
        if self.config.username.is_empty() {
            return Err(ConfigError::MissingField("username".into()).into());
        }
        if self.config.pool_size == 0 {
            return Err(ConfigError::InvalidValue {
                field: "pool_size".into(),
                message: "Pool size must be greater than 0".into(),
            }
            .into());
        }
        Ok(())
    }
}

/// Security configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub max_query_length: usize,
    pub rate_limit_per_second: u32,
    pub max_concurrent_queries: u32,
    pub allowed_schemas: Vec<String>,
    pub read_only: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            max_query_length: 10000,
            rate_limit_per_second: 100,
            max_concurrent_queries: 10,
            allowed_schemas: vec!["public".into(), "dbo".into()],
            read_only: true,
        }
    }
}

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub name: Cow<'static, str>,
    pub version: Cow<'static, str>,
    pub database: DatabaseConfig,
    pub security: SecurityConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            name: "r3nz-database-mcp".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            database: DatabaseConfig::default(),
            security: SecurityConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder::default()
    }
}

/// Builder for ServerConfig.
#[derive(Default)]
pub struct ServerConfigBuilder {
    config: ServerConfig,
}

impl ServerConfigBuilder {
    pub fn name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.config.name = name.into();
        self
    }

    pub fn database(mut self, database: DatabaseConfig) -> Self {
        self.config.database = database;
        self
    }

    pub fn security(mut self, security: SecurityConfig) -> Self {
        self.config.security = security;
        self
    }

    pub fn build(self) -> ServerConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_type_from_str() {
        assert_eq!(
            DatabaseType::parse("postgres"),
            Some(DatabaseType::Postgres)
        );
        assert_eq!(
            DatabaseType::parse("postgresql"),
            Some(DatabaseType::Postgres)
        );
        assert_eq!(DatabaseType::parse("mssql"), Some(DatabaseType::Mssql));
        assert_eq!(DatabaseType::parse("unknown"), None);
    }

    #[test]
    fn test_database_type_try_from() {
        assert_eq!(
            DatabaseType::try_from("postgres").unwrap(),
            DatabaseType::Postgres
        );
        assert_eq!(
            DatabaseType::try_from("MSSQL").unwrap(),
            DatabaseType::Mssql
        );
        assert!(DatabaseType::try_from("unknown").is_err());
    }

    #[test]
    fn test_database_config_builder() {
        let config = DatabaseConfigBuilder::new()
            .database_type(DatabaseType::Postgres)
            .host("localhost")
            .database("testdb")
            .username("user")
            .password("pass")
            .build()
            .unwrap();

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, "testdb");
    }

    #[test]
    fn test_from_url() {
        let config = DatabaseConfigBuilder::new()
            .from_url("postgres://user:pass@localhost:5432/mydb")
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(config.database_type, DatabaseType::Postgres);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, "mydb");
        assert_eq!(config.username, "user");
        assert_eq!(config.password, "pass");
    }
}
