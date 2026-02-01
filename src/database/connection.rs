//! Thread-safe lazy connection initialization with `Arc<RwLock<Option<ConnectionState>>>`.

use crate::config::{DatabaseConfig, DatabaseType};
use crate::database::{DatabaseDriver, create_driver};
use crate::error::{DatabaseError, DbResult};
use parking_lot::RwLock;
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, info};

pub struct ConnectionState {
    pub driver: Arc<dyn DatabaseDriver>,
    pub config: ConnectionConfig,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub database_type: DatabaseType,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
}

impl ConnectionConfig {
    pub fn from_database_config(config: &DatabaseConfig) -> Self {
        Self {
            database_type: config.database_type,
            host: config.host.clone(),
            port: config.port,
            database: config.database.clone(),
            username: config.username.clone(),
        }
    }

    #[cfg(test)]
    pub fn with_database(&self, database: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionMetadata {
    pub connected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

impl ConnectionMetadata {
    pub fn disconnected() -> Self {
        Self {
            connected: false,
            database_type: None,
            host: None,
            port: None,
            database: None,
            username: None,
        }
    }

    pub fn from_config(config: &ConnectionConfig) -> Self {
        Self {
            connected: true,
            database_type: Some(match config.database_type {
                DatabaseType::Mssql => "mssql".to_string(),
                DatabaseType::Postgres => "postgres".to_string(),
            }),
            host: Some(config.host.clone()),
            port: Some(config.port),
            database: Some(config.database.clone()),
            username: Some(config.username.clone()),
        }
    }
}

impl ConnectionState {
    pub fn new(driver: Arc<dyn DatabaseDriver>, config: ConnectionConfig) -> Self {
        Self { driver, config }
    }

    pub fn metadata(&self) -> ConnectionMetadata {
        ConnectionMetadata::from_config(&self.config)
    }
}

pub struct ConnectionManager {
    state: Arc<RwLock<Option<ConnectionState>>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(None)),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.state.read().is_some()
    }

    pub fn metadata(&self) -> ConnectionMetadata {
        match &*self.state.read() {
            Some(state) => state.metadata(),
            None => ConnectionMetadata::disconnected(),
        }
    }

    /// Returns `NotConnected` error if no active connection.
    pub fn get_driver(&self) -> DbResult<Arc<dyn DatabaseDriver>> {
        self.state
            .read()
            .as_ref()
            .map(|s| Arc::clone(&s.driver))
            .ok_or(DatabaseError::NotConnected)
    }

    pub fn get_config(&self) -> Option<ConnectionConfig> {
        self.state.read().as_ref().map(|s| s.config.clone())
    }

    /// If already connected, disconnects first.
    pub async fn connect(&self, config: DatabaseConfig) -> DbResult<ConnectionMetadata> {
        if self.is_connected() {
            debug!("Disconnecting existing connection before new connection");
            self.disconnect()?;
        }

        info!(
            "Connecting to {} database at {}:{}",
            match config.database_type {
                DatabaseType::Mssql => "MSSQL",
                DatabaseType::Postgres => "PostgreSQL",
            },
            config.host,
            config.port
        );

        let driver = create_driver(config.clone()).await?;
        let connection_config = ConnectionConfig::from_database_config(&config);
        let state = ConnectionState::new(driver, connection_config);
        let metadata = state.metadata();

        *self.state.write() = Some(state);

        info!(
            "Connected to {} on {}",
            metadata.database.as_deref().unwrap_or("unknown"),
            metadata.host.as_deref().unwrap_or("unknown")
        );

        Ok(metadata)
    }

    pub fn disconnect(&self) -> DbResult<ConnectionMetadata> {
        let old_state = self.state.write().take();

        match old_state {
            Some(state) => {
                let metadata = state.metadata();
                info!(
                    "Disconnected from {} on {}",
                    metadata.database.as_deref().unwrap_or("unknown"),
                    metadata.host.as_deref().unwrap_or("unknown")
                );
                Ok(metadata)
            }
            None => Ok(ConnectionMetadata::disconnected()),
        }
    }

    /// Creates a new connection with the same credentials but different database.
    pub async fn switch_database(
        &self,
        new_database: &str,
        password: &str,
    ) -> DbResult<ConnectionMetadata> {
        // Get current config
        let current_config = self.get_config().ok_or(DatabaseError::NotConnected)?;

        info!(
            "Switching database from {} to {}",
            current_config.database, new_database
        );

        // Create new config with same server but different database
        let new_config = DatabaseConfig {
            database_type: current_config.database_type,
            host: current_config.host,
            port: current_config.port,
            database: new_database.to_string(),
            username: current_config.username,
            password: password.to_string(),
            use_tls: true,
            trust_cert: true,
            pool_size: 20,
            connection_timeout: std::time::Duration::from_secs(30),
            query_timeout: std::time::Duration::from_secs(60),
        };

        // Connect to new database (this handles disconnect)
        self.connect(new_config).await
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ConnectionManager {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_manager_initial_state() {
        let manager = ConnectionManager::new();
        assert!(!manager.is_connected());
        assert!(manager.get_driver().is_err());
    }

    #[test]
    fn test_connection_metadata_disconnected() {
        let metadata = ConnectionMetadata::disconnected();
        assert!(!metadata.connected);
        assert!(metadata.host.is_none());
    }

    #[test]
    fn test_connection_config_with_database() {
        let config = ConnectionConfig {
            database_type: DatabaseType::Mssql,
            host: "localhost".into(),
            port: 1433,
            database: "db1".into(),
            username: "sa".into(),
        };

        let new_config = config.with_database("db2");
        assert_eq!(new_config.database, "db2");
        assert_eq!(new_config.host, "localhost");
    }

    #[test]
    fn test_connection_metadata_serialization() {
        let metadata = ConnectionMetadata {
            connected: true,
            database_type: Some("mssql".into()),
            host: Some("localhost".into()),
            port: Some(1433),
            database: Some("test".into()),
            username: Some("sa".into()),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("\"connected\":true"));
        assert!(json.contains("\"database\":\"test\""));
    }
}
