//! Server state management.

use crate::config::ServerConfig;
use crate::database::{ConnectionManager, DatabaseDriver};
use crate::error::DbResult;
use crate::protocol::ClientInfo;
use crate::security::{RateLimiter, SqlValidator};
use crate::tools::ToolRegistry;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub struct ServerState {
    pub config: ServerConfig,
    pub connection_manager: Arc<ConnectionManager>,
    pub tools: ToolRegistry,
    pub validator: SqlValidator,
    pub rate_limiter: Arc<RateLimiter>,
    initialized: AtomicBool,
    client_info: RwLock<Option<ClientInfo>>,
    request_count: AtomicU64,
}

impl ServerState {
    pub fn new(
        config: ServerConfig,
        connection_manager: Arc<ConnectionManager>,
        tools: ToolRegistry,
        validator: SqlValidator,
        rate_limiter: Arc<RateLimiter>,
    ) -> Self {
        Self {
            config,
            connection_manager,
            tools,
            validator,
            rate_limiter,
            initialized: AtomicBool::new(false),
            client_info: RwLock::new(None),
            request_count: AtomicU64::new(0),
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    pub fn set_initialized(&self, client_info: ClientInfo) {
        *self.client_info.write() = Some(client_info);
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub fn client_info(&self) -> Option<ClientInfo> {
        self.client_info.read().clone()
    }

    pub fn next_request_id(&self) -> u64 {
        self.request_count.fetch_add(1, Ordering::SeqCst)
    }

    pub fn request_count(&self) -> u64 {
        self.request_count.load(Ordering::SeqCst)
    }

    pub fn get_driver(&self) -> DbResult<Arc<dyn DatabaseDriver>> {
        self.connection_manager.get_driver()
    }

    pub fn is_connected(&self) -> bool {
        self.connection_manager.is_connected()
    }
}

pub struct ServerStateBuilder {
    config: Option<ServerConfig>,
    connection_manager: Option<Arc<ConnectionManager>>,
    validator: Option<SqlValidator>,
    rate_limiter: Option<Arc<RateLimiter>>,
}

impl ServerStateBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            connection_manager: None,
            validator: None,
            rate_limiter: None,
        }
    }

    pub fn config(mut self, config: ServerConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn connection_manager(mut self, connection_manager: Arc<ConnectionManager>) -> Self {
        self.connection_manager = Some(connection_manager);
        self
    }

    pub fn validator(mut self, validator: SqlValidator) -> Self {
        self.validator = Some(validator);
        self
    }

    pub fn rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    pub fn build(self) -> Result<ServerState, &'static str> {
        let config = self.config.ok_or("Config is required")?;
        let validator = self.validator.unwrap_or_default();
        let rate_limiter = self.rate_limiter.unwrap_or_else(|| {
            Arc::new(RateLimiter::new(
                config.security.rate_limit_per_second,
                config.security.max_concurrent_queries,
            ))
        });

        let connection_manager = self
            .connection_manager
            .unwrap_or_else(|| Arc::new(ConnectionManager::new()));

        let tools = crate::tools::create_registry(
            Arc::clone(&connection_manager),
            validator.clone(),
            Arc::clone(&rate_limiter),
        );

        Ok(ServerState::new(
            config,
            connection_manager,
            tools,
            validator,
            rate_limiter,
        ))
    }
}

impl Default for ServerStateBuilder {
    fn default() -> Self {
        Self::new()
    }
}
