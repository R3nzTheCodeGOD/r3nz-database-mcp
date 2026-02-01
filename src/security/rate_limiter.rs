//! Token bucket rate limiter.

use crate::error::{SecurityError, SecurityResult};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use tracing::{debug, warn};

/// Token bucket rate limiter.
pub struct RateLimiter {
    /// Maximum tokens (requests) allowed.
    max_tokens: u32,
    /// Tokens refilled per second.
    refill_rate: f64,
    /// Current token count.
    tokens: Mutex<f64>,
    /// Last refill time.
    last_refill: Mutex<Instant>,
    /// Current concurrent queries.
    concurrent_queries: AtomicU32,
    /// Maximum concurrent queries allowed.
    max_concurrent: u32,
}

impl RateLimiter {
    /// Create a new rate limiter.
    pub fn new(max_per_second: u32, max_concurrent: u32) -> Self {
        Self {
            max_tokens: max_per_second,
            refill_rate: max_per_second as f64,
            tokens: Mutex::new(max_per_second as f64),
            last_refill: Mutex::new(Instant::now()),
            concurrent_queries: AtomicU32::new(0),
            max_concurrent,
        }
    }

    /// Try to acquire a permit for a query.
    pub fn try_acquire(&self) -> SecurityResult<RateLimitGuard<'_>> {
        // Check concurrent limit
        let current = self.concurrent_queries.load(Ordering::SeqCst);
        if current >= self.max_concurrent {
            warn!(
                "Concurrent query limit exceeded: {}/{}",
                current, self.max_concurrent
            );
            return Err(SecurityError::ConcurrentLimitExceeded(self.max_concurrent));
        }

        // Check rate limit
        self.refill();

        let mut tokens = self.tokens.lock();
        if *tokens < 1.0 {
            warn!("Rate limit exceeded");
            return Err(SecurityError::RateLimitExceeded(self.max_tokens));
        }

        *tokens -= 1.0;
        self.concurrent_queries.fetch_add(1, Ordering::SeqCst);

        debug!(
            "Rate limit permit acquired: {:.1} tokens remaining, {} concurrent",
            *tokens,
            self.concurrent_queries.load(Ordering::SeqCst)
        );

        Ok(RateLimitGuard { limiter: self })
    }

    /// Refill tokens based on elapsed time.
    fn refill(&self) {
        let mut last_refill = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill).as_secs_f64();

        if elapsed > 0.0 {
            let mut tokens = self.tokens.lock();
            let new_tokens = elapsed * self.refill_rate;
            *tokens = (*tokens + new_tokens).min(self.max_tokens as f64);
            *last_refill = now;
        }
    }

    /// Release a concurrent query slot.
    fn release(&self) {
        self.concurrent_queries.fetch_sub(1, Ordering::SeqCst);
        debug!(
            "Rate limit permit released: {} concurrent",
            self.concurrent_queries.load(Ordering::SeqCst)
        );
    }

    /// Get current status.
    pub fn status(&self) -> RateLimitStatus {
        self.refill();
        let tokens = *self.tokens.lock();
        let concurrent = self.concurrent_queries.load(Ordering::SeqCst);

        RateLimitStatus {
            available_tokens: tokens as u32,
            max_tokens: self.max_tokens,
            concurrent_queries: concurrent,
            max_concurrent: self.max_concurrent,
        }
    }
}

/// RAII guard that releases the rate limit permit on drop.
pub struct RateLimitGuard<'a> {
    limiter: &'a RateLimiter,
}

impl<'a> Drop for RateLimitGuard<'a> {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

/// Rate limiter status.
#[derive(Debug, Clone)]
pub struct RateLimitStatus {
    pub available_tokens: u32,
    pub max_tokens: u32,
    pub concurrent_queries: u32,
    pub max_concurrent: u32,
}

impl RateLimitStatus {
    pub fn utilization(&self) -> f64 {
        1.0 - (self.available_tokens as f64 / self.max_tokens as f64)
    }

    pub fn concurrent_utilization(&self) -> f64 {
        self.concurrent_queries as f64 / self.max_concurrent as f64
    }
}

/// Rate limiter builder.
pub struct RateLimiterBuilder {
    max_per_second: u32,
    max_concurrent: u32,
}

impl Default for RateLimiterBuilder {
    fn default() -> Self {
        Self {
            max_per_second: 100,
            max_concurrent: 10,
        }
    }
}

impl RateLimiterBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_per_second(mut self, rate: u32) -> Self {
        self.max_per_second = rate;
        self
    }

    pub fn max_concurrent(mut self, limit: u32) -> Self {
        self.max_concurrent = limit;
        self
    }

    pub fn build(self) -> RateLimiter {
        RateLimiter::new(self.max_per_second, self.max_concurrent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_creation() {
        let limiter = RateLimiter::new(10, 5);
        let status = limiter.status();
        assert_eq!(status.max_tokens, 10);
        assert_eq!(status.max_concurrent, 5);
    }

    #[test]
    fn test_acquire_permit() {
        let limiter = RateLimiter::new(10, 5);
        let guard = limiter.try_acquire().unwrap();
        let status = limiter.status();
        assert_eq!(status.concurrent_queries, 1);
        assert!(status.available_tokens < 10);
        drop(guard);
        let status = limiter.status();
        assert_eq!(status.concurrent_queries, 0);
    }

    #[test]
    fn test_concurrent_limit() {
        let limiter = RateLimiter::new(100, 2);
        let _guard1 = limiter.try_acquire().unwrap();
        let _guard2 = limiter.try_acquire().unwrap();
        let result = limiter.try_acquire();
        assert!(result.is_err());
    }

    #[test]
    fn test_rate_limit() {
        let limiter = RateLimiter::new(2, 10);
        let _guard1 = limiter.try_acquire().unwrap();
        let _guard2 = limiter.try_acquire().unwrap();
        let result = limiter.try_acquire();
        assert!(result.is_err());
    }

    #[test]
    fn test_builder() {
        let limiter = RateLimiterBuilder::new()
            .max_per_second(50)
            .max_concurrent(20)
            .build();
        let status = limiter.status();
        assert_eq!(status.max_tokens, 50);
        assert_eq!(status.max_concurrent, 20);
    }
}
