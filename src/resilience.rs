//! Resilience utilities: retry logic, backoff, rate limiting, bulkheads.
//!
//! This module provides patterns to protect services from overload:
//!
//! - [`RetryConfig`]: Exponential backoff for transient failures
//! - [`RateLimiter`]: Token bucket to prevent thundering herd
//! - [`Bulkhead`]: Semaphore to limit concurrent operations
//!
//! # Example
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), replication_engine::resilience::BulkheadFull> {
//! use replication_engine::resilience::{RateLimiter, Bulkhead, RateLimitConfig};
//!
//! // Rate limit: max 1000 events/sec with burst of 100
//! let limiter = RateLimiter::new(RateLimitConfig::default());
//! limiter.acquire().await; // Blocks if over limit
//!
//! // Bulkhead: max 10 concurrent connections
//! let bulkhead = Bulkhead::new(10);
//! let _permit = bulkhead.acquire().await?;
//! // permit dropped = slot released
//! # Ok(())
//! # }
//! ```

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use governor::{Quota, RateLimiter as GovLimiter, state::{InMemoryState, NotKeyed}, clock::DefaultClock, middleware::NoOpMiddleware};
use tokio::sync::{Semaphore, OwnedSemaphorePermit};

/// Configuration for connection retry behavior.
///
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    /// Set to `usize::MAX` for infinite retries (daemon mode).
    pub max_attempts: usize,

    /// Initial delay before first retry.
    pub initial_delay: Duration,

    /// Maximum delay between retries (ceiling for exponential backoff).
    pub max_delay: Duration,

    /// Backoff multiplier (e.g., 2.0 = double delay each retry).
    pub backoff_factor: f64,

    /// Timeout for each individual connection attempt.
    pub connection_timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_factor: 2.0,
            connection_timeout: Duration::from_secs(5),
        }
    }
}

impl RetryConfig {
    /// Fast-fail retry for initial startup connection.
    ///
    /// Attempts connection 20 times with exponential backoff, failing after
    /// approximately 30 seconds total. Use this during daemon startup to
    /// detect configuration errors quickly.
    ///
    /// # Backoff Schedule
    ///
    /// ```text
    /// Attempt  Delay     Cumulative
    /// -------  -----     ----------
    /// 1        500ms     500ms
    /// 2        750ms     1.25s
    /// 3        1.12s     2.37s
    /// ...
    /// 20       30s       ~45s (total)
    /// ```
    pub fn startup() -> Self {
        Self {
            max_attempts: 20,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            backoff_factor: 1.5,
            connection_timeout: Duration::from_secs(10),
        }
    }

    /// Infinite retry for long-running daemon (never give up!).
    ///
    /// Retries forever with exponential backoff capped at 5 minutes.
    /// Use this for runtime reconnection after initial startup succeeds.
    ///
    /// # Backoff Schedule
    ///
    /// ```text
    /// Attempt  Delay     Reasoning
    /// -------  -----     ---------
    /// 1        1s        Immediate transient retry
    /// 2        2s        Brief network blip
    /// 3        4s        DNS propagation
    /// 4        8s        Container restart
    /// 5        16s       Service recovery
    /// 6        32s       Load balancer failover
    /// 7        64s       Datacenter maintenance
    /// 8        128s      Extended outage
    /// 9        256s      Multi-hour incident
    /// 10+      300s      Cap at 5 minutes, retry forever
    /// ```
    ///
    /// Real-world example: DNS outages can last 24+ hours. This ensures
    /// the daemon automatically recovers without manual restart.
    pub fn daemon() -> Self {
        Self {
            max_attempts: usize::MAX, // Infinite retries
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300), // Cap at 5 minutes
            backoff_factor: 2.0,
            connection_timeout: Duration::from_secs(30),
        }
    }

    /// Fast-fail retry for tests.
    ///
    /// Fails quickly to avoid slow tests.
    pub fn testing() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            backoff_factor: 2.0,
            connection_timeout: Duration::from_millis(500), // Fast timeout for tests
        }
    }

    /// Calculate delay for a given attempt number (1-indexed).
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return self.initial_delay;
        }

        let multiplier = self.backoff_factor.powi((attempt - 1) as i32);
        let delay_secs = self.initial_delay.as_secs_f64() * multiplier;
        let delay = Duration::from_secs_f64(delay_secs);

        std::cmp::min(delay, self.max_delay)
    }
}

// =============================================================================
// Rate Limiting
// =============================================================================

/// Configuration for rate limiting.
///
/// Uses a token bucket algorithm: tokens refill at `refill_rate` per second,
/// up to `burst_size` tokens. Each operation consumes one token.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum tokens that can be accumulated (burst capacity).
    pub burst_size: u32,

    /// Tokens added per second (sustained rate).
    pub refill_rate: u32,
}

impl Default for RateLimitConfig {
    /// Default: 1000 ops/sec with burst of 100.
    fn default() -> Self {
        Self {
            burst_size: 100,
            refill_rate: 1000,
        }
    }
}

impl RateLimitConfig {
    /// Aggressive rate limit for testing or constrained environments.
    pub fn conservative() -> Self {
        Self {
            burst_size: 10,
            refill_rate: 100,
        }
    }

    /// High throughput for production with beefy hardware.
    pub fn high_throughput() -> Self {
        Self {
            burst_size: 500,
            refill_rate: 10_000,
        }
    }

    /// No rate limiting (unlimited).
    pub fn unlimited() -> Self {
        Self {
            burst_size: u32::MAX,
            refill_rate: u32::MAX,
        }
    }
}

/// Token bucket rate limiter.
///
/// Prevents thundering herd by limiting the rate of operations.
/// Thread-safe and async-aware.
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() {
/// use replication_engine::resilience::{RateLimiter, RateLimitConfig};
/// let limiter = RateLimiter::new(RateLimitConfig::default());
///
/// // In hot path loop:
/// let events = vec![1, 2, 3];
/// for event in events {
///     limiter.acquire().await; // Blocks if over limit
///     // process(event);
/// }
/// # }
/// ```
pub struct RateLimiter {
    limiter: GovLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>,
    config: RateLimitConfig,
}

impl RateLimiter {
    /// Create a new rate limiter with the given configuration.
    pub fn new(config: RateLimitConfig) -> Self {
        // Create quota: `burst_size` tokens, refilling at `refill_rate` per second
        let quota = Quota::per_second(NonZeroU32::new(config.refill_rate).unwrap_or(NonZeroU32::MIN))
            .allow_burst(NonZeroU32::new(config.burst_size).unwrap_or(NonZeroU32::MIN));

        let limiter = GovLimiter::direct(quota);

        Self { limiter, config }
    }

    /// Acquire a permit, blocking until one is available.
    ///
    /// This method is cancel-safe.
    pub async fn acquire(&self) {
        self.limiter.until_ready().await;
    }

    /// Try to acquire a permit without blocking.
    ///
    /// Returns `true` if acquired, `false` if rate limit exceeded.
    pub fn try_acquire(&self) -> bool {
        self.limiter.check().is_ok()
    }

    /// Acquire multiple permits at once.
    ///
    /// Useful for batched operations where you want to rate limit
    /// the batch as a whole rather than each item.
    pub async fn acquire_many(&self, n: u32) {
        if n == 0 {
            return;
        }
        // For multiple permits, we loop and acquire one at a time
        // This is simpler than trying to batch with governor
        for _ in 0..n {
            self.limiter.until_ready().await;
        }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }
}

// =============================================================================
// Bulkhead (Concurrency Limiter)
// =============================================================================

/// Error when bulkhead is full.
#[derive(Debug, Clone, thiserror::Error)]
#[error("bulkhead full: max {max_concurrent} concurrent operations")]
pub struct BulkheadFull {
    /// Maximum concurrent operations allowed.
    pub max_concurrent: usize,
}

/// Bulkhead pattern: limits concurrent operations to prevent resource exhaustion.
///
/// Uses a semaphore to limit how many operations can run simultaneously.
/// When the bulkhead is "full", new operations either wait or fail fast.
///
/// # Use Cases
///
/// - Limit concurrent peer connections
/// - Limit concurrent sync-engine calls
/// - Prevent thread/connection pool exhaustion
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() -> Result<(), replication_engine::resilience::BulkheadFull> {
/// use replication_engine::resilience::Bulkhead;
/// let bulkhead = Bulkhead::new(10); // Max 10 concurrent
///
/// // Blocking acquire
/// let permit = bulkhead.acquire().await?;
/// // do_work().await;
/// drop(permit); // Release slot
///
/// // Non-blocking (fail fast)
/// if let Some(permit) = bulkhead.try_acquire() {
///     // do_work().await;
///     drop(permit);
/// } else {
///     // return Err("service overloaded");
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Bulkhead {
    semaphore: Arc<Semaphore>,
    max_concurrent: usize,
}

impl Bulkhead {
    /// Create a new bulkhead with the given concurrency limit.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_concurrent,
        }
    }

    /// Create a bulkhead for limiting peer connections.
    ///
    /// Default: 50 concurrent connections.
    pub fn for_peers() -> Self {
        Self::new(50)
    }

    /// Create a bulkhead for limiting sync-engine operations.
    ///
    /// Default: 100 concurrent operations.
    pub fn for_sync_engine() -> Self {
        Self::new(100)
    }

    /// Acquire a permit, waiting if necessary.
    ///
    /// Returns a permit that releases the slot when dropped.
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, BulkheadFull> {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BulkheadFull {
                max_concurrent: self.max_concurrent,
            })
    }

    /// Try to acquire a permit without waiting.
    ///
    /// Returns `None` if the bulkhead is full.
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.semaphore.clone().try_acquire_owned().ok()
    }

    /// Get the number of available permits.
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get the maximum concurrent operations allowed.
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    /// Check if the bulkhead is full (no permits available).
    pub fn is_full(&self) -> bool {
        self.semaphore.available_permits() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daemon_config() {
        let config = RetryConfig::daemon();
        assert_eq!(config.max_attempts, usize::MAX);
        assert_eq!(config.max_delay, Duration::from_secs(300));
    }

    #[test]
    fn test_startup_config() {
        let config = RetryConfig::startup();
        assert_eq!(config.max_attempts, 20);
        assert_eq!(config.initial_delay, Duration::from_millis(500));
    }

    #[test]
    fn test_delay_for_attempt() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            backoff_factor: 2.0,
            connection_timeout: Duration::from_secs(5),
        };

        assert_eq!(config.delay_for_attempt(1), Duration::from_secs(1));
        assert_eq!(config.delay_for_attempt(2), Duration::from_secs(2));
        assert_eq!(config.delay_for_attempt(3), Duration::from_secs(4));
        assert_eq!(config.delay_for_attempt(4), Duration::from_secs(8));
        assert_eq!(config.delay_for_attempt(5), Duration::from_secs(16));
        // Should cap at max_delay
        assert_eq!(config.delay_for_attempt(10), Duration::from_secs(30));
    }

    // =========================================================================
    // Rate Limiter Tests
    // =========================================================================

    #[test]
    fn test_rate_limit_config_presets() {
        let default = RateLimitConfig::default();
        assert_eq!(default.burst_size, 100);
        assert_eq!(default.refill_rate, 1000);

        let conservative = RateLimitConfig::conservative();
        assert_eq!(conservative.burst_size, 10);
        assert_eq!(conservative.refill_rate, 100);

        let high = RateLimitConfig::high_throughput();
        assert_eq!(high.burst_size, 500);
        assert_eq!(high.refill_rate, 10_000);
    }

    #[test]
    fn test_rate_limiter_try_acquire_burst() {
        let limiter = RateLimiter::new(RateLimitConfig {
            burst_size: 5,
            refill_rate: 1000,
        });

        // Should be able to acquire burst_size permits immediately
        for _ in 0..5 {
            assert!(limiter.try_acquire(), "should acquire within burst");
        }

        // Next acquire should fail (burst exhausted)
        assert!(!limiter.try_acquire(), "should fail after burst exhausted");
    }

    #[tokio::test]
    async fn test_rate_limiter_acquire_blocks() {
        let limiter = RateLimiter::new(RateLimitConfig {
            burst_size: 1,
            refill_rate: 1000, // 1ms per token
        });

        // Exhaust burst
        limiter.acquire().await;

        // Next acquire should complete quickly (high refill rate)
        let start = std::time::Instant::now();
        limiter.acquire().await;
        let elapsed = start.elapsed();

        // Should be roughly 1ms (1000/sec = 1 per ms), but allow for scheduling
        assert!(elapsed < Duration::from_millis(100), "should refill quickly");
    }

    #[tokio::test]
    async fn test_rate_limiter_acquire_many() {
        let limiter = RateLimiter::new(RateLimitConfig {
            burst_size: 10,
            refill_rate: 10_000,
        });

        // Should acquire all 10 quickly
        let start = std::time::Instant::now();
        limiter.acquire_many(10).await;
        let elapsed = start.elapsed();

        assert!(elapsed < Duration::from_millis(50), "batch acquire should be fast");
    }

    // =========================================================================
    // Bulkhead Tests
    // =========================================================================

    #[test]
    fn test_bulkhead_new() {
        let bulkhead = Bulkhead::new(10);
        assert_eq!(bulkhead.max_concurrent(), 10);
        assert_eq!(bulkhead.available(), 10);
        assert!(!bulkhead.is_full());
    }

    #[test]
    fn test_bulkhead_presets() {
        let peers = Bulkhead::for_peers();
        assert_eq!(peers.max_concurrent(), 50);

        let sync = Bulkhead::for_sync_engine();
        assert_eq!(sync.max_concurrent(), 100);
    }

    #[test]
    fn test_bulkhead_try_acquire() {
        let bulkhead = Bulkhead::new(2);

        let p1 = bulkhead.try_acquire();
        assert!(p1.is_some());
        assert_eq!(bulkhead.available(), 1);

        let p2 = bulkhead.try_acquire();
        assert!(p2.is_some());
        assert_eq!(bulkhead.available(), 0);
        assert!(bulkhead.is_full());

        // Should fail - bulkhead full
        let p3 = bulkhead.try_acquire();
        assert!(p3.is_none());

        // Drop one permit
        drop(p1);
        assert_eq!(bulkhead.available(), 1);
        assert!(!bulkhead.is_full());

        // Now should succeed
        let p4 = bulkhead.try_acquire();
        assert!(p4.is_some());
    }

    #[tokio::test]
    async fn test_bulkhead_acquire_waits() {
        let bulkhead = Arc::new(Bulkhead::new(1));
        let bulkhead2 = Arc::clone(&bulkhead);

        // Acquire the only permit
        let permit = bulkhead.acquire().await.unwrap();
        assert!(bulkhead.is_full());

        // Spawn a task that will wait for the permit
        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            let _p = bulkhead2.acquire().await.unwrap();
            start.elapsed()
        });

        // Wait a bit, then release
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(permit);

        // The waiting task should complete
        let wait_time = handle.await.unwrap();
        assert!(wait_time >= Duration::from_millis(40), "should have waited");
    }

    #[test]
    fn test_bulkhead_full_error() {
        let err = BulkheadFull { max_concurrent: 10 };
        assert_eq!(
            err.to_string(),
            "bulkhead full: max 10 concurrent operations"
        );
    }

    // =========================================================================
    // Additional RetryConfig Tests
    // =========================================================================

    #[test]
    fn test_retry_config_testing_preset() {
        let config = RetryConfig::testing();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(10));
        assert_eq!(config.max_delay, Duration::from_millis(100));
        assert_eq!(config.connection_timeout, Duration::from_millis(500));
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 10);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert_eq!(config.backoff_factor, 2.0);
        assert_eq!(config.connection_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_delay_for_attempt_zero() {
        let config = RetryConfig::default();
        // Attempt 0 should return initial_delay
        assert_eq!(config.delay_for_attempt(0), config.initial_delay);
    }

    #[test]
    fn test_delay_for_attempt_caps_at_max() {
        let config = RetryConfig {
            max_attempts: 100,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            backoff_factor: 2.0,
            connection_timeout: Duration::from_secs(5),
        };
        // After enough attempts (but not too many to overflow), should cap at max_delay
        assert_eq!(config.delay_for_attempt(10), Duration::from_secs(10));
        assert_eq!(config.delay_for_attempt(20), Duration::from_secs(10));
    }

    #[test]
    fn test_retry_config_clone() {
        let config = RetryConfig::daemon();
        let cloned = config.clone();
        assert_eq!(cloned.max_attempts, config.max_attempts);
        assert_eq!(cloned.max_delay, config.max_delay);
    }

    #[test]
    fn test_retry_config_debug() {
        let config = RetryConfig::testing();
        let debug = format!("{:?}", config);
        assert!(debug.contains("RetryConfig"));
        assert!(debug.contains("max_attempts"));
    }

    // =========================================================================
    // Additional RateLimitConfig Tests
    // =========================================================================

    #[test]
    fn test_rate_limit_config_unlimited() {
        let config = RateLimitConfig::unlimited();
        assert_eq!(config.burst_size, u32::MAX);
        assert_eq!(config.refill_rate, u32::MAX);
    }

    #[test]
    fn test_rate_limit_config_clone() {
        let config = RateLimitConfig::conservative();
        let cloned = config.clone();
        assert_eq!(cloned.burst_size, config.burst_size);
        assert_eq!(cloned.refill_rate, config.refill_rate);
    }

    #[test]
    fn test_rate_limit_config_debug() {
        let config = RateLimitConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("RateLimitConfig"));
        assert!(debug.contains("burst_size"));
        assert!(debug.contains("refill_rate"));
    }

    #[test]
    fn test_rate_limiter_config_accessor() {
        let config = RateLimitConfig::conservative();
        let limiter = RateLimiter::new(config.clone());
        let retrieved = limiter.config();
        assert_eq!(retrieved.burst_size, config.burst_size);
        assert_eq!(retrieved.refill_rate, config.refill_rate);
    }

    #[tokio::test]
    async fn test_rate_limiter_acquire_many_zero() {
        let limiter = RateLimiter::new(RateLimitConfig::default());
        // Acquiring 0 should be instant
        let start = std::time::Instant::now();
        limiter.acquire_many(0).await;
        assert!(start.elapsed() < Duration::from_millis(1));
    }

    // =========================================================================
    // Additional Bulkhead Tests
    // =========================================================================

    #[test]
    fn test_bulkhead_debug() {
        let bulkhead = Bulkhead::new(5);
        let debug = format!("{:?}", bulkhead);
        assert!(debug.contains("Bulkhead"));
    }

    #[test]
    fn test_bulkhead_full_error_clone() {
        let err = BulkheadFull { max_concurrent: 25 };
        let cloned = err.clone();
        assert_eq!(cloned.max_concurrent, 25);
    }

    #[test]
    fn test_bulkhead_full_error_debug() {
        let err = BulkheadFull { max_concurrent: 42 };
        let debug = format!("{:?}", err);
        assert!(debug.contains("BulkheadFull"));
        assert!(debug.contains("42"));
    }

    #[tokio::test]
    async fn test_bulkhead_available_after_release() {
        let bulkhead = Bulkhead::new(3);
        
        let p1 = bulkhead.acquire().await.unwrap();
        let p2 = bulkhead.acquire().await.unwrap();
        assert_eq!(bulkhead.available(), 1);
        
        drop(p1);
        assert_eq!(bulkhead.available(), 2);
        
        drop(p2);
        assert_eq!(bulkhead.available(), 3);
    }
}
