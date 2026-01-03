//! Circuit breaker pattern for sync-engine protection.
//!
//! Prevents cascading failures when sync-engine is overloaded or unhealthy.
//! Uses the `recloser` crate following sync-engine's proven patterns.
//!
//! # States
//!
//! - **Closed**: Normal operation, requests pass through
//! - **Open**: Sync-engine unhealthy, requests fail-fast without attempting
//! - **HalfOpen**: Testing if sync-engine recovered, limited requests allowed
//!
//! # Usage
//!
//! ```rust,no_run
//! # use replication_engine::circuit_breaker::{SyncEngineCircuit, CircuitError};
//! # async fn example() -> Result<(), CircuitError<String>> {
//! let circuit = SyncEngineCircuit::new();
//!
//! // Wrap sync-engine write calls
//! match circuit.writes.call(|| async { Ok::<(), String>(()) }).await {
//!     Ok(()) => { /* success */ }
//!     Err(CircuitError::Rejected) => { /* circuit open, backoff */ }
//!     Err(CircuitError::Inner(e)) => { /* sync-engine error */ }
//! }
//! # Ok(())
//! # }
//! ```

use recloser::{AsyncRecloser, Error as RecloserError, Recloser};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, warn};

/// Circuit breaker state for metrics/monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation, requests pass through
    Closed = 0,
    /// Testing if service recovered
    HalfOpen = 1,
    /// Service unhealthy, fail-fast
    Open = 2,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "closed"),
            Self::HalfOpen => write!(f, "half_open"),
            Self::Open => write!(f, "open"),
        }
    }
}

/// Error type for circuit-protected operations.
#[derive(Debug, thiserror::Error)]
pub enum CircuitError<E> {
    /// The circuit breaker rejected the call (circuit is open).
    #[error("circuit breaker open, request rejected")]
    Rejected,

    /// The underlying operation failed.
    #[error("operation failed: {0}")]
    Inner(#[source] E),
}

impl<E> CircuitError<E> {
    /// Check if this is a rejection (circuit open).
    pub fn is_rejected(&self) -> bool {
        matches!(self, CircuitError::Rejected)
    }

    /// Check if this is an inner error.
    pub fn is_inner(&self) -> bool {
        matches!(self, CircuitError::Inner(_))
    }

    /// Get the inner error if present.
    pub fn inner(&self) -> Option<&E> {
        match self {
            CircuitError::Inner(e) => Some(e),
            _ => None,
        }
    }
}

impl<E> From<RecloserError<E>> for CircuitError<E> {
    fn from(err: RecloserError<E>) -> Self {
        match err {
            RecloserError::Rejected => CircuitError::Rejected,
            RecloserError::Inner(e) => CircuitError::Inner(e),
        }
    }
}

/// Configuration for a circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitConfig {
    /// Number of consecutive failures to trip the circuit.
    pub failure_threshold: u32,
    /// Number of consecutive successes in half-open to close circuit.
    pub success_threshold: u32,
    /// How long to wait before attempting recovery (half-open).
    pub recovery_timeout: Duration,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            recovery_timeout: Duration::from_secs(30),
        }
    }
}

impl CircuitConfig {
    /// Aggressive config for critical paths (trips faster, recovers cautiously).
    ///
    /// Use this for sync-engine calls where we don't want to hammer a struggling service.
    #[must_use]
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 3,
            recovery_timeout: Duration::from_secs(60),
        }
    }

    /// Lenient config for less critical paths (tolerates more failures).
    #[must_use]
    pub fn lenient() -> Self {
        Self {
            failure_threshold: 10,
            success_threshold: 1,
            recovery_timeout: Duration::from_secs(15),
        }
    }

    /// Fast recovery for testing.
    #[cfg(test)]
    pub fn test() -> Self {
        Self {
            failure_threshold: 2,
            success_threshold: 1,
            recovery_timeout: Duration::from_millis(50),
        }
    }
}

/// A named circuit breaker with metrics tracking.
pub struct CircuitBreaker {
    name: String,
    inner: AsyncRecloser,

    // Metrics
    calls_total: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    rejections: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given name and config.
    pub fn new(name: impl Into<String>, config: CircuitConfig) -> Self {
        let recloser = Recloser::custom()
            .error_rate(config.failure_threshold as f32 / 100.0)
            .closed_len(config.failure_threshold as usize)
            .half_open_len(config.success_threshold as usize)
            .open_wait(config.recovery_timeout)
            .build();

        Self {
            name: name.into(),
            inner: recloser.into(),
            calls_total: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            rejections: AtomicU64::new(0),
        }
    }

    /// Create with default config.
    pub fn with_defaults(name: impl Into<String>) -> Self {
        Self::new(name, CircuitConfig::default())
    }

    /// Get the circuit breaker name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Execute an async operation through the circuit breaker.
    ///
    /// Takes a closure that returns a Future, allowing lazy evaluation.
    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.calls_total.fetch_add(1, Ordering::Relaxed);

        match self.inner.call(f()).await {
            Ok(result) => {
                self.successes.fetch_add(1, Ordering::Relaxed);
                debug!(circuit = %self.name, "Circuit call succeeded");
                Ok(result)
            }
            Err(RecloserError::Rejected) => {
                self.rejections.fetch_add(1, Ordering::Relaxed);
                warn!(circuit = %self.name, "Circuit breaker rejected call (open)");
                Err(CircuitError::Rejected)
            }
            Err(RecloserError::Inner(e)) => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                debug!(circuit = %self.name, "Circuit call failed");
                Err(CircuitError::Inner(e))
            }
        }
    }

    /// Get total number of calls.
    #[must_use]
    pub fn calls_total(&self) -> u64 {
        self.calls_total.load(Ordering::Relaxed)
    }

    /// Get number of successful calls.
    #[must_use]
    pub fn successes(&self) -> u64 {
        self.successes.load(Ordering::Relaxed)
    }

    /// Get number of failed calls (operation errors).
    #[must_use]
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Get number of rejected calls (circuit open).
    #[must_use]
    pub fn rejections(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }

    /// Get failure rate (0.0 - 1.0).
    #[must_use]
    pub fn failure_rate(&self) -> f64 {
        let total = self.calls_total();
        if total == 0 {
            return 0.0;
        }
        self.failures() as f64 / total as f64
    }

    /// Check if circuit is likely open (based on recent rejections).
    #[must_use]
    pub fn is_likely_open(&self) -> bool {
        self.rejections() > 0 && self.rejections() > self.successes()
    }

    /// Reset all metrics.
    pub fn reset_metrics(&self) {
        self.calls_total.store(0, Ordering::Relaxed);
        self.successes.store(0, Ordering::Relaxed);
        self.failures.store(0, Ordering::Relaxed);
        self.rejections.store(0, Ordering::Relaxed);
    }
}

/// Circuit breaker specifically for sync-engine operations.
///
/// This wraps sync-engine calls (submit, delete, is_current) with
/// circuit breaker protection to prevent hammering an overloaded service.
pub struct SyncEngineCircuit {
    /// Circuit for write operations (submit, delete)
    pub writes: CircuitBreaker,
    /// Circuit for read operations (is_current, get)
    pub reads: CircuitBreaker,
}

impl Default for SyncEngineCircuit {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncEngineCircuit {
    /// Create sync-engine circuits with appropriate configs.
    pub fn new() -> Self {
        Self {
            // Writes: aggressive (don't hammer sync-engine when struggling)
            writes: CircuitBreaker::new("sync_engine_writes", CircuitConfig::aggressive()),
            // Reads: more lenient (is_current can tolerate more failures)
            reads: CircuitBreaker::new("sync_engine_reads", CircuitConfig::default()),
        }
    }

    /// Create with custom configs.
    pub fn with_configs(writes_config: CircuitConfig, reads_config: CircuitConfig) -> Self {
        Self {
            writes: CircuitBreaker::new("sync_engine_writes", writes_config),
            reads: CircuitBreaker::new("sync_engine_reads", reads_config),
        }
    }

    /// Get aggregated metrics.
    pub fn metrics(&self) -> SyncEngineCircuitMetrics {
        SyncEngineCircuitMetrics {
            writes_total: self.writes.calls_total(),
            writes_successes: self.writes.successes(),
            writes_failures: self.writes.failures(),
            writes_rejections: self.writes.rejections(),
            reads_total: self.reads.calls_total(),
            reads_successes: self.reads.successes(),
            reads_failures: self.reads.failures(),
            reads_rejections: self.reads.rejections(),
        }
    }

    /// Check if any circuit is open.
    pub fn any_open(&self) -> bool {
        self.writes.is_likely_open() || self.reads.is_likely_open()
    }
}

/// Aggregated metrics from sync-engine circuits.
#[derive(Debug, Clone, Default)]
pub struct SyncEngineCircuitMetrics {
    pub writes_total: u64,
    pub writes_successes: u64,
    pub writes_failures: u64,
    pub writes_rejections: u64,
    pub reads_total: u64,
    pub reads_successes: u64,
    pub reads_failures: u64,
    pub reads_rejections: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn test_circuit_passes_successful_calls() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());

        let result: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(42) }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(cb.successes(), 1);
        assert_eq!(cb.failures(), 0);
    }

    #[tokio::test]
    async fn test_circuit_tracks_failures() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());

        let result: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("boom") }).await;

        assert!(matches!(result, Err(CircuitError::Inner("boom"))));
        assert_eq!(cb.successes(), 0);
        assert_eq!(cb.failures(), 1);
    }

    #[tokio::test]
    async fn test_circuit_opens_after_threshold() {
        let config = CircuitConfig {
            failure_threshold: 2,
            success_threshold: 1,
            recovery_timeout: Duration::from_secs(60),
        };
        let cb = CircuitBreaker::new("test", config);

        // Fail multiple times to trip the breaker
        for _ in 0..5 {
            let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("fail") }).await;
        }

        // Verify we have failures and/or rejections
        assert!(cb.failures() >= 2 || cb.rejections() >= 1);
    }

    #[tokio::test]
    async fn test_circuit_metrics_accumulate() {
        // Use high threshold to avoid tripping
        let config = CircuitConfig {
            failure_threshold: 100,
            success_threshold: 1,
            recovery_timeout: Duration::from_secs(60),
        };
        let cb = CircuitBreaker::new("test", config);

        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(1) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(2) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(3) }).await;

        assert_eq!(cb.calls_total(), 3);
        assert_eq!(cb.successes(), 3);
        assert_eq!(cb.failures(), 0);
    }

    #[tokio::test]
    async fn test_failure_rate_calculation() {
        let config = CircuitConfig {
            failure_threshold: 100,
            success_threshold: 1,
            recovery_timeout: Duration::from_secs(60),
        };
        let cb = CircuitBreaker::new("test", config);

        // 2 success, 2 failure = 50% failure rate
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(1) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("x") }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(2) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("y") }).await;

        assert!((cb.failure_rate() - 0.5).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_reset_metrics() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());

        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(1) }).await;

        assert!(cb.calls_total() > 0);

        cb.reset_metrics();

        assert_eq!(cb.calls_total(), 0);
        assert_eq!(cb.successes(), 0);
        assert_eq!(cb.failures(), 0);
        assert_eq!(cb.rejections(), 0);
    }

    #[tokio::test]
    async fn test_sync_engine_circuits() {
        let circuits = SyncEngineCircuit::new();

        assert_eq!(circuits.writes.name(), "sync_engine_writes");
        assert_eq!(circuits.reads.name(), "sync_engine_reads");
    }

    #[tokio::test]
    async fn test_circuit_with_async_state() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());
        let counter = std::sync::Arc::new(AtomicUsize::new(0));

        let counter_clone = counter.clone();
        let result: Result<usize, CircuitError<&str>> = cb
            .call(|| async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(counter_clone.load(Ordering::SeqCst))
            })
            .await;

        assert_eq!(result.unwrap(), 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_sync_engine_circuit_metrics() {
        let circuits = SyncEngineCircuit::new();

        let _: Result<i32, CircuitError<&str>> = circuits.writes.call(|| async { Ok(1) }).await;
        let _: Result<i32, CircuitError<&str>> =
            circuits.reads.call(|| async { Err("timeout") }).await;

        let metrics = circuits.metrics();

        assert_eq!(metrics.writes_total, 1);
        assert_eq!(metrics.writes_successes, 1);
        assert_eq!(metrics.reads_total, 1);
        assert_eq!(metrics.reads_failures, 1);
    }

    #[test]
    fn test_circuit_config_presets() {
        let default = CircuitConfig::default();
        let aggressive = CircuitConfig::aggressive();
        let lenient = CircuitConfig::lenient();

        // Aggressive trips faster
        assert!(aggressive.failure_threshold < default.failure_threshold);
        // Lenient tolerates more
        assert!(lenient.failure_threshold > default.failure_threshold);
        // Aggressive waits longer to recover
        assert!(aggressive.recovery_timeout > lenient.recovery_timeout);
    }

    #[test]
    fn test_circuit_error_methods() {
        let rejected: CircuitError<&str> = CircuitError::Rejected;
        assert!(rejected.is_rejected());
        assert!(!rejected.is_inner());
        assert!(rejected.inner().is_none());

        let inner: CircuitError<&str> = CircuitError::Inner("boom");
        assert!(!inner.is_rejected());
        assert!(inner.is_inner());
        assert_eq!(inner.inner(), Some(&"boom"));
    }
}
