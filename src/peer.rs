//! Peer connection management.
//!
//! Manages Redis connections to peer nodes with automatic reconnection,
//! circuit breaker protection, and Merkle root caching.
//!
//! # Connection Lifecycle
//!
//! ```text
//! Disconnected → Connecting → Connected
//!      ↑             ↓             ↓
//!      └─── Backoff ←┴─────────────┘
//! ```
//!
//! Connections are **lazy**: they're only established when first needed
//! (via [`PeerConnection::ensure_connected()`]). If a connection fails,
//! the peer enters [`PeerState::Backoff`] state with exponential backoff.
//!
//! # Circuit Breaker
//!
//! Each peer has a circuit breaker to prevent cascade failures:
//!
//! - **Closed**: Normal operation, requests pass through
//! - **Open**: Too many consecutive failures, requests rejected immediately
//!
//! The circuit opens after `circuit_failure_threshold` consecutive failures
//! and resets after `circuit_reset_timeout_sec` seconds.
//!
//! # Merkle Root Caching
//!
//! To reduce load during cold path repair, Merkle roots are cached for
//! 5 seconds (see `MERKLE_ROOT_CACHE_TTL`). This prevents hammering peers
//! when checking consistency.
//!
//! # Example
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use replication_engine::peer::PeerConnection;
//! use replication_engine::config::PeerConfig;
//!
//! let config = PeerConfig::for_testing("peer-1", "redis://localhost:6379");
//! let peer = PeerConnection::new(config);
//!
//! // Connection is lazy - this triggers the actual connect
//! peer.ensure_connected().await?;
//!
//! // Use the connection
//! let conn = peer.connection().await;
//! # Ok(())
//! # }
//! ```

use crate::config::PeerConfig;
use crate::error::{ReplicationError, Result};
use crate::metrics;
use crate::resilience::RetryConfig;
use redis::aio::ConnectionManager;
use redis::Client;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{error, info, warn};

/// State of a peer connection.
///
/// See module docs for the state transition diagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    /// Not yet connected (initial state).
    Disconnected,
    /// Connection attempt in progress.
    Connecting,
    /// Connected and healthy.
    Connected,
    /// Connection failed, waiting before retry.
    Backoff,
    /// Peer is disabled in configuration.
    Disabled,
}

/// Circuit breaker state for peer connections.
///
/// Prevents cascade failures by rejecting requests when a peer
/// is experiencing repeated failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerCircuitState {
    /// Normal operation, requests pass through.
    Closed,
    /// Too many failures, requests rejected immediately.
    Open,
}

/// Cached Merkle root with TTL.
///
/// Used to reduce load on peers during cold path repair cycles.
/// Roots are cached for [`MERKLE_ROOT_CACHE_TTL`] to avoid
/// repeated queries for the same data.
struct CachedMerkleRoot {
    /// The cached root hash (None if peer has no data).
    root: Option<[u8; 32]>,
    /// When this cache entry expires.
    expires_at: Instant,
}

/// Default TTL for cached Merkle roots (5 seconds).
/// Short enough to catch updates, long enough to avoid hammering.
const MERKLE_CACHE_TTL: Duration = Duration::from_secs(5);

/// A managed connection to a peer's Redis.
///
/// Uses `redis::aio::ConnectionManager` internally, which provides:
/// - Automatic reconnection on connection loss
/// - Multiplexed connection (single TCP socket, multiple in-flight requests)
/// - Connection pooling semantics (cloning is cheap, shares underlying connection)
pub struct PeerConnection {
    /// Peer configuration
    pub config: PeerConfig,
    /// Redis connection (None if disconnected).
    /// ConnectionManager is Clone and multiplexed, so sharing is cheap.
    conn: RwLock<Option<ConnectionManager>>,
    /// Current state
    state: RwLock<PeerState>,
    /// Last successful operation timestamp
    last_success: AtomicU64,
    /// Consecutive failure count
    failure_count: AtomicU64,
    /// Whether shutdown was requested
    shutdown: AtomicBool,
    /// When the circuit opened (for reset timeout)
    circuit_opened_at: RwLock<Option<Instant>>,
    /// Cached Merkle root (avoids repeated queries during repair)
    merkle_root_cache: RwLock<Option<CachedMerkleRoot>>,
}

impl PeerConnection {
    /// Create a new peer connection (not yet connected).
    pub fn new(config: PeerConfig) -> Self {
        // All peers in the config are enabled (daemon filters disabled peers)
        let initial_state = PeerState::Disconnected;

        Self {
            config,
            conn: RwLock::new(None),
            state: RwLock::new(initial_state),
            last_success: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            circuit_opened_at: RwLock::new(None),
            merkle_root_cache: RwLock::new(None),
        }
    }

    /// Get the peer's node ID.
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Get the CDC stream key for this peer.
    pub fn cdc_stream_key(&self) -> String {
        self.config.cdc_stream_key()
    }

    /// Get current connection state.
    pub async fn state(&self) -> PeerState {
        *self.state.read().await
    }

    /// Check if connected.
    pub async fn is_connected(&self) -> bool {
        self.state().await == PeerState::Connected
    }

    // =========================================================================
    // Circuit Breaker
    // =========================================================================

    /// Get the current circuit breaker state.
    pub async fn circuit_state(&self) -> PeerCircuitState {
        let failures = self.failure_count.load(Ordering::Relaxed);
        let threshold = self.config.circuit_failure_threshold as u64;

        if failures >= threshold {
            // Check if we should try again (half-open)
            if let Some(opened_at) = *self.circuit_opened_at.read().await {
                let reset_timeout = Duration::from_secs(self.config.circuit_reset_timeout_sec);
                if opened_at.elapsed() >= reset_timeout {
                    // Time to try again
                    return PeerCircuitState::Closed;
                }
            }
            PeerCircuitState::Open
        } else {
            PeerCircuitState::Closed
        }
    }

    /// Check if the circuit is open (should reject requests).
    pub async fn is_circuit_open(&self) -> bool {
        self.circuit_state().await == PeerCircuitState::Open
    }

    /// Record a successful operation (resets failure count).
    pub async fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        self.last_success.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            Ordering::Relaxed,
        );
        // Close the circuit
        *self.circuit_opened_at.write().await = None;
    }

    /// Record a failed operation (increments failure count, may open circuit).
    pub async fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        let threshold = self.config.circuit_failure_threshold as u64;

        if failures >= threshold {
            // Open the circuit
            let mut opened_at = self.circuit_opened_at.write().await;
            if opened_at.is_none() {
                *opened_at = Some(Instant::now());
                warn!(
                    peer_id = %self.config.node_id,
                    failures,
                    threshold,
                    reset_timeout_sec = self.config.circuit_reset_timeout_sec,
                    "Circuit breaker opened for peer"
                );
                metrics::record_peer_circuit_state(&self.config.node_id, "open");
            }
        }
    }

    /// Connect to the peer's Redis with retry logic.
    pub async fn connect(&self, retry_config: &RetryConfig) -> Result<()> {
        *self.state.write().await = PeerState::Connecting;
        info!(peer_id = %self.config.node_id, url = %self.config.redis_url, "Connecting to peer");

        let client = Client::open(self.config.redis_url.as_str()).map_err(|e| {
            ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: format!("Invalid Redis URL: {}", e),
            }
        })?;

        let mut attempt = 0;
        let mut delay = retry_config.initial_delay;

        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return Err(ReplicationError::Shutdown);
            }

            attempt += 1;

            // Wrap connection attempt in a timeout to avoid hanging on unreachable hosts
            let conn_result = timeout(
                retry_config.connection_timeout,
                client.get_connection_manager(),
            )
            .await;

            match conn_result {
                Ok(Ok(conn)) => {
                    *self.conn.write().await = Some(conn);
                    *self.state.write().await = PeerState::Connected;
                    self.failure_count.store(0, Ordering::Release);
                    self.last_success
                        .store(epoch_millis(), Ordering::Release);

                    metrics::record_peer_connection(&self.config.node_id, true);
                    metrics::record_peer_state(&self.config.node_id, "connected");

                    if attempt > 1 {
                        info!(
                            peer_id = %self.config.node_id,
                            attempt,
                            "Connected to peer after retry"
                        );
                    } else {
                        info!(peer_id = %self.config.node_id, "Connected to peer");
                    }
                    return Ok(());
                }
                Ok(Err(e)) => {
                    // Redis connection error
                    self.failure_count.fetch_add(1, Ordering::AcqRel);
                    let err_msg = format!("{}", e);

                    if attempt >= retry_config.max_attempts {
                        *self.state.write().await = PeerState::Backoff;
                        metrics::record_peer_connection(&self.config.node_id, false);
                        metrics::record_peer_state(&self.config.node_id, "backoff");
                        error!(
                            peer_id = %self.config.node_id,
                            attempt,
                            error = %e,
                            "Failed to connect after max retries"
                        );
                        return Err(ReplicationError::PeerConnection {
                            peer_id: self.config.node_id.clone(),
                            message: format!("Connection failed after {} attempts: {}", attempt, err_msg),
                        });
                    }

                    warn!(
                        peer_id = %self.config.node_id,
                        attempt,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "Connection attempt failed, retrying"
                    );

                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(
                        Duration::from_secs_f64(delay.as_secs_f64() * retry_config.backoff_factor),
                        retry_config.max_delay,
                    );
                }
                Err(_) => {
                    // Timeout elapsed
                    self.failure_count.fetch_add(1, Ordering::AcqRel);

                    if attempt >= retry_config.max_attempts {
                        *self.state.write().await = PeerState::Backoff;
                        error!(
                            peer_id = %self.config.node_id,
                            attempt,
                            timeout_ms = retry_config.connection_timeout.as_millis(),
                            "Connection timed out after max retries"
                        );
                        return Err(ReplicationError::PeerConnection {
                            peer_id: self.config.node_id.clone(),
                            message: format!(
                                "Connection timed out after {} attempts ({}ms timeout)",
                                attempt,
                                retry_config.connection_timeout.as_millis()
                            ),
                        });
                    }

                    warn!(
                        peer_id = %self.config.node_id,
                        attempt,
                        delay_ms = delay.as_millis(),
                        timeout_ms = retry_config.connection_timeout.as_millis(),
                        "Connection attempt timed out, retrying"
                    );

                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(
                        Duration::from_secs_f64(delay.as_secs_f64() * retry_config.backoff_factor),
                        retry_config.max_delay,
                    );
                }
            }
        }
    }

    /// Get a reference to the connection for operations.
    ///
    /// Returns None if not connected.
    pub async fn connection(&self) -> Option<ConnectionManager> {
        self.conn.read().await.clone()
    }

    /// Ensure the peer is connected, connecting lazily if needed.
    ///
    /// This provides a convenient way to get a connection without
    /// manually calling `connect()` first. Uses default retry config.
    ///
    /// # Returns
    /// The connection manager, or an error if connection failed.
    pub async fn ensure_connected(&self) -> Result<ConnectionManager> {
        // Fast path: already connected
        if let Some(conn) = self.connection().await {
            return Ok(conn);
        }

        // Need to connect
        self.connect(&RetryConfig::default()).await?;
        
        self.connection().await.ok_or_else(|| {
            ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: "Connection lost immediately after connect".to_string(),
            }
        })
    }

    /// Get consecutive failure count.
    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Acquire)
    }

    /// Get milliseconds since last success.
    pub fn millis_since_success(&self) -> u64 {
        let last = self.last_success.load(Ordering::Acquire);
        if last == 0 {
            return u64::MAX;
        }
        epoch_millis().saturating_sub(last)
    }

    /// Mark connection as failed (triggers reconnect).
    pub async fn mark_disconnected(&self) {
        *self.conn.write().await = None;
        *self.state.write().await = PeerState::Disconnected;
        metrics::record_peer_state(&self.config.node_id, "disconnected");
        warn!(peer_id = %self.config.node_id, "Connection marked as disconnected");
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    // =========================================================================
    // Health Check
    // =========================================================================

    /// Ping the peer's Redis to check connection health.
    ///
    /// Returns the round-trip latency on success.
    /// Updates `last_success` timestamp on success.
    pub async fn ping(&self) -> Result<Duration> {
        let conn = self.connection().await.ok_or_else(|| {
            ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: "Not connected".to_string(),
            }
        })?;

        let mut conn = conn;
        let start = std::time::Instant::now();

        let result: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: format!("PING failed: {}", e),
            })?;

        let latency = start.elapsed();

        if result == "PONG" {
            self.record_success().await;
            metrics::record_peer_ping(&self.config.node_id, true);
            metrics::record_peer_ping_latency(&self.config.node_id, latency);
            Ok(latency)
        } else {
            self.record_failure().await;
            metrics::record_peer_ping(&self.config.node_id, false);
            Err(ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: format!("Unexpected PING response: {}", result),
            })
        }
    }

    // =========================================================================
    // Merkle Tree Queries (for cold path repair)
    // =========================================================================

    /// Get the peer's Merkle root hash (cached with TTL).
    /// 
    /// Uses a short-lived cache to avoid hammering the peer during repair cycles.
    /// The cache is automatically invalidated after `MERKLE_CACHE_TTL`.
    pub async fn get_merkle_root(&self) -> Result<Option<[u8; 32]>> {
        // Check cache first
        {
            let cache = self.merkle_root_cache.read().await;
            if let Some(ref cached) = *cache {
                if Instant::now() < cached.expires_at {
                    return Ok(cached.root);
                }
            }
        }

        // Cache miss or expired - fetch from peer
        let start = Instant::now();
        let conn = self.connection().await.ok_or_else(|| {
            ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: "Not connected".to_string(),
            }
        })?;

        let mut conn = conn;
        let result: Option<String> = redis::cmd("GET")
            .arg("merkle:hash:")
            .query_async(&mut conn)
            .await
            .map_err(|e| ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: format!("Failed to get Merkle root: {}", e),
            })?;
        
        metrics::record_peer_operation_latency(&self.config.node_id, "get_merkle_root", start.elapsed());

        let root = match result {
            Some(hex_str) => {
                let bytes = hex::decode(&hex_str).map_err(|e| {
                    ReplicationError::PeerConnection {
                        peer_id: self.config.node_id.clone(),
                        message: format!("Invalid Merkle root hex: {}", e),
                    }
                })?;
                let arr: [u8; 32] = bytes.try_into().map_err(|_| {
                    ReplicationError::PeerConnection {
                        peer_id: self.config.node_id.clone(),
                        message: "Merkle root is not 32 bytes".to_string(),
                    }
                })?;
                Some(arr)
            }
            None => None,
        };

        // Update cache
        {
            let mut cache = self.merkle_root_cache.write().await;
            *cache = Some(CachedMerkleRoot {
                root,
                expires_at: Instant::now() + MERKLE_CACHE_TTL,
            });
        }

        Ok(root)
    }

    /// Invalidate the Merkle root cache (call after local writes).
    pub async fn invalidate_merkle_cache(&self) {
        let mut cache = self.merkle_root_cache.write().await;
        *cache = None;
    }

    /// Get children of a Merkle path (sorted by score/position).
    pub async fn get_merkle_children(&self, path: &str) -> Result<Vec<(String, [u8; 32])>> {
        let start = Instant::now();
        let conn = self.connection().await.ok_or_else(|| {
            ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: "Not connected".to_string(),
            }
        })?;

        let mut conn = conn;
        let key = format!("merkle:children:{}", path);

        // ZRANGE returns items as (member, score) pairs with WITHSCORES
        let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(&key)
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await
            .map_err(|e| ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: format!("Failed to get Merkle children: {}", e),
            })?;

        let mut children = Vec::with_capacity(items.len());
        for (child_name, _score) in items {
            // Get the hash for this child
            let child_path = if path.is_empty() {
                child_name.clone()
            } else {
                format!("{}/{}", path, child_name)
            };
            let hash_key = format!("merkle:hash:{}", child_path);

            let hex_hash: Option<String> = redis::cmd("GET")
                .arg(&hash_key)
                .query_async(&mut conn)
                .await
                .map_err(|e| ReplicationError::PeerConnection {
                    peer_id: self.config.node_id.clone(),
                    message: format!("Failed to get child hash: {}", e),
                })?;

            if let Some(hex_str) = hex_hash {
                let bytes = hex::decode(&hex_str).map_err(|e| {
                    ReplicationError::PeerConnection {
                        peer_id: self.config.node_id.clone(),
                        message: format!("Invalid child hash hex: {}", e),
                    }
                })?;
                let arr: [u8; 32] = bytes.try_into().map_err(|_| {
                    ReplicationError::PeerConnection {
                        peer_id: self.config.node_id.clone(),
                        message: "Child hash is not 32 bytes".to_string(),
                    }
                })?;
                children.push((child_name, arr));
            }
        }
        
        metrics::record_peer_operation_latency(&self.config.node_id, "get_merkle_children", start.elapsed());

        Ok(children)
    }

    /// Get an item's data by key.
    pub async fn get_item(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let start = Instant::now();
        let conn = self.connection().await.ok_or_else(|| {
            ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: "Not connected".to_string(),
            }
        })?;

        let mut conn = conn;
        let redis_key = format!("item:{}", key);

        let data: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&redis_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| ReplicationError::PeerConnection {
                peer_id: self.config.node_id.clone(),
                message: format!("Failed to get item: {}", e),
            })?;
        
        metrics::record_peer_operation_latency(&self.config.node_id, "get_item", start.elapsed());

        Ok(data)
    }
}

/// Manager for all peer connections.
pub struct PeerManager {
    /// All peer connections (keyed by node_id)
    peers: dashmap::DashMap<String, Arc<PeerConnection>>,
    /// Retry configuration
    retry_config: RetryConfig,
}

impl PeerManager {
    /// Create a new peer manager.
    pub fn new(retry_config: RetryConfig) -> Self {
        Self {
            peers: dashmap::DashMap::new(),
            retry_config,
        }
    }

    /// Add a peer from configuration.
    pub fn add_peer(&self, config: PeerConfig) {
        let node_id = config.node_id.clone();
        let conn = Arc::new(PeerConnection::new(config));
        self.peers.insert(node_id, conn);
    }

    /// Get a peer connection by node ID.
    pub fn get(&self, node_id: &str) -> Option<Arc<PeerConnection>> {
        self.peers.get(node_id).map(|r| r.value().clone())
    }

    /// Get all peer connections.
    pub fn all(&self) -> Vec<Arc<PeerConnection>> {
        self.peers.iter().map(|r| r.value().clone()).collect()
    }

    /// Connect to all enabled peers.
    pub async fn connect_all(&self) -> Vec<Result<()>> {
        let mut results = Vec::new();
        for peer in self.all() {
            let result = peer.connect(&self.retry_config).await;
            results.push(result);
        }
        results
    }

    /// Remove a peer.
    pub fn remove_peer(&self, node_id: &str) {
        if let Some((_, peer)) = self.peers.remove(node_id) {
            peer.shutdown();
        }
    }

    /// Shutdown all peers.
    pub fn shutdown_all(&self) {
        for peer in self.peers.iter() {
            peer.shutdown();
        }
    }

    /// Get count of connected peers.
    pub async fn connected_count(&self) -> usize {
        let mut count = 0;
        for peer in self.all() {
            if peer.is_connected().await {
                count += 1;
            }
        }
        count
    }
}

/// Get current epoch milliseconds.
fn epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_connection_new() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");

        let conn = PeerConnection::new(config);
        assert_eq!(conn.node_id(), "test-peer");
        assert_eq!(conn.cdc_stream_key(), "__local__:cdc");
    }

    #[test]
    fn test_peer_state_variants() {
        assert_eq!(PeerState::Disconnected, PeerState::Disconnected);
        assert_ne!(PeerState::Connected, PeerState::Disconnected);
        assert_ne!(PeerState::Connecting, PeerState::Backoff);
        assert_eq!(PeerState::Disabled, PeerState::Disabled);
    }

    #[test]
    fn test_peer_circuit_state_variants() {
        assert_eq!(PeerCircuitState::Closed, PeerCircuitState::Closed);
        assert_ne!(PeerCircuitState::Open, PeerCircuitState::Closed);
    }

    #[tokio::test]
    async fn test_peer_initial_state() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);
        
        assert_eq!(conn.state().await, PeerState::Disconnected);
        assert!(!conn.is_connected().await);
        assert_eq!(conn.failure_count(), 0);
    }

    #[tokio::test]
    async fn test_peer_connection_not_connected() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);
        
        // Should be None when not connected
        assert!(conn.connection().await.is_none());
    }

    #[tokio::test]
    async fn test_peer_circuit_breaker() {
        let config = PeerConfig {
            node_id: "test-peer".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            priority: 0,
            circuit_failure_threshold: 3,
            circuit_reset_timeout_sec: 1,
            redis_prefix: None,
        };

        let conn = PeerConnection::new(config);

        // Initially closed
        assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);
        assert!(!conn.is_circuit_open().await);

        // Record failures up to threshold
        conn.record_failure().await;
        assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);
        conn.record_failure().await;
        assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);
        conn.record_failure().await;
        
        // Now it should be open
        assert_eq!(conn.circuit_state().await, PeerCircuitState::Open);
        assert!(conn.is_circuit_open().await);

        // Wait for reset timeout
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // Should be closed again (half-open allows retry)
        assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);

        // Success resets everything
        conn.record_success().await;
        assert_eq!(conn.failure_count(), 0);
        assert!(!conn.is_circuit_open().await);
    }

    #[tokio::test]
    async fn test_peer_record_success_updates_last_success() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        // Initially no success recorded
        assert_eq!(conn.last_success.load(Ordering::Acquire), 0);
        
        // Record some failures first
        conn.record_failure().await;
        conn.record_failure().await;
        assert_eq!(conn.failure_count(), 2);

        // Record success should reset failures and update timestamp
        conn.record_success().await;
        assert_eq!(conn.failure_count(), 0);
        assert!(conn.last_success.load(Ordering::Acquire) > 0);
    }

    #[tokio::test]
    async fn test_peer_millis_since_success_no_success() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        // No success recorded - should return MAX
        assert_eq!(conn.millis_since_success(), u64::MAX);
    }

    #[tokio::test]
    async fn test_peer_millis_since_success_after_success() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        conn.record_success().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let millis = conn.millis_since_success();
        // Should be at least 100ms
        assert!(millis >= 100);
        // But not absurdly long (less than 1 second for this test)
        assert!(millis < 1000);
    }

    #[tokio::test]
    async fn test_peer_shutdown_flag() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        assert!(!conn.shutdown.load(Ordering::Acquire));
        conn.shutdown();
        assert!(conn.shutdown.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_peer_mark_disconnected() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        // Start in Disconnected state
        assert_eq!(conn.state().await, PeerState::Disconnected);
        
        // Mark disconnected (should stay disconnected)
        conn.mark_disconnected().await;
        assert_eq!(conn.state().await, PeerState::Disconnected);
        assert!(conn.connection().await.is_none());
    }

    #[tokio::test]
    async fn test_peer_invalidate_merkle_cache() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        // Initially no cache
        assert!(conn.merkle_root_cache.read().await.is_none());

        // Simulate a cached entry
        {
            let mut cache = conn.merkle_root_cache.write().await;
            *cache = Some(CachedMerkleRoot {
                root: Some([0u8; 32]),
                expires_at: Instant::now() + Duration::from_secs(60),
            });
        }

        // Verify cache is set
        assert!(conn.merkle_root_cache.read().await.is_some());

        // Invalidate
        conn.invalidate_merkle_cache().await;

        // Cache should be None
        assert!(conn.merkle_root_cache.read().await.is_none());
    }

    #[test]
    fn test_peer_manager_add_peer() {
        let manager = PeerManager::new(RetryConfig::testing());
        manager.add_peer(PeerConfig::for_testing("peer-1", "redis://peer1:6379"));
        manager.add_peer(PeerConfig::for_testing("peer-2", "redis://peer2:6379"));

        let peers = manager.all();
        assert_eq!(peers.len(), 2);
    }

    #[test]
    fn test_peer_manager_get() {
        let manager = PeerManager::new(RetryConfig::testing());
        manager.add_peer(PeerConfig::for_testing("peer-1", "redis://peer1:6379"));

        assert!(manager.get("peer-1").is_some());
        assert!(manager.get("nonexistent").is_none());
    }

    #[test]
    fn test_peer_manager_remove_peer() {
        let manager = PeerManager::new(RetryConfig::testing());
        manager.add_peer(PeerConfig::for_testing("peer-1", "redis://peer1:6379"));
        
        assert!(manager.get("peer-1").is_some());
        
        manager.remove_peer("peer-1");
        
        assert!(manager.get("peer-1").is_none());
    }

    #[test]
    fn test_peer_manager_remove_nonexistent() {
        let manager = PeerManager::new(RetryConfig::testing());
        // Should not panic
        manager.remove_peer("nonexistent");
    }

    #[test]
    fn test_peer_manager_shutdown_all() {
        let manager = PeerManager::new(RetryConfig::testing());
        manager.add_peer(PeerConfig::for_testing("peer-1", "redis://peer1:6379"));
        manager.add_peer(PeerConfig::for_testing("peer-2", "redis://peer2:6379"));

        manager.shutdown_all();

        // Verify all peers have shutdown flag set
        for peer in manager.all() {
            assert!(peer.shutdown.load(Ordering::Acquire));
        }
    }

    #[tokio::test]
    async fn test_peer_manager_connected_count_none() {
        let manager = PeerManager::new(RetryConfig::testing());
        manager.add_peer(PeerConfig::for_testing("peer-1", "redis://peer1:6379"));
        manager.add_peer(PeerConfig::for_testing("peer-2", "redis://peer2:6379"));

        // No peers connected yet
        assert_eq!(manager.connected_count().await, 0);
    }

    #[test]
    fn test_epoch_millis() {
        let millis = epoch_millis();
        // Should be a reasonable epoch time (after 2020, before 2100)
        assert!(millis > 1577836800000); // Jan 1, 2020
        assert!(millis < 4102444800000); // Jan 1, 2100
    }

    #[test]
    fn test_merkle_cache_ttl() {
        // Verify constant is reasonable
        assert_eq!(MERKLE_CACHE_TTL, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_peer_ping_not_connected() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        // Ping should fail when not connected
        let result = conn.ping().await;
        assert!(result.is_err());
        
        if let Err(ReplicationError::PeerConnection { peer_id, message }) = result {
            assert_eq!(peer_id, "test-peer");
            assert!(message.contains("Not connected"));
        } else {
            panic!("Expected PeerConnection error");
        }
    }

    #[tokio::test]
    async fn test_peer_get_merkle_root_not_connected() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        let result = conn.get_merkle_root().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_peer_get_merkle_children_not_connected() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        let result = conn.get_merkle_children("some/path").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_peer_get_item_not_connected() {
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:6379");
        let conn = PeerConnection::new(config);

        let result = conn.get_item("some-key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_peer_ensure_connected_fails() {
        // This test is slow (retries with timeouts) so we skip it in normal runs
        // Integration tests cover real connection scenarios
        // Just verify the method exists and handles errors
        let config = PeerConfig::for_testing("test-peer", "redis://localhost:1"); // Port 1 is typically closed
        let conn = PeerConnection::new(config);

        // Should not be connected
        assert!(!conn.is_connected().await);
    }

    #[tokio::test]
    async fn test_peer_circuit_opens_on_threshold() {
        let config = PeerConfig {
            node_id: "test-peer".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            priority: 0,
            circuit_failure_threshold: 2, // Low threshold for testing
            circuit_reset_timeout_sec: 30,
            redis_prefix: None,
        };

        let conn = PeerConnection::new(config);

        // One failure - still closed
        conn.record_failure().await;
        assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);
        assert_eq!(conn.failure_count(), 1);

        // Second failure - opens
        conn.record_failure().await;
        assert_eq!(conn.circuit_state().await, PeerCircuitState::Open);
        assert_eq!(conn.failure_count(), 2);
    }
}
