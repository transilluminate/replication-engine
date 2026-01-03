//! Configuration for the replication engine.
//!
//! This module defines all configuration types needed to run the replication engine.
//! Configuration is passed to [`ReplicationEngine::new()`](crate::ReplicationEngine::new)
//! and can be constructed programmatically or deserialized from YAML/JSON.
//!
//! # Quick Start
//!
//! ```rust
//! use replication_engine::config::{ReplicationConfig, PeerConfig};
//!
//! let config = ReplicationConfig {
//!     local_node_id: "node-1".into(),
//!     peers: vec![
//!         PeerConfig::for_testing("node-2", "redis://peer2:6379"),
//!     ],
//!     ..Default::default()
//! };
//! ```
//!
//! # Configuration Structure
//!
//! ```text
//! ReplicationConfig
//! ├── local_node_id: String        # This node's unique ID
//! ├── settings: ReplicationSettings
//! │   ├── hot_path: HotPathConfig  # CDC stream tailing
//! │   ├── cold_path: ColdPathConfig # Merkle anti-entropy  
//! │   ├── peer_health: PeerHealthConfig
//! │   └── slo: SloConfig           # SLO thresholds
//! ├── peers: Vec<PeerConfig>       # Remote nodes to replicate from
//! └── cursor: CursorConfig         # SQLite cursor persistence
//! ```
//!
//! # YAML Example
//!
//! ```yaml
//! local_node_id: "uk.node.london-1"
//!
//! settings:
//!   hot_path:
//!     enabled: true
//!     batch_size: 100
//!     block_timeout: "5s"
//!   cold_path:
//!     enabled: true
//!     interval_sec: 60
//!
//! peers:
//!   - node_id: "uk.node.manchester-1"
//!     redis_url: "redis://peer1:6379"
//!
//! cursor:
//!   sqlite_path: "/var/lib/app/cursors.db"
//! ```

use serde::{Deserialize, Serialize};
use std::time::Duration;

// ═══════════════════════════════════════════════════════════════════════════════
// Top-level config: passed from daemon to ReplicationEngine::new()
// ═══════════════════════════════════════════════════════════════════════════════

/// The top-level config object passed to `ReplicationEngine::new()`.
///
/// # Fields
///
/// - `local_node_id`: Unique identifier for this node. Used to filter self from peer lists.
/// - `settings`: Tunable parameters for hot path, cold path, health checks, and SLOs.
/// - `peers`: List of remote nodes to replicate from.
/// - `cursor`: SQLite cursor persistence settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// The identity of the local node running this engine.
    /// Used to filter "self" out of peer lists and identify our own streams.
    pub local_node_id: String,

    /// General settings for the replication logic (timeouts, batch sizes, etc.)
    pub settings: ReplicationSettings,

    /// The list of peers to replicate from.
    /// Each peer represents a remote node with a Redis CDC stream.
    pub peers: Vec<PeerConfig>,

    /// Cursor persistence settings.
    /// Cursors are stored in SQLite for crash recovery.
    #[serde(default)]
    pub cursor: CursorConfig,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            local_node_id: "local.dev.node.default".to_string(),
            settings: ReplicationSettings::default(),
            peers: Vec::new(),
            cursor: CursorConfig::default(),
        }
    }
}

impl ReplicationConfig {
    /// Create a minimal config for testing.
    pub fn for_testing(local_node_id: &str) -> Self {
        Self {
            local_node_id: local_node_id.to_string(),
            settings: ReplicationSettings::default(),
            peers: Vec::new(),
            cursor: CursorConfig::in_memory(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ReplicationSettings: hot path and cold path config
// ═══════════════════════════════════════════════════════════════════════════════

/// General settings for the replication logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct ReplicationSettings {
    #[serde(default)]
    pub hot_path: HotPathConfig,
    #[serde(default)]
    pub cold_path: ColdPathConfig,
    #[serde(default)]
    pub peer_health: PeerHealthConfig,
    #[serde(default)]
    pub slo: SloConfig,
}


// ═══════════════════════════════════════════════════════════════════════════════
// SloConfig: Service Level Objectives for alerting
// ═══════════════════════════════════════════════════════════════════════════════

/// SLO thresholds for detecting performance degradation.
///
/// These thresholds trigger warnings when exceeded, helping operators
/// detect issues before they become critical. Violations are logged
/// and exposed via metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloConfig {
    /// Maximum acceptable stream read latency (ms).
    /// Exceeding this triggers a warning.
    #[serde(default = "default_max_stream_read_latency_ms")]
    pub max_stream_read_latency_ms: u64,

    /// Maximum acceptable peer operation latency (ms).
    /// Applies to Merkle queries, key fetches, etc.
    #[serde(default = "default_max_peer_op_latency_ms")]
    pub max_peer_op_latency_ms: u64,

    /// Maximum acceptable batch flush latency (ms).
    #[serde(default = "default_max_batch_flush_latency_ms")]
    pub max_batch_flush_latency_ms: u64,

    /// Maximum acceptable replication lag (seconds).
    /// If we fall this far behind the stream head, trigger warning.
    #[serde(default = "default_max_replication_lag_sec")]
    pub max_replication_lag_sec: u64,
}

fn default_max_stream_read_latency_ms() -> u64 {
    100 // 100ms
}

fn default_max_peer_op_latency_ms() -> u64 {
    500 // 500ms
}

fn default_max_batch_flush_latency_ms() -> u64 {
    200 // 200ms
}

fn default_max_replication_lag_sec() -> u64 {
    30 // 30 seconds
}

impl Default for SloConfig {
    fn default() -> Self {
        Self {
            max_stream_read_latency_ms: 100,
            max_peer_op_latency_ms: 500,
            max_batch_flush_latency_ms: 200,
            max_replication_lag_sec: 30,
        }
    }
}

impl SloConfig {
    /// Check if a stream read latency violates SLO.
    pub fn is_stream_read_violation(&self, latency: Duration) -> bool {
        latency.as_millis() as u64 > self.max_stream_read_latency_ms
    }

    /// Check if a peer operation latency violates SLO.
    pub fn is_peer_op_violation(&self, latency: Duration) -> bool {
        latency.as_millis() as u64 > self.max_peer_op_latency_ms
    }

    /// Check if a batch flush latency violates SLO.
    pub fn is_batch_flush_violation(&self, latency: Duration) -> bool {
        latency.as_millis() as u64 > self.max_batch_flush_latency_ms
    }
}

/// Peer health check configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerHealthConfig {
    /// Whether to enable idle peer ping checks.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// How often to check for idle peers (in seconds).
    #[serde(default = "default_ping_interval_sec")]
    pub ping_interval_sec: u64,

    /// Consider a peer idle if no successful contact for this many seconds.
    #[serde(default = "default_idle_threshold_sec")]
    pub idle_threshold_sec: u64,
}

fn default_ping_interval_sec() -> u64 {
    30
}

fn default_idle_threshold_sec() -> u64 {
    60
}

impl Default for PeerHealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ping_interval_sec: 30,
            idle_threshold_sec: 60,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PeerConfig: one entry per remote node
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for a single peer node.
///
/// Each peer represents a remote node whose CDC stream we tail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    /// Peer's unique node ID (for logging and deduplication).
    pub node_id: String,

    /// Redis URL for connecting to the peer's CDC stream.
    /// Example: `"redis://peer1.example.com:6379"`
    pub redis_url: String,

    /// Optional: Priority for sync (lower = higher priority).
    /// Can be used for tiered replication strategies.
    #[serde(default)]
    pub priority: u32,

    /// Number of consecutive failures before circuit opens.
    #[serde(default = "default_circuit_failure_threshold")]
    pub circuit_failure_threshold: u32,

    /// How long to wait before trying again after circuit opens (seconds).
    #[serde(default = "default_circuit_reset_timeout")]
    pub circuit_reset_timeout_sec: u64,

    /// Redis key prefix used by the peer (e.g., "sync:").
    /// Must match the peer's sync-engine configuration.
    #[serde(default)]
    pub redis_prefix: Option<String>,
}

fn default_circuit_failure_threshold() -> u32 {
    5
}

fn default_circuit_reset_timeout() -> u64 {
    30
}

impl PeerConfig {
    /// Get the CDC stream key for this peer.
    /// Uses the configured prefix + "__local__:cdc".
    pub fn cdc_stream_key(&self) -> String {
        let prefix = self.redis_prefix.as_deref().unwrap_or("");
        format!("{}__local__:cdc", prefix)
    }

    /// Create a peer config for testing.
    pub fn for_testing(node_id: &str, redis_url: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            redis_url: redis_url.to_string(),
            priority: 0,
            circuit_failure_threshold: 5,
            circuit_reset_timeout_sec: 30,
            redis_prefix: None,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HotPathConfig: CDC stream tailing settings
// ═══════════════════════════════════════════════════════════════════════════════

/// Hot path (CDC stream tailing) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotPathConfig {
    /// Whether hot path replication is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum entries to read per XREAD call (initial batch size).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// XREAD block timeout as a duration string (e.g., "5s").
    /// Parsed to Duration internally.
    #[serde(default = "default_block_timeout")]
    pub block_timeout: String,

    /// Enable adaptive batch sizing (AIMD - Additive Increase, Multiplicative Decrease).
    /// When enabled, batch size adjusts based on replication lag:
    /// - Increases when caught up (empty reads)
    /// - Decreases when lagging (full batches)
    #[serde(default = "default_false")]
    pub adaptive_batch_size: bool,

    /// Minimum batch size for adaptive sizing.
    #[serde(default = "default_min_batch_size")]
    pub min_batch_size: usize,

    /// Maximum batch size for adaptive sizing.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,

    // ─────────────────────────────────────────────────────────────────────────
    // Rate Limiting (thundering herd prevention)
    // ─────────────────────────────────────────────────────────────────────────

    /// Enable rate limiting for event processing.
    /// Prevents thundering herd when many peers reconnect simultaneously.
    #[serde(default = "default_false")]
    pub rate_limit_enabled: bool,

    /// Maximum events per second (sustained rate).
    /// Tokens refill at this rate.
    #[serde(default = "default_rate_limit_per_sec")]
    pub rate_limit_per_sec: u32,

    /// Maximum burst size for rate limiting.
    /// Allows short bursts above the sustained rate.
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: u32,
}

fn default_rate_limit_per_sec() -> u32 {
    10_000 // 10k events/sec default
}

fn default_rate_limit_burst() -> u32 {
    1000 // Allow bursts of 1000 events
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_batch_size() -> usize {
    100
}

fn default_min_batch_size() -> usize {
    50
}

fn default_max_batch_size() -> usize {
    1000
}

fn default_block_timeout() -> String {
    "5s".to_string()
}

impl Default for HotPathConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 100,
            block_timeout: "5s".to_string(),
            adaptive_batch_size: false,
            min_batch_size: 50,
            max_batch_size: 1000,
            rate_limit_enabled: false,
            rate_limit_per_sec: 10_000,
            rate_limit_burst: 1000,
        }
    }
}

impl HotPathConfig {
    /// Parse the block_timeout string to a Duration.
    pub fn block_timeout_duration(&self) -> Duration {
        humantime::parse_duration(&self.block_timeout).unwrap_or(Duration::from_secs(5))
    }

    /// Create rate limit configuration from hot path settings.
    ///
    /// Returns `None` if rate limiting is disabled.
    pub fn rate_limit_config(&self) -> Option<crate::resilience::RateLimitConfig> {
        if self.rate_limit_enabled {
            Some(crate::resilience::RateLimitConfig {
                burst_size: self.rate_limit_burst,
                refill_rate: self.rate_limit_per_sec,
            })
        } else {
            None
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ColdPathConfig: Merkle anti-entropy settings
// ═══════════════════════════════════════════════════════════════════════════════

/// Cold path (Merkle anti-entropy) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdPathConfig {
    /// Whether cold path repair is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// How often to run Merkle comparison (in seconds).
    #[serde(default = "default_interval_sec")]
    pub interval_sec: u64,

    /// Maximum items to repair per cycle (prevents memory pressure).
    /// If more items are divergent, remaining items will be repaired in subsequent cycles.
    #[serde(default = "default_max_items_per_cycle")]
    pub max_items_per_cycle: usize,

    /// Base backoff time in seconds when a peer fails repair.
    /// Actual backoff = min(base * 2^consecutive_failures, max).
    #[serde(default = "default_backoff_base_sec")]
    pub backoff_base_sec: u64,

    /// Maximum backoff time in seconds (ceiling).
    #[serde(default = "default_backoff_max_sec")]
    pub backoff_max_sec: u64,
}

fn default_interval_sec() -> u64 {
    60
}

fn default_max_items_per_cycle() -> usize {
    1000
}

fn default_backoff_base_sec() -> u64 {
    5
}

fn default_backoff_max_sec() -> u64 {
    300 // 5 minutes
}

impl Default for ColdPathConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_sec: 60,
            max_items_per_cycle: 1000,
            backoff_base_sec: 5,
            backoff_max_sec: 300,
        }
    }
}

impl ColdPathConfig {
    /// Get the interval as a Duration.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_sec)
    }

    /// Calculate backoff duration for a given number of consecutive failures.
    pub fn backoff_for_failures(&self, consecutive_failures: u32) -> Duration {
        let backoff_secs = self.backoff_base_sec.saturating_mul(
            2u64.saturating_pow(consecutive_failures)
        );
        Duration::from_secs(backoff_secs.min(self.backoff_max_sec))
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CursorConfig: cursor persistence (internal, not from daemon)
// ═══════════════════════════════════════════════════════════════════════════════

/// Cursor persistence configuration.
///
/// Cursors track the last-read position in each peer's CDC stream.
/// We persist to SQLite because Redis streams are ephemeral (may be trimmed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorConfig {
    /// Path to SQLite database for cursor storage.
    pub sqlite_path: String,

    /// Whether to use WAL mode for SQLite (recommended).
    #[serde(default = "default_true")]
    pub wal_mode: bool,
}

impl Default for CursorConfig {
    fn default() -> Self {
        Self {
            sqlite_path: "replication_cursors.db".to_string(),
            wal_mode: true,
        }
    }
}

impl CursorConfig {
    /// Create an in-memory config for testing.
    pub fn in_memory() -> Self {
        Self {
            sqlite_path: ":memory:".to_string(),
            wal_mode: false,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_cdc_stream_key() {
        let peer = PeerConfig::for_testing("test-node", "redis://localhost:6379");
        assert_eq!(peer.cdc_stream_key(), "__local__:cdc");
    }

    #[test]
    fn test_peer_config_defaults() {
        let peer = PeerConfig::for_testing("node-1", "redis://host:6379");
        assert_eq!(peer.node_id, "node-1");
        assert_eq!(peer.redis_url, "redis://host:6379");
        assert_eq!(peer.priority, 0);
        assert_eq!(peer.circuit_failure_threshold, 5);
        assert_eq!(peer.circuit_reset_timeout_sec, 30);
    }

    #[test]
    fn test_hot_path_block_timeout_parsing() {
        let config = HotPathConfig {
            enabled: true,
            batch_size: 50,
            block_timeout: "10s".to_string(),
            adaptive_batch_size: false,
            min_batch_size: 50,
            max_batch_size: 1000,
            rate_limit_enabled: false,
            rate_limit_per_sec: 10_000,
            rate_limit_burst: 1000,
        };
        assert_eq!(config.block_timeout_duration(), Duration::from_secs(10));
    }

    #[test]
    fn test_hot_path_block_timeout_various_formats() {
        let test_cases = [
            ("5s", Duration::from_secs(5)),
            ("1m", Duration::from_secs(60)),
            ("500ms", Duration::from_millis(500)),
            ("2min", Duration::from_secs(120)),
        ];

        for (input, expected) in test_cases {
            let config = HotPathConfig {
                block_timeout: input.to_string(),
                ..Default::default()
            };
            assert_eq!(config.block_timeout_duration(), expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_hot_path_block_timeout_invalid_fallback() {
        let config = HotPathConfig {
            block_timeout: "invalid".to_string(),
            ..Default::default()
        };
        // Should fall back to 5 seconds
        assert_eq!(config.block_timeout_duration(), Duration::from_secs(5));
    }

    #[test]
    fn test_hot_path_rate_limit_config() {
        let mut config = HotPathConfig::default();
        
        // Disabled by default
        assert!(config.rate_limit_config().is_none());

        // Enable it
        config.rate_limit_enabled = true;
        config.rate_limit_per_sec = 5000;
        config.rate_limit_burst = 500;
        
        let rate_config = config.rate_limit_config().unwrap();
        assert_eq!(rate_config.refill_rate, 5000);
        assert_eq!(rate_config.burst_size, 500);
    }

    #[test]
    fn test_hot_path_default() {
        let config = HotPathConfig::default();
        assert!(config.enabled);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.block_timeout, "5s");
        assert!(!config.adaptive_batch_size);
        assert_eq!(config.min_batch_size, 50);
        assert_eq!(config.max_batch_size, 1000);
        assert!(!config.rate_limit_enabled);
        assert_eq!(config.rate_limit_per_sec, 10_000);
        assert_eq!(config.rate_limit_burst, 1000);
    }

    #[test]
    fn test_cold_path_interval() {
        let config = ColdPathConfig {
            enabled: true,
            interval_sec: 120,
            max_items_per_cycle: 500,
            backoff_base_sec: 5,
            backoff_max_sec: 300,
        };
        assert_eq!(config.interval(), Duration::from_secs(120));
    }

    #[test]
    fn test_cold_path_default() {
        let config = ColdPathConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval_sec, 60);
        assert_eq!(config.max_items_per_cycle, 1000);
        assert_eq!(config.backoff_base_sec, 5);
        assert_eq!(config.backoff_max_sec, 300);
    }

    #[test]
    fn test_cold_path_backoff() {
        let config = ColdPathConfig {
            enabled: true,
            interval_sec: 60,
            max_items_per_cycle: 1000,
            backoff_base_sec: 5,
            backoff_max_sec: 300,
        };

        // 0 failures = 5 * 2^0 = 5 seconds (but we start from 1 failure in practice)
        assert_eq!(config.backoff_for_failures(0), Duration::from_secs(5));

        // 1 failure = 5 * 2^1 = 10 seconds
        assert_eq!(config.backoff_for_failures(1), Duration::from_secs(10));

        // 2 failures = 5 * 2^2 = 20 seconds
        assert_eq!(config.backoff_for_failures(2), Duration::from_secs(20));

        // 3 failures = 5 * 2^3 = 40 seconds
        assert_eq!(config.backoff_for_failures(3), Duration::from_secs(40));

        // 6 failures = 5 * 2^6 = 320 seconds, capped at 300
        assert_eq!(config.backoff_for_failures(6), Duration::from_secs(300));

        // Many failures = still capped at 300
        assert_eq!(config.backoff_for_failures(10), Duration::from_secs(300));
    }

    #[test]
    fn test_slo_config_default() {
        let config = SloConfig::default();
        assert_eq!(config.max_stream_read_latency_ms, 100);
        assert_eq!(config.max_peer_op_latency_ms, 500);
        assert_eq!(config.max_batch_flush_latency_ms, 200);
        assert_eq!(config.max_replication_lag_sec, 30);
    }

    #[test]
    fn test_slo_stream_read_violation() {
        let config = SloConfig::default(); // 100ms threshold

        // Under threshold - no violation
        assert!(!config.is_stream_read_violation(Duration::from_millis(50)));
        assert!(!config.is_stream_read_violation(Duration::from_millis(100)));

        // Over threshold - violation
        assert!(config.is_stream_read_violation(Duration::from_millis(101)));
        assert!(config.is_stream_read_violation(Duration::from_secs(1)));
    }

    #[test]
    fn test_slo_peer_op_violation() {
        let config = SloConfig::default(); // 500ms threshold

        assert!(!config.is_peer_op_violation(Duration::from_millis(250)));
        assert!(!config.is_peer_op_violation(Duration::from_millis(500)));
        assert!(config.is_peer_op_violation(Duration::from_millis(501)));
    }

    #[test]
    fn test_slo_batch_flush_violation() {
        let config = SloConfig::default(); // 200ms threshold

        assert!(!config.is_batch_flush_violation(Duration::from_millis(100)));
        assert!(!config.is_batch_flush_violation(Duration::from_millis(200)));
        assert!(config.is_batch_flush_violation(Duration::from_millis(201)));
    }

    #[test]
    fn test_peer_health_config_default() {
        let config = PeerHealthConfig::default();
        assert!(config.enabled);
        assert_eq!(config.ping_interval_sec, 30);
        assert_eq!(config.idle_threshold_sec, 60);
    }

    #[test]
    fn test_cursor_config_default() {
        let config = CursorConfig::default();
        assert_eq!(config.sqlite_path, "replication_cursors.db");
        assert!(config.wal_mode);
    }

    #[test]
    fn test_cursor_config_in_memory() {
        let config = CursorConfig::in_memory();
        assert_eq!(config.sqlite_path, ":memory:");
        assert!(!config.wal_mode);
    }

    #[test]
    fn test_replication_config_default() {
        let config = ReplicationConfig::default();
        assert_eq!(config.local_node_id, "local.dev.node.default");
        assert!(config.peers.is_empty());
        assert!(config.settings.hot_path.enabled);
        assert!(config.settings.cold_path.enabled);
    }

    #[test]
    fn test_default_config_serializes() {
        let config = ReplicationConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains("local.dev.node.default"));
    }

    #[test]
    fn test_for_testing_config() {
        let config = ReplicationConfig::for_testing("test-node-1");
        assert_eq!(config.local_node_id, "test-node-1");
        assert_eq!(config.cursor.sqlite_path, ":memory:");
    }

    #[test]
    fn test_replication_settings_default() {
        let settings = ReplicationSettings::default();
        assert!(settings.hot_path.enabled);
        assert!(settings.cold_path.enabled);
        assert!(settings.peer_health.enabled);
        assert_eq!(settings.slo.max_replication_lag_sec, 30);
    }

    #[test]
    fn test_config_json_roundtrip() {
        let config = ReplicationConfig {
            local_node_id: "node-roundtrip".to_string(),
            settings: ReplicationSettings::default(),
            peers: vec![
                PeerConfig::for_testing("peer-1", "redis://peer1:6379"),
                PeerConfig::for_testing("peer-2", "redis://peer2:6379"),
            ],
            cursor: CursorConfig::default(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: ReplicationConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.local_node_id, "node-roundtrip");
        assert_eq!(parsed.peers.len(), 2);
        assert_eq!(parsed.peers[0].node_id, "peer-1");
        assert_eq!(parsed.peers[1].node_id, "peer-2");
    }

    #[test]
    fn test_hot_path_config_json_roundtrip() {
        let config = HotPathConfig {
            enabled: true,
            batch_size: 200,
            block_timeout: "10s".to_string(),
            adaptive_batch_size: true,
            min_batch_size: 25,
            max_batch_size: 2000,
            rate_limit_enabled: true,
            rate_limit_per_sec: 5000,
            rate_limit_burst: 500,
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: HotPathConfig = serde_json::from_str(&json).unwrap();

        assert!(parsed.enabled);
        assert_eq!(parsed.batch_size, 200);
        assert_eq!(parsed.block_timeout, "10s");
        assert!(parsed.adaptive_batch_size);
        assert_eq!(parsed.min_batch_size, 25);
        assert_eq!(parsed.max_batch_size, 2000);
        assert!(parsed.rate_limit_enabled);
        assert_eq!(parsed.rate_limit_per_sec, 5000);
        assert_eq!(parsed.rate_limit_burst, 500);
    }
}
