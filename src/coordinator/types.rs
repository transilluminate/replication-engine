// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Engine state types.
//!
//! Defines the state machine for the replication engine lifecycle.
//!
//! # State Transitions
//!
//! ```text
//!                  start()
//! Created ───────────────────→ Connecting
//!    │                              │
//!    │ (already stopped)            │ (peers connected)
//!    ↓                              ↓
//! Stopped                       Running ←──────────────┐
//!    ↑                              │                  │
//!    │                    shutdown()│   (recoverable   │
//!    │                              ↓    error)        │
//!    └────────────────── ShuttingDown ─────────────────┘
//!                              │
//!                    (unrecoverable error)
//!                              ↓
//!                           Failed
//! ```
//!
//! # State Descriptions
//!
//! - **Created**: Initial state after `ReplicationEngine::new()`. No connections established.
//! - **Connecting**: `start()` called, connecting to peers.
//! - **Running**: Normal operation. Hot path tailing streams, cold path doing repairs.
//! - **ShuttingDown**: `shutdown()` called. Draining in-flight batches.
//! - **Stopped**: Graceful shutdown complete. Safe to drop.
//! - **Failed**: Unrecoverable error. Engine cannot continue.

/// State of the replication engine.
///
/// See module docs for the state transition diagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    /// Engine created but not started.
    ///
    /// Call [`start()`](super::ReplicationEngine::start) to begin replication.
    Created,

    /// Connecting to peers.
    ///
    /// The engine is establishing connections to configured peers.
    /// Transitions to `Running` when ready, or `Failed` if connection fails.
    Connecting,

    /// Running and replicating.
    ///
    /// Hot path is tailing CDC streams from peers.
    /// Cold path is periodically comparing Merkle roots.
    Running,

    /// Shutting down gracefully.
    ///
    /// In-flight batches are being drained and cursors persisted.
    /// Transitions to `Stopped` when complete.
    ShuttingDown,

    /// Stopped.
    ///
    /// Engine has shut down cleanly. Safe to drop or restart.
    Stopped,

    /// Failed to start or unrecoverable error.
    ///
    /// Check logs for error details. Engine cannot recover from this state.
    Failed,
}

impl std::fmt::Display for EngineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineState::Created => write!(f, "Created"),
            EngineState::Connecting => write!(f, "Connecting"),
            EngineState::Running => write!(f, "Running"),
            EngineState::ShuttingDown => write!(f, "ShuttingDown"),
            EngineState::Stopped => write!(f, "Stopped"),
            EngineState::Failed => write!(f, "Failed"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HealthCheck: Comprehensive health status for /ready and /health endpoints
// ═══════════════════════════════════════════════════════════════════════════════

/// Comprehensive health status of the replication engine.
///
/// This struct provides all the information needed for:
/// - `/ready` endpoint: Just check `ready` field
/// - `/health` endpoint: Full diagnostics with per-peer status
/// - Metrics dashboards: Peer connectivity, lag, circuit states
///
/// # Data Sources
///
/// All data is **cached** from internal state - no network I/O during health check:
/// - **Engine state**: From internal watch channel
/// - **Peer status**: From `PeerConnection` atomics and mutexes
/// - **Sync-engine health**: From `SyncEngineRef::should_accept_writes()`
///
/// # Example
///
/// ```text
/// let health = engine.health_check().await;
/// if health.ready {
///     // Ready for traffic
/// } else {
///     // Log diagnostics
///     eprintln!("Engine not ready: state={}, peers_connected={}/{}",
///         health.state, health.peers_connected, health.peers_total);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct HealthCheck {
    // ═══════════════════════════════════════════════════════════════════════
    // ENGINE STATE (cached from internal state machine)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Current engine lifecycle state.
    /// **Source**: Cached from `state_tx` watch channel.
    pub state: EngineState,
    
    /// Whether the engine is ready for traffic.
    /// **Source**: Derived from `state == Running && peers_connected > 0`.
    pub ready: bool,
    
    // ═══════════════════════════════════════════════════════════════════════
    // SYNC-ENGINE BACKPRESSURE (from SyncEngineRef)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Whether the local sync-engine is accepting writes.
    /// **Source**: From `sync_engine.should_accept_writes()`.
    /// `false` when sync-engine is under critical memory pressure.
    pub sync_engine_accepting_writes: bool,
    
    // ═══════════════════════════════════════════════════════════════════════
    // PEER SUMMARY (aggregated from all peers)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Total number of configured peers.
    pub peers_total: usize,
    
    /// Number of peers currently connected.
    pub peers_connected: usize,
    
    /// Number of peers with open circuit breakers (temporarily excluded).
    pub peers_circuit_open: usize,
    
    /// Number of peers we're currently catching up with (lagging behind).
    pub peers_catching_up: usize,
    
    /// Per-peer health details.
    pub peers: Vec<PeerHealth>,
    
    // ═══════════════════════════════════════════════════════════════════════
    // OVERALL VERDICT (derived)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Overall health verdict for load balancer decisions.
    /// **Source**: Derived from:
    /// - `state == Running`
    /// - `peers_connected > 0`
    /// - `sync_engine_accepting_writes == true`
    pub healthy: bool,
}

impl HealthCheck {
    /// Check if at least one peer is healthy and connected.
    pub fn has_healthy_peer(&self) -> bool {
        self.peers.iter().any(|p| p.connected && !p.circuit_open)
    }
    
    /// Get the maximum lag across all peers (in milliseconds).
    /// Returns `None` if no peers have reported lag.
    pub fn max_lag_ms(&self) -> Option<u64> {
        self.peers.iter().filter_map(|p| p.lag_ms).max()
    }
    
    /// Get peer IDs that have open circuit breakers.
    pub fn unhealthy_peers(&self) -> Vec<&str> {
        self.peers.iter()
            .filter(|p| p.circuit_open || !p.connected)
            .map(|p| p.node_id.as_str())
            .collect()
    }
}

/// Health status of a single peer.
#[derive(Debug, Clone)]
pub struct PeerHealth {
    /// Peer's node ID.
    pub node_id: String,
    
    /// Whether the Redis connection is established.
    /// **Source**: Cached from `PeerConnection::is_connected()`.
    pub connected: bool,
    
    /// Circuit breaker state.
    /// **Source**: Cached from `PeerConnection::circuit_state()`.
    pub circuit_state: crate::peer::PeerCircuitState,
    
    /// Whether the circuit breaker is open (peer temporarily excluded).
    /// **Source**: Derived from `circuit_state == Open`.
    pub circuit_open: bool,
    
    /// Consecutive failure count.
    /// **Source**: Cached from `PeerConnection::failure_count()`.
    pub failure_count: u64,
    
    /// Milliseconds since last successful operation.
    /// **Source**: Cached from `PeerConnection::millis_since_success()`.
    /// `0` if never contacted successfully.
    pub millis_since_success: u64,
    
    /// Current replication lag in milliseconds (cursor behind stream head).
    /// **Source**: From hot path lag tracking.
    /// `None` if not yet measured or peer not connected.
    pub lag_ms: Option<u64>,
    
    /// Whether we're catching up with this peer's stream.
    /// **Source**: From hot path state.
    pub catching_up: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_state_display() {
        assert_eq!(EngineState::Created.to_string(), "Created");
        assert_eq!(EngineState::Connecting.to_string(), "Connecting");
        assert_eq!(EngineState::Running.to_string(), "Running");
        assert_eq!(EngineState::ShuttingDown.to_string(), "ShuttingDown");
        assert_eq!(EngineState::Stopped.to_string(), "Stopped");
        assert_eq!(EngineState::Failed.to_string(), "Failed");
    }

    #[test]
    fn test_engine_state_equality() {
        assert_eq!(EngineState::Created, EngineState::Created);
        assert_ne!(EngineState::Created, EngineState::Running);
    }

    #[test]
    fn test_engine_state_debug() {
        let state = EngineState::Running;
        let debug = format!("{:?}", state);
        assert_eq!(debug, "Running");
    }

    #[test]
    fn test_engine_state_clone() {
        let state = EngineState::ShuttingDown;
        // EngineState is Copy, but we test Clone trait is also available
        #[allow(clippy::clone_on_copy)]
        let cloned = state.clone();
        assert_eq!(state, cloned);
    }

    #[test]
    fn test_engine_state_copy() {
        let state = EngineState::Failed;
        let copied: EngineState = state; // Copy
        assert_eq!(state, copied); // Original still usable
    }
}
