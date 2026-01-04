// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Replication engine coordinator.
//!
//! The main orchestrator that ties together:
//! - Peer connections via [`crate::peer::PeerConnection`]
//! - Stream tailing (hot path) via [`crate::stream::StreamTailer`]
//! - Cursor persistence via [`crate::cursor::CursorStore`]
//! - Merkle repair (cold path) for consistency
//!
//! # Architecture
//!
//! The coordinator manages the full replication lifecycle:
//! 1. Connects to configured peers
//! 2. Tails CDC streams for real-time updates (hot path)
//! 3. Periodically runs Merkle tree repair (cold path)
//! 4. Handles graceful shutdown with in-flight batch draining

mod types;
mod hot_path;
mod cold_path;

pub use types::{EngineState, HealthCheck, PeerHealth};

use crate::circuit_breaker::SyncEngineCircuit;
use crate::config::ReplicationConfig;
use crate::cursor::CursorStore;
use crate::error::{ReplicationError, Result};
use crate::metrics;
use crate::peer::PeerManager;
use crate::resilience::{RetryConfig, RateLimiter};
use crate::sync_engine::{SyncEngineRef, NoOpSyncEngine};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, warn};

/// The main replication engine.
///
/// Manages bidirectional data sync between this node and its mesh peers.
/// 
/// # Sync Engine Integration
/// 
/// The replication engine is passed a reference to the local sync-engine by the daemon.
/// We use this to:
/// - Write replicated data from peers (`submit`)
/// - Check for duplicates before applying (`is_current`)
/// - Query Merkle tree for cold path repair
///
/// We **never** write to any CDC stream — that's sync-engine's responsibility.
/// We only **read** from peer CDC streams and **write** to local sync-engine.
pub struct ReplicationEngine<S: SyncEngineRef = NoOpSyncEngine> {
    /// Configuration (can be updated at runtime)
    config: ReplicationConfig,

    /// Runtime config updates
    #[allow(dead_code)]
    config_rx: watch::Receiver<ReplicationConfig>,

    /// Engine state (broadcast to watchers)
    state_tx: watch::Sender<EngineState>,

    /// Engine state receiver (for internal use)
    state_rx: watch::Receiver<EngineState>,

    /// Reference to local sync-engine (passed from daemon)
    sync_engine: Arc<S>,

    /// Circuit breaker for sync-engine protection
    circuit: Arc<SyncEngineCircuit>,

    /// Peer connection manager
    peer_manager: Arc<PeerManager>,

    /// Cursor persistence store
    cursor_store: Arc<RwLock<Option<CursorStore>>>,

    /// Shutdown signal sender
    shutdown_tx: watch::Sender<bool>,

    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,

    /// Count of peers currently catching up (cursor far behind stream head).
    /// When > 0, cold path repair is skipped to avoid duplicate work.
    catching_up_count: Arc<AtomicUsize>,

    /// Hot path task handles
    hot_path_handles: RwLock<Vec<tokio::task::JoinHandle<()>>>,
}

impl ReplicationEngine<NoOpSyncEngine> {
    /// Create a new replication engine with no-op sync engine (for testing/standalone).
    ///
    /// The engine starts in `Created` state. Call [`start()`](Self::start)
    /// to connect to peers and begin replication.
    pub fn new(
        config: ReplicationConfig,
        config_rx: watch::Receiver<ReplicationConfig>,
    ) -> Self {
        Self::with_sync_engine(config, config_rx, Arc::new(NoOpSyncEngine))
    }
}

impl<S: SyncEngineRef> ReplicationEngine<S> {
    /// Create a new replication engine with a sync-engine reference.
    ///
    /// This is the primary constructor used by the daemon.
    ///
    /// # Arguments
    /// * `config` - Replication configuration
    /// * `config_rx` - Watch channel for config updates
    /// * `sync_engine` - Reference to local sync-engine (for writes and dedup)
    pub fn with_sync_engine(
        config: ReplicationConfig,
        config_rx: watch::Receiver<ReplicationConfig>,
        sync_engine: Arc<S>,
    ) -> Self {
        let (state_tx, state_rx) = watch::channel(EngineState::Created);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Create peer manager with daemon retry config
        let retry_config = RetryConfig::daemon();
        let peer_manager = Arc::new(PeerManager::new(retry_config));

        // Add peers from config
        for peer_config in &config.peers {
            peer_manager.add_peer(peer_config.clone());
        }

        Self {
            config,
            config_rx,
            state_tx,
            state_rx,
            sync_engine,
            circuit: Arc::new(SyncEngineCircuit::new()),
            peer_manager,
            cursor_store: Arc::new(RwLock::new(None)),
            shutdown_tx,
            shutdown_rx,
            catching_up_count: Arc::new(AtomicUsize::new(0)),
            hot_path_handles: RwLock::new(Vec::new()),
        }
    }

    /// Get a reference to the sync engine.
    pub fn sync_engine(&self) -> &Arc<S> {
        &self.sync_engine
    }

    /// Get a reference to the circuit breaker for sync-engine protection.
    pub fn circuit(&self) -> &Arc<SyncEngineCircuit> {
        &self.circuit
    }

    /// Get current engine state.
    pub fn state(&self) -> EngineState {
        *self.state_rx.borrow()
    }

    /// Get a receiver to watch state changes.
    pub fn state_receiver(&self) -> watch::Receiver<EngineState> {
        self.state_rx.clone()
    }

    /// Check if engine is running.
    pub fn is_running(&self) -> bool {
        matches!(self.state(), EngineState::Running)
    }

    /// Get comprehensive health status for monitoring endpoints.
    ///
    /// Returns a [`HealthCheck`] struct containing:
    /// - Engine state and readiness
    /// - Sync-engine backpressure status
    /// - Per-peer connectivity, circuit breaker state, and lag
    ///
    /// **Performance**: This method performs no network I/O. All data is
    /// collected from cached internal state (atomics, mutexes, watch channels).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let health = engine.health_check().await;
    /// 
    /// // For /ready endpoint
    /// if health.ready {
    ///     HttpResponse::Ok()
    /// } else {
    ///     HttpResponse::ServiceUnavailable()
    /// }
    ///
    /// // For /health endpoint (full diagnostics)
    /// HttpResponse::Ok().json(serde_json::json!({
    ///     "healthy": health.healthy,
    ///     "state": health.state.to_string(),
    ///     "peers_connected": health.peers_connected,
    ///     "peers_total": health.peers_total,
    ///     "sync_engine_accepting_writes": health.sync_engine_accepting_writes,
    /// }))
    /// ```
    pub async fn health_check(&self) -> HealthCheck {
        use crate::peer::PeerCircuitState;
        
        let state = self.state();
        let sync_engine_accepting_writes = self.sync_engine.should_accept_writes();
        
        // Collect peer health
        let all_peers = self.peer_manager.all();
        let peers_total = all_peers.len();
        let mut peers = Vec::with_capacity(peers_total);
        let mut peers_connected = 0;
        let mut peers_circuit_open = 0;
        let peers_catching_up = self.catching_up_count.load(std::sync::atomic::Ordering::Relaxed);
        
        for peer in all_peers {
            let connected = peer.is_connected().await;
            let circuit_state = peer.circuit_state().await;
            let circuit_open = circuit_state == PeerCircuitState::Open;
            
            if connected {
                peers_connected += 1;
            }
            if circuit_open {
                peers_circuit_open += 1;
            }
            
            peers.push(PeerHealth {
                node_id: peer.node_id().to_string(),
                connected,
                circuit_state,
                circuit_open,
                failure_count: peer.failure_count(),
                millis_since_success: peer.millis_since_success(),
                lag_ms: None, // TODO: Track per-peer lag in hot path
                catching_up: false, // TODO: Track per-peer catching up state
            });
        }
        
        // Determine readiness and health
        let ready = state == EngineState::Running && peers_connected > 0;
        let healthy = ready && sync_engine_accepting_writes;
        
        HealthCheck {
            state,
            ready,
            sync_engine_accepting_writes,
            peers_total,
            peers_connected,
            peers_circuit_open,
            peers_catching_up,
            peers,
            healthy,
        }
    }

    /// Start the replication engine.
    ///
    /// 1. Opens cursor store (SQLite)
    /// 2. Connects to all enabled peers
    /// 3. Spawns hot path tailers for each peer
    /// 4. Spawns cold path repair task (if enabled)
    pub async fn start(&mut self) -> Result<()> {
        if self.state() != EngineState::Created {
            return Err(ReplicationError::InvalidState {
                expected: "Created".to_string(),
                actual: format!("{:?}", self.state()),
            });
        }

        info!(
            node_id = %self.config.local_node_id,
            peer_count = self.config.peers.len(),
            "Starting replication engine"
        );

        // Update state
        let _ = self.state_tx.send(EngineState::Connecting);
        metrics::set_engine_state("Connecting");

        // Initialize cursor store
        let cursor_store = CursorStore::new(&self.config.cursor.sqlite_path).await?;
        *self.cursor_store.write().await = Some(cursor_store);
        info!(path = %self.config.cursor.sqlite_path, "Cursor store initialized");

        // Connect to peers
        let results = self.peer_manager.connect_all().await;
        let connected = results.iter().filter(|r| r.is_ok()).count();
        let failed = results.iter().filter(|r| r.is_err()).count();

        // Update connected peers metric
        metrics::set_connected_peers(connected);

        if failed > 0 {
            warn!(connected, failed, "Some peer connections failed");
        }

        if connected == 0 && !self.config.peers.is_empty() {
            error!("Failed to connect to any peers");
            let _ = self.state_tx.send(EngineState::Failed);
            metrics::set_engine_state("Failed");
            return Err(ReplicationError::Internal(
                "No peers connected".to_string(),
            ));
        }

        // Spawn hot path tasks for each connected peer
        self.spawn_hot_path_tasks().await;

        // Spawn cold path task if enabled
        if self.config.settings.cold_path.enabled {
            self.spawn_cold_path_task().await;
        }

        // Spawn cursor flush task (debounced writes)
        self.spawn_cursor_flush_task().await;

        // Spawn peer health check task if enabled
        if self.config.settings.peer_health.enabled {
            self.spawn_peer_health_task().await;
        }

        let _ = self.state_tx.send(EngineState::Running);
        metrics::set_engine_state("Running");
        info!(
            connected,
            total = self.config.peers.len(),
            "Replication engine running"
        );

        Ok(())
    }

    /// Spawn hot path tailer tasks for each peer.
    async fn spawn_hot_path_tasks(&self) {
        let peers = self.peer_manager.all();
        let mut handles = self.hot_path_handles.write().await;

        // Create shared rate limiter if enabled (shared across all peers)
        let rate_limiter: Option<Arc<RateLimiter>> = self.config.settings.hot_path
            .rate_limit_config()
            .map(|cfg| {
                info!(
                    rate_per_sec = cfg.refill_rate,
                    burst = cfg.burst_size,
                    "Rate limiting enabled for hot path"
                );
                Arc::new(RateLimiter::new(cfg))
            });

        for peer in peers {
            if !peer.is_connected().await {
                continue;
            }

            let peer_id = peer.node_id().to_string();
            let peer = Arc::clone(&peer);
            let cursor_store = Arc::clone(&self.cursor_store);
            let sync_engine = Arc::clone(&self.sync_engine);
            let circuit = Arc::clone(&self.circuit);
            let shutdown_rx = self.shutdown_rx.clone();
            let config = self.config.settings.hot_path.clone();
            let catching_up_count = Arc::clone(&self.catching_up_count);
            let rate_limiter = rate_limiter.clone();

            let handle = tokio::spawn(async move {
                hot_path::run_tailer(peer, cursor_store, sync_engine, circuit, config, shutdown_rx, catching_up_count, rate_limiter).await;
            });

            info!(peer_id = %peer_id, "Spawned hot path tailer");
            handles.push(handle);
        }
    }

    /// Spawn cold path repair task.
    async fn spawn_cold_path_task(&self)
    where
        S: Send + Sync + 'static,
    {
        let sync_engine = Arc::clone(&self.sync_engine);
        let peer_manager = Arc::clone(&self.peer_manager);
        let shutdown_rx = self.shutdown_rx.clone();
        let config = self.config.settings.cold_path.clone();
        let catching_up_count = Arc::clone(&self.catching_up_count);

        let handle = tokio::spawn(async move {
            cold_path::run_repair(sync_engine, peer_manager, config, shutdown_rx, catching_up_count).await;
        });

        info!("Spawned cold path repair task");
        self.hot_path_handles.write().await.push(handle);
    }

    /// Spawn cursor flush task for debounced writes.
    ///
    /// Periodically flushes dirty cursors to SQLite (every 5 seconds).
    async fn spawn_cursor_flush_task(&self) {
        let cursor_store = Arc::clone(&self.cursor_store);
        let mut shutdown_rx = self.shutdown_rx.clone();

        let handle = tokio::spawn(async move {
            let flush_interval = std::time::Duration::from_secs(5);
            let mut timer = tokio::time::interval(flush_interval);

            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        let store_guard = cursor_store.read().await;
                        if let Some(ref store) = *store_guard {
                            if let Err(e) = store.flush_dirty().await {
                                warn!(error = %e, "Failed to flush cursors");
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("Cursor flush task stopping");
                            break;
                        }
                    }
                }
            }
        });

        debug!("Spawned cursor flush task");
        self.hot_path_handles.write().await.push(handle);
    }

    /// Spawn peer health check task.
    ///
    /// Periodically pings idle peers to check connection health
    /// and record latency for observability.
    async fn spawn_peer_health_task(&self) {
        let peer_manager = Arc::clone(&self.peer_manager);
        let mut shutdown_rx = self.shutdown_rx.clone();
        let config = self.config.settings.peer_health.clone();

        let handle = tokio::spawn(async move {
            let ping_interval = std::time::Duration::from_secs(config.ping_interval_sec);
            let idle_threshold_ms = config.idle_threshold_sec * 1000;
            let mut timer = tokio::time::interval(ping_interval);

            info!(
                ping_interval_sec = config.ping_interval_sec,
                idle_threshold_sec = config.idle_threshold_sec,
                "Starting peer health check task"
            );

            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        // Check each connected peer
                        for peer in peer_manager.all() {
                            if !peer.is_connected().await {
                                continue;
                            }

                            // Only ping if idle (no recent successful contact)
                            let idle_ms = peer.millis_since_success();
                            if idle_ms < idle_threshold_ms {
                                continue;
                            }

                            let peer_id = peer.node_id().to_string();
                            debug!(
                                peer_id = %peer_id,
                                idle_ms,
                                "Pinging idle peer"
                            );

                            match peer.ping().await {
                                Ok(latency) => {
                                    debug!(
                                        peer_id = %peer_id,
                                        latency_ms = latency.as_millis(),
                                        "Peer ping successful"
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        peer_id = %peer_id,
                                        error = %e,
                                        "Peer ping failed"
                                    );
                                    // Connection may be stale - mark disconnected
                                    peer.mark_disconnected().await;
                                }
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("Peer health task stopping");
                            break;
                        }
                    }
                }
            }

            info!("Peer health check task stopped");
        });

        info!("Spawned peer health check task");
        self.hot_path_handles.write().await.push(handle);
    }

    /// Shutdown the replication engine gracefully.
    ///
    /// Shutdown sequence:
    /// 1. Signal all hot/cold path tasks to stop
    /// 2. Wait for tasks to flush pending batches (with timeout)
    /// 3. Shutdown peer connections
    /// 4. Checkpoint and close cursor store
    pub async fn shutdown(&mut self) {
        info!("Shutting down replication engine");
        let _ = self.state_tx.send(EngineState::ShuttingDown);
        metrics::set_engine_state("ShuttingDown");

        // Signal shutdown to all tasks
        let _ = self.shutdown_tx.send(true);

        // Wait for tasks to complete gracefully (they'll flush pending batches)
        let handles: Vec<_> = {
            let mut guard = self.hot_path_handles.write().await;
            std::mem::take(&mut *guard)
        };

        let task_count = handles.len();
        if task_count > 0 {
            info!(task_count, "Waiting for tasks to drain and complete");
        }

        // Give tasks time to flush their batches (10 seconds should be plenty)
        let drain_timeout = std::time::Duration::from_secs(10);
        for (i, handle) in handles.into_iter().enumerate() {
            match tokio::time::timeout(drain_timeout, handle).await {
                Ok(Ok(())) => {
                    debug!(task = i + 1, "Task completed gracefully");
                }
                Ok(Err(e)) => {
                    warn!(task = i + 1, error = %e, "Task panicked during shutdown");
                }
                Err(_) => {
                    warn!(task = i + 1, "Task timed out during shutdown (batch may be lost)");
                }
            }
        }

        // Shutdown peer manager
        self.peer_manager.shutdown_all();
        metrics::set_connected_peers(0);

        // Close cursor store (includes WAL checkpoint)
        if let Some(cursor_store) = self.cursor_store.write().await.take() {
            cursor_store.close().await;
        }

        let _ = self.state_tx.send(EngineState::Stopped);
        metrics::set_engine_state("Stopped");
        info!("Replication engine stopped");
    }

    /// Get the peer manager (for metrics/diagnostics).
    pub fn peer_manager(&self) -> &Arc<PeerManager> {
        &self.peer_manager
    }

    /// Get the node ID.
    pub fn node_id(&self) -> &str {
        &self.config.local_node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PeerConfig, CursorConfig};

    fn test_config() -> ReplicationConfig {
        ReplicationConfig {
            local_node_id: "test-node".to_string(),
            peers: vec![],
            settings: Default::default(),
            cursor: CursorConfig::in_memory(),
        }
    }

    #[test]
    fn test_engine_initial_state() {
        let (_tx, rx) = watch::channel(test_config());
        let engine = ReplicationEngine::new(test_config(), rx);
        
        assert_eq!(engine.state(), EngineState::Created);
        assert!(!engine.is_running());
        assert_eq!(engine.node_id(), "test-node");
    }

    #[test]
    fn test_engine_state_receiver() {
        let (_tx, rx) = watch::channel(test_config());
        let engine = ReplicationEngine::new(test_config(), rx);
        
        let state_rx = engine.state_receiver();
        assert_eq!(*state_rx.borrow(), EngineState::Created);
    }

    #[test]
    fn test_engine_with_sync_engine() {
        let (_tx, rx) = watch::channel(test_config());
        let sync_engine = Arc::new(NoOpSyncEngine);
        let engine = ReplicationEngine::with_sync_engine(
            test_config(),
            rx,
            sync_engine,
        );
        
        assert_eq!(engine.state(), EngineState::Created);
        // sync_engine accessor should work
        let _ = engine.sync_engine();
    }

    #[test]
    fn test_engine_circuit_accessor() {
        let (_tx, rx) = watch::channel(test_config());
        let engine = ReplicationEngine::new(test_config(), rx);
        
        // Circuit breaker should be accessible
        let circuit = engine.circuit();
        // Just verify we can access it
        let _ = circuit.clone();
    }

    #[test]
    fn test_engine_peer_manager_accessor() {
        let (_tx, rx) = watch::channel(test_config());
        let engine = ReplicationEngine::new(test_config(), rx);
        
        let pm = engine.peer_manager();
        // Empty config = no peers
        assert!(pm.all().is_empty());
    }

    #[test]
    fn test_engine_with_peers() {
        let mut config = test_config();
        config.peers.push(PeerConfig::for_testing("peer-1", "redis://localhost:6379"));
        config.peers.push(PeerConfig::for_testing("peer-2", "redis://localhost:6380"));
        
        let (_tx, rx) = watch::channel(config.clone());
        let engine = ReplicationEngine::new(config, rx);
        
        let pm = engine.peer_manager();
        // Both peers added
        assert_eq!(pm.all().len(), 2);
    }

    #[tokio::test]
    async fn test_engine_start_invalid_state() {
        let (_tx, rx) = watch::channel(test_config());
        let mut engine = ReplicationEngine::new(test_config(), rx);
        
        // Force state to Running (simulating already started)
        let _ = engine.state_tx.send(EngineState::Running);
        
        // Trying to start should fail
        let result = engine.start().await;
        assert!(result.is_err());
        
        if let Err(ReplicationError::InvalidState { expected, actual }) = result {
            assert_eq!(expected, "Created");
            assert_eq!(actual, "Running");
        } else {
            panic!("Expected InvalidState error");
        }
    }

    #[tokio::test]
    async fn test_engine_shutdown_from_created() {
        let (_tx, rx) = watch::channel(test_config());
        let mut engine = ReplicationEngine::new(test_config(), rx);
        
        // Shutdown from Created state should work
        engine.shutdown().await;
        
        assert_eq!(engine.state(), EngineState::Stopped);
        assert!(!engine.is_running());
    }

    #[test]
    fn test_engine_state_is_running() {
        let (_tx, rx) = watch::channel(test_config());
        let engine = ReplicationEngine::new(test_config(), rx);
        
        // Initially not running
        assert!(!engine.is_running());
        
        // Manually set to running
        let _ = engine.state_tx.send(EngineState::Running);
        assert!(engine.is_running());
        
        // Set to stopped
        let _ = engine.state_tx.send(EngineState::Stopped);
        assert!(!engine.is_running());
    }
}
