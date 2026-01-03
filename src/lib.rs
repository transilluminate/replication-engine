//! # Replication Engine
//!
//! A mesh replication agent for synchronizing data between `sync-engine` nodes.
//!
//! ## Architecture
//!
//! The replication engine sits between the local `sync-engine` and remote peers,
//! managing bidirectional data flow via Redis Streams:
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────────────────────┐
//! │                          replication-engine                               │
//! │                                                                           │
//! │  ┌─────────────┐    ┌──────────────┐    ┌──────────────────────────────┐  │
//! │  │ PeerManager │───►│ StreamTailer │───►│ Dedup + Apply to sync-engine │  │
//! │  │ (per peer)  │    │ (XREAD)      │    │ (hash comparison)            │  │
//! │  └─────────────┘    └──────────────┘    └──────────────────────────────┘  │
//! │         │                                            │                    │
//! │         ▼                                            ▼                    │
//! │  ┌─────────────┐                          ┌─────────────────────────┐     │
//! │  │ CursorStore │                          │ MerkleRepair (cold path)│     │
//! │  │ (SQLite)    │                          │ (periodic anti-entropy) │     │
//! │  └─────────────┘                          └─────────────────────────┘     │
//! └───────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Two-Path Replication
//!
//! 1. **Hot Path (CDC Streams)**: Real-time tailing of peer `__local__:cdc` streams
//! 2. **Cold Path (Merkle Repair)**: Periodic anti-entropy using Merkle tree comparison
//!
//! ## Usage
//!
//! ```rust,no_run
//! use replication_engine::{ReplicationEngine, ReplicationConfig};
//! use tokio::sync::watch;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = ReplicationConfig::default();
//!     let (_tx, rx) = watch::channel(config.clone());
//!     
//!     let mut engine = ReplicationEngine::new(config, rx);
//!     engine.start().await.expect("Failed to start");
//!     
//!     // Engine runs until shutdown signal
//!     engine.shutdown().await;
//! }
//! ```

pub mod batch;
pub mod circuit_breaker;
pub mod config;
pub mod coordinator;
pub mod cursor;
pub mod error;
pub mod metrics;
pub mod peer;
pub mod resilience;
pub mod stream;
pub mod sync_engine;

// Re-exports for convenience
pub use circuit_breaker::{CircuitBreaker, CircuitConfig, CircuitError, SyncEngineCircuit};
pub use config::{ReplicationConfig, ReplicationSettings, PeerConfig, HotPathConfig, ColdPathConfig};
pub use coordinator::ReplicationEngine;
pub use error::{ReplicationError, Result};
pub use cursor::CursorStore;
pub use peer::PeerManager;
pub use stream::{StreamTailer, CdcEvent};
pub use sync_engine::{SyncEngineRef, NoOpSyncEngine, SyncError};
