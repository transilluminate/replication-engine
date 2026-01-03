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
