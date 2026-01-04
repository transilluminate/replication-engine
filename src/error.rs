// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Error types for the replication engine.
//!
//! This module defines the error types used throughout the replication engine.
//! Errors are categorized by their source (Redis, SQLite, etc.) and include
//! context to help with debugging.
//!
//! # Error Categories
//!
//! | Error Type | Retryable | Description |
//! |------------|-----------|-------------|
//! | `Redis` | Yes | Network errors, timeouts, connection failures |
//! | `PeerConnection` | Yes | Peer unreachable, connection dropped |
//! | `SyncEngine` | Yes | Sync engine temporarily unavailable |
//! | `CursorStore` | No | Local SQLite errors (needs operator attention) |
//! | `Config` | No | Configuration invalid |
//! | `Decompression` | No | Data corruption (zstd decode failed) |
//! | `StreamParse` | No | Malformed CDC event |
//! | `InvalidState` | No | Engine state machine violation |
//! | `Shutdown` | No | Engine is shutting down |
//! | `Internal` | No | Unexpected internal error |
//!
//! # Retry Behavior
//!
//! Use [`ReplicationError::is_retryable()`] to determine if an operation
//! should be retried with backoff. Retryable errors indicate transient
//! network or availability issues. Non-retryable errors indicate bugs,
//! configuration problems, or data corruption.

use thiserror::Error;

/// Result type alias for replication operations.
pub type Result<T> = std::result::Result<T, ReplicationError>;

/// Errors that can occur during replication.
///
/// Each variant includes context about where the error occurred.
/// Use [`is_retryable()`](Self::is_retryable) to check if the operation
/// should be retried.
#[derive(Error, Debug)]
pub enum ReplicationError {
    /// Redis connection or command error.
    ///
    /// Occurs when communicating with a peer's Redis instance.
    /// These are typically retryable (network timeouts, connection drops).
    #[error("Redis error ({operation}): {message}")]
    Redis {
        operation: String,
        message: String,
        #[source]
        source: Option<redis::RedisError>,
    },

    /// SQLite error during cursor persistence.
    ///
    /// Occurs when reading/writing cursor positions to SQLite.
    /// Not retryable - indicates local database issues that need attention.
    #[error("Cursor store error: {0}")]
    CursorStore(#[from] sqlx::Error),

    /// Invalid or missing configuration.
    ///
    /// Occurs during engine initialization if config is malformed.
    /// Not retryable - fix the configuration and restart.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Peer connection failure.
    ///
    /// Occurs when a peer is unreachable or the connection drops.
    /// Retryable with exponential backoff.
    #[error("Peer connection error ({peer_id}): {message}")]
    PeerConnection { peer_id: String, message: String },

    /// Zstd decompression failure.
    ///
    /// Occurs when CDC event payload is corrupted or truncated.
    /// Not retryable - the data is corrupt at the source.
    #[error("Decompression error: {0}")]
    Decompression(String),

    /// CDC stream event parsing failure.
    ///
    /// Occurs when a Redis stream entry has unexpected format.
    /// Not retryable - the event is malformed at the source.
    #[error("Stream parse error: {0}")]
    StreamParse(String),

    /// Sync engine communication failure.
    ///
    /// Occurs when submitting to the local sync engine fails.
    /// Retryable - sync engine may be temporarily overloaded.
    #[error("Sync engine error: {0}")]
    SyncEngine(String),

    /// Engine state machine violation.
    ///
    /// Occurs when an operation is attempted in the wrong state
    /// (e.g., calling `start()` on an already-running engine).
    /// Not retryable - indicates a bug in the caller.
    #[error("Invalid state: expected {expected}, got {actual}")]
    InvalidState { expected: String, actual: String },

    /// Shutdown in progress.
    ///
    /// Returned when operations are attempted during shutdown.
    /// Not retryable - engine is terminating.
    #[error("Shutdown in progress")]
    Shutdown,

    /// Unexpected internal error.
    ///
    /// Catch-all for errors that shouldn't happen.
    /// Not retryable - indicates a bug that needs investigation.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl ReplicationError {
    /// Create a Redis error from a redis::RedisError
    pub fn redis(operation: impl Into<String>, source: redis::RedisError) -> Self {
        Self::Redis {
            operation: operation.into(),
            message: source.to_string(),
            source: Some(source),
        }
    }

    /// Create a Redis error without source
    pub fn redis_msg(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Redis {
            operation: operation.into(),
            message: message.into(),
            source: None,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Redis { .. } => true, // Network errors are retryable
            Self::PeerConnection { .. } => true,
            Self::SyncEngine(_) => true,
            Self::CursorStore(_) => false, // Local DB issues need attention
            Self::Config(_) => false,
            Self::Decompression(_) => false, // Data corruption
            Self::StreamParse(_) => false,   // Data corruption
            Self::InvalidState { .. } => false,
            Self::Shutdown => false,
            Self::Internal(_) => false,
        }
    }
}

impl From<redis::RedisError> for ReplicationError {
    fn from(e: redis::RedisError) -> Self {
        Self::redis("unknown", e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_retryable_redis() {
        let err = ReplicationError::redis_msg("XREAD", "connection reset");
        assert!(err.is_retryable());
        assert!(err.to_string().contains("XREAD"));
    }

    #[test]
    fn test_is_retryable_peer_connection() {
        let err = ReplicationError::PeerConnection {
            peer_id: "peer-1".to_string(),
            message: "connection refused".to_string(),
        };
        assert!(err.is_retryable());
        assert!(err.to_string().contains("peer-1"));
    }

    #[test]
    fn test_is_retryable_sync_engine() {
        let err = ReplicationError::SyncEngine("overloaded".to_string());
        assert!(err.is_retryable());
    }

    #[test]
    fn test_not_retryable_config() {
        let err = ReplicationError::Config("invalid peer URL".to_string());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_not_retryable_decompression() {
        let err = ReplicationError::Decompression("invalid zstd header".to_string());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_not_retryable_stream_parse() {
        let err = ReplicationError::StreamParse("missing op field".to_string());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_not_retryable_invalid_state() {
        let err = ReplicationError::InvalidState {
            expected: "Running".to_string(),
            actual: "Stopped".to_string(),
        };
        assert!(!err.is_retryable());
        assert!(err.to_string().contains("Running"));
        assert!(err.to_string().contains("Stopped"));
    }

    #[test]
    fn test_not_retryable_shutdown() {
        let err = ReplicationError::Shutdown;
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_not_retryable_internal() {
        let err = ReplicationError::Internal("unexpected panic".to_string());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_redis_error_formatting() {
        let err = ReplicationError::Redis {
            operation: "XRANGE".to_string(),
            message: "timeout".to_string(),
            source: None,
        };
        let msg = err.to_string();
        assert!(msg.contains("Redis error"));
        assert!(msg.contains("XRANGE"));
        assert!(msg.contains("timeout"));
    }
}
