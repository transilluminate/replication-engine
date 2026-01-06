// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Sync engine integration traits.
//!
//! Defines the interface for integrating with a sync/storage backend.
//! Uses `sync_engine::SyncItem` directly for tight integration with the sync-engine crate.
//!
//! # Example
//!
//! ```rust,no_run
//! use replication_engine::sync_engine::{SyncEngineRef, SyncResult, SyncError, BoxFuture};
//! use sync_engine::SyncItem;
//! use std::pin::Pin;
//! use std::future::Future;
//!
//! struct MyBackend { /* ... */ }
//!
//! impl SyncEngineRef for MyBackend {
//!     fn should_accept_writes(&self) -> bool {
//!         true // Always accept in example
//!     }
//!
//!     fn is_current(&self, _key: &str, _hash: &str) -> Pin<Box<dyn Future<Output = SyncResult<bool>> + Send + '_>> {
//!         Box::pin(async move { Ok(true) })
//!     }
//!
//!     fn submit(&self, item: SyncItem) -> Pin<Box<dyn Future<Output = SyncResult<()>> + Send + '_>> {
//!         Box::pin(async move { Ok(()) })
//!     }
//!
//!     fn delete(&self, _key: String) -> Pin<Box<dyn Future<Output = SyncResult<bool>> + Send + '_>> {
//!         Box::pin(async move { Ok(true) })
//!     }
//!
//!     fn get_merkle_root(&self) -> BoxFuture<'_, Option<[u8; 32]>> {
//!         Box::pin(async move { Ok(None) })
//!     }
//!
//!     fn get_merkle_children(&self, _path: &str) -> BoxFuture<'_, Vec<(String, [u8; 32])>> {
//!         Box::pin(async move { Ok(vec![]) })
//!     }
//!
//!     fn get(&self, _key: &str) -> BoxFuture<'_, Option<Vec<u8>>> {
//!         Box::pin(async move { Ok(None) })
//!     }
//! }
//! ```

use std::future::Future;
use std::pin::Pin;

// Re-export SyncItem and BackpressureLevel for convenience
pub use sync_engine::{BackpressureLevel, SyncItem};

/// Result type for sync engine operations.
pub type SyncResult<T> = std::result::Result<T, SyncError>;

/// Type alias for boxed async futures (reduces trait signature complexity).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = SyncResult<T>> + Send + 'a>>;

/// Simplified error for sync engine operations.
#[derive(Debug, Clone)]
pub struct SyncError(pub String);

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SyncError {}

/// Trait defining what we need from sync-engine.
///
/// The daemon provides an implementation of this trait, allowing us to:
/// 1. Write replicated data (`submit`)
/// 2. Check for duplicates (`is_current`)
/// 3. Query Merkle tree for cold path repair
///
/// This trait allows testing with mocks and decouples us from sync-engine internals.
pub trait SyncEngineRef: Send + Sync + 'static {
    /// Check if the sync-engine is accepting writes (backpressure check).
    ///
    /// Returns `false` when the engine is under critical pressure (>= 90% memory).
    /// Callers should pause ingestion when this returns `false` to avoid
    /// wasting CPU on events that will be rejected.
    ///
    /// Default implementation returns `true` (always accept).
    fn should_accept_writes(&self) -> bool {
        true
    }

    /// Submit an item to the local sync-engine.
    ///
    /// This is how we write replicated data from peers.
    /// The `SyncItem` contains all necessary fields (key, content, hash, version).
    fn submit(
        &self,
        item: SyncItem,
    ) -> Pin<Box<dyn Future<Output = SyncResult<()>> + Send + '_>>;

    /// Delete an item from the local sync-engine.
    fn delete(
        &self,
        key: String,
    ) -> Pin<Box<dyn Future<Output = SyncResult<bool>> + Send + '_>>;

    /// Check if we already have content with this hash.
    ///
    /// Returns `true` if the item exists AND its content hash matches.
    /// Used for CDC deduplication (loop prevention).
    fn is_current(
        &self,
        key: &str,
        content_hash: &str,
    ) -> Pin<Box<dyn Future<Output = SyncResult<bool>> + Send + '_>>;

    /// Get the Merkle root hash (for cold path comparison).
    fn get_merkle_root(&self) -> BoxFuture<'_, Option<[u8; 32]>>;

    /// Get children of a Merkle path (for cold path drill-down).
    fn get_merkle_children(&self, path: &str) -> BoxFuture<'_, Vec<(String, [u8; 32])>>;

    /// Fetch an item by key (for cold path repair).
    fn get(&self, key: &str) -> BoxFuture<'_, Option<Vec<u8>>>;
    
    /// Get clean branches (no pending merkle recalcs) with their hashes.
    ///
    /// Returns branches that are safe to compare with peers.
    /// Branches with pending writes should be skipped during cold path sync.
    fn get_clean_branches(&self) -> BoxFuture<'_, Vec<(String, [u8; 32])>> {
        // Default: return empty (no branch hygiene info available)
        Box::pin(async { Ok(Vec::new()) })
    }
    
    /// Check if a specific branch is clean (no pending merkle recalcs).
    ///
    /// Returns `true` if the branch has no dirty items and is safe to compare.
    fn is_branch_clean(&self, _prefix: &str) -> BoxFuture<'_, bool> {
        // Default: assume clean (conservative for mocks)
        Box::pin(async { Ok(true) })
    }
}

/// Implementation of SyncEngineRef for the real SyncEngine.
///
/// This allows the replication engine to drive the real storage backend directly.
impl SyncEngineRef for sync_engine::SyncEngine {
    fn should_accept_writes(&self) -> bool {
        self.pressure().should_accept_writes()
    }

    fn submit(
        &self,
        item: SyncItem,
    ) -> Pin<Box<dyn Future<Output = SyncResult<()>> + Send + '_>> {
        Box::pin(async move {
            self.submit(item).await.map_err(|e| SyncError(e.to_string()))
        })
    }

    fn delete(
        &self,
        key: String,
    ) -> Pin<Box<dyn Future<Output = SyncResult<bool>> + Send + '_>> {
        Box::pin(async move {
            self.delete(&key).await.map_err(|e| SyncError(e.to_string()))
        })
    }

    fn is_current(
        &self,
        key: &str,
        content_hash: &str,
    ) -> Pin<Box<dyn Future<Output = SyncResult<bool>> + Send + '_>> {
        let key = key.to_string();
        let hash = content_hash.to_string();
        Box::pin(async move {
            // Check if we have the item and the hash matches
            match self.get_verified(&key).await {
                Ok(Some(item)) => Ok(item.content_hash == hash),
                Ok(None) => Ok(false),
                Err(_) => Ok(false), // If corrupt or error, assume not current to force re-sync
            }
        })
    }

    fn get_merkle_root(&self) -> BoxFuture<'_, Option<[u8; 32]>> {
        Box::pin(async move {
            Ok(self.get_merkle_root().await.ok().flatten())
        })
    }

    fn get_merkle_children(&self, path: &str) -> BoxFuture<'_, Vec<(String, [u8; 32])>> {
        let path = path.to_string();
        Box::pin(async move {
            Ok(match self.get_merkle_children(&path).await {
                Ok(children_map) => children_map.into_iter().collect(),
                Err(_) => Vec::new(),
            })
        })
    }

    fn get(&self, key: &str) -> BoxFuture<'_, Option<Vec<u8>>> {
        let key = key.to_string();
        Box::pin(async move {
            Ok(match self.get(&key).await {
                Ok(Some(item)) => Some(item.content),
                _ => None,
            })
        })
    }
    
    fn get_clean_branches(&self) -> BoxFuture<'_, Vec<(String, [u8; 32])>> {
        Box::pin(async move {
            Ok(match self.get_clean_branches().await {
                Ok(branches_map) => branches_map.into_iter().collect(),
                Err(_) => Vec::new(),
            })
        })
    }
    
    fn is_branch_clean(&self, prefix: &str) -> BoxFuture<'_, bool> {
        let prefix = prefix.to_string();
        Box::pin(async move {
            Ok(match self.branch_dirty_count(&prefix).await {
                Ok(count) => count == 0,
                Err(_) => false, // Assume dirty on error (conservative)
            })
        })
    }
}

/// A no-op implementation for testing/standalone mode.
///
/// Logs operations but doesn't actually store anything.
#[derive(Clone)]
pub struct NoOpSyncEngine;

impl SyncEngineRef for NoOpSyncEngine {
    fn submit(
        &self,
        item: SyncItem,
    ) -> Pin<Box<dyn Future<Output = SyncResult<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::debug!(
                key = %item.object_id,
                hash = %item.content_hash,
                len = item.content.len(),
                version = item.version,
                "NoOp: would submit item"
            );
            Ok(())
        })
    }

    fn delete(
        &self,
        key: String,
    ) -> Pin<Box<dyn Future<Output = SyncResult<bool>> + Send + '_>> {
        Box::pin(async move {
            tracing::debug!(key = %key, "NoOp: would delete item");
            Ok(true)
        })
    }

    fn is_current(
        &self,
        key: &str,
        content_hash: &str,
    ) -> Pin<Box<dyn Future<Output = SyncResult<bool>> + Send + '_>> {
        let key = key.to_string();
        let hash = content_hash.to_string();
        Box::pin(async move {
            tracing::trace!(key = %key, hash = %hash, "NoOp: is_current check (returning false)");
            Ok(false) // Always apply in no-op mode
        })
    }

    fn get_merkle_root(&self) -> BoxFuture<'_, Option<[u8; 32]>> {
        Box::pin(async { Ok(None) })
    }

    fn get_merkle_children(&self, _path: &str) -> BoxFuture<'_, Vec<(String, [u8; 32])>> {
        Box::pin(async { Ok(vec![]) })
    }

    fn get(&self, _key: &str) -> BoxFuture<'_, Option<Vec<u8>>> {
        Box::pin(async { Ok(None) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_noop_sync_engine_submit() {
        let engine = NoOpSyncEngine;
        
        // Submit should succeed
        let item = SyncItem::new("test.key".to_string(), b"data".to_vec());
        let result = engine.submit(item).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_noop_sync_engine_submit_large_content() {
        let engine = NoOpSyncEngine;
        
        // Large content should work
        let large_content = vec![0u8; 1024 * 1024]; // 1MB
        let item = SyncItem::new("large.key".to_string(), large_content);
        let result = engine.submit(item).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_noop_sync_engine_delete() {
        let engine = NoOpSyncEngine;
        
        // Delete should succeed and return true
        let result = engine.delete("some.key".to_string()).await;
        assert!(result.is_ok());
        assert!(result.unwrap()); // NoOp always returns true
    }

    #[tokio::test]
    async fn test_noop_sync_engine_is_current() {
        let engine = NoOpSyncEngine;
        
        // is_current should return false (no dedup in noop mode)
        let result = engine.is_current("test.key", "hash123").await;
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Always false in noop mode
    }

    #[tokio::test]
    async fn test_noop_sync_engine_is_current_various_keys() {
        let engine = NoOpSyncEngine;
        
        // All keys should return false
        assert!(!engine.is_current("key1", "hash1").await.unwrap());
        assert!(!engine.is_current("key2", "hash2").await.unwrap());
        assert!(!engine.is_current("", "").await.unwrap());
    }

    #[tokio::test]
    async fn test_noop_sync_engine_get_merkle_root() {
        let engine = NoOpSyncEngine;
        
        // Should return None
        let result = engine.get_merkle_root().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_noop_sync_engine_get_merkle_children() {
        let engine = NoOpSyncEngine;
        
        // Should return empty vec
        let result = engine.get_merkle_children("some/path").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        // Even for empty path
        let result = engine.get_merkle_children("").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_noop_sync_engine_get() {
        let engine = NoOpSyncEngine;
        
        // Should return None
        let result = engine.get("some.key").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_sync_error_display() {
        let error = SyncError("test error message".to_string());
        assert_eq!(format!("{}", error), "test error message");
    }

    #[test]
    fn test_sync_error_debug() {
        let error = SyncError("debug message".to_string());
        let debug = format!("{:?}", error);
        assert!(debug.contains("debug message"));
    }

    #[test]
    fn test_sync_error_is_error() {
        let error = SyncError("error".to_string());
        // Verify it implements std::error::Error
        let _: &dyn std::error::Error = &error;
    }

    #[test]
    fn test_noop_sync_engine_clone() {
        let engine = NoOpSyncEngine;
        let _cloned = engine.clone();
        // Just verify Clone works
    }

    #[test]
    fn test_sync_error_clone() {
        let error = SyncError("original".to_string());
        let cloned = error.clone();
        assert_eq!(error.0, cloned.0);
    }
}
