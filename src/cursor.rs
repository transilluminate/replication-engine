// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Cursor persistence for stream positions.
//!
//! Stores the last-read stream ID for each peer in SQLite.
//! This survives Redis restarts (we're not relying on AOF!) and daemon restarts.
//!
//! # Debounced Writes
//!
//! To reduce SQLite write pressure, cursors are debounced:
//! - `set()` updates the in-memory cache immediately and marks the cursor dirty
//! - `flush_dirty()` persists all dirty cursors to disk in a batch
//! - The coordinator calls `flush_dirty()` periodically (every few seconds)
//! - On shutdown, `flush_dirty()` is called to ensure no data loss
//!
//! This means a crash between `set()` and `flush_dirty()` could lose up to
//! one flush interval of cursor progress. On restart, we'd re-read some
//! events that were already applied (idempotent, safe).
//!
//! # SQLite Busy Handling
//!
//! SQLite can return SQLITE_BUSY/SQLITE_LOCKED when the database is
//! contended. We handle this with:
//! - Automatic retry with exponential backoff
//! - Configurable max retries (default 5)
//! - Cache-first writes (cache is updated immediately, disk write retried)
//!
//! ## Why SQLite?
//!
//! - Redis may be ephemeral (no AOF/RDB persistence)
//! - We need to survive both Redis and calling daemon restarts
//! - Cursors are small and low-write (updated every few seconds)
//! - SQLite WAL mode gives us durability with good performance
//!
//! ## Cursor Semantics
//!
//! The cursor stores the **last successfully applied** stream ID.
//! On restart, we resume from `cursor + 1` (exclusive read).
//!
//! ```text
//! read event 1234 → apply to sync-engine → persist cursor 1234
//!                   (crash here = re-read 1234, idempotent)
//! ```

use crate::error::{ReplicationError, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for SQLite busy retry behavior
const SQLITE_RETRY_MAX_ATTEMPTS: u32 = 5;
const SQLITE_RETRY_BASE_DELAY_MS: u64 = 10;
const SQLITE_RETRY_MAX_DELAY_MS: u64 = 500;

/// Check if an error is a retryable SQLite busy/locked error
fn is_sqlite_busy_error(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(db_err) => {
            // SQLite error codes: SQLITE_BUSY = 5, SQLITE_LOCKED = 6
            if let Some(code) = db_err.code() {
                return code == "5" || code == "6";
            }
            // Fallback to message matching
            let msg = db_err.message().to_lowercase();
            msg.contains("database is locked") || msg.contains("database is busy")
        }
        _ => false,
    }
}

/// Execute a database operation with retry on SQLITE_BUSY/SQLITE_LOCKED
async fn execute_with_retry<F, Fut, T>(operation_name: &str, mut f: F) -> std::result::Result<T, sqlx::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, sqlx::Error>>,
{
    let mut attempts = 0;
    let mut delay_ms = SQLITE_RETRY_BASE_DELAY_MS;

    loop {
        attempts += 1;
        match f().await {
            Ok(result) => {
                if attempts > 1 {
                    debug!(
                        operation = operation_name,
                        attempts,
                        "SQLite operation succeeded after retry"
                    );
                }
                return Ok(result);
            }
            Err(e) if is_sqlite_busy_error(&e) && attempts < SQLITE_RETRY_MAX_ATTEMPTS => {
                warn!(
                    operation = operation_name,
                    attempts,
                    max_attempts = SQLITE_RETRY_MAX_ATTEMPTS,
                    delay_ms,
                    "SQLite busy, retrying"
                );
                crate::metrics::cursor_retries_total(operation_name);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                // Exponential backoff with cap
                delay_ms = (delay_ms * 2).min(SQLITE_RETRY_MAX_DELAY_MS);
            }
            Err(e) => {
                if is_sqlite_busy_error(&e) {
                    warn!(
                        operation = operation_name,
                        attempts,
                        "SQLite busy, max retries exceeded"
                    );
                }
                return Err(e);
            }
        }
    }
}

/// Stream cursor entry
#[derive(Debug, Clone)]
pub struct CursorEntry {
    /// Peer node ID
    pub peer_id: String,
    /// Last successfully applied stream ID (e.g., "1234567890123-0")
    pub stream_id: String,
    /// Timestamp of last update
    pub updated_at: i64,
}

/// Persistent cursor storage backed by SQLite.
///
/// Supports debounced writes: updates go to cache immediately,
/// and are flushed to disk periodically via `flush_dirty()`.
pub struct CursorStore {
    /// SQLite connection pool
    pool: SqlitePool,
    /// In-memory cache for fast reads
    cache: Arc<RwLock<HashMap<String, String>>>,
    /// Peer IDs with dirty (not yet persisted) cursors
    dirty: Arc<RwLock<HashSet<String>>>,
    /// Path to database file
    path: String,
}

impl CursorStore {
    /// Create a new cursor store at the given path.
    ///
    /// Creates the database and tables if they don't exist.
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        info!(path = %path_str, "Initializing cursor store");

        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}?mode=rwc", path_str))
            .map_err(|e| ReplicationError::Config(format!("Invalid SQLite path: {}", e)))?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(2) // Low concurrency needed
            .connect_with(options)
            .await?;

        // Create table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cursors (
                peer_id TEXT PRIMARY KEY,
                stream_id TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;

        // Load existing cursors into cache
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT peer_id, stream_id FROM cursors")
                .fetch_all(&pool)
                .await?;

        let mut cache = HashMap::new();
        for (peer_id, stream_id) in rows {
            debug!(peer_id = %peer_id, stream_id = %stream_id, "Loaded cursor from disk");
            cache.insert(peer_id, stream_id);
        }

        if !cache.is_empty() {
            info!(count = cache.len(), "Restored cursors from previous run");
        }

        Ok(Self {
            pool,
            cache: Arc::new(RwLock::new(cache)),
            dirty: Arc::new(RwLock::new(HashSet::new())),
            path: path_str,
        })
    }

    /// Get the cursor for a peer (from cache).
    ///
    /// Returns `None` if no cursor exists (first sync with this peer).
    pub async fn get(&self, peer_id: &str) -> Option<String> {
        self.cache.read().await.get(peer_id).cloned()
    }

    /// Get the cursor, or return "0" for first-time sync.
    ///
    /// Redis XREAD interprets "0" as "from the beginning".
    pub async fn get_or_start(&self, peer_id: &str) -> String {
        self.get(peer_id).await.unwrap_or_else(|| "0".to_string())
    }

    /// Update the cursor for a peer (debounced).
    ///
    /// Updates cache immediately, marks cursor as dirty.
    /// Call `flush_dirty()` periodically to persist to disk.
    ///
    /// This is the preferred method for hot path updates where
    /// we want to minimize SQLite writes.
    pub async fn set(&self, peer_id: &str, stream_id: &str) {
        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(peer_id.to_string(), stream_id.to_string());
        }

        // Mark as dirty
        {
            let mut dirty = self.dirty.write().await;
            dirty.insert(peer_id.to_string());
        }

        debug!(peer_id = %peer_id, stream_id = %stream_id, "Cursor updated (pending flush)");
    }

    /// Flush all dirty cursors to disk.
    ///
    /// Call this periodically (e.g., every 5 seconds) and on shutdown.
    /// Returns the number of cursors flushed.
    pub async fn flush_dirty(&self) -> Result<usize> {
        // Swap out dirty set atomically
        let dirty_peers: Vec<String> = {
            let mut dirty = self.dirty.write().await;
            let peers: Vec<String> = dirty.drain().collect();
            peers
        };

        if dirty_peers.is_empty() {
            return Ok(0);
        }

        let now = chrono::Utc::now().timestamp_millis();
        let cache = self.cache.read().await;
        let pool = &self.pool;

        let mut flushed = 0;
        let mut errors = 0;

        for peer_id in &dirty_peers {
            if let Some(stream_id) = cache.get(peer_id) {
                let peer_id_owned = peer_id.clone();
                let stream_id_owned = stream_id.clone();

                let result = execute_with_retry("cursor_flush", || async {
                    sqlx::query(
                        r#"
                        INSERT INTO cursors (peer_id, stream_id, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(peer_id) DO UPDATE SET
                            stream_id = excluded.stream_id,
                            updated_at = excluded.updated_at
                        "#,
                    )
                    .bind(&peer_id_owned)
                    .bind(&stream_id_owned)
                    .bind(now)
                    .execute(pool)
                    .await
                })
                .await;

                match result {
                    Ok(_) => {
                        flushed += 1;
                    }
                    Err(e) => {
                        errors += 1;
                        warn!(peer_id = %peer_id, error = %e, "Failed to flush cursor");
                        // Re-mark as dirty so we retry next flush
                        self.dirty.write().await.insert(peer_id.clone());
                    }
                }
            }
        }

        if flushed > 0 {
            debug!(flushed, errors, "Flushed dirty cursors");
            crate::metrics::record_cursor_flush(flushed, errors);
        }

        if errors > 0 {
            return Err(ReplicationError::Internal(format!(
                "Failed to flush {} cursors",
                errors
            )));
        }

        Ok(flushed)
    }

    /// Check if there are any dirty (unflushed) cursors.
    pub async fn has_dirty(&self) -> bool {
        !self.dirty.read().await.is_empty()
    }

    /// Get count of dirty cursors pending flush.
    pub async fn dirty_count(&self) -> usize {
        self.dirty.read().await.len()
    }

    /// Delete cursor for a peer (e.g., when peer is removed from mesh).
    /// Retries on SQLITE_BUSY/SQLITE_LOCKED with exponential backoff.
    pub async fn delete(&self, peer_id: &str) -> Result<()> {
        {
            let mut cache = self.cache.write().await;
            cache.remove(peer_id);
        }

        let pool = &self.pool;
        let peer_id_owned = peer_id.to_string();

        execute_with_retry("cursor_delete", || async {
            sqlx::query("DELETE FROM cursors WHERE peer_id = ?")
                .bind(&peer_id_owned)
                .execute(pool)
                .await
        })
        .await?;

        info!(peer_id = %peer_id, "Deleted cursor");
        Ok(())
    }

    /// Get all cursors (for metrics/debugging).
    pub async fn get_all(&self) -> HashMap<String, String> {
        self.cache.read().await.clone()
    }

    /// Get database path (for diagnostics).
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Force flush WAL to main database (for clean shutdown).
    /// Retries on SQLITE_BUSY/SQLITE_LOCKED with exponential backoff.
    pub async fn checkpoint(&self) -> Result<()> {
        let pool = &self.pool;

        execute_with_retry("cursor_checkpoint", || async {
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(pool)
                .await
        })
        .await?;

        debug!("WAL checkpoint complete");
        Ok(())
    }

    /// Close the connection pool gracefully.
    ///
    /// Flushes any dirty cursors and checkpoints WAL before closing.
    pub async fn close(&self) {
        // Flush any pending cursor updates
        if self.has_dirty().await {
            match self.flush_dirty().await {
                Ok(count) => {
                    if count > 0 {
                        info!(count, "Flushed dirty cursors on close");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to flush dirty cursors on close");
                }
            }
        }

        // Checkpoint WAL for clean shutdown
        if let Err(e) = self.checkpoint().await {
            warn!(error = %e, "Failed to checkpoint WAL on close");
        }
        self.pool.close().await;
        info!("Cursor store closed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_cursor_store_basic() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_cursors.db");

        let store = CursorStore::new(&db_path).await.unwrap();

        // Initially no cursor
        assert!(store.get("peer1").await.is_none());
        assert_eq!(store.get_or_start("peer1").await, "0");

        // Set cursor (debounced - updates cache only)
        store.set("peer1", "1234567890123-0").await;
        assert_eq!(store.get("peer1").await, Some("1234567890123-0".to_string()));
        assert!(store.has_dirty().await);

        // Update cursor
        store.set("peer1", "1234567890124-0").await;
        assert_eq!(store.get("peer1").await, Some("1234567890124-0".to_string()));

        // Flush to disk
        let flushed = store.flush_dirty().await.unwrap();
        assert_eq!(flushed, 1);
        assert!(!store.has_dirty().await);

        store.close().await;
    }

    #[tokio::test]
    async fn test_cursor_store_persistence() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_persist.db");

        // Create and set cursor
        {
            let store = CursorStore::new(&db_path).await.unwrap();
            store.set("peer1", "9999-0").await;
            store.flush_dirty().await.unwrap(); // Must flush before close!
            store.close().await;
        }

        // Reopen and verify
        {
            let store = CursorStore::new(&db_path).await.unwrap();
            assert_eq!(store.get("peer1").await, Some("9999-0".to_string()));
            store.close().await;
        }
    }

    #[tokio::test]
    async fn test_cursor_store_delete() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_delete.db");

        let store = CursorStore::new(&db_path).await.unwrap();
        store.set("peer1", "1234-0").await;
        store.set("peer2", "5678-0").await;
        store.flush_dirty().await.unwrap();

        store.delete("peer1").await.unwrap();

        assert!(store.get("peer1").await.is_none());
        assert_eq!(store.get("peer2").await, Some("5678-0".to_string()));

        store.close().await;
    }

    #[tokio::test]
    async fn test_cursor_debounce_multiple_updates() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_debounce.db");

        let store = CursorStore::new(&db_path).await.unwrap();

        // Multiple rapid updates to same peer
        store.set("peer1", "100-0").await;
        store.set("peer1", "200-0").await;
        store.set("peer1", "300-0").await;

        // Should only have one dirty entry
        assert_eq!(store.dirty_count().await, 1);

        // Cache should have latest value
        assert_eq!(store.get("peer1").await, Some("300-0".to_string()));

        // Flush should only write once
        let flushed = store.flush_dirty().await.unwrap();
        assert_eq!(flushed, 1);

        store.close().await;
    }

    #[tokio::test]
    async fn test_execute_with_retry_succeeds_immediately() {
        let mut attempt_count = 0;

        let result: std::result::Result<i32, sqlx::Error> =
            execute_with_retry("test_op", || {
                attempt_count += 1;
                async { Ok(42) }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempt_count, 1);
    }

    #[tokio::test]
    async fn test_execute_with_retry_fails_on_non_busy_error() {
        let mut attempt_count = 0;

        let result: std::result::Result<i32, sqlx::Error> =
            execute_with_retry("test_op", || {
                attempt_count += 1;
                async { Err(sqlx::Error::RowNotFound) }
            })
            .await;

        assert!(result.is_err());
        // Non-busy errors should not retry
        assert_eq!(attempt_count, 1);
    }

    #[tokio::test]
    async fn test_cursor_store_get_all() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_get_all.db");

        let store = CursorStore::new(&db_path).await.unwrap();
        
        store.set("peer1", "100-0").await;
        store.set("peer2", "200-0").await;
        store.set("peer3", "300-0").await;

        let all = store.get_all().await;
        assert_eq!(all.len(), 3);
        assert_eq!(all.get("peer1"), Some(&"100-0".to_string()));
        assert_eq!(all.get("peer2"), Some(&"200-0".to_string()));
        assert_eq!(all.get("peer3"), Some(&"300-0".to_string()));

        store.close().await;
    }

    #[tokio::test]
    async fn test_cursor_store_path() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_path.db");

        let store = CursorStore::new(&db_path).await.unwrap();
        assert!(store.path().contains("test_path.db"));

        store.close().await;
    }

    #[tokio::test]
    async fn test_cursor_store_checkpoint() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_checkpoint.db");

        let store = CursorStore::new(&db_path).await.unwrap();
        store.set("peer1", "100-0").await;
        store.flush_dirty().await.unwrap();

        // Checkpoint should succeed
        let result = store.checkpoint().await;
        assert!(result.is_ok());

        store.close().await;
    }

    #[tokio::test]
    async fn test_cursor_store_dirty_count() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_dirty_count.db");

        let store = CursorStore::new(&db_path).await.unwrap();

        assert_eq!(store.dirty_count().await, 0);
        assert!(!store.has_dirty().await);

        store.set("peer1", "100-0").await;
        assert_eq!(store.dirty_count().await, 1);
        assert!(store.has_dirty().await);

        store.set("peer2", "200-0").await;
        assert_eq!(store.dirty_count().await, 2);

        // Same peer update doesn't increase count
        store.set("peer1", "150-0").await;
        assert_eq!(store.dirty_count().await, 2);

        store.flush_dirty().await.unwrap();
        assert_eq!(store.dirty_count().await, 0);
        assert!(!store.has_dirty().await);

        store.close().await;
    }

    #[tokio::test]
    async fn test_cursor_store_close_flushes_dirty() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_close_flush.db");

        // Set cursor but don't manually flush
        {
            let store = CursorStore::new(&db_path).await.unwrap();
            store.set("peer1", "999-0").await;
            // close() should flush automatically
            store.close().await;
        }

        // Verify it persisted
        {
            let store = CursorStore::new(&db_path).await.unwrap();
            assert_eq!(store.get("peer1").await, Some("999-0".to_string()));
            store.close().await;
        }
    }

    #[tokio::test]
    async fn test_cursor_store_get_or_start() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_get_or_start.db");

        let store = CursorStore::new(&db_path).await.unwrap();

        // Non-existent peer should return "0"
        assert_eq!(store.get_or_start("new_peer").await, "0");

        // After setting, should return the value
        store.set("new_peer", "123-0").await;
        assert_eq!(store.get_or_start("new_peer").await, "123-0");

        store.close().await;
    }

    #[tokio::test]
    async fn test_cursor_store_multiple_peers() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_multi_peer.db");

        let store = CursorStore::new(&db_path).await.unwrap();

        // Set cursors for many peers
        for i in 0..10 {
            store.set(&format!("peer{}", i), &format!("{}-0", i * 100)).await;
        }

        assert_eq!(store.dirty_count().await, 10);
        
        let flushed = store.flush_dirty().await.unwrap();
        assert_eq!(flushed, 10);

        // Verify all are correct
        for i in 0..10 {
            let expected = format!("{}-0", i * 100);
            assert_eq!(store.get(&format!("peer{}", i)).await, Some(expected));
        }

        store.close().await;
    }

    #[tokio::test]
    async fn test_cursor_store_delete_nonexistent() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_delete_nonexistent.db");

        let store = CursorStore::new(&db_path).await.unwrap();

        // Deleting a non-existent peer should not error
        let result = store.delete("nonexistent").await;
        assert!(result.is_ok());

        store.close().await;
    }

    #[test]
    fn test_is_sqlite_busy_error_row_not_found() {
        let error = sqlx::Error::RowNotFound;
        assert!(!is_sqlite_busy_error(&error));
    }

    #[test]
    fn test_is_sqlite_busy_error_pool_timed_out() {
        let error = sqlx::Error::PoolTimedOut;
        assert!(!is_sqlite_busy_error(&error));
    }
}
