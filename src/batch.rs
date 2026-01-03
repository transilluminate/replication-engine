//! Batch processor for CDC events.
//!
//! Collects events with key deduplication (latest wins) and flushes to sync-engine
//! in batches for efficiency.
//!
//! # Design
//!
//! ```text
//! CDC Events ──┬──▶ BatchProcessor ──┬──▶ Debounce (time/count)
//!              │                     │
//!              │ HashMap<key, state> │
//!              │ (latest wins)       │
//!              │                     ▼
//!              └─────────────────────┼──▶ Parallel is_current() dedup
//!                                    │
//!                                    ▼
//!                      submit_many() / delete_many()
//! ```
//!
use crate::error::Result;
use crate::stream::{CdcEvent, CdcOp};
use crate::sync_engine::{SyncEngineRef, SyncItem};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, info, instrument, warn};

/// Configuration for batch processing.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum events before forcing a flush.
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing.
    pub max_batch_delay: Duration,
    /// Maximum concurrent is_current() checks.
    pub max_concurrent_checks: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_batch_delay: Duration::from_millis(50),
            max_concurrent_checks: 32,
        }
    }
}

impl BatchConfig {
    /// Fast flush for testing.
    pub fn testing() -> Self {
        Self {
            max_batch_size: 10,
            max_batch_delay: Duration::from_millis(5),
            max_concurrent_checks: 4,
        }
    }
}

/// Pending state for a key (latest wins).
#[derive(Debug, Clone)]
enum PendingOp {
    Put {
        data: Vec<u8>,
        hash: String,
        version: u64,
    },
    Delete,
}

/// Result of processing a batch.
#[derive(Debug, Default)]
pub struct BatchResult {
    /// Number of events in the batch (after dedup).
    pub total: usize,
    /// Number of items skipped (already current).
    pub skipped: usize,
    /// Number of items submitted.
    pub submitted: usize,
    /// Number of items deleted.
    pub deleted: usize,
    /// Number of errors.
    pub errors: usize,
}

impl BatchResult {
    /// Check if all operations succeeded.
    pub fn is_success(&self) -> bool {
        self.errors == 0
    }
}

/// Accumulates CDC events and flushes to sync-engine in batches.
pub struct BatchProcessor<S: SyncEngineRef> {
    /// Pending operations keyed by object ID (latest wins).
    pending: HashMap<String, PendingOp>,
    /// When the current batch started accumulating.
    batch_start: Option<Instant>,
    /// Configuration.
    config: BatchConfig,
    /// Sync engine to submit to.
    sync_engine: Arc<S>,
    /// Peer ID (for logging).
    peer_id: String,
}

impl<S: SyncEngineRef> BatchProcessor<S> {
    /// Create a new batch processor.
    pub fn new(sync_engine: Arc<S>, peer_id: String, config: BatchConfig) -> Self {
        Self {
            pending: HashMap::new(),
            batch_start: None,
            config,
            sync_engine,
            peer_id,
        }
    }

    /// Add a CDC event to the batch.
    ///
    /// If the same key has a pending operation, the new one replaces it (latest wins).
    pub fn add(&mut self, event: CdcEvent) {
        // Start batch timer on first event
        if self.batch_start.is_none() {
            self.batch_start = Some(Instant::now());
        }

        // Latest operation for this key wins
        let op = match event.op {
            CdcOp::Put => {
                // PUT requires data and hash
                if let (Some(data), Some(hash)) = (event.data, event.hash) {
                    let version = event.meta.map(|m| m.version).unwrap_or(0);
                    PendingOp::Put { data, hash, version }
                } else {
                    warn!(
                        peer_id = %self.peer_id,
                        key = %event.key,
                        "PUT event missing data or hash, skipping"
                    );
                    return;
                }
            }
            CdcOp::Delete => PendingOp::Delete,
        };

        self.pending.insert(event.key, op);
    }

    /// Check if the batch should be flushed.
    pub fn should_flush(&self) -> bool {
        // Size threshold
        if self.pending.len() >= self.config.max_batch_size {
            return true;
        }

        // Time threshold
        if let Some(start) = self.batch_start {
            if start.elapsed() >= self.config.max_batch_delay {
                return true;
            }
        }

        false
    }

    /// Number of pending events.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Check if batch is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Flush the batch to sync-engine.
    ///
    /// 1. Partition by operation type
    /// 2. Parallel is_current() checks for PUTs
    /// 3. Submit filtered PUTs and DELETEs
    #[instrument(skip(self), fields(peer_id = %self.peer_id))]
    pub async fn flush(&mut self) -> Result<BatchResult> {
        if self.pending.is_empty() {
            return Ok(BatchResult::default());
        }

        let batch = std::mem::take(&mut self.pending);
        self.batch_start = None;

        let total = batch.len();
        debug!(
            peer_id = %self.peer_id,
            batch_size = total,
            "Flushing batch"
        );

        // Partition by operation type
        let mut puts: Vec<(String, Vec<u8>, String, u64)> = Vec::new();
        let mut deletes: Vec<String> = Vec::new();

        for (key, op) in batch {
            match op {
                PendingOp::Put { data, hash, version } => {
                    puts.push((key, data, hash, version));
                }
                PendingOp::Delete => {
                    deletes.push(key);
                }
            }
        }

        let mut result = BatchResult {
            total,
            ..Default::default()
        };

        // Phase 1: Parallel is_current() checks for dedup
        let filtered_puts = self.dedup_puts(puts).await?;
        result.skipped = total - filtered_puts.len() - deletes.len();

        // Phase 2: Submit filtered puts
        for (key, data, hash, version) in filtered_puts {
            let mut item = SyncItem::new(key.clone(), data);
            item.version = version;
            // Hash is already computed by SyncItem::new(), but we use the peer's hash
            // to ensure we detect duplicates correctly during dedup phase
            item.content_hash = hash;
            
            match self.sync_engine.submit(item).await {
                Ok(()) => {
                    result.submitted += 1;
                }
                Err(e) => {
                    warn!(
                        peer_id = %self.peer_id,
                        key = %key,
                        error = %e,
                        "Failed to submit item"
                    );
                    result.errors += 1;
                }
            }
        }

        // Phase 3: Process deletes
        for key in deletes {
            match self.sync_engine.delete(key.clone()).await {
                Ok(_deleted) => {
                    result.deleted += 1;
                }
                Err(e) => {
                    warn!(
                        peer_id = %self.peer_id,
                        key = %key,
                        error = %e,
                        "Failed to delete item"
                    );
                    result.errors += 1;
                }
            }
        }

        info!(
            peer_id = %self.peer_id,
            total = result.total,
            skipped = result.skipped,
            submitted = result.submitted,
            deleted = result.deleted,
            errors = result.errors,
            "Batch flush complete"
        );

        Ok(result)
    }

    /// Filter out PUTs that are already current (dedup).
    async fn dedup_puts(
        &self,
        puts: Vec<(String, Vec<u8>, String, u64)>,
    ) -> Result<Vec<(String, Vec<u8>, String, u64)>> {
        if puts.is_empty() {
            return Ok(Vec::new());
        }

        let mut to_submit = Vec::with_capacity(puts.len());
        let mut join_set: JoinSet<(String, Vec<u8>, String, u64, bool)> = JoinSet::new();

        // Spawn parallel is_current() checks
        for (key, data, hash, version) in puts {
            let sync_engine = Arc::clone(&self.sync_engine);
            let key_clone = key.clone();
            let hash_clone = hash.clone();

            join_set.spawn(async move {
                let is_current = sync_engine
                    .is_current(&key_clone, &hash_clone)
                    .await
                    .unwrap_or(false);
                (key, data, hash, version, is_current)
            });
        }

        // Collect results
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((key, data, hash, version, is_current)) => {
                    if !is_current {
                        to_submit.push((key, data, hash, version));
                    } else {
                        debug!(key = %key, "Skipping (already current)");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "is_current check failed (JoinError)");
                }
            }
        }

        Ok(to_submit)
    }
}

/// Thread-safe wrapper around BatchProcessor.
pub struct SharedBatchProcessor<S: SyncEngineRef> {
    inner: Mutex<BatchProcessor<S>>,
}

impl<S: SyncEngineRef> SharedBatchProcessor<S> {
    /// Create a new shared batch processor.
    pub fn new(sync_engine: Arc<S>, peer_id: String, config: BatchConfig) -> Self {
        Self {
            inner: Mutex::new(BatchProcessor::new(sync_engine, peer_id, config)),
        }
    }

    /// Add an event and potentially flush.
    pub async fn add(&self, event: CdcEvent) -> Result<Option<BatchResult>> {
        let mut processor = self.inner.lock().await;
        processor.add(event);

        if processor.should_flush() {
            Ok(Some(processor.flush().await?))
        } else {
            Ok(None)
        }
    }

    /// Force a flush regardless of thresholds.
    pub async fn flush(&self) -> Result<BatchResult> {
        let mut processor = self.inner.lock().await;
        processor.flush().await
    }

    /// Check if there are pending events.
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }

    /// Get pending event count.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_engine::SyncResult;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Test sync engine that tracks calls.
    struct TrackingSyncEngine {
        submit_count: AtomicUsize,
        delete_count: AtomicUsize,
        is_current_count: AtomicUsize,
        always_current: AtomicBool,
    }

    impl TrackingSyncEngine {
        fn new() -> Self {
            Self {
                submit_count: AtomicUsize::new(0),
                delete_count: AtomicUsize::new(0),
                is_current_count: AtomicUsize::new(0),
                always_current: AtomicBool::new(false),
            }
        }

        fn set_always_current(&self, value: bool) {
            self.always_current.store(value, Ordering::SeqCst);
        }
    }

    impl SyncEngineRef for TrackingSyncEngine {
        fn submit(
            &self,
            _item: SyncItem,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncResult<()>> + Send + '_>> {
            self.submit_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }

        fn delete(
            &self,
            _key: String,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncResult<bool>> + Send + '_>> {
            self.delete_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(true) })
        }

        fn is_current(
            &self,
            _key: &str,
            _content_hash: &str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncResult<bool>> + Send + '_>> {
            self.is_current_count.fetch_add(1, Ordering::SeqCst);
            let result = self.always_current.load(Ordering::SeqCst);
            Box::pin(async move { Ok(result) })
        }

        fn get_merkle_root(&self) -> crate::sync_engine::BoxFuture<'_, Option<[u8; 32]>> {
            Box::pin(async { Ok(None) })
        }

        fn get_merkle_children(
            &self,
            _path: &str,
        ) -> crate::sync_engine::BoxFuture<'_, Vec<(String, [u8; 32])>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn get(&self, _key: &str) -> crate::sync_engine::BoxFuture<'_, Option<Vec<u8>>> {
            Box::pin(async { Ok(None) })
        }
    }

    fn make_put_event(key: &str, data: &str) -> CdcEvent {
        CdcEvent {
            stream_id: "0-0".to_string(),
            op: CdcOp::Put,
            key: key.to_string(),
            hash: Some(format!("hash_{}", key)),
            data: Some(data.as_bytes().to_vec()),
            meta: None,
        }
    }

    fn make_delete_event(key: &str) -> CdcEvent {
        CdcEvent {
            stream_id: "0-0".to_string(),
            op: CdcOp::Delete,
            key: key.to_string(),
            hash: None,
            data: None,
            meta: None,
        }
    }

    #[tokio::test]
    async fn test_batch_key_deduplication() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        // Add multiple events for same key - only latest should be processed
        processor.add(make_put_event("user.1", "v1"));
        processor.add(make_put_event("user.1", "v2"));
        processor.add(make_put_event("user.1", "v3")); // This one wins

        assert_eq!(processor.len(), 1);

        let result = processor.flush().await.unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.submitted, 1);
        assert_eq!(engine.submit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_batch_dedup_skips_current() {
        let engine = Arc::new(TrackingSyncEngine::new());
        engine.set_always_current(true); // Everything is already current

        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        processor.add(make_put_event("user.1", "data1"));
        processor.add(make_put_event("user.2", "data2"));

        let result = processor.flush().await.unwrap();
        assert_eq!(result.total, 2);
        assert_eq!(result.skipped, 2);
        assert_eq!(result.submitted, 0);

        // is_current should be called for each
        assert_eq!(engine.is_current_count.load(Ordering::SeqCst), 2);
        // submit should not be called
        assert_eq!(engine.submit_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_batch_mixed_operations() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        processor.add(make_put_event("user.1", "data1"));
        processor.add(make_delete_event("user.2"));
        processor.add(make_put_event("user.3", "data3"));

        let result = processor.flush().await.unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.submitted, 2); // 2 puts
        assert_eq!(result.deleted, 1);   // 1 delete
    }

    #[tokio::test]
    async fn test_put_then_delete_same_key() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        // PUT then DELETE - DELETE wins (latest)
        processor.add(make_put_event("user.1", "data"));
        processor.add(make_delete_event("user.1"));

        let result = processor.flush().await.unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.submitted, 0);
        assert_eq!(result.deleted, 1);
    }

    #[tokio::test]
    async fn test_delete_then_put_same_key() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        // DELETE then PUT - PUT wins (latest)
        processor.add(make_delete_event("user.1"));
        processor.add(make_put_event("user.1", "data"));

        let result = processor.flush().await.unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.submitted, 1);
        assert_eq!(result.deleted, 0);
    }

    #[tokio::test]
    async fn test_should_flush_by_size() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig {
                max_batch_size: 3,
                max_batch_delay: Duration::from_secs(60),
                max_concurrent_checks: 4,
            },
        );

        processor.add(make_put_event("a", "1"));
        assert!(!processor.should_flush());

        processor.add(make_put_event("b", "2"));
        assert!(!processor.should_flush());

        processor.add(make_put_event("c", "3"));
        assert!(processor.should_flush()); // Hit max_batch_size
    }

    #[tokio::test]
    async fn test_empty_flush() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        let result = processor.flush().await.unwrap();
        assert_eq!(result.total, 0);
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_batch_is_empty() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        assert!(processor.is_empty());
        assert_eq!(processor.len(), 0);

        processor.add(make_put_event("key", "data"));
        assert!(!processor.is_empty());
        assert_eq!(processor.len(), 1);

        processor.flush().await.unwrap();
        assert!(processor.is_empty());
        assert_eq!(processor.len(), 0);
    }

    #[tokio::test]
    async fn test_should_flush_by_time() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig {
                max_batch_size: 1000, // High, won't trigger
                max_batch_delay: Duration::from_millis(10),
                max_concurrent_checks: 4,
            },
        );

        processor.add(make_put_event("a", "1"));
        assert!(!processor.should_flush());

        // Wait for delay
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(processor.should_flush());
    }

    #[tokio::test]
    async fn test_batch_result_is_success() {
        let mut result = BatchResult::default();
        assert!(result.is_success());

        result.errors = 1;
        assert!(!result.is_success());

        result.errors = 0;
        result.submitted = 10;
        result.skipped = 5;
        assert!(result.is_success());
    }

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.max_batch_delay, Duration::from_millis(50));
        assert_eq!(config.max_concurrent_checks, 32);
    }

    #[test]
    fn test_batch_config_testing() {
        let config = BatchConfig::testing();
        assert_eq!(config.max_batch_size, 10);
        assert_eq!(config.max_batch_delay, Duration::from_millis(5));
        assert_eq!(config.max_concurrent_checks, 4);
    }

    #[tokio::test]
    async fn test_put_event_without_data() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        // PUT without data should be skipped
        let event = CdcEvent {
            stream_id: "0-0".to_string(),
            op: CdcOp::Put,
            key: "key1".to_string(),
            hash: Some("hash".to_string()),
            data: None, // Missing data
            meta: None,
        };
        processor.add(event);

        assert_eq!(processor.len(), 0); // Should not be added
    }

    #[tokio::test]
    async fn test_put_event_without_hash() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        // PUT without hash should be skipped
        let event = CdcEvent {
            stream_id: "0-0".to_string(),
            op: CdcOp::Put,
            key: "key1".to_string(),
            hash: None, // Missing hash
            data: Some(vec![1, 2, 3]),
            meta: None,
        };
        processor.add(event);

        assert_eq!(processor.len(), 0); // Should not be added
    }

    #[tokio::test]
    async fn test_multiple_puts_same_key_different_data() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        // First put
        processor.add(make_put_event("user.1", "first"));
        // Second put with different data - should replace
        processor.add(make_put_event("user.1", "second"));
        // Third put - final value
        processor.add(make_put_event("user.1", "third"));

        assert_eq!(processor.len(), 1);

        let result = processor.flush().await.unwrap();
        assert_eq!(result.submitted, 1);
    }

    #[tokio::test]
    async fn test_many_different_keys() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        // Many different keys
        for i in 0..50 {
            processor.add(make_put_event(&format!("key.{}", i), &format!("data{}", i)));
        }

        assert_eq!(processor.len(), 50);

        let result = processor.flush().await.unwrap();
        assert_eq!(result.total, 50);
        assert_eq!(result.submitted, 50);
    }

    #[tokio::test]
    async fn test_interleaved_operations() {
        let engine = Arc::new(TrackingSyncEngine::new());
        let mut processor = BatchProcessor::new(
            Arc::clone(&engine),
            "test-peer".to_string(),
            BatchConfig::testing(),
        );

        // Interleaved PUTs and DELETEs on different keys
        processor.add(make_put_event("a", "1"));
        processor.add(make_delete_event("b"));
        processor.add(make_put_event("c", "3"));
        processor.add(make_delete_event("d"));
        processor.add(make_put_event("e", "5"));

        assert_eq!(processor.len(), 5);

        let result = processor.flush().await.unwrap();
        assert_eq!(result.total, 5);
        assert_eq!(result.submitted, 3);
        assert_eq!(result.deleted, 2);
    }
}
