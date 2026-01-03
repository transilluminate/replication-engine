//! Mock SyncEngineRef for testing.
//!
//! Records all calls to submit(), delete(), is_current() for assertions.
//! Configurable responses for is_current() to test dedup logic.
//! Includes Merkle tree simulation for cold path testing.

use replication_engine::sync_engine::{BoxFuture, SyncEngineRef, SyncError, SyncResult};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;

/// A recorded submit() call.
#[derive(Debug, Clone)]
pub struct SubmitCall {
    pub key: String,
    #[allow(dead_code)] // Recorded for future detailed assertions
    content: Vec<u8>,
    #[allow(dead_code)]
    content_hash: String,
    #[allow(dead_code)]
    version: u64,
}

/// A recorded delete() call.
#[derive(Debug, Clone)]
pub struct DeleteCall {
    pub key: String,
}

/// A recorded is_current() call.
#[derive(Debug, Clone)]
pub struct IsCurrentCall {
    #[allow(dead_code)] // Recorded for future detailed assertions
    key: String,
    #[allow(dead_code)]
    content_hash: String,
}

/// Mock Merkle tree data for cold path testing.
#[derive(Default)]
struct MockMerkle {
    /// Root hash
    root: Option<[u8; 32]>,
    /// Children by path: path -> Vec<(segment, hash)>
    children: HashMap<String, Vec<(String, [u8; 32])>>,
    /// Stored items: key -> data
    items: HashMap<String, Vec<u8>>,
}

/// Mock implementation of SyncEngineRef that records all calls.
///
/// # Example
/// ```rust,ignore
/// let mock = MockSyncEngine::new();
/// 
/// // Configure responses
/// mock.set_is_current_response("key1", "hash123", true).await;
/// 
/// // Use in tests...
/// 
/// // Assert what was called
/// let submits = mock.submitted().await;
/// assert_eq!(submits.len(), 5);
/// ```
pub struct MockSyncEngine {
    /// Recorded submit() calls
    submits: RwLock<Vec<SubmitCall>>,
    /// Recorded delete() calls
    deletes: RwLock<Vec<DeleteCall>>,
    /// Recorded is_current() calls
    is_current_calls: RwLock<Vec<IsCurrentCall>>,
    /// Configured is_current() responses (key -> hash -> response)
    is_current_responses: RwLock<HashMap<String, HashMap<String, bool>>>,
    /// Default is_current() response when not configured
    default_is_current: bool,
    /// Simulate failures after N calls
    fail_after_submits: AtomicUsize,
    /// Counter for submit calls
    submit_count: AtomicUsize,
    /// Merkle tree data for cold path
    merkle: RwLock<MockMerkle>,
}

impl MockSyncEngine {
    /// Create a new mock that accepts all items (is_current = false).
    pub fn new() -> Self {
        Self {
            submits: RwLock::new(Vec::new()),
            deletes: RwLock::new(Vec::new()),
            is_current_calls: RwLock::new(Vec::new()),
            is_current_responses: RwLock::new(HashMap::new()),
            default_is_current: false,
            fail_after_submits: AtomicUsize::new(usize::MAX),
            submit_count: AtomicUsize::new(0),
            merkle: RwLock::new(MockMerkle::default()),
        }
    }

    /// Create a mock that rejects all items (is_current = true, meaning "already have it").
    pub fn rejecting() -> Self {
        Self {
            default_is_current: true,
            ..Self::new()
        }
    }

    // =========================================================================
    // Merkle Tree Configuration (for cold path testing)
    // =========================================================================

    /// Set the local Merkle root hash.
    pub async fn set_merkle_root(&self, hash: [u8; 32]) {
        let mut merkle = self.merkle.write().await;
        merkle.root = Some(hash);
    }

    /// Set Merkle children for a path.
    #[allow(dead_code)] // For cold path testing
    pub async fn set_merkle_children(&self, path: &str, children: Vec<(String, [u8; 32])>) {
        let mut merkle = self.merkle.write().await;
        merkle.children.insert(path.to_string(), children);
    }

    /// Store an item in the mock (for get() to return).
    #[allow(dead_code)] // For cold path testing
    pub async fn store_item(&self, key: &str, data: Vec<u8>) {
        let mut merkle = self.merkle.write().await;
        merkle.items.insert(key.to_string(), data);
    }

    // =========================================================================
    // Query Methods
    // =========================================================================

    /// Configure is_current() to return a specific value for a key+hash pair.
    pub async fn set_is_current_response(&self, key: &str, hash: &str, response: bool) {
        let mut responses = self.is_current_responses.write().await;
        responses
            .entry(key.to_string())
            .or_default()
            .insert(hash.to_string(), response);
    }

    /// Configure submit() to fail after N successful calls.
    pub fn fail_after(&self, n: usize) {
        self.fail_after_submits.store(n, Ordering::SeqCst);
    }

    /// Get all recorded submit() calls.
    pub async fn submitted(&self) -> Vec<SubmitCall> {
        self.submits.read().await.clone()
    }

    /// Get all recorded delete() calls.
    pub async fn deleted(&self) -> Vec<DeleteCall> {
        self.deletes.read().await.clone()
    }

    /// Get all recorded is_current() calls.
    pub async fn is_current_calls(&self) -> Vec<IsCurrentCall> {
        self.is_current_calls.read().await.clone()
    }

    /// Get count of submitted items.
    #[allow(dead_code)] // Useful for future tests
    pub async fn submit_count(&self) -> usize {
        self.submits.read().await.len()
    }

    /// Get count of deleted items.
    #[allow(dead_code)] // Useful for future tests
    pub async fn delete_count(&self) -> usize {
        self.deletes.read().await.len()
    }

    /// Clear all recorded calls.
    #[allow(dead_code)] // Useful for future tests
    pub async fn reset(&self) {
        self.submits.write().await.clear();
        self.deletes.write().await.clear();
        self.is_current_calls.write().await.clear();
        self.submit_count.store(0, Ordering::SeqCst);
    }

    /// Check if a specific key was submitted.
    #[allow(dead_code)] // Useful for future tests
    pub async fn was_submitted(&self, key: &str) -> bool {
        self.submits.read().await.iter().any(|s| s.key == key)
    }

    /// Check if a specific key was deleted.
    #[allow(dead_code)] // Useful for future tests
    pub async fn was_deleted(&self, key: &str) -> bool {
        self.deletes.read().await.iter().any(|d| d.key == key)
    }
}

impl Default for MockSyncEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncEngineRef for MockSyncEngine {
    fn submit(
        &self,
        key: String,
        content: Vec<u8>,
        content_hash: String,
        version: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncResult<()>> + Send + '_>> {
        Box::pin(async move {
            // Check if we should fail
            let count = self.submit_count.fetch_add(1, Ordering::SeqCst);
            if count >= self.fail_after_submits.load(Ordering::SeqCst) {
                return Err(SyncError("Simulated failure".to_string()));
            }

            // Record the call
            self.submits.write().await.push(SubmitCall {
                key,
                content,
                content_hash,
                version,
            });
            Ok(())
        })
    }

    fn delete(
        &self,
        key: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncResult<bool>> + Send + '_>> {
        Box::pin(async move {
            self.deletes.write().await.push(DeleteCall { key });
            Ok(true)
        })
    }

    fn is_current(
        &self,
        key: &str,
        content_hash: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncResult<bool>> + Send + '_>> {
        let key = key.to_string();
        let hash = content_hash.to_string();
        Box::pin(async move {
            // Record the call
            self.is_current_calls.write().await.push(IsCurrentCall {
                key: key.clone(),
                content_hash: hash.clone(),
            });

            // Check for configured response
            let responses = self.is_current_responses.read().await;
            if let Some(key_responses) = responses.get(&key) {
                if let Some(&response) = key_responses.get(&hash) {
                    return Ok(response);
                }
            }

            // Return default
            Ok(self.default_is_current)
        })
    }

    fn get_merkle_root(&self) -> BoxFuture<'_, Option<[u8; 32]>> {
        Box::pin(async {
            let merkle = self.merkle.read().await;
            Ok(merkle.root)
        })
    }

    fn get_merkle_children(&self, path: &str) -> BoxFuture<'_, Vec<(String, [u8; 32])>> {
        let path = path.to_string();
        Box::pin(async move {
            let merkle = self.merkle.read().await;
            Ok(merkle.children.get(&path).cloned().unwrap_or_default())
        })
    }

    fn get(&self, key: &str) -> BoxFuture<'_, Option<Vec<u8>>> {
        let key = key.to_string();
        Box::pin(async move {
            let merkle = self.merkle.read().await;
            Ok(merkle.items.get(&key).cloned())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_records_submits() {
        let mock = MockSyncEngine::new();

        mock.submit(
            "key1".to_string(),
            b"data".to_vec(),
            "hash1".to_string(),
            1,
        )
        .await
        .unwrap();

        let submits = mock.submitted().await;
        assert_eq!(submits.len(), 1);
        assert_eq!(submits[0].key, "key1");
    }

    #[tokio::test]
    async fn test_mock_is_current_responses() {
        let mock = MockSyncEngine::new();

        // Default is false (accept)
        assert!(!mock.is_current("key1", "hash1").await.unwrap());

        // Configure specific response
        mock.set_is_current_response("key1", "hash1", true).await;
        assert!(mock.is_current("key1", "hash1").await.unwrap());

        // Different hash still uses default
        assert!(!mock.is_current("key1", "hash2").await.unwrap());
    }

    #[tokio::test]
    async fn test_mock_fail_after() {
        let mock = MockSyncEngine::new();
        mock.fail_after(2);

        // First two succeed
        assert!(mock
            .submit("k1".into(), vec![], "h1".into(), 1)
            .await
            .is_ok());
        assert!(mock
            .submit("k2".into(), vec![], "h2".into(), 2)
            .await
            .is_ok());

        // Third fails
        assert!(mock
            .submit("k3".into(), vec![], "h3".into(), 3)
            .await
            .is_err());
    }
}
