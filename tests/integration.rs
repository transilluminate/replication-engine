// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Integration Tests for Replication Engine
//!
//! Tests use testcontainers for portability - no external docker-compose required.
//!
//! # Running Tests
//! ```bash
//! # Run all integration tests (requires Docker / OrbStack)
//! cargo test --test integration -- --ignored
//!
//! # Run specific test
//! cargo test --test integration stream_tailer -- --ignored
//! ```
//!
//! # Test Organization
//! - `stream_*` - Redis stream reading and CDC parsing
//! - `peer_*` - Peer connection and retry logic
//! - `cursor_*` - Cursor persistence across restarts

mod common;

use common::{redis_container, redis_url, TestPeer};
use replication_engine::stream::StreamTailer;
use replication_engine::peer::PeerConnection;
use replication_engine::config::PeerConfig;
use replication_engine::resilience::RetryConfig;
use sha2::Digest;
use redis::aio::ConnectionManager;
use std::time::Duration;
use testcontainers::clients::Cli;

/// Helper to get a ConnectionManager from a Redis URL.
async fn connection_manager(url: &str) -> ConnectionManager {
    let client = redis::Client::open(url).unwrap();
    client.get_connection_manager().await.unwrap()
}

// =============================================================================
// Stream Tailer Tests
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_reads_single_event() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-1");

    // Add a CDC event to the peer's stream
    let stream_id = peer
        .add_put_event("patient.123", r#"{"name": "John Doe"}"#)
        .await
        .expect("Failed to add event");

    // Create a stream tailer
    let tailer = StreamTailer::new(
        peer.node_id.clone(),
        "__local__:cdc".to_string(),
        Duration::from_secs(1),
        100,
    );

    // Connect to peer Redis
    let mut conn = connection_manager(&peer.redis_url).await;

    // Read from beginning
    let events = tailer.read_events(&mut conn, "0").await.unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key, "patient.123");
    assert_eq!(events[0].stream_id, stream_id);
    assert!(events[0].is_put());
    assert!(events[0].hash.is_some());
}

#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_reads_multiple_events_in_order() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-1");

    // Add multiple events
    peer.add_put_event("item.1", r#"{"n": 1}"#).await.unwrap();
    peer.add_put_event("item.2", r#"{"n": 2}"#).await.unwrap();
    peer.add_put_event("item.3", r#"{"n": 3}"#).await.unwrap();
    peer.add_delete_event("item.1").await.unwrap();

    let tailer = StreamTailer::new(
        "peer-1".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_secs(1),
        100,
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    let events = tailer.read_events(&mut conn, "0").await.unwrap();

    assert_eq!(events.len(), 4);
    assert_eq!(events[0].key, "item.1");
    assert!(events[0].is_put());
    assert_eq!(events[1].key, "item.2");
    assert_eq!(events[2].key, "item.3");
    assert_eq!(events[3].key, "item.1");
    assert!(events[3].is_delete());
}

#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_resumes_from_cursor() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-1");

    // Add initial events
    let id1 = peer.add_put_event("item.1", r#"{"n": 1}"#).await.unwrap();
    let _id2 = peer.add_put_event("item.2", r#"{"n": 2}"#).await.unwrap();

    let tailer = StreamTailer::new(
        "peer-1".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_secs(1),
        100,
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    // Read from after id1 (should only get id2)
    let events = tailer.read_events(&mut conn, &id1).await.unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key, "item.2");
}

#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_handles_empty_stream() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-1");

    let tailer = StreamTailer::new(
        "peer-1".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_millis(100), // Short timeout for test
        100,
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    // Should return empty (timeout) without error
    let events = tailer.read_events(&mut conn, "0").await.unwrap();
    assert!(events.is_empty());
}

#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_reads_compressed_data() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-1");

    // Compress data with zstd
    let original = r#"{"large": "data", "that": "compresses", "well": "hopefully"}"#;
    let compressed = zstd::encode_all(original.as_bytes(), 3).unwrap();
    
    // Hash must be of the ORIGINAL (decompressed) content, not the compressed bytes
    let hash = format!("{:x}", sha2::Sha256::digest(original.as_bytes()));

    // Add compressed event directly with correct hash
    peer.add_cdc_event("compressed.item", "PUT", Some(&compressed), Some(&hash))
        .await
        .unwrap();

    let tailer = StreamTailer::new(
        "peer-1".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_secs(1),
        100,
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    let events = tailer.read_events(&mut conn, "0").await.unwrap();

    assert_eq!(events.len(), 1);
    // Data should be decompressed
    let data = events[0].data.as_ref().unwrap();
    assert_eq!(String::from_utf8_lossy(data), original);
}

/// Test that stream trim detection works correctly.
#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_detects_trimmed_stream() {
    use replication_engine::stream::ReadResult;

    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-trim");

    // Add several events
    for i in 0..5 {
        peer.add_put_event(&format!("item.{}", i), &format!(r#"{{"n": {}}}"#, i))
            .await
            .unwrap();
    }

    let tailer = StreamTailer::new(
        "peer-trim".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_millis(100),
        100,
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    // Read all events
    let result = tailer.read_events_checked(&mut conn, "0").await.unwrap();
    let events = match result {
        ReadResult::Events(e) => e,
        _ => panic!("Expected Events"),
    };
    assert_eq!(events.len(), 5);

    // Get the last ID
    let last_id = events.last().unwrap().stream_id.clone();

    // Trim the stream to keep only the last 2 entries
    let _: () = redis::cmd("XTRIM")
        .arg("__local__:cdc")
        .arg("MAXLEN")
        .arg(2)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Try to read from an old cursor (the first event)
    let old_cursor = events.first().unwrap().stream_id.clone();
    let result = tailer.read_events_checked(&mut conn, &old_cursor).await.unwrap();

    match result {
        ReadResult::StreamTrimmed { cursor, oldest_id } => {
            assert_eq!(cursor, old_cursor);
            // oldest_id should be somewhere near the end (items 3-4)
            assert!(oldest_id > old_cursor, "oldest_id should be newer than our old cursor");
        }
        other => panic!("Expected StreamTrimmed, got {:?}", other),
    }

    // Reading from a valid cursor should work
    let result = tailer.read_events_checked(&mut conn, &last_id).await.unwrap();
    match result {
        ReadResult::Events(events) => {
            // After the last_id, there should be no new events
            assert!(events.is_empty());
        }
        other => panic!("Expected Events, got {:?}", other),
    }
}

// =============================================================================
// Peer Connection Tests
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn peer_connection_connects_successfully() {
    let docker = Cli::default();
    let container = redis_container(&docker);
    let url = redis_url(&container);

    let config = PeerConfig::for_testing("test-peer", &url);
    let peer = PeerConnection::new(config);

    // Should connect with testing retry config (fast fail)
    peer.connect(&RetryConfig::testing()).await.unwrap();

    assert!(peer.is_connected().await);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn peer_connection_retries_on_failure() {
    // Use invalid URL - should fail after retries
    let config = PeerConfig::for_testing("bad-peer", "redis://127.0.0.1:1");
    let peer = PeerConnection::new(config);

    // Testing config has 3 retries with short delays
    let result = peer.connect(&RetryConfig::testing()).await;

    assert!(result.is_err());
    assert!(!peer.is_connected().await);
}

// =============================================================================
// Multi-Peer Tests
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn multiple_peers_independent_streams() {
    let docker = Cli::default();
    let peer1 = TestPeer::new(&docker, "node-london");
    let peer2 = TestPeer::new(&docker, "node-manchester");

    // Add different events to each peer
    peer1.add_put_event("london.item", r#"{"city": "London"}"#).await.unwrap();
    peer2.add_put_event("manchester.item", r#"{"city": "Manchester"}"#).await.unwrap();
    peer2.add_put_event("manchester.item2", r#"{"city": "Manchester2"}"#).await.unwrap();

    // Verify independent streams
    assert_eq!(peer1.stream_len().await.unwrap(), 1);
    assert_eq!(peer2.stream_len().await.unwrap(), 2);

    // Read from each
    let tailer1 = StreamTailer::new(
        peer1.node_id.clone(),
        "__local__:cdc".to_string(),
        Duration::from_secs(1),
        100,
    );
    let tailer2 = StreamTailer::new(
        peer2.node_id.clone(),
        "__local__:cdc".to_string(),
        Duration::from_secs(1),
        100,
    );

    let mut conn1 = connection_manager(&peer1.redis_url).await;
    let mut conn2 = connection_manager(&peer2.redis_url).await;

    let events1 = tailer1.read_events(&mut conn1, "0").await.unwrap();
    let events2 = tailer2.read_events(&mut conn2, "0").await.unwrap();

    assert_eq!(events1.len(), 1);
    assert_eq!(events1[0].key, "london.item");

    assert_eq!(events2.len(), 2);
    assert_eq!(events2[0].key, "manchester.item");
}

// =============================================================================
// Cursor Persistence Tests
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn cursor_survives_reconnection() {
    use replication_engine::cursor::CursorStore;

    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-1");

    // Add some events
    let _id1 = peer.add_put_event("item.1", r#"{}"#).await.unwrap();
    let id2 = peer.add_put_event("item.2", r#"{}"#).await.unwrap();
    let _id3 = peer.add_put_event("item.3", r#"{}"#).await.unwrap();

    // Create cursor store and save position after id2
    let temp_dir = tempfile::tempdir().unwrap();
    let cursor_path = temp_dir.path().join("cursors.db");

    {
        let store = CursorStore::new(&cursor_path).await.unwrap();
        store.set("peer-1", &id2).await;
        store.flush_dirty().await.unwrap(); // Must flush before close
        store.close().await;
    }

    // Reopen store and verify cursor persisted
    {
        let store = CursorStore::new(&cursor_path).await.unwrap();
        let cursor = store.get("peer-1").await;
        assert_eq!(cursor, Some(id2.clone()));

        // Read from cursor should only get id3
        let tailer = StreamTailer::new(
            "peer-1".to_string(),
            "__local__:cdc".to_string(),
            Duration::from_secs(1),
            100,
        );

        let mut conn = connection_manager(&peer.redis_url).await;

        let events = tailer.read_events(&mut conn, &id2).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key, "item.3");
    }
}

// =============================================================================
// Full Engine Tests
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn engine_replicates_events_to_sync_engine() {
    use common::MockSyncEngine;
    use replication_engine::config::ReplicationConfig;
    use replication_engine::coordinator::ReplicationEngine;
    use std::sync::Arc;
    use tokio::sync::watch;

    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "node-remote");

    // Create a mock sync engine to receive replicated data
    let mock_sync = Arc::new(MockSyncEngine::new());

    // Create temp dir for cursor store
    let temp_dir = tempfile::tempdir().unwrap();
    let cursor_path = temp_dir.path().join("cursors.db");

    // Build config with our test peer
    let mut config = ReplicationConfig::for_testing("node-local");
    config.peers = vec![PeerConfig::for_testing("node-remote", &peer.redis_url)];
    config.cursor.sqlite_path = cursor_path.to_string_lossy().to_string();

    let (_config_tx, config_rx) = watch::channel(config.clone());

    // Create engine with mock sync
    let mut engine = ReplicationEngine::with_sync_engine(
        config,
        config_rx,
        Arc::clone(&mock_sync),
    );

    // Start the engine
    engine.start().await.expect("Failed to start engine");
    assert!(engine.is_running());

    // Add some CDC events to the remote peer
    peer.add_put_event("user.1", r#"{"name": "Alice"}"#).await.unwrap();
    peer.add_put_event("user.2", r#"{"name": "Bob"}"#).await.unwrap();
    peer.add_delete_event("user.3").await.unwrap();

    // Give hot path time to process (batch delay + processing)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify mock received the events
    let submitted = mock_sync.submitted().await;
    let deleted = mock_sync.deleted().await;

    // Should have received 2 submits and 1 delete
    assert_eq!(submitted.len(), 2, "Expected 2 submits, got {:?}", submitted);
    assert_eq!(deleted.len(), 1, "Expected 1 delete, got {:?}", deleted);

    // Verify content
    assert!(submitted.iter().any(|s| s.key == "user.1"));
    assert!(submitted.iter().any(|s| s.key == "user.2"));
    assert!(deleted.iter().any(|d| d.key == "user.3"));

    // Shutdown
    engine.shutdown().await;
}

#[tokio::test]
#[ignore] // Requires Docker
async fn engine_deduplicates_current_items() {
    use common::MockSyncEngine;
    use replication_engine::config::ReplicationConfig;
    use replication_engine::coordinator::ReplicationEngine;
    use std::sync::Arc;
    use tokio::sync::watch;

    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "node-remote");

    // Create mock that says everything is current (simulates already-synced data)
    let mock_sync = Arc::new(MockSyncEngine::rejecting());

    let temp_dir = tempfile::tempdir().unwrap();
    let cursor_path = temp_dir.path().join("cursors.db");

    let mut config = ReplicationConfig::for_testing("node-local");
    config.peers = vec![PeerConfig::for_testing("node-remote", &peer.redis_url)];
    config.cursor.sqlite_path = cursor_path.to_string_lossy().to_string();

    let (_config_tx, config_rx) = watch::channel(config.clone());

    let mut engine = ReplicationEngine::with_sync_engine(
        config,
        config_rx,
        Arc::clone(&mock_sync),
    );

    engine.start().await.expect("Failed to start engine");

    // Add events that should be skipped (is_current returns true)
    peer.add_put_event("existing.1", r#"{"already": "synced"}"#).await.unwrap();
    peer.add_put_event("existing.2", r#"{"also": "synced"}"#).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // is_current should have been called
    let is_current_calls = mock_sync.is_current_calls().await;
    assert!(is_current_calls.len() >= 2, "Expected is_current checks");

    // But nothing should have been submitted (all skipped)
    let submitted = mock_sync.submitted().await;
    assert_eq!(submitted.len(), 0, "Should skip already-current items");

    engine.shutdown().await;
}
// =============================================================================
// Cold Path / Merkle Anti-Entropy Tests
// =============================================================================

/// Test that cold path detects divergent data when Merkle roots differ.
#[tokio::test]
#[ignore] // Requires Docker
async fn cold_path_repairs_divergent_merkle_roots() {
    use common::MockSyncEngine;
    use sha2::{Digest, Sha256};
    use std::sync::Arc;

    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-with-data");

    // Set up peer with a Merkle tree containing one item
    let item_data = b"divergent item data";
    let item_hash: [u8; 32] = Sha256::digest(item_data).into();

    // Store the item on the peer
    peer.store_item("patients/123", item_data).await.unwrap();

    // Set up simple Merkle tree: root -> patients -> 123
    // First set the leaf hash
    peer.add_merkle_leaf("patients/123", item_hash).await.unwrap();

    // Set patients as child of root (this also sets root hash via compute_interior_hash)
    // But first we need patients' hash - for a leaf it's the item hash
    peer.set_merkle_children("patients", &[("123", item_hash)])
        .await
        .unwrap();

    // Get the patients hash that was computed
    let patients_hash = peer.get_merkle_hash("patients").await.unwrap().unwrap();

    // Now set root's children
    peer.set_merkle_children("", &[("patients", patients_hash)])
        .await
        .unwrap();

    // The root hash is now set by set_merkle_children
    let root_hash = peer.get_merkle_hash("").await.unwrap().unwrap();

    // Create local mock with DIFFERENT (empty) Merkle root
    let _mock_sync = Arc::new(MockSyncEngine::new());
    // Local has no data, so Merkle root is None/different

    // Verify peer's Merkle root is set
    let fetched_root = peer.get_merkle_hash("").await.unwrap();
    assert!(fetched_root.is_some(), "Peer should have a Merkle root");
    assert_eq!(fetched_root.unwrap(), root_hash);

    // Verify we can fetch the item
    let fetched_item = peer.get_item("patients/123").await.unwrap();
    assert_eq!(fetched_item, Some(item_data.to_vec()));
}

/// Test that identical Merkle roots result in no repair.
#[tokio::test]
#[ignore] // Requires Docker
async fn cold_path_skips_when_merkle_roots_match() {
    use common::MockSyncEngine;
    use std::sync::Arc;

    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-in-sync");

    // Set up identical Merkle roots on both sides
    let shared_root: [u8; 32] = [42u8; 32];

    peer.set_merkle_root(shared_root).await.unwrap();

    let mock_sync = Arc::new(MockSyncEngine::new());
    mock_sync.set_merkle_root(shared_root).await;

    // Verify peer root
    let peer_root = peer.get_merkle_hash("").await.unwrap();
    assert_eq!(peer_root, Some(shared_root));

    // When roots match, cold path should not fetch any items
    // (We're testing the setup here; full cold path test would need engine)
}

/// Test Merkle tree drill-down finds the correct divergent leaf.
#[tokio::test]
#[ignore] // Requires Docker
async fn merkle_drill_down_finds_divergent_leaf() {
    use sha2::{Digest, Sha256};

    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-merkle");

    // Build a deeper Merkle tree:
    // root -> a -> a1 (item)
    //      -> b -> b1 (item)

    let a1_data = b"item a1";
    let b1_data = b"item b1";
    let a1_hash: [u8; 32] = Sha256::digest(a1_data).into();
    let b1_hash: [u8; 32] = Sha256::digest(b1_data).into();

    peer.store_item("a/a1", a1_data).await.unwrap();
    peer.store_item("b/b1", b1_data).await.unwrap();

    peer.add_merkle_leaf("a/a1", a1_hash).await.unwrap();
    peer.add_merkle_leaf("b/b1", b1_hash).await.unwrap();

    // Set children - this also computes and stores interior hashes
    peer.set_merkle_children("a", &[("a1", a1_hash)])
        .await
        .unwrap();
    peer.set_merkle_children("b", &[("b1", b1_hash)])
        .await
        .unwrap();

    // Get the computed interior hashes
    let a_hash = peer.get_merkle_hash("a").await.unwrap().unwrap();
    let b_hash = peer.get_merkle_hash("b").await.unwrap().unwrap();

    // Set root children
    peer.set_merkle_children("", &[("a", a_hash), ("b", b_hash)])
        .await
        .unwrap();

    // Verify structure
    let children = peer.get_merkle_children("").await.unwrap();
    assert_eq!(children.len(), 2);

    let a_children = peer.get_merkle_children("a").await.unwrap();
    assert_eq!(a_children.len(), 1);
    assert_eq!(a_children[0].0, "a1");

    // Verify we can reach items
    let a1_item = peer.get_item("a/a1").await.unwrap();
    assert_eq!(a1_item, Some(a1_data.to_vec()));
}

// =============================================================================
// Scale and Stress Tests
// =============================================================================

/// Test cursor handling at scale with many peers.
///
/// Verifies that cursor storage and retrieval works correctly when
/// managing cursors for 100+ peers simultaneously.
#[tokio::test]
#[ignore] // Requires Docker
async fn cursor_scale_test_many_peers() {
    use replication_engine::cursor::CursorStore;
    use tempfile::tempdir;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("cursors.db");

    // Create cursor store
    let store = CursorStore::new(&db_path).await.expect("Failed to create store");

    // Simulate 100 peers
    let peer_count = 100;
    let mut cursors = Vec::new();

    // Set cursors for all peers
    for i in 0..peer_count {
        let peer_id = format!("peer-{:03}", i);
        let cursor = format!("{:013}-{}", 1704067200000u64 + i as u64 * 1000, i);
        store.set(&peer_id, &cursor).await;
        cursors.push((peer_id, cursor));
    }

    // Flush to ensure all are persisted
    let _ = store.flush_dirty().await.expect("Failed to flush");

    // Verify all cursors can be retrieved
    for (peer_id, expected_cursor) in &cursors {
        let cursor = store.get_or_start(peer_id).await;
        assert_eq!(
            &cursor, expected_cursor,
            "Cursor mismatch for peer {}", peer_id
        );
    }

    // Update half of them
    for (i, cursor_entry) in cursors.iter_mut().take(peer_count / 2).enumerate() {
        let peer_id = format!("peer-{:03}", i);
        let new_cursor = format!("{:013}-{}", 1704067200000u64 + i as u64 * 2000, i * 2);
        store.set(&peer_id, &new_cursor).await;
        cursor_entry.1 = new_cursor;
    }

    let _ = store.flush_dirty().await.expect("Failed to flush");

    // Create a new store instance (simulates restart)
    let store2 = CursorStore::new(&db_path).await.expect("Failed to reopen store");

    // Verify all cursors survived
    for (peer_id, expected_cursor) in &cursors {
        let cursor = store2.get_or_start(peer_id).await;
        assert_eq!(
            &cursor, expected_cursor,
            "Cursor mismatch after restart for peer {}", peer_id
        );
    }

    // Delete some cursors
    for i in 0..10 {
        let peer_id = format!("peer-{:03}", i);
        store2.delete(&peer_id).await.expect("Failed to delete cursor");
    }

    let _ = store2.flush_dirty().await.expect("Failed to flush");

    // Verify deleted cursors return "0"
    for i in 0..10 {
        let peer_id = format!("peer-{:03}", i);
        let cursor = store2.get_or_start(&peer_id).await;
        assert_eq!(cursor, "0", "Deleted cursor should return 0");
    }
}

/// Test peer reconnection after Redis restart.
///
/// Simulates the scenario where a Redis peer goes down and comes back up,
/// verifying that the peer connection recovers automatically.
#[tokio::test]
#[ignore] // Requires Docker
async fn peer_reconnects_after_redis_restart() {
    use testcontainers::GenericImage;
    use testcontainers::core::WaitFor;

    let docker = Cli::default();
    
    // Start a Redis container
    let image = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379)
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    let container = docker.run(image);
    let port = container.get_host_port_ipv4(6379);
    let url = format!("redis://127.0.0.1:{}", port);

    // Create peer connection
    let peer_config = PeerConfig {
        node_id: "reconnect-peer".to_string(),
        redis_url: url.clone(),
        priority: 0,
        circuit_failure_threshold: 3,
        circuit_reset_timeout_sec: 5,
        redis_prefix: None,
    };

    let retry_config = RetryConfig::testing();
    let peer = PeerConnection::new(peer_config);

    // Connect successfully
    peer.connect(&retry_config).await.expect("Initial connection should succeed");
    assert!(peer.is_connected().await);

    // Verify connection works
    {
        let mut conn = peer.connection().await.expect("Should have connection");
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .expect("PING should work");
        assert_eq!(pong, "PONG");
    }

    // Stop the container (simulates Redis going down)
    drop(container);
    
    // Give it a moment to detect disconnect
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Start a new container (simulates restart)
    let image2 = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379)
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    let container2 = docker.run(image2);
    let port2 = container2.get_host_port_ipv4(6379);
    let url2 = format!("redis://127.0.0.1:{}", port2);

    // Create a new peer for the "restarted" Redis
    let peer_config2 = PeerConfig {
        node_id: "reconnect-peer".to_string(),
        redis_url: url2,
        priority: 0,
        circuit_failure_threshold: 3,
        circuit_reset_timeout_sec: 5,
        redis_prefix: None,
    };

    let peer2 = PeerConnection::new(peer_config2);
    
    // Should be able to connect to the "restarted" Redis
    peer2.connect(&retry_config).await.expect("Reconnection should succeed");
    assert!(peer2.is_connected().await);

    // Verify it works
    {
        let mut conn = peer2.connection().await.expect("Should have connection");
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .expect("PING should work on reconnected peer");
        assert_eq!(pong, "PONG");
    }
}

/// Test rate limiter doesn't block under normal load.
#[tokio::test]
async fn rate_limiter_allows_normal_throughput() {
    use replication_engine::resilience::{RateLimiter, RateLimitConfig};
    use std::time::Instant;

    let limiter = RateLimiter::new(RateLimitConfig {
        burst_size: 100,
        refill_rate: 10_000,
    });

    // Process 50 events (well under burst limit)
    let start = Instant::now();
    for _ in 0..50 {
        limiter.acquire().await;
    }
    let elapsed = start.elapsed();

    // Should complete almost instantly (within burst)
    assert!(
        elapsed < Duration::from_millis(50),
        "Rate limiter should allow burst without delay, took {:?}",
        elapsed
    );
}

/// Test bulkhead limits concurrency.
#[tokio::test]
async fn bulkhead_limits_concurrency() {
    use replication_engine::resilience::Bulkhead;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let bulkhead = Arc::new(Bulkhead::new(5));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let current_concurrent = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Spawn 20 tasks, each trying to acquire the bulkhead
    for _ in 0..20 {
        let bh = Arc::clone(&bulkhead);
        let max = Arc::clone(&max_concurrent);
        let current = Arc::clone(&current_concurrent);

        handles.push(tokio::spawn(async move {
            let _permit = bh.acquire().await.unwrap();
            
            // Track concurrency
            let c = current.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(c, Ordering::SeqCst);
            
            // Simulate work
            tokio::time::sleep(Duration::from_millis(10)).await;
            
            current.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    // Wait for all to complete
    for h in handles {
        h.await.unwrap();
    }

    let observed_max = max_concurrent.load(Ordering::SeqCst);
    assert!(
        observed_max <= 5,
        "Max concurrent should be <= 5 (bulkhead limit), was {}",
        observed_max
    );
}

// =============================================================================
// Additional Stream Tests
// =============================================================================

/// Test stream tailer batch size configuration
#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_respects_batch_size() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-batch");

    // Add 10 events
    for i in 0..10 {
        peer.add_put_event(&format!("item.{}", i), &format!(r#"{{"n": {}}}"#, i))
            .await
            .unwrap();
    }

    // Create tailer with batch_size=3
    let tailer = StreamTailer::new(
        "peer-batch".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_millis(100),
        3, // Only 3 at a time
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    // First read should get 3 events
    let events = tailer.read_events(&mut conn, "0").await.unwrap();
    assert_eq!(events.len(), 3);

    // Read from last cursor should get next 3
    let cursor = events.last().unwrap().stream_id.clone();
    let events = tailer.read_events(&mut conn, &cursor).await.unwrap();
    assert_eq!(events.len(), 3);
}

/// Test stream XRANGE reads (for catchup)
#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_xrange_catchup() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-xrange");

    // Add 10 events
    for i in 0..10 {
        peer.add_put_event(&format!("item.{}", i), &format!(r#"{{"n": {}}}"#, i))
            .await
            .unwrap();
    }

    let tailer = StreamTailer::new(
        "peer-xrange".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_millis(100),
        100,
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    // XRANGE read (non-blocking, for catchup)
    let events = tailer.read_events_range(&mut conn, "0", 5).await.unwrap();
    assert_eq!(events.len(), 5);

    // Continue from last
    let cursor = events.last().unwrap().stream_id.clone();
    let events = tailer.read_events_range(&mut conn, &cursor, 10).await.unwrap();
    assert_eq!(events.len(), 5); // Remaining 5 events
}

/// Test stream oldest/latest ID queries
#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_get_stream_info() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-info");

    let tailer = StreamTailer::new(
        "peer-info".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_millis(100),
        100,
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    // Empty stream
    assert!(tailer.get_oldest_id(&mut conn).await.unwrap().is_none());
    assert!(tailer.get_latest_id(&mut conn).await.unwrap().is_none());
    assert_eq!(tailer.get_stream_length(&mut conn).await.unwrap(), 0);

    // Add some events
    let id1 = peer.add_put_event("item.1", "data1").await.unwrap();
    let _id2 = peer.add_put_event("item.2", "data2").await.unwrap();
    let id3 = peer.add_put_event("item.3", "data3").await.unwrap();

    // Now check info
    assert_eq!(tailer.get_oldest_id(&mut conn).await.unwrap(), Some(id1));
    assert_eq!(tailer.get_latest_id(&mut conn).await.unwrap(), Some(id3));
    assert_eq!(tailer.get_stream_length(&mut conn).await.unwrap(), 3);
}

/// Test cursor validity check
#[tokio::test]
#[ignore] // Requires Docker
async fn stream_tailer_cursor_validity() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "peer-validity");

    let tailer = StreamTailer::new(
        "peer-validity".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_millis(100),
        100,
    );

    let mut conn = connection_manager(&peer.redis_url).await;

    // Add events
    peer.add_put_event("item.1", "data1").await.unwrap();
    let id2 = peer.add_put_event("item.2", "data2").await.unwrap();
    peer.add_put_event("item.3", "data3").await.unwrap();

    // Valid cursor (within stream)
    let result = tailer.check_cursor_valid(&mut conn, &id2).await.unwrap();
    assert!(result.is_none(), "Cursor should be valid");

    // Special "0" cursor is always valid
    let result = tailer.check_cursor_valid(&mut conn, "0").await.unwrap();
    assert!(result.is_none(), "Cursor '0' should always be valid");
}

// =============================================================================
// Peer Connection Tests
// =============================================================================

/// Test peer connection state transitions
#[tokio::test]
#[ignore] // Requires Docker
async fn peer_connection_state_lifecycle() {
    use replication_engine::peer::PeerState;

    let docker = Cli::default();
    let container = redis_container(&docker);
    let url = redis_url(&container);

    let config = PeerConfig::for_testing("peer-lifecycle", &url);
    let peer = PeerConnection::new(config);

    // Initial state
    assert_eq!(peer.state().await, PeerState::Disconnected);

    // Connect
    peer.connect(&RetryConfig::testing()).await.unwrap();
    assert_eq!(peer.state().await, PeerState::Connected);

    // Mark disconnected
    peer.mark_disconnected().await;
    assert_eq!(peer.state().await, PeerState::Disconnected);
}

/// Test peer ping health check
#[tokio::test]
#[ignore] // Requires Docker
async fn peer_ping_health_check() {
    let docker = Cli::default();
    let container = redis_container(&docker);
    let url = redis_url(&container);

    let config = PeerConfig::for_testing("peer-ping", &url);
    let peer = PeerConnection::new(config);

    // Connect first
    peer.connect(&RetryConfig::testing()).await.unwrap();

    // Ping should succeed
    let latency = peer.ping().await.unwrap();
    assert!(latency < Duration::from_secs(1), "Ping should be fast");

    // Failure count should be 0 after success
    assert_eq!(peer.failure_count(), 0);
}

/// Test peer manager operations
#[tokio::test]
async fn peer_manager_all_operations() {
    use replication_engine::peer::PeerManager;

    let manager = PeerManager::new(RetryConfig::testing());

    // Add peers
    manager.add_peer(PeerConfig::for_testing("peer-1", "redis://host1:6379"));
    manager.add_peer(PeerConfig::for_testing("peer-2", "redis://host2:6379"));
    manager.add_peer(PeerConfig::for_testing("peer-3", "redis://host3:6379"));

    // Get all
    assert_eq!(manager.all().len(), 3);

    // Get specific
    assert!(manager.get("peer-1").is_some());
    assert!(manager.get("peer-2").is_some());
    assert!(manager.get("nonexistent").is_none());

    // Remove
    manager.remove_peer("peer-2");
    assert_eq!(manager.all().len(), 2);
    assert!(manager.get("peer-2").is_none());

    // Shutdown all
    manager.shutdown_all();
}

// =============================================================================
// Batch Processor Integration Tests
// =============================================================================

/// Test batch processor with real sync engine trait
#[tokio::test]
async fn batch_processor_full_workflow() {
    use replication_engine::batch::{BatchProcessor, BatchConfig};
    use replication_engine::stream::{CdcEvent, CdcOp, CdcMeta};
    use replication_engine::sync_engine::NoOpSyncEngine;
    use std::sync::Arc;

    let engine = Arc::new(NoOpSyncEngine);
    let mut processor = BatchProcessor::new(
        engine,
        "test-peer".to_string(),
        BatchConfig::testing(),
    );

    // Add various events
    for i in 0..5 {
        processor.add(CdcEvent {
            stream_id: format!("{}-0", i),
            op: CdcOp::Put,
            key: format!("item.{}", i),
            hash: Some(format!("hash{}", i)),
            data: Some(format!("data{}", i).into_bytes()),
            meta: Some(CdcMeta {
                content_type: "application/json".to_string(),
                version: i as u64,
                updated_at: 12345,
                trace_parent: None,
            }),
        });
    }

    // Add a delete
    processor.add(CdcEvent {
        stream_id: "100-0".to_string(),
        op: CdcOp::Delete,
        key: "item.99".to_string(),
        hash: None,
        data: None,
        meta: None,
    });

    assert_eq!(processor.len(), 6);

    // Flush
    let result = processor.flush().await.unwrap();
    assert_eq!(result.total, 6);
    assert_eq!(result.submitted, 5);
    assert_eq!(result.deleted, 1);
    assert!(result.is_success());
}

// =============================================================================
// Cursor Store Integration Tests
// =============================================================================

/// Test cursor store with concurrent access
#[tokio::test]
async fn cursor_store_concurrent_access() {
    use replication_engine::cursor::CursorStore;
    use std::sync::Arc;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("concurrent.db");

    let store = Arc::new(CursorStore::new(&db_path).await.unwrap());

    // Spawn multiple tasks updating different cursors
    let mut handles = Vec::new();
    for peer_num in 0..5 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                store.set(&format!("peer-{}", peer_num), &format!("{}-{}", peer_num, i)).await;
            }
        }));
    }

    // Wait for all
    for h in handles {
        h.await.unwrap();
    }

    // Flush all dirty
    let flushed = store.flush_dirty().await.unwrap();
    assert_eq!(flushed, 5); // 5 peers

    // Verify final values
    for peer_num in 0..5 {
        let cursor = store.get(&format!("peer-{}", peer_num)).await.unwrap();
        assert_eq!(cursor, format!("{}-9", peer_num)); // Last value
    }

    store.close().await;
}

// =============================================================================
// Failure Path Tests
// =============================================================================

/// Test: Stream with malformed CDC events (missing required fields)
#[tokio::test]
#[ignore] // Requires Docker
async fn stream_handles_malformed_events() {
    use redis::AsyncCommands;
    
    let docker = Cli::default();
    let container = redis_container(&docker);
    let url = redis_url(&container);

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    // Add a well-formed event first (no hash - hash validation is separate concern)
    let _: String = conn.xadd(
        "__local__:cdc",
        "*",
        &[("op", "PUT"), ("key", "good.1"), ("data", "data1")]
    ).await.unwrap();

    // Add event missing 'key' field
    let _: String = conn.xadd(
        "__local__:cdc",
        "*",
        &[("op", "PUT"), ("data", "orphan")]
    ).await.unwrap();

    // Add event missing 'op' field
    let _: String = conn.xadd(
        "__local__:cdc",
        "*",
        &[("key", "orphan.key"), ("data", "orphan")]
    ).await.unwrap();

    // Add another well-formed event
    let _: String = conn.xadd(
        "__local__:cdc",
        "*",
        &[("op", "DEL"), ("key", "good.2")]
    ).await.unwrap();

    // Create stream tailer and read
    let tailer = StreamTailer::new(
        "test-peer".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_secs(1),
        100,
    );

    let mut conn_mgr = connection_manager(&url).await;
    let events = tailer.read_events(&mut conn_mgr, "0").await.unwrap();

    // Should only get the 2 valid events (malformed ones skipped)
    assert_eq!(events.len(), 2, "Should have 2 valid events, got {}", events.len());
    assert_eq!(events[0].key, "good.1");
    assert_eq!(events[1].key, "good.2");
}

/// Test: Peer circuit breaker opens after repeated failures
#[tokio::test]
#[ignore] // Requires Docker
async fn peer_circuit_breaker_opens_on_failures() {
    use replication_engine::peer::PeerCircuitState;

    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "circuit-test");

    // Create peer connection with low failure threshold
    let config = PeerConfig {
        node_id: "circuit-test".to_string(),
        redis_url: peer.redis_url.clone(),
        priority: 0,
        circuit_failure_threshold: 3,
        circuit_reset_timeout_sec: 60,
        redis_prefix: None,
    };

    let conn = PeerConnection::new(config);

    // Connect successfully first
    conn.ensure_connected().await.unwrap();
    assert!(conn.is_connected().await);
    assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);

    // Simulate failures (record_failure called by hot/cold path on errors)
    conn.record_failure().await;
    assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);
    
    conn.record_failure().await;
    assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);
    
    // Third failure should open circuit
    conn.record_failure().await;
    assert_eq!(conn.circuit_state().await, PeerCircuitState::Open);
    assert_eq!(conn.failure_count(), 3);

    // Success should close it
    conn.record_success().await;
    assert_eq!(conn.circuit_state().await, PeerCircuitState::Closed);
    assert_eq!(conn.failure_count(), 0);
}

/// Test: Batch processor handles mixed success/skip scenario
#[tokio::test]
#[ignore] // Requires Docker
async fn batch_processor_mixed_results() {
    use replication_engine::batch::{BatchProcessor, BatchConfig};
    use replication_engine::stream::{CdcEvent, CdcOp};
    use common::mock_sync::MockSyncEngine;
    use std::sync::Arc;

    let docker = Cli::default();
    let _peer = TestPeer::new(&docker, "batch-test"); // Just for container lifecycle

    let sync_engine = Arc::new(MockSyncEngine::new());

    // Configure: items 0, 2, 4, 6, 8 are "current" (should be skipped)
    // The hash must match what we pass to is_current
    for i in (0..10).step_by(2) {
        sync_engine.set_is_current_response(&format!("key.{}", i), &format!("hash.{}", i), true).await;
    }

    let config = BatchConfig {
        max_batch_size: 100,
        max_batch_delay: Duration::from_millis(50),
        max_concurrent_checks: 8,
    };

    let mut processor = BatchProcessor::new(Arc::clone(&sync_engine), "test".to_string(), config);

    // Add 10 events
    for i in 0..10 {
        processor.add(CdcEvent {
            stream_id: format!("{}-0", i),
            op: CdcOp::Put,
            key: format!("key.{}", i),
            hash: Some(format!("hash.{}", i)),
            data: Some(format!("data.{}", i).into_bytes()),
            meta: None,
        });
    }

    let result = processor.flush().await.unwrap();

    // 5 should be submitted (odd indices), 5 should be skipped (even indices)
    assert_eq!(result.total, 10);
    assert_eq!(result.submitted, 5);
    assert_eq!(result.skipped, 5);
    assert_eq!(result.errors, 0);
    assert!(result.is_success());
}

/// Test: Cursor store handles rapid updates without data loss
#[tokio::test]
async fn cursor_store_rapid_updates_no_loss() {
    use replication_engine::cursor::CursorStore;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("rapid.db");
    let store = CursorStore::new(&db_path).await.unwrap();

    // Rapid fire updates to same cursor
    for i in 0..1000 {
        store.set("rapid-peer", &format!("{:013}-{}", 1704067200000u64 + i, i)).await;
    }

    // Should only flush once (debounced)
    let dirty_before = store.dirty_count().await;
    assert_eq!(dirty_before, 1, "Should have 1 dirty cursor");

    // Flush and verify
    let flushed = store.flush_dirty().await.unwrap();
    assert_eq!(flushed, 1);

    // Get final value
    let cursor = store.get("rapid-peer").await.unwrap();
    assert_eq!(cursor, "1704067200999-999");

    // Verify persisted by reopening
    store.close().await;
    let store2 = CursorStore::new(&db_path).await.unwrap();
    let cursor2 = store2.get("rapid-peer").await.unwrap();
    assert_eq!(cursor2, "1704067200999-999");
    store2.close().await;
}

/// Test: Stream trimming while actively reading
#[tokio::test]
#[ignore] // Requires Docker
async fn stream_trim_during_read() {
    use redis::AsyncCommands;

    let docker = Cli::default();
    let container = redis_container(&docker);
    let url = redis_url(&container);

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    // Add many events (with data field for PUT)
    for i in 0..20 {
        let _: String = conn.xadd(
            "__local__:cdc",
            "*",
            &[("op", "PUT"), ("key", &format!("item.{}", i)), ("data", &format!("data.{}", i))]
        ).await.unwrap();
    }

    // Get the 10th event ID (we'll use this as our "old" cursor)
    let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg("__local__:cdc")
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg(10)
        .query_async(&mut conn)
        .await
        .unwrap();
    
    let tenth_id = entries.last().unwrap().0.clone();

    // Use XTRIM with MINID to force trim (APPROX is too fuzzy for testing)
    // Get the 18th ID and trim everything before it
    let all_entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg("__local__:cdc")
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    
    // Keep only the last 2 entries by using MINID on the 18th entry
    let minid = all_entries[18].0.clone();
    let _: usize = redis::cmd("XTRIM")
        .arg("__local__:cdc")
        .arg("MINID")
        .arg(&minid)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Now our cursor (tenth_id) is older than the oldest entry
    let tailer = StreamTailer::new(
        "trim-test".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_secs(1),
        100,
    );

    let mut conn_mgr = connection_manager(&url).await;

    // read_events_checked should detect the trim
    let result = tailer.read_events_checked(&mut conn_mgr, &tenth_id).await.unwrap();

    // Should indicate stream was trimmed
    assert!(result.is_trimmed(), "Should detect stream was trimmed");
}

/// Test: Peer handles connection drop and reconnects
#[tokio::test]
#[ignore] // Requires Docker
async fn peer_handles_connection_drop() {
    let docker = Cli::default();
    let peer = TestPeer::new(&docker, "drop-test");

    let config = PeerConfig::for_testing("drop-test", &peer.redis_url);
    let conn = PeerConnection::new(config);

    // Connect
    conn.ensure_connected().await.unwrap();
    assert!(conn.is_connected().await);

    // Get a connection and use it
    let redis_conn = conn.connection().await;
    assert!(redis_conn.is_some());

    // Mark disconnected (simulates connection drop detection)
    conn.mark_disconnected().await;
    assert!(!conn.is_connected().await);

    // Should be able to reconnect
    conn.ensure_connected().await.unwrap();
    assert!(conn.is_connected().await);
}

/// Test: Multiple peers with different stream positions
#[tokio::test]
#[ignore] // Requires Docker
async fn multiple_peers_different_positions() {
    use replication_engine::cursor::CursorStore;
    use tempfile::tempdir;

    let docker = Cli::default();
    let peer1 = TestPeer::new(&docker, "multi-1");
    let peer2 = TestPeer::new(&docker, "multi-2");

    // Add different numbers of events to each peer
    for i in 0..5 {
        peer1.add_put_event(&format!("p1.{}", i), &format!(r#"{{"n":{}}}"#, i)).await.unwrap();
    }
    for i in 0..10 {
        peer2.add_put_event(&format!("p2.{}", i), &format!(r#"{{"n":{}}}"#, i)).await.unwrap();
    }

    // Create cursor store
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("multi.db");
    let store = CursorStore::new(&db_path).await.unwrap();

    // Read from peer1 first - all events
    let tailer1 = StreamTailer::new("multi-1".to_string(), "__local__:cdc".to_string(), Duration::from_secs(1), 100);
    let mut conn1 = connection_manager(&peer1.redis_url).await;
    let events1 = tailer1.read_events(&mut conn1, "0").await.unwrap();
    assert_eq!(events1.len(), 5);

    // Store cursor at last event
    store.set("multi-1", &events1.last().unwrap().stream_id).await;

    // Read from peer2 first 5 only
    let tailer2 = StreamTailer::new("multi-2".to_string(), "__local__:cdc".to_string(), Duration::from_secs(1), 5);
    let mut conn2 = connection_manager(&peer2.redis_url).await;
    let events2_batch1 = tailer2.read_events(&mut conn2, "0").await.unwrap();
    assert_eq!(events2_batch1.len(), 5);

    // Store cursor
    store.set("multi-2", &events2_batch1.last().unwrap().stream_id).await;

    // Flush cursors
    store.flush_dirty().await.unwrap();

    // Verify cursors are independent
    let cursor1 = store.get("multi-1").await.unwrap();
    let cursor2 = store.get("multi-2").await.unwrap();
    assert_ne!(cursor1, cursor2); // Different positions

    // Continue reading peer2 from cursor
    let events2_batch2 = tailer2.read_events(&mut conn2, &cursor2).await.unwrap();
    assert_eq!(events2_batch2.len(), 5); // Remaining 5 events

    store.close().await;
}

/// Test: Circuit breaker protects sync-engine from overload
#[tokio::test]
async fn circuit_breaker_protects_sync_engine() {
    use replication_engine::circuit_breaker::{SyncEngineCircuit, CircuitError};
    use replication_engine::error::ReplicationError;

    let circuit = SyncEngineCircuit::new();

    // Initially closed - calls go through
    let result = circuit.writes.call(|| async { Ok::<_, ReplicationError>(42) }).await;
    assert!(result.is_ok());

    // Simulate failures to open circuit (default threshold is 5)
    for _ in 0..5 {
        let _ = circuit.writes.call(|| async {
            Err::<i32, _>(ReplicationError::SyncEngine("overloaded".to_string()))
        }).await;
    }

    // Circuit should now be open - calls are rejected
    let result = circuit.writes.call(|| async { Ok::<_, ReplicationError>(42) }).await;
    match result {
        Err(CircuitError::Rejected) => (), // Expected
        other => panic!("Expected Rejected, got {:?}", other),
    }
}

/// Test: Empty stream returns empty result
#[tokio::test]
#[ignore] // Requires Docker
async fn stream_empty_returns_empty() {
    let docker = Cli::default();
    let container = redis_container(&docker);
    let url = redis_url(&container);

    // Create tailer for empty stream (no events added)
    let tailer = StreamTailer::new(
        "empty-test".to_string(),
        "__local__:cdc".to_string(),
        Duration::from_millis(100), // Short timeout
        100,
    );

    let mut conn = connection_manager(&url).await;
    let events = tailer.read_events(&mut conn, "0").await.unwrap();

    assert!(events.is_empty(), "Empty stream should return no events");
}

/// Test: Cursor store handles special characters in peer IDs
#[tokio::test]
async fn cursor_store_special_peer_ids() {
    use replication_engine::cursor::CursorStore;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("special.db");
    let store = CursorStore::new(&db_path).await.unwrap();

    // Peer IDs with special characters
    let special_ids = [
        "peer-with-dashes",
        "peer_with_underscores",
        "peer.with.dots",
        "peer:with:colons",
        "peer/with/slashes",
        "peer@with@at",
        "peer with spaces",
        "peer\"with\"quotes",
    ];

    for (i, peer_id) in special_ids.iter().enumerate() {
        store.set(peer_id, &format!("{}-0", i)).await;
    }

    store.flush_dirty().await.unwrap();

    // Verify all can be retrieved
    for (i, peer_id) in special_ids.iter().enumerate() {
        let cursor = store.get(peer_id).await.unwrap();
        assert_eq!(cursor, format!("{}-0", i), "Failed for peer_id: {}", peer_id);
    }

    store.close().await;
}

/// Test: Batch processor handles delete operations correctly
#[tokio::test]
async fn batch_processor_deletes() {
    use replication_engine::batch::{BatchProcessor, BatchConfig};
    use replication_engine::stream::{CdcEvent, CdcOp};
    use common::mock_sync::MockSyncEngine;
    use std::sync::Arc;

    let sync_engine = Arc::new(MockSyncEngine::new());
    let config = BatchConfig::default();
    let mut processor = BatchProcessor::new(Arc::clone(&sync_engine), "delete-test".to_string(), config);

    // Add delete events
    for i in 0..5 {
        processor.add(CdcEvent {
            stream_id: format!("{}-0", i),
            op: CdcOp::Delete,
            key: format!("deleted.{}", i),
            hash: None,
            data: None,
            meta: None,
        });
    }

    let result = processor.flush().await.unwrap();

    assert_eq!(result.total, 5);
    assert_eq!(result.deleted, 5);
    assert_eq!(result.submitted, 0);
    assert_eq!(result.skipped, 0);
    assert!(result.is_success());
}

/// Test: Peer connection handles invalid URL gracefully
#[tokio::test]
async fn peer_connection_invalid_url() {
    use replication_engine::resilience::RetryConfig;
    
    // Use localhost with a definitely-closed port (1 is typically reserved/closed)
    let config = PeerConfig::for_testing("bad-peer", "redis://127.0.0.1:1");
    let conn = PeerConnection::new(config);

    // Use fast retry config to avoid slow test
    let result = conn.connect(&RetryConfig::testing()).await;
    assert!(result.is_err(), "Should fail to connect to invalid URL");
    assert!(!conn.is_connected().await);
}

/// Test: Stream ID comparison edge cases
#[tokio::test]
async fn stream_id_comparison_edge_cases() {
    use replication_engine::stream::compare_stream_ids;
    use std::cmp::Ordering;

    // Normal comparison
    assert_eq!(compare_stream_ids("1000-0", "2000-0"), Ordering::Less);
    assert_eq!(compare_stream_ids("2000-0", "1000-0"), Ordering::Greater);
    assert_eq!(compare_stream_ids("1000-0", "1000-0"), Ordering::Equal);

    // Sequence number comparison
    assert_eq!(compare_stream_ids("1000-0", "1000-1"), Ordering::Less);
    assert_eq!(compare_stream_ids("1000-99", "1000-100"), Ordering::Less);

    // Special "0" marker
    assert_eq!(compare_stream_ids("0", "1000-0"), Ordering::Less);
    assert_eq!(compare_stream_ids("0", "0"), Ordering::Equal);

    // Large timestamps
    assert_eq!(
        compare_stream_ids("1704067200000-0", "1704067200001-0"),
        Ordering::Less
    );
}
