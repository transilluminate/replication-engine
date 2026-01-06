// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Cold path: Merkle tree anti-entropy repair.
//!
//! Periodically compares Merkle roots with peers and repairs divergent data.
//! This catches any events missed by the hot path (network issues, stream trimming, etc.).
//!
//! # Algorithm
//!
//! 1. Get local Merkle root from sync-engine
//! 2. Get peer Merkle root from peer's Redis
//! 3. If roots match → data is in sync, done
//! 4. If roots differ → drill down to find divergent subtrees
//! 5. For each divergent leaf, fetch the item from peer
//! 6. Submit to local sync-engine (it decides if update is needed)
//!
//! # Backoff on Failure
//!
//! When repair fails for a peer, we apply exponential backoff to avoid
//! hammering a struggling peer. Backoff resets on success.

use crate::config::ColdPathConfig;
use crate::error::ReplicationError;
use crate::metrics;
use crate::peer::{PeerConnection, PeerManager};
use crate::sync_engine::{SyncEngineRef, SyncItem};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;
use tracing::{debug, info, instrument, warn, Instrument};

/// Statistics from a repair cycle.
#[derive(Debug, Default, Clone)]
pub struct RepairStats {
    /// Number of peers checked
    pub peers_checked: usize,
    /// Number of peers that were in sync
    pub peers_in_sync: usize,
    /// Number of items fetched from peers
    pub items_fetched: usize,
    /// Number of items submitted to sync-engine
    pub items_submitted: usize,
    /// Number of errors encountered
    pub errors: usize,
}

/// Run the cold path repair task.
///
/// This function runs until shutdown is signaled.
///
/// # Hot Path Coordination
///
/// When `catching_up_count > 0`, any hot path tailer is still catching up
/// with its stream. We skip repair during this time to avoid duplicate work,
/// since hot path will apply the same events.
pub async fn run_repair<S: SyncEngineRef + Send + Sync + 'static>(
    sync_engine: Arc<S>,
    peer_manager: Arc<PeerManager>,
    config: ColdPathConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    catching_up_count: Arc<std::sync::atomic::AtomicUsize>,
) {
    let span = tracing::info_span!("cold_path");

    async move {
        // Mark initial shutdown value as seen so changed() only fires on actual changes
        let _ = shutdown_rx.borrow_and_update();

        let interval = config.interval();

        info!(
            interval_secs = interval.as_secs(),
            backoff_base_sec = config.backoff_base_sec,
            backoff_max_sec = config.backoff_max_sec,
            "Starting cold path repair task"
        );

        let mut timer = tokio::time::interval(interval);
        // Skip missed ticks instead of bursting to catch up
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track per-peer backoff state: (consecutive_failures, backoff_until)
        let mut peer_backoff: HashMap<String, (u32, Instant)> = HashMap::new();

        loop {
            // Wait for next interval or shutdown
            tokio::select! {
                biased;
                
                // Priority: check shutdown first
                result = shutdown_rx.changed() => {
                    if result.is_err() || *shutdown_rx.borrow() {
                        info!("Shutdown signal received, stopping repair task");
                        break;
                    }
                    // Value changed but not to shutdown - continue waiting
                    continue;
                }
                
                _ = timer.tick() => {
                    // Time to run a repair cycle
                }
            }

            // Skip repair if hot path is still catching up
            let catching_up = catching_up_count.load(std::sync::atomic::Ordering::SeqCst);
            if catching_up > 0 {
                debug!(
                    peers_catching_up = catching_up,
                    "Skipping repair cycle - hot path is catching up"
                );
                metrics::record_repair_skipped("hot_path_catching_up");
                continue;
            }

            // Run repair cycle with backoff tracking
            let cycle_start = Instant::now();
            let (stats, failures) = run_repair_cycle_with_backoff(
                sync_engine.as_ref(),
                &peer_manager,
                &config,
                &peer_backoff,
            ).await;

            // Update backoff state based on results
            for (peer_id, success) in failures {
                if success {
                    // Reset backoff on success
                    peer_backoff.remove(&peer_id);
                } else {
                    // Increment failure count and calculate new backoff
                    let current_failures = peer_backoff
                        .get(&peer_id)
                        .map(|(f, _)| *f)
                        .unwrap_or(0);
                    let new_failures = current_failures + 1;
                    let backoff_duration = config.backoff_for_failures(new_failures);
                    let backoff_until = Instant::now() + backoff_duration;
                    peer_backoff.insert(peer_id, (new_failures, backoff_until));
                }
            }

            let duration = cycle_start.elapsed();

            // Emit metrics
            metrics::record_repair_cycle_complete(
                stats.peers_checked,
                stats.peers_in_sync,
                stats.items_fetched,
                stats.items_submitted,
                stats.errors,
                duration,
            );

            if stats.items_submitted > 0 {
                info!(
                    peers_checked = stats.peers_checked,
                    peers_in_sync = stats.peers_in_sync,
                    items_fetched = stats.items_fetched,
                    items_submitted = stats.items_submitted,
                    duration_ms = duration.as_millis(),
                    "Repair cycle complete with updates"
                );
            } else {
                debug!(
                    peers_checked = stats.peers_checked,
                    peers_in_sync = stats.peers_in_sync,
                    "Repair cycle complete, all in sync"
                );
            }
        }

        info!("Cold path repair task stopped");
    }
    .instrument(span)
    .await
}

/// Run a single repair cycle with per-peer backoff tracking.
///
/// Returns (stats, peer_results) where peer_results is a vec of (peer_id, success).
#[instrument(skip_all, fields(max_items = config.max_items_per_cycle))]
async fn run_repair_cycle_with_backoff<S: SyncEngineRef>(
    sync_engine: &S,
    peer_manager: &PeerManager,
    config: &ColdPathConfig,
    peer_backoff: &HashMap<String, (u32, Instant)>,
) -> (RepairStats, Vec<(String, bool)>) {
    let mut stats = RepairStats::default();
    let mut peer_results = Vec::new();
    let mut remaining_budget = config.max_items_per_cycle;
    let now = Instant::now();

    // Get connected peers
    let peers = peer_manager.all();
    let connected: Vec<_> = futures::future::join_all(
        peers
            .iter()
            .map(|p| async { (p.clone(), p.is_connected().await) }),
    )
    .await
    .into_iter()
    .filter(|(_, connected)| *connected)
    .map(|(p, _)| p)
    .collect();

    if connected.is_empty() {
        debug!("No connected peers, skipping repair cycle");
        return (stats, peer_results);
    }

    debug!(peer_count = connected.len(), "Starting repair cycle");

    // Get local Merkle root
    let local_root = sync_engine
        .get_merkle_root()
        .await
        .ok()
        .flatten();

    for peer in connected {
        if remaining_budget == 0 {
            debug!("Repair budget exhausted, deferring remaining peers to next cycle");
            break;
        }

        let peer_id = peer.node_id().to_string();

        // Check if peer is in backoff
        if let Some((failures, backoff_until)) = peer_backoff.get(&peer_id) {
            if now < *backoff_until {
                let remaining_secs = (*backoff_until - now).as_secs();
                debug!(
                    peer_id = %peer_id,
                    consecutive_failures = failures,
                    backoff_remaining_secs = remaining_secs,
                    "Skipping peer - in backoff"
                );
                metrics::record_repair_skipped("peer_backoff");
                continue;
            }
        }

        stats.peers_checked += 1;

        match repair_with_peer(sync_engine, &peer, local_root.as_ref(), remaining_budget).await {
            Ok(peer_stats) => {
                if peer_stats.items_submitted == 0 {
                    stats.peers_in_sync += 1;
                }
                stats.items_fetched += peer_stats.items_fetched;
                stats.items_submitted += peer_stats.items_submitted;
                
                // Deduct from budget
                remaining_budget = remaining_budget.saturating_sub(peer_stats.items_fetched);

                // Record success
                peer_results.push((peer_id, true));
            }
            Err(e) => {
                stats.errors += 1;
                warn!(
                    peer_id = %peer.node_id(),
                    error = %e,
                    "Failed to repair with peer"
                );

                // Record failure
                peer_results.push((peer_id, false));
            }
        }
    }

    debug!("Repair cycle complete");
    (stats, peer_results)
}

/// Repair divergent data with a single peer.
///
/// `max_items` limits how many items to fetch/submit (prevents memory pressure).
/// Returns stats including items processed, which should be deducted from the caller's budget.
#[instrument(skip(sync_engine, peer, local_root), fields(peer_id = %peer.node_id(), max_items))]
async fn repair_with_peer<S: SyncEngineRef>(
    sync_engine: &S,
    peer: &PeerConnection,
    local_root: Option<&[u8; 32]>,
    max_items: usize,
) -> Result<RepairStats, ReplicationError> {
    let mut stats = RepairStats::default();
    let peer_id = peer.node_id();

    // Get peer's Merkle root
    let peer_root = peer.get_merkle_root().await?;

    // Compare roots
    match (local_root, peer_root.as_ref()) {
        (Some(local), Some(remote)) if local == remote => {
            debug!(peer_id = %peer_id, "Merkle roots match, in sync");
            return Ok(stats);
        }
        (None, None) => {
            debug!(peer_id = %peer_id, "Both sides empty, in sync");
            return Ok(stats);
        }
        (Some(_), None) => {
            debug!(peer_id = %peer_id, "Peer has no data, nothing to fetch");
            return Ok(stats);
        }
        (None, Some(_)) | (Some(_), Some(_)) => {
            debug!(peer_id = %peer_id, "Merkle roots differ, drilling down");
            metrics::record_merkle_divergence(peer_id);
        }
    }

    // Drill down to find divergent items
    let divergent_keys = find_divergent_keys(sync_engine, peer, "").await?;

    let total_divergent = divergent_keys.len();
    let keys_to_process = if total_divergent > max_items {
        warn!(
            peer_id = %peer_id,
            total_divergent,
            max_items,
            "More divergent keys than budget allows, deferring {} to next cycle",
            total_divergent - max_items
        );
        &divergent_keys[..max_items]
    } else {
        &divergent_keys[..]
    };

    debug!(
        peer_id = %peer_id,
        divergent_count = total_divergent,
        processing_count = keys_to_process.len(),
        "Found divergent keys"
    );

    // Fetch and submit divergent items
    for key in keys_to_process {
        stats.items_fetched += 1;

        match peer.get_item(key).await? {
            Some(data) => {
                // Create SyncItem - hash is computed automatically by SyncItem::new()
                let item = SyncItem::new(key.clone(), data);

                // Submit to sync-engine (it will decide if update is needed)
                if let Err(e) = sync_engine
                    .submit(item)
                    .await
                {
                    warn!(key = %key, error = %e, "Failed to submit repaired item");
                    stats.errors += 1;
                } else {
                    stats.items_submitted += 1;
                    debug!(key = %key, "Submitted repaired item");
                }
            }
            None => {
                debug!(key = %key, "Item not found on peer, skipping");
            }
        }
    }

    Ok(stats)
}

/// Maximum concurrent Merkle drill-down tasks per level.
const MAX_PARALLEL_MERKLE_DRILL: usize = 8;

/// Recursively find divergent keys by drilling down the Merkle tree.
/// 
/// Children at each level are processed in parallel for faster traversal.
fn find_divergent_keys<'a, S: SyncEngineRef>(
    sync_engine: &'a S,
    peer: &'a PeerConnection,
    path: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<String>, ReplicationError>> + Send + 'a>> {
    Box::pin(async move {
        // Get local and remote children for this path (in parallel)
        let (local_children, remote_result) = tokio::join!(
            sync_engine.get_merkle_children(path),
            peer.get_merkle_children(path)
        );
        
        let local_children = local_children.unwrap_or_default();
        let remote_children = remote_result?;

        // Build lookup maps
        let local_map: std::collections::HashMap<_, _> = local_children.into_iter().collect();
        let remote_map: std::collections::HashMap<_, _> = remote_children.into_iter().collect();

        // Find all unique child names
        let mut all_children: HashSet<String> = HashSet::new();
        all_children.extend(local_map.keys().cloned());
        all_children.extend(remote_map.keys().cloned());

        // Collect children that need drill-down
        let mut needs_drilldown: Vec<String> = Vec::new();
        let mut divergent: Vec<String> = Vec::new();

        for child_name in all_children {
            let local_hash = local_map.get(&child_name);
            let remote_hash = remote_map.get(&child_name);

            let child_path = if path.is_empty() {
                child_name.clone()
            } else {
                format!("{}/{}", path, child_name)
            };

            match (local_hash, remote_hash) {
                (Some(local), Some(remote)) if local == remote => {
                    // Hashes match, subtree is in sync
                    continue;
                }
                (Some(_), None) => {
                    // Peer doesn't have this, nothing to fetch
                    continue;
                }
                _ => {
                    // Divergent or missing locally - need to drill down
                    needs_drilldown.push(child_path);
                }
            }
        }

        // Process drill-downs in parallel batches
        for chunk in needs_drilldown.chunks(MAX_PARALLEL_MERKLE_DRILL) {
            let futures: Vec<_> = chunk
                .iter()
                .map(|child_path| find_divergent_keys(sync_engine, peer, child_path))
                .collect();

            let results = futures::future::join_all(futures).await;

            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(sub_divergent) => {
                        if sub_divergent.is_empty() {
                            // This is a leaf node
                            divergent.push(chunk[i].clone());
                        } else {
                            divergent.extend(sub_divergent);
                        }
                    }
                    Err(e) => {
                        warn!(path = %chunk[i], error = %e, "Failed to drill down Merkle path");
                        // Continue with other paths rather than failing entirely
                    }
                }
            }
        }

        Ok(divergent)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repair_stats_default() {
        let stats = RepairStats::default();
        assert_eq!(stats.peers_checked, 0);
        assert_eq!(stats.peers_in_sync, 0);
        assert_eq!(stats.items_fetched, 0);
        assert_eq!(stats.items_submitted, 0);
        assert_eq!(stats.errors, 0);
    }

    #[test]
    fn test_repair_stats_clone() {
        let stats = RepairStats {
            peers_checked: 5,
            peers_in_sync: 3,
            items_fetched: 100,
            items_submitted: 50,
            errors: 2,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.peers_checked, 5);
        assert_eq!(cloned.peers_in_sync, 3);
        assert_eq!(cloned.items_fetched, 100);
        assert_eq!(cloned.items_submitted, 50);
        assert_eq!(cloned.errors, 2);
    }

    #[test]
    fn test_repair_stats_debug() {
        let stats = RepairStats {
            peers_checked: 2,
            peers_in_sync: 1,
            items_fetched: 10,
            items_submitted: 5,
            errors: 0,
        };
        let debug = format!("{:?}", stats);
        assert!(debug.contains("RepairStats"));
        assert!(debug.contains("peers_checked: 2"));
        assert!(debug.contains("peers_in_sync: 1"));
        assert!(debug.contains("items_fetched: 10"));
        assert!(debug.contains("items_submitted: 5"));
        assert!(debug.contains("errors: 0"));
    }

    #[test]
    fn test_repair_stats_all_in_sync() {
        // Scenario: All peers are in sync
        let stats = RepairStats {
            peers_checked: 10,
            peers_in_sync: 10,
            items_fetched: 0,
            items_submitted: 0,
            errors: 0,
        };
        assert_eq!(stats.peers_checked, stats.peers_in_sync);
        assert_eq!(stats.items_fetched, 0);
    }

    #[test]
    fn test_repair_stats_partial_repair() {
        // Scenario: Some peers diverged, repair happened
        let stats = RepairStats {
            peers_checked: 5,
            peers_in_sync: 2,
            items_fetched: 150,
            items_submitted: 100,
            errors: 1,
        };
        // 3 peers were out of sync
        assert_eq!(stats.peers_checked - stats.peers_in_sync, 3);
        // Not all fetched items were submitted (dedup/is_current filtered some)
        assert!(stats.items_submitted < stats.items_fetched);
    }

    #[test]
    fn test_repair_stats_high_error_rate() {
        // Scenario: Many errors during repair
        let stats = RepairStats {
            peers_checked: 10,
            peers_in_sync: 0,
            items_fetched: 50,
            items_submitted: 10,
            errors: 40,
        };
        // High error rate indicates problems
        assert!(stats.errors > stats.items_submitted);
    }
}
