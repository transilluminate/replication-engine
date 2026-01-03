//! Hot path: CDC stream tailing with batch processing.
//!
//! Each peer has a dedicated tailer task that:
//! 1. Reads events from the peer's CDC stream (XREAD)
//! 2. Batches events with key deduplication (latest wins)
//! 3. Parallel is_current() dedup checks
//! 4. Batch submit/delete to sync-engine
//! 5. Persists cursor after successful flush
//!
//! # Graceful Shutdown
//!
//! When a shutdown signal is received:
//! 1. The tailer stops reading new events immediately (via tokio::select!)
//! 2. Any pending batch is flushed through the circuit breaker
//! 3. Cursor is persisted to ensure we can resume correctly
//! 4. Confirmation is logged before the task exits
//!
//! # Circuit Breaker Protection
//!
//! The hot path uses a shared circuit breaker to protect sync-engine from
//! overload. When the circuit opens (too many consecutive failures), the
//! tailer will back off and wait for recovery instead of hammering
//! a struggling service.

use crate::batch::{BatchConfig, BatchProcessor, BatchResult};
use crate::circuit_breaker::{CircuitError, SyncEngineCircuit};
use crate::config::HotPathConfig;
use crate::cursor::CursorStore;
use crate::error::ReplicationError;
use crate::metrics;
use crate::peer::PeerConnection;
use crate::resilience::RateLimiter;
use crate::stream::{calculate_lag_ms, ReadResult, StreamTailer};
use crate::sync_engine::SyncEngineRef;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, info_span, warn, Instrument};

/// Threshold for considering a tailer to be "catching up".
/// If we read this many events in a single batch, we're probably behind.
const CATCHING_UP_THRESHOLD: usize = 50;

/// How often to check and emit lag metrics (in loop iterations).
const LAG_CHECK_INTERVAL: u32 = 10;

/// Run the hot path tailer for a single peer.
///
/// This function runs until shutdown is signaled.
///
/// # Hot Path Catching Up Detection
///
/// When the tailer reads a full batch of events (>= CATCHING_UP_THRESHOLD),
/// it's likely behind the stream head. During this time, the cold path
/// should skip repair to avoid duplicate work.
///
/// # Graceful Shutdown
///
/// The tailer uses `tokio::select!` to respond immediately to shutdown signals,
/// even during blocked XREAD operations. On shutdown:
/// 1. Current XREAD is cancelled
/// 2. Pending batch is flushed through circuit breaker
/// 3. Cursor is persisted for resume
///
/// # Circuit Breaker
///
/// The `circuit` parameter protects sync-engine from overload. When too many
/// consecutive batch flushes fail, the circuit opens and the tailer backs off
/// instead of continuing to hammer the struggling service. This allows
/// sync-engine time to recover.
///
/// # Rate Limiting
///
/// The optional `rate_limiter` prevents thundering herd scenarios when many
/// peers reconnect simultaneously. If provided, each event processed will
/// consume a token from the rate limiter, blocking if the limit is exceeded.
#[allow(clippy::too_many_arguments)]
pub async fn run_tailer<S: SyncEngineRef>(
    peer: Arc<PeerConnection>,
    cursor_store: Arc<RwLock<Option<CursorStore>>>,
    sync_engine: Arc<S>,
    circuit: Arc<SyncEngineCircuit>,
    config: HotPathConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    catching_up_count: Arc<AtomicUsize>,
    rate_limiter: Option<Arc<RateLimiter>>,
) {
    let peer_id = peer.node_id().to_string();
    let span = tracing::info_span!("hot_path", peer_id = %peer_id);

    async move {
        info!("Starting hot path tailer");

        // Track if we're currently catching up (to decrement on exit)
        let mut is_catching_up = false;

        // Parse block_timeout from config
        let block_timeout = config.block_timeout_duration();

        // Adaptive batch sizing state
        let adaptive_enabled = config.adaptive_batch_size;
        let min_batch = config.min_batch_size;
        let max_batch = config.max_batch_size;
        let mut current_batch_size = config.batch_size;

        let mut tailer = StreamTailer::new(
            peer_id.clone(),
            peer.cdc_stream_key(),
            block_timeout,
            current_batch_size,
        );

        // Configure batch processor
        let batch_config = BatchConfig {
            max_batch_size: config.batch_size,
            max_batch_delay: Duration::from_millis(50),
            max_concurrent_checks: 32,
        };
        let mut batch_processor = BatchProcessor::new(
            Arc::clone(&sync_engine),
            peer_id.clone(),
            batch_config,
        );

        // Record initial batch size
        if adaptive_enabled {
            metrics::record_adaptive_batch_size(&peer_id, current_batch_size);
        }

        // Get initial cursor
        let mut cursor = {
            let store_guard = cursor_store.read().await;
            if let Some(ref store) = *store_guard {
                store.get_or_start(&peer_id).await
            } else {
                "0".to_string()
            }
        };

        info!(cursor = %cursor, "Resuming from cursor");

        // Track last event ID for cursor updates
        let mut last_event_id: Option<String> = None;

        // Lag tracking: iteration counter for periodic lag checks
        let mut iteration_count: u32 = 0;

        // Backoff for errors
        let error_backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(300);
        let mut consecutive_errors = 0u32;
        let mut current_backoff = error_backoff;

        loop {
            iteration_count = iteration_count.wrapping_add(1);

            // Get connection
            let conn = peer.connection().await;
            let mut conn = match conn {
                Some(c) => c,
                None => {
                    // Check shutdown while waiting for reconnect
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                    warn!("Peer disconnected, waiting for reconnect");
                    continue;
                }
            };

            // Read events - use XRANGE when catching up (faster), XREAD when tailing
            let read_start = Instant::now();
            let read_result = if is_catching_up {
                // Catchup mode: use XRANGE for fast non-blocking bulk reads
                // No need for select! here since XRANGE doesn't block
                if *shutdown_rx.borrow() {
                    info!("Shutdown signal received during catchup read");
                    break;
                }
                
                // Check for stream trim before reading
                match tailer.check_cursor_valid(&mut conn, &cursor).await {
                    Ok(Some(oldest_id)) => Ok(ReadResult::StreamTrimmed {
                        cursor: cursor.clone(),
                        oldest_id,
                    }),
                    Ok(None) => {
                        // Cursor is valid, do the XRANGE read
                        match tailer.read_events_range(&mut conn, &cursor, current_batch_size).await {
                            Ok(events) => Ok(ReadResult::Events(events)),
                            Err(e) => Err(e),
                        }
                    }
                    Err(e) => Err(e),
                }
            } else {
                // Tailing mode: use XREAD with blocking for efficient real-time streaming
                tokio::select! {
                    biased;
                    
                    // Priority: check shutdown first
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            info!("Shutdown signal received during read");
                            break;
                        }
                        continue;
                    }
                    
                    // Read from stream (may block up to block_timeout)
                    result = tailer.read_events_checked(&mut conn, &cursor) => result,
                }
            };
            
            // Record read latency (includes network round-trip)
            metrics::record_stream_read_latency(&peer_id, read_start.elapsed());

            match read_result {
                Ok(read_result) => {
                    consecutive_errors = 0;
                    current_backoff = error_backoff;

                    match read_result {
                        ReadResult::StreamTrimmed { cursor: old_cursor, oldest_id } => {
                            // Stream was trimmed past our cursor!
                            // This is a potential data gap - log prominently.
                            warn!(
                                old_cursor = %old_cursor,
                                oldest_id = %oldest_id,
                                "Stream trimmed - resetting cursor to oldest. Data gap possible!"
                            );
                            
                            metrics::record_stream_trimmed(&peer_id);

                            // Reset cursor to oldest available entry
                            cursor = oldest_id.clone();
                            persist_cursor(&cursor_store, &peer_id, &cursor).await;
                            
                            // Cold path will eventually repair any gaps
                            continue;
                        }
                        ReadResult::Empty => {
                            // Stream is empty, nothing to do
                            debug!("Stream is empty");
                            continue;
                        }
                        ReadResult::Events(events) if events.is_empty() => {
                            // No new events (XREAD timeout) - we're caught up!
                            if is_catching_up {
                                is_catching_up = false;
                                catching_up_count.fetch_sub(1, Ordering::SeqCst);
                                info!("Caught up with stream, resuming normal operation");
                            }

                            // AIMD: Decrease when caught up (we don't need speed, save resources)
                            // Slowly reduce batch size since we're keeping up fine
                            if adaptive_enabled && current_batch_size > min_batch {
                                // Reduce by 10% or 10, whichever is larger
                                let decrease = std::cmp::max(current_batch_size / 10, 10);
                                current_batch_size = std::cmp::max(current_batch_size.saturating_sub(decrease), min_batch);
                                tailer.set_batch_size(current_batch_size);
                                metrics::record_adaptive_batch_size(&peer_id, current_batch_size);
                                debug!(
                                    batch_size = current_batch_size,
                                    "AIMD: Decreased batch size (caught up)"
                                );
                            }

                            // Check if batch should flush by time
                            if batch_processor.should_flush() {
                                match flush_and_persist(
                                    &mut batch_processor,
                                    &cursor_store,
                                    &circuit,
                                    &peer_id,
                                    &last_event_id,
                                ).await {
                                    Ok(Some((result, duration))) => {
                                        if let Some(ref id) = last_event_id {
                                            cursor = id.clone();
                                        }
                                        log_batch_result(&peer_id, &result, duration);
                                    }
                                    Ok(None) => {
                                        // Circuit rejected - back off before retry
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Failed to flush batch");
                                    }
                                }
                            }
                            continue;
                        }
                        ReadResult::Events(events) => {
                            // Record events read
                            let event_count = events.len();
                            if event_count > 0 {
                                metrics::record_cdc_events_read(&peer_id, event_count);
                            }

                            // Track if we're catching up (reading full batches = behind)
                            if event_count >= CATCHING_UP_THRESHOLD {
                                if !is_catching_up {
                                    is_catching_up = true;
                                    catching_up_count.fetch_add(1, Ordering::SeqCst);
                                    info!(event_count, "Catching up with stream, cold path will pause");

                                    // AIMD: Increase when falling behind (need more throughput to catch up)
                                    // Double batch size to process events faster
                                    if adaptive_enabled && current_batch_size < max_batch {
                                        current_batch_size = std::cmp::min(current_batch_size * 2, max_batch);
                                        tailer.set_batch_size(current_batch_size);
                                        metrics::record_adaptive_batch_size(&peer_id, current_batch_size);
                                        info!(
                                            batch_size = current_batch_size,
                                            "AIMD: Doubled batch size (catching up)"
                                        );
                                    }
                                }
                            } else if is_catching_up {
                                // Small batch = caught up
                                is_catching_up = false;
                                catching_up_count.fetch_sub(1, Ordering::SeqCst);
                                info!("Caught up with stream, resuming normal operation");
                            }

                            // Rate limit: acquire tokens before processing batch
                            // This prevents thundering herd when many peers reconnect
                            if let Some(ref limiter) = rate_limiter {
                                limiter.acquire_many(event_count as u32).await;
                            }

                            // Process events with tracing context
                            for event in events {
                                // Create span with trace context from upstream if available
                                let span = if let Some(trace_id) = event.trace_id() {
                                    info_span!(
                                        "process_cdc_event",
                                        peer_id = %peer_id,
                                        key = %event.key,
                                        op = ?event.op,
                                        stream_id = %event.stream_id,
                                        trace_id = %trace_id,
                                        parent_span_id = event.parent_span_id().unwrap_or("none")
                                    )
                                } else {
                                    info_span!(
                                        "process_cdc_event",
                                        peer_id = %peer_id,
                                        key = %event.key,
                                        op = ?event.op,
                                        stream_id = %event.stream_id
                                    )
                                };
                                let _guard = span.enter();
                                
                                last_event_id = Some(event.stream_id.clone());
                                batch_processor.add(event);
                            }

                            // Periodically check and emit lag metrics
                            if iteration_count % LAG_CHECK_INTERVAL == 0 {
                                if let Some(ref current_cursor) = last_event_id {
                                    // Get stream length and latest ID for lag calculation
                                    if let Ok(stream_len) = tailer.get_stream_length(&mut conn).await {
                                        // Approximate lag: if we're at cursor X and stream has N entries,
                                        // we're roughly (N - position) behind. Since we don't track position
                                        // directly, use time-based lag instead.
                                        if let Ok(Some(latest_id)) = tailer.get_latest_id(&mut conn).await {
                                            if let Some(lag_ms) = calculate_lag_ms(current_cursor, &latest_id) {
                                                metrics::record_replication_lag_ms(&peer_id, lag_ms);
                                                // Also record as seconds for the existing metric
                                                metrics::record_replication_lag(&peer_id, lag_ms as f64 / 1000.0);
                                                
                                                if lag_ms > 5000 {
                                                    debug!(
                                                        peer_id = %peer_id,
                                                        lag_ms,
                                                        stream_len,
                                                        "Replication lag detected"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Flush if thresholds met
                            if batch_processor.should_flush() {
                                match flush_and_persist(
                                    &mut batch_processor,
                                    &cursor_store,
                                    &circuit,
                                    &peer_id,
                                    &last_event_id,
                                ).await {
                                    Ok(Some((result, duration))) => {
                                        if let Some(ref id) = last_event_id {
                                            cursor = id.clone();
                                        }
                                        log_batch_result(&peer_id, &result, duration);
                                        peer.record_success().await;
                                    }
                                    Ok(None) => {
                                        // Circuit rejected - back off before retry
                                        warn!("Circuit breaker open, backing off for 5s");
                                        metrics::record_circuit_rejection("sync_engine_writes");
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                        peer.record_failure().await;
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Failed to flush batch");
                                        metrics::record_error(&peer_id, "batch_flush");
                                        peer.record_failure().await;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    peer.record_failure().await;

                    if e.is_retryable() {
                        warn!(
                            error = %e,
                            consecutive_errors,
                            backoff_ms = current_backoff.as_millis(),
                            "Retryable error, backing off"
                        );

                        tokio::time::sleep(current_backoff).await;
                        current_backoff = std::cmp::min(
                            Duration::from_secs_f64(current_backoff.as_secs_f64() * 2.0),
                            max_backoff,
                        );

                        if consecutive_errors > 5 {
                            warn!("Too many consecutive errors, marking peer disconnected");
                            peer.mark_disconnected().await;
                        }
                    } else {
                        error!(error = %e, "Non-retryable error");
                    }
                }
            }
        }

        // Graceful shutdown: flush any pending batch
        if !batch_processor.is_empty() {
            info!(
                pending_events = batch_processor.len(),
                "Flushing pending batch before shutdown"
            );
            
            match flush_and_persist(
                &mut batch_processor,
                &cursor_store,
                &circuit,
                &peer_id,
                &last_event_id,
            ).await {
                Ok(Some((result, duration))) => {
                    log_batch_result(&peer_id, &result, duration);
                    info!(
                        submitted = result.submitted,
                        deleted = result.deleted,
                        "Final batch flushed successfully"
                    );
                }
                Ok(None) => {
                    // Circuit rejected on shutdown - try direct flush as last resort
                    warn!("Circuit breaker open on shutdown, forcing direct flush");
                    if let Err(e) = batch_processor.flush().await {
                        error!(error = %e, "Failed to flush final batch on shutdown");
                    } else if let Some(ref id) = last_event_id {
                        let _ = persist_cursor(&cursor_store, &peer_id, id).await;
                        info!("Final batch force-flushed on shutdown");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to flush final batch on shutdown");
                }
            }
        } else {
            debug!("No pending events to flush on shutdown");
        }

        // Decrement catching up count if we were catching up
        if is_catching_up {
            catching_up_count.fetch_sub(1, Ordering::SeqCst);
        }

        info!("Hot path tailer stopped");
    }
    .instrument(span)
    .await
}

/// Flush batch through circuit breaker and persist cursor.
///
/// Returns `Ok(None)` if the circuit rejected the call (sync-engine is overloaded).
/// Returns `Ok(Some((result, duration)))` if the flush completed (successfully or with partial errors).
/// Returns `Err(e)` if the flush failed in a way that couldn't be handled.
async fn flush_and_persist<S: SyncEngineRef>(
    processor: &mut BatchProcessor<S>,
    cursor_store: &Arc<RwLock<Option<CursorStore>>>,
    circuit: &SyncEngineCircuit,
    peer_id: &str,
    last_event_id: &Option<String>,
) -> Result<Option<(BatchResult, Duration)>, ReplicationError> {
    let start = Instant::now();

    // Use the writes circuit since flush does submit/delete
    let result = circuit
        .writes
        .call(|| async { processor.flush().await })
        .await;

    let duration = start.elapsed();

    match result {
        Ok(batch_result) => {
            // Record circuit success
            metrics::record_circuit_call("sync_engine_writes", "success");

            // Only persist cursor if batch was successful
            if batch_result.is_success() {
                if let Some(ref event_id) = last_event_id {
                    persist_cursor(cursor_store, peer_id, event_id).await;
                    metrics::record_cursor_persist(peer_id, true);
                }
            }
            Ok(Some((batch_result, duration)))
        }
        Err(CircuitError::Rejected) => {
            // Circuit is open, sync-engine is struggling
            // Don't flush, the batch will be retried later
            metrics::record_circuit_call("sync_engine_writes", "rejected");
            warn!(
                peer_id = %peer_id,
                "Circuit breaker open, skipping flush (sync-engine overloaded)"
            );
            Ok(None)
        }
        Err(CircuitError::Inner(e)) => {
            // Actual flush error - propagate it
            metrics::record_circuit_call("sync_engine_writes", "failure");
            Err(e)
        }
    }
}

/// Log batch result at appropriate level and emit metrics.
fn log_batch_result(peer_id: &str, result: &BatchResult, duration: Duration) {
    if result.total == 0 {
        return;
    }

    // Emit metrics
    metrics::record_cdc_events_applied(peer_id, result.submitted + result.deleted);
    metrics::record_cdc_events_deduped(peer_id, result.skipped);
    metrics::record_batch_flush(
        peer_id,
        result.total,
        result.submitted,
        result.deleted,
        result.skipped,
        result.errors,
        duration,
    );

    if result.is_success() {
        debug!(
            peer_id = %peer_id,
            total = result.total,
            submitted = result.submitted,
            deleted = result.deleted,
            skipped = result.skipped,
            duration_ms = duration.as_millis(),
            "Batch processed"
        );
    } else {
        warn!(
            peer_id = %peer_id,
            total = result.total,
            errors = result.errors,
            "Batch had errors"
        );
    }
}

/// Persist cursor to store (debounced - updates cache, will be flushed later).
async fn persist_cursor(
    cursor_store: &Arc<RwLock<Option<CursorStore>>>,
    peer_id: &str,
    cursor: &str,
) {
    let store_guard = cursor_store.read().await;
    if let Some(ref store) = *store_guard {
        store.set(peer_id, cursor).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchResult;

    #[test]
    fn test_catching_up_threshold() {
        // Verify the threshold is reasonable
        assert_eq!(CATCHING_UP_THRESHOLD, 50);
    }

    #[test]
    fn test_lag_check_interval() {
        // Verify the lag check interval is reasonable
        assert_eq!(LAG_CHECK_INTERVAL, 10);
    }

    #[test]
    fn test_log_batch_result_empty() {
        // Empty batch should be a no-op (no panic)
        let result = BatchResult {
            total: 0,
            submitted: 0,
            deleted: 0,
            skipped: 0,
            errors: 0,
        };
        // Just verify it doesn't panic
        log_batch_result("peer-1", &result, Duration::from_millis(10));
    }

    #[test]
    fn test_log_batch_result_success() {
        // Successful batch
        let result = BatchResult {
            total: 100,
            submitted: 80,
            deleted: 10,
            skipped: 10,
            errors: 0,
        };
        // Should not panic
        log_batch_result("peer-1", &result, Duration::from_millis(50));
    }

    #[test]
    fn test_log_batch_result_with_errors() {
        // Batch with errors
        let result = BatchResult {
            total: 50,
            submitted: 30,
            deleted: 5,
            skipped: 5,
            errors: 10,
        };
        // Should not panic
        log_batch_result("peer-1", &result, Duration::from_millis(100));
    }

    #[test]
    fn test_batch_result_is_success() {
        let success = BatchResult {
            total: 100,
            submitted: 80,
            deleted: 20,
            skipped: 0,
            errors: 0,
        };
        assert!(success.is_success());

        let failure = BatchResult {
            total: 100,
            submitted: 80,
            deleted: 10,
            skipped: 5,
            errors: 5,
        };
        assert!(!failure.is_success());
    }

    #[tokio::test]
    async fn test_persist_cursor_empty_store() {
        // When store is None, persist_cursor should not panic
        let cursor_store: Arc<RwLock<Option<CursorStore>>> = Arc::new(RwLock::new(None));
        persist_cursor(&cursor_store, "peer-1", "1234567890-0").await;
        // No panic = success
    }
}