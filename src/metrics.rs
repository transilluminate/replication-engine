//! Metrics for observability.
//!
//! Exports Prometheus-compatible metrics for:
//! - Peer connection status
//! - Stream tailing performance
//! - Replication lag
//! - Deduplication stats
//! - Circuit breaker state
//! - Batch processing stats
//!
//! # Metric Naming Convention
//!
//! All metrics are prefixed with `replication_` and follow Prometheus conventions:
//! - Counters end in `_total`
//! - Gauges represent current state
//! - Histograms track distributions (duration, size)
//!
//! # Usage
//!
//! ```rust,no_run
//! use replication_engine::metrics;
//! use std::time::Duration;
//!
//! // In hot_path after reading events
//! metrics::record_cdc_events_read("peer-1", 42);
//!
//! // In batch processor after flush
//! metrics::record_batch_flush("peer-1", 100, 85, 5, 8, 2, Duration::from_millis(50));
//! ```

use metrics::{counter, gauge, histogram};
use std::time::Duration;

/// Record a peer connection event.
pub fn record_peer_connection(peer_id: &str, success: bool) {
    let status = if success { "success" } else { "failure" };
    counter!("replication_peer_connections_total", "peer_id" => peer_id.to_string(), "status" => status).increment(1);
}

/// Record peer connection state.
pub fn record_peer_state(peer_id: &str, state: &str) {
    // This is a state gauge - we set it to 1 for the current state
    // In practice, you'd want to use a label for the state
    gauge!("replication_peer_state", "peer_id" => peer_id.to_string(), "state" => state.to_string()).set(1.0);
}

/// Record peer ping latency (for idle peer health checks).
pub fn record_peer_ping_latency(peer_id: &str, latency: Duration) {
    histogram!("replication_peer_ping_latency_seconds", "peer_id" => peer_id.to_string())
        .record(latency.as_secs_f64());
}

/// Record peer ping result.
pub fn record_peer_ping(peer_id: &str, success: bool) {
    let status = if success { "success" } else { "failure" };
    counter!("replication_peer_pings_total", "peer_id" => peer_id.to_string(), "status" => status).increment(1);
}

/// Record peer circuit breaker state change.
pub fn record_peer_circuit_state(peer_id: &str, state: &str) {
    // Use a counter to track state transitions
    counter!("replication_peer_circuit_transitions_total", "peer_id" => peer_id.to_string(), "state" => state.to_string()).increment(1);
}

/// Record CDC events read from a peer.
pub fn record_cdc_events_read(peer_id: &str, count: usize) {
    counter!("replication_cdc_events_read_total", "peer_id" => peer_id.to_string()).increment(count as u64);
}

/// Record CDC events applied (not deduplicated).
pub fn record_cdc_events_applied(peer_id: &str, count: usize) {
    counter!("replication_cdc_events_applied_total", "peer_id" => peer_id.to_string()).increment(count as u64);
}

/// Record CDC events deduplicated (skipped).
pub fn record_cdc_events_deduped(peer_id: &str, count: usize) {
    counter!("replication_cdc_events_deduped_total", "peer_id" => peer_id.to_string()).increment(count as u64);
}

/// Record stream read (XREAD) latency.
pub fn record_stream_read_latency(peer_id: &str, duration: Duration) {
    histogram!("replication_stream_read_duration_seconds", "peer_id" => peer_id.to_string())
        .record(duration.as_secs_f64());
}

/// Record peer Redis operation latency by operation type.
/// Useful for tracking Merkle queries, item fetches, etc.
pub fn record_peer_operation_latency(peer_id: &str, operation: &str, duration: Duration) {
    histogram!(
        "replication_peer_operation_duration_seconds",
        "peer_id" => peer_id.to_string(),
        "operation" => operation.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Record event processing latency.
pub fn record_event_processing_latency(peer_id: &str, duration: Duration) {
    histogram!("replication_event_processing_duration_seconds", "peer_id" => peer_id.to_string())
        .record(duration.as_secs_f64());
}

/// Record cursor persistence.
pub fn record_cursor_persist(peer_id: &str, success: bool) {
    let status = if success { "success" } else { "failure" };
    counter!("replication_cursor_persists_total", "peer_id" => peer_id.to_string(), "status" => status).increment(1);
}

/// Record cursor flush batch (debounced writes).
pub fn record_cursor_flush(flushed: usize, errors: usize) {
    counter!("replication_cursor_flushes_total").increment(1);
    counter!("replication_cursor_flushed_count").increment(flushed as u64);
    if errors > 0 {
        counter!("replication_cursor_flush_errors_total").increment(errors as u64);
    }
}

/// Record cursor SQLite retry (for SQLITE_BUSY/SQLITE_LOCKED).
pub fn cursor_retries_total(operation: &str) {
    counter!("replication_cursor_retries_total", "operation" => operation.to_string()).increment(1);
}

/// Record replication lag (time since last successful sync).
pub fn record_replication_lag(peer_id: &str, lag_seconds: f64) {
    gauge!("replication_lag_seconds", "peer_id" => peer_id.to_string()).set(lag_seconds);
}

/// Record replication lag in events (how many events behind stream head).
pub fn record_replication_lag_events(peer_id: &str, lag_events: u64) {
    gauge!("replication_lag_events", "peer_id" => peer_id.to_string()).set(lag_events as f64);
}

/// Record replication lag in milliseconds (based on stream ID timestamps).
pub fn record_replication_lag_ms(peer_id: &str, lag_ms: u64) {
    gauge!("replication_lag_ms", "peer_id" => peer_id.to_string()).set(lag_ms as f64);
}

/// Record current adaptive batch size for a peer.
pub fn record_adaptive_batch_size(peer_id: &str, batch_size: usize) {
    gauge!("replication_adaptive_batch_size", "peer_id" => peer_id.to_string()).set(batch_size as f64);
}

/// Record cold path repair cycle.
pub fn record_repair_cycle(items_repaired: usize, duration: Duration) {
    counter!("replication_repair_items_total").increment(items_repaired as u64);
    histogram!("replication_repair_cycle_duration_seconds").record(duration.as_secs_f64());
}

/// Record cold path repair cycle skipped.
pub fn record_repair_skipped(reason: &str) {
    counter!("replication_repair_skipped_total", "reason" => reason.to_string()).increment(1);
}

/// Record errors by type.
pub fn record_error(peer_id: &str, error_type: &str) {
    counter!("replication_errors_total", "peer_id" => peer_id.to_string(), "error_type" => error_type.to_string()).increment(1);
}

/// Gauge for number of connected peers.
pub fn set_connected_peers(count: usize) {
    gauge!("replication_connected_peers").set(count as f64);
}

/// Gauge for engine state.
pub fn set_engine_state(state: &str) {
    // Encode state as numeric for alerting (0=stopped, 1=running, 2=degraded, etc.)
    let value = match state {
        "Created" => 0.0,
        "Connecting" => 1.0,
        "Running" => 2.0,
        "ShuttingDown" => 3.0,
        "Stopped" => 4.0,
        "Failed" => 5.0,
        _ => -1.0,
    };
    gauge!("replication_engine_state").set(value);
}

// =============================================================================
// Circuit Breaker Metrics
// =============================================================================

/// Record circuit breaker call outcome.
pub fn record_circuit_call(circuit_name: &str, outcome: &str) {
    counter!(
        "replication_circuit_calls_total",
        "circuit" => circuit_name.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

/// Set circuit breaker state gauge (0=closed, 1=half_open, 2=open).
pub fn set_circuit_state(circuit_name: &str, state: &str) {
    let value = match state {
        "closed" => 0.0,
        "half_open" => 1.0,
        "open" => 2.0,
        _ => -1.0,
    };
    gauge!("replication_circuit_state", "circuit" => circuit_name.to_string()).set(value);
}

/// Record circuit breaker rejection (circuit was open).
pub fn record_circuit_rejection(circuit_name: &str) {
    counter!(
        "replication_circuit_rejections_total",
        "circuit" => circuit_name.to_string()
    )
    .increment(1);
}

// =============================================================================
// Batch Processing Metrics
// =============================================================================

/// Record batch flush with detailed stats.
pub fn record_batch_flush(
    peer_id: &str,
    total: usize,
    submitted: usize,
    deleted: usize,
    skipped: usize,
    errors: usize,
    duration: Duration,
) {
    let peer = peer_id.to_string();

    counter!("replication_batch_events_total", "peer_id" => peer.clone())
        .increment(total as u64);
    counter!("replication_batch_submitted_total", "peer_id" => peer.clone())
        .increment(submitted as u64);
    counter!("replication_batch_deleted_total", "peer_id" => peer.clone())
        .increment(deleted as u64);
    counter!("replication_batch_skipped_total", "peer_id" => peer.clone())
        .increment(skipped as u64);

    if errors > 0 {
        counter!("replication_batch_errors_total", "peer_id" => peer.clone())
            .increment(errors as u64);
    }

    histogram!("replication_batch_flush_duration_seconds", "peer_id" => peer.clone())
        .record(duration.as_secs_f64());
    histogram!("replication_batch_size", "peer_id" => peer).record(total as f64);
}

/// Record batch dedup stats (for monitoring dedup efficiency).
pub fn record_batch_dedup(peer_id: &str, before_dedup: usize, after_dedup: usize) {
    let deduped = before_dedup.saturating_sub(after_dedup);
    if deduped > 0 {
        counter!("replication_batch_deduped_total", "peer_id" => peer_id.to_string())
            .increment(deduped as u64);
    }
}

// =============================================================================
// Stream Trim Metrics
// =============================================================================

/// Record stream trimmed event (potential data gap).
pub fn record_stream_trimmed(peer_id: &str) {
    counter!("replication_stream_trimmed_total", "peer_id" => peer_id.to_string()).increment(1);
}

/// Record stream read result.
pub fn record_stream_read(peer_id: &str, events_count: usize, duration: Duration) {
    counter!("replication_stream_reads_total", "peer_id" => peer_id.to_string()).increment(1);
    if events_count > 0 {
        counter!("replication_stream_events_read_total", "peer_id" => peer_id.to_string())
            .increment(events_count as u64);
    }
    histogram!("replication_stream_read_duration_seconds", "peer_id" => peer_id.to_string())
        .record(duration.as_secs_f64());
}

// =============================================================================
// Cold Path Repair Metrics
// =============================================================================

/// Record cold path repair cycle completion.
pub fn record_repair_cycle_complete(
    peers_checked: usize,
    peers_in_sync: usize,
    items_fetched: usize,
    items_submitted: usize,
    errors: usize,
    duration: Duration,
) {
    counter!("replication_repair_cycles_total").increment(1);
    counter!("replication_repair_peers_checked_total").increment(peers_checked as u64);
    counter!("replication_repair_peers_in_sync_total").increment(peers_in_sync as u64);
    counter!("replication_repair_items_fetched_total").increment(items_fetched as u64);
    counter!("replication_repair_items_submitted_total").increment(items_submitted as u64);

    if errors > 0 {
        counter!("replication_repair_errors_total").increment(errors as u64);
    }

    histogram!("replication_repair_cycle_duration_seconds").record(duration.as_secs_f64());
}

/// Record divergent peer detected during repair.
pub fn record_merkle_divergence(peer_id: &str) {
    counter!("replication_merkle_divergence_total", "peer_id" => peer_id.to_string()).increment(1);
}

// =============================================================================
// SLO Violation Metrics
// =============================================================================

/// Record an SLO violation (latency threshold exceeded).
///
/// Labels:
/// - `peer_id`: The peer that violated the SLO
/// - `slo_type`: The type of SLO violated (stream_read, peer_op, batch_flush)
/// - `latency_ms`: The actual latency observed
pub fn record_slo_violation(peer_id: &str, slo_type: &str, latency_ms: u64) {
    counter!(
        "replication_slo_violations_total",
        "peer_id" => peer_id.to_string(),
        "slo_type" => slo_type.to_string()
    )
    .increment(1);

    histogram!(
        "replication_slo_violation_latency_ms",
        "peer_id" => peer_id.to_string(),
        "slo_type" => slo_type.to_string()
    )
    .record(latency_ms as f64);
}

/// Set current replication lag gauge (for SLO monitoring).
pub fn set_replication_lag_slo(peer_id: &str, lag_ms: u64, threshold_ms: u64) {
    gauge!("replication_lag_slo_ms", "peer_id" => peer_id.to_string()).set(lag_ms as f64);
    
    // Track whether we're over threshold
    let is_violation = if lag_ms > threshold_ms { 1.0 } else { 0.0 };
    gauge!("replication_lag_slo_violation", "peer_id" => peer_id.to_string()).set(is_violation);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: The metrics crate uses global state. In tests, we just verify that
    // the functions don't panic and handle edge cases correctly.
    // For full integration testing, you'd use metrics-util's DebuggingRecorder.

    #[test]
    fn test_record_peer_connection_success() {
        // Should not panic with valid inputs
        record_peer_connection("peer-1", true);
        record_peer_connection("peer-2", false);
    }

    #[test]
    fn test_record_peer_connection_empty_peer_id() {
        // Should handle empty peer_id gracefully
        record_peer_connection("", true);
    }

    #[test]
    fn test_record_peer_state() {
        record_peer_state("peer-1", "connected");
        record_peer_state("peer-1", "disconnected");
        record_peer_state("peer-1", "connecting");
    }

    #[test]
    fn test_record_peer_ping_latency() {
        record_peer_ping_latency("peer-1", Duration::from_millis(50));
        record_peer_ping_latency("peer-1", Duration::from_secs(1));
        record_peer_ping_latency("peer-1", Duration::ZERO);
    }

    #[test]
    fn test_record_peer_ping() {
        record_peer_ping("peer-1", true);
        record_peer_ping("peer-1", false);
    }

    #[test]
    fn test_record_peer_circuit_state() {
        record_peer_circuit_state("peer-1", "closed");
        record_peer_circuit_state("peer-1", "open");
        record_peer_circuit_state("peer-1", "half_open");
    }

    #[test]
    fn test_record_cdc_events() {
        record_cdc_events_read("peer-1", 100);
        record_cdc_events_read("peer-1", 0);
        record_cdc_events_applied("peer-1", 50);
        record_cdc_events_deduped("peer-1", 10);
    }

    #[test]
    fn test_record_stream_read_latency() {
        record_stream_read_latency("peer-1", Duration::from_millis(100));
        record_stream_read_latency("peer-1", Duration::from_micros(500));
    }

    #[test]
    fn test_record_peer_operation_latency() {
        record_peer_operation_latency("peer-1", "merkle_query", Duration::from_millis(25));
        record_peer_operation_latency("peer-1", "item_fetch", Duration::from_millis(100));
        record_peer_operation_latency("peer-1", "xread", Duration::from_secs(5));
    }

    #[test]
    fn test_record_event_processing_latency() {
        record_event_processing_latency("peer-1", Duration::from_micros(100));
    }

    #[test]
    fn test_record_cursor_persist() {
        record_cursor_persist("peer-1", true);
        record_cursor_persist("peer-1", false);
    }

    #[test]
    fn test_record_cursor_flush() {
        record_cursor_flush(10, 0);
        record_cursor_flush(5, 2);
        record_cursor_flush(0, 0);
    }

    #[test]
    fn test_cursor_retries_total() {
        cursor_retries_total("get");
        cursor_retries_total("set");
        cursor_retries_total("flush");
    }

    #[test]
    fn test_record_replication_lag() {
        record_replication_lag("peer-1", 5.5);
        record_replication_lag("peer-1", 0.0);
        record_replication_lag("peer-1", 1000.0);
    }

    #[test]
    fn test_record_replication_lag_events() {
        record_replication_lag_events("peer-1", 100);
        record_replication_lag_events("peer-1", 0);
    }

    #[test]
    fn test_record_replication_lag_ms() {
        record_replication_lag_ms("peer-1", 500);
        record_replication_lag_ms("peer-1", 0);
    }

    #[test]
    fn test_record_adaptive_batch_size() {
        record_adaptive_batch_size("peer-1", 100);
        record_adaptive_batch_size("peer-1", 1000);
    }

    #[test]
    fn test_record_repair_cycle() {
        record_repair_cycle(50, Duration::from_secs(10));
        record_repair_cycle(0, Duration::ZERO);
    }

    #[test]
    fn test_record_repair_skipped() {
        record_repair_skipped("no_peers");
        record_repair_skipped("all_healthy");
        record_repair_skipped("disabled");
    }

    #[test]
    fn test_record_error() {
        record_error("peer-1", "connection_failed");
        record_error("peer-1", "timeout");
        record_error("peer-1", "parse_error");
    }

    #[test]
    fn test_set_connected_peers() {
        set_connected_peers(0);
        set_connected_peers(5);
        set_connected_peers(100);
    }

    #[test]
    fn test_set_engine_state_all_states() {
        // Test all known states
        set_engine_state("Created");
        set_engine_state("Connecting");
        set_engine_state("Running");
        set_engine_state("ShuttingDown");
        set_engine_state("Stopped");
        set_engine_state("Failed");
        // Unknown state should map to -1
        set_engine_state("Unknown");
    }

    #[test]
    fn test_record_circuit_call() {
        record_circuit_call("peer-1-circuit", "success");
        record_circuit_call("peer-1-circuit", "failure");
        record_circuit_call("peer-1-circuit", "rejected");
    }

    #[test]
    fn test_set_circuit_state_all_states() {
        set_circuit_state("peer-1-circuit", "closed");
        set_circuit_state("peer-1-circuit", "half_open");
        set_circuit_state("peer-1-circuit", "open");
        // Unknown state
        set_circuit_state("peer-1-circuit", "unknown");
    }

    #[test]
    fn test_record_circuit_rejection() {
        record_circuit_rejection("peer-1-circuit");
    }

    #[test]
    fn test_record_batch_flush() {
        // Normal batch
        record_batch_flush("peer-1", 100, 80, 10, 5, 5, Duration::from_millis(50));
        // Zero errors
        record_batch_flush("peer-1", 50, 50, 0, 0, 0, Duration::from_millis(10));
        // Empty batch
        record_batch_flush("peer-1", 0, 0, 0, 0, 0, Duration::ZERO);
    }

    #[test]
    fn test_record_batch_dedup() {
        // Normal dedup
        record_batch_dedup("peer-1", 100, 80);
        // No dedup (same count)
        record_batch_dedup("peer-1", 50, 50);
        // Edge case: after > before (shouldn't happen but should handle)
        record_batch_dedup("peer-1", 10, 20);
    }

    #[test]
    fn test_record_stream_trimmed() {
        record_stream_trimmed("peer-1");
    }

    #[test]
    fn test_record_stream_read() {
        record_stream_read("peer-1", 100, Duration::from_millis(50));
        record_stream_read("peer-1", 0, Duration::from_millis(5));
    }

    #[test]
    fn test_record_repair_cycle_complete() {
        // Normal repair
        record_repair_cycle_complete(5, 4, 100, 90, 10, Duration::from_secs(30));
        // No errors
        record_repair_cycle_complete(3, 3, 0, 0, 0, Duration::from_secs(5));
        // All errors
        record_repair_cycle_complete(1, 0, 50, 0, 50, Duration::from_secs(60));
    }

    #[test]
    fn test_record_merkle_divergence() {
        record_merkle_divergence("peer-1");
        record_merkle_divergence("peer-2");
    }

    #[test]
    fn test_record_slo_violation() {
        record_slo_violation("peer-1", "stream_read", 150);
        record_slo_violation("peer-1", "peer_op", 500);
        record_slo_violation("peer-1", "batch_flush", 1000);
    }

    #[test]
    fn test_set_replication_lag_slo_under_threshold() {
        // Under threshold - no violation
        set_replication_lag_slo("peer-1", 50, 100);
    }

    #[test]
    fn test_set_replication_lag_slo_over_threshold() {
        // Over threshold - violation
        set_replication_lag_slo("peer-1", 150, 100);
    }

    #[test]
    fn test_set_replication_lag_slo_at_threshold() {
        // At threshold - no violation (not > threshold)
        set_replication_lag_slo("peer-1", 100, 100);
    }
}
