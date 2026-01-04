# Replication Engine

Mesh replication agent for `sync-engine` nodes with bidirectional data synchronization

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)

## Philosophy: Two-Path Replication

replication-engine provides reliable data sync across a cluster of [sync-engine](https://github.com/transilluminate/sync-engine/) instances using two complementary strategies:
- **Hot Path** → Real-time CDC stream tailing for low-latency sync
- **Cold Path** → Periodic Merkle tree comparison for guaranteed consistency

This dual approach ensures both speed (hot path catches changes immediately) and reliability (cold path catches anything missed due to network issues, stream trimming, or restarts).

## Architecture

```
Node A                                   Node B
┌─────────────────┐                     ┌─────────────────┐
│   sync-engine   │                     │   sync-engine   │
│   writes to     │                     │                 │
│  __local__:cdc  │                     │                 │
└────────┬────────┘                     └────────▲────────┘
         │                                       │
         │ CDC events                            │ submit()
         ▼                                       │
┌─────────────────┐      tail stream    ┌─────────────────┐
│ replication-eng │◄────────────────────│ replication-eng │
│  (A's instance) │   (via A's Redis)   │  (B's instance) │
└─────────────────┘                     └─────────────────┘
         │                                       │
         │                                       │
         ▼                                       ▼
┌─────────────────┐                     ┌─────────────────┐
│ SQLite Cursors  │                     │ SQLite Cursors  │
│  (crash-safe)   │                     │  (crash-safe)   │
└─────────────────┘                     └─────────────────┘
```

## Features

- **Hot Path (Real-time)**: XREAD-based CDC stream tailing with adaptive batch sizing
- **Cold Path (Anti-entropy)**: Merkle tree comparison with parallel drill-down
- **Content Deduplication**: Skip items already present via content hash comparison
- **Crash Recovery**: SQLite WAL-mode cursor persistence survives Redis restarts
- **Circuit Breakers**: Protect peers from cascade failures during outages
- **Backpressure Handling**: Pauses ingestion when sync-engine is under memory pressure
- **Adaptive Batching**: AIMD-style batch sizing based on replication lag
- **Graceful Shutdown**: Drain in-flight batches before exit
- **Prometheus Metrics**: Comprehensive observability for operations
- **W3C Trace Context**: Propagate trace IDs from CDC events for distributed tracing
- **TLS Support**: Use `rediss://` URLs for encrypted peer connections

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
replication-engine = "0.1.2"
tokio = { version = "1", features = ["full"] }
```

Basic usage:

```rust
use replication_engine::{ReplicationEngine, ReplicationConfig, PeerConfig};
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    let config = ReplicationConfig {
        local_node_id: "uk.node.london-1".into(),
        peers: vec![
            PeerConfig::for_testing(
                "uk.node.manchester-1",
                "redis://peer1:6379"
            ),
        ],
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = ReplicationEngine::new(config, rx);
    engine.start().await.expect("Failed to start");

    // Engine runs until shutdown signal
    tokio::signal::ctrl_c().await.unwrap();
    engine.shutdown().await;
}
```

## Hot Path (Real-time Replication)

The hot path tails each peer's CDC stream for low-latency replication:

```
┌──────────────────────────────────────────────────────────────┐
│                         Hot Path                             │
│  XREAD __local__:cdc → Parse CDC → Dedup → submit() → Cursor │
└──────────────────────────────────────────────────────────────┘
```

**Flow:**
1. `XREAD BLOCK 5000` on peer's `__local__:cdc` stream
2. Parse CDC events (PUT with zstd-compressed payload, DELETE)
3. Batch deduplicate using `is_current(key, hash)` check
4. Apply to local sync-engine via `submit()`/`delete()`
5. Persist cursor to SQLite after each batch

**Adaptive Batch Sizing:**
When enabled, uses AIMD (Additive Increase, Multiplicative Decrease):
- Empty reads → additive increase (catching up)
- Full batches → multiplicative decrease (falling behind)

## Cold Path (Anti-entropy)

The cold path periodically verifies consistency via Merkle tree comparison:

```
┌──────────────────────────────────────────────────────────────┐
│                        Cold Path                             │
│  Compare roots → Drill down → Fetch divergent → Submit       │
└──────────────────────────────────────────────────────────────┘
```

**Flow:**
1. Fetch Merkle root from each peer
2. If roots differ, drill down to find divergent branches
3. Fetch items from divergent leaves
4. Apply missing items to local sync-engine

**Optimizations:**
- Parallel drill-down (up to 8 concurrent branches)
- Cached Merkle roots (5s TTL)
- Exponential backoff on repeated failures
- Configurable max items per cycle

## Configuration

All configuration options with defaults:

| Option | Default | Description |
|--------|---------|-------------|
| **Identity** |||
| `local_node_id` | Required | This node's unique identifier |
| **Hot Path** |||
| `settings.hot_path.enabled` | `true` | Enable CDC stream tailing |
| `settings.hot_path.batch_size` | `100` | Initial XREAD batch size |
| `settings.hot_path.block_timeout` | `"5s"` | XREAD block timeout |
| `settings.hot_path.adaptive_batch_size` | `false` | Enable AIMD batch sizing |
| `settings.hot_path.min_batch_size` | `50` | Minimum batch size (AIMD) |
| `settings.hot_path.max_batch_size` | `1000` | Maximum batch size (AIMD) |
| `settings.hot_path.rate_limit_enabled` | `false` | Enable rate limiting (thundering herd prevention) |
| `settings.hot_path.rate_limit_per_sec` | `10000` | Max events/second (sustained rate) |
| `settings.hot_path.rate_limit_burst` | `1000` | Max burst size above sustained rate |
| **Cold Path** |||
| `settings.cold_path.enabled` | `true` | Enable Merkle anti-entropy |
| `settings.cold_path.interval_sec` | `60` | Seconds between repair cycles |
| `settings.cold_path.max_items_per_cycle` | `1000` | Max items to repair per cycle |
| `settings.cold_path.backoff_base_sec` | `5` | Base backoff on failure |
| `settings.cold_path.backoff_max_sec` | `300` | Maximum backoff (5 min) |
| **Peer Health** |||
| `settings.peer_health.enabled` | `true` | Enable idle peer health checks |
| `settings.peer_health.ping_interval_sec` | `30` | Seconds between pings |
| `settings.peer_health.idle_threshold_sec` | `60` | Idle time before ping |
| **SLO Thresholds** |||
| `settings.slo.max_stream_read_latency_ms` | `100` | Warn if XREAD exceeds (ms) |
| `settings.slo.max_peer_op_latency_ms` | `500` | Warn if peer op exceeds (ms) |
| `settings.slo.max_batch_flush_latency_ms` | `200` | Warn if flush exceeds (ms) |
| `settings.slo.max_replication_lag_sec` | `30` | Warn if lag exceeds (sec) |
| **Peers** |||
| `peers[].node_id` | Required | Peer's unique node ID |
| `peers[].redis_url` | Required | Redis URL for CDC stream |
| `peers[].priority` | `0` | Sync priority (lower = higher) |
| `peers[].circuit_failure_threshold` | `5` | Failures before circuit opens |
| `peers[].circuit_reset_timeout_sec` | `30` | Seconds before retry |
| **Cursor Persistence** |||
| `cursor.sqlite_path` | `"replication_cursors.db"` | SQLite database path |
| `cursor.wal_mode` | `true` | Use WAL mode |

## YAML Configuration Example

```yaml
replication:
  local_node_id: "uk.node.london-1"
  
  settings:
    hot_path:
      enabled: true
      batch_size: 100
      block_timeout: "5s"
      adaptive_batch_size: true
      min_batch_size: 50
      max_batch_size: 1000
      rate_limit_enabled: true
      rate_limit_per_sec: 10000
      rate_limit_burst: 1000
    
    cold_path:
      enabled: true
      interval_sec: 60
      max_items_per_cycle: 1000
    
    peer_health:
      enabled: true
      ping_interval_sec: 30
    
    slo:
      max_replication_lag_sec: 60
  
  peers:
    - node_id: "uk.node.manchester-1"
      redis_url: "redis://peer1:6379"        # or rediss:// for TLS
      priority: 0
      circuit_failure_threshold: 5
      circuit_reset_timeout_sec: 30
    
    - node_id: "uk.node.edinburgh-1"
      redis_url: "rediss://user:pass@peer2:6379"  # TLS + auth
      priority: 1
  
  cursor:
    sqlite_path: "/var/lib/redsqrl/replication_cursors.db"
    wal_mode: true
```

## Testing

Comprehensive test suite with 200+ tests covering unit, property-based, chaos, and integration testing:

| Test Suite | Count | Description |
|------------|-------|-------------|
| **Unit Tests** | 230 ✅ | Fast, no external deps |
| **Property Tests** | 16 ✅ | Proptest fuzzing for invariants |
| **Chaos Tests** | 11 ✅ | Failure injection, corruption handling |
| **Integration Tests** | 44 ✅ | Real Redis via testcontainers |
| **Total** | **280+** ✅ | ~85% code coverage |

### Fuzz Testing

Three fuzz targets with ~4 million runs and zero crashes:

| Target | Runs | Description |
|--------|------|-------------|
| `fuzz_decompress` | 321K | Arbitrary byte decompression |
| `fuzz_stream_id` | 1.7M | Stream ID comparison |
| `fuzz_lag_calc` | 1.86M | Lag calculation |

### Running Tests

```bash
# Unit tests (fast, no Docker)
cargo test --lib

# Property-based tests
cargo test --test property_tests

# Chaos tests (no Docker)
cargo test --test chaos_tests

# Integration tests (requires Docker/OrbStack)
cargo test --test integration -- --ignored

# All tests
cargo test -- --include-ignored

# Fuzz testing (requires nightly)
cargo +nightly fuzz run fuzz_decompress -- -max_total_time=60

# Coverage
cargo llvm-cov --all-features --lib --tests -- --include-ignored
```

## Metrics

Prometheus-style metrics exposed for operational visibility:

| Metric | Type | Description |
|--------|------|-------------|
| **Hot Path** |||
| `replication_cdc_events_read_total` | Counter | CDC events read from peers |
| `replication_cdc_events_applied_total` | Counter | Events applied to sync-engine |
| `replication_cdc_events_deduped_total` | Counter | Events skipped (already current) |
| `replication_batch_flush_total` | Counter | Batch flushes to sync-engine |
| `replication_batch_flush_duration_seconds` | Histogram | Flush latency |
| `replication_lag_ms` | Gauge | Cursor lag behind stream head |
| `replication_lag_events` | Gauge | Estimated events behind |
| `replication_backpressure_pauses_total` | Counter | Ingestion pauses due to sync-engine pressure |
| **Cold Path** |||
| `replication_repair_cycles_total` | Counter | Merkle repair cycles run |
| `replication_repair_items_fetched_total` | Counter | Items fetched from peers |
| `replication_repair_items_submitted_total` | Counter | Items submitted to sync-engine |
| `replication_merkle_divergences_total` | Counter | Divergent branches found |
| **Peer Health** |||
| `replication_peer_connected` | Gauge | 1 if connected, 0 if not |
| `replication_peer_circuit_state` | Gauge | 0=closed, 1=half-open, 2=open |
| `replication_peer_latency_seconds` | Histogram | Peer operation latency |
| **Engine State** |||
| `replication_engine_state` | Gauge | 0=stopped, 1=starting, 2=running, 3=stopping |

## Project Structure

```
src/
├── lib.rs              # Public API exports
├── config.rs           # Configuration types
├── error.rs            # Error types
├── sync_engine.rs      # SyncEngineRef trait + SyncItem integration
├── cursor.rs           # SQLite cursor persistence
├── peer.rs             # Peer connection management
├── stream.rs           # CDC stream parsing + decompression
├── batch.rs            # Batch processor with deduplication
├── circuit_breaker.rs  # Circuit breaker pattern
├── resilience.rs       # Retry, rate limiting, bulkhead
├── metrics.rs          # Prometheus metrics
└── coordinator/
    ├── mod.rs          # Main ReplicationEngine
    ├── types.rs        # State types
    ├── hot_path.rs     # Stream tailing task
    └── cold_path.rs    # Merkle repair task

tests/
├── property_tests.rs   # Proptest-based property testing
├── chaos_tests.rs      # Failure injection, corruption handling
├── integration.rs      # Testcontainers integration tests
└── common/
    ├── mod.rs          # Test utilities
    ├── containers.rs   # Redis testcontainer helpers
    └── mock_sync.rs    # Mock SyncEngineRef for testing

fuzz/
└── fuzz_targets/
    ├── fuzz_decompress.rs  # Arbitrary byte decompression
    ├── fuzz_stream_id.rs   # Stream ID comparison
    └── fuzz_lag_calc.rs    # Lag calculation
```

## Integration with redsqrl-daemon

The replication-engine is designed to be instantiated by a parent daemon:

```rust
// In daemon startup:
let sync_engine = SyncEngine::new(sync_config, sync_config_rx);
sync_engine.start().await?;

let replication_engine = ReplicationEngine::with_sync_engine(
    replication_config,
    replication_config_rx,
    Arc::new(sync_engine),
);
replication_engine.start().await?;

// Graceful shutdown
replication_engine.shutdown().await;
sync_engine.shutdown().await;
```

## License

[GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0)

For commercial licensing options, contact: adrian.j.robinson@gmail.com
