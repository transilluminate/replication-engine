//! Property-based tests using proptest.
//!
//! These tests verify invariants that should hold for all inputs,
//! helping catch edge cases that unit tests might miss.

use proptest::prelude::*;
use replication_engine::stream::{compare_stream_ids, maybe_decompress, parse_stream_id_timestamp};

// =============================================================================
// Stream ID Comparison Properties
// =============================================================================

proptest! {
    /// Stream ID comparison is reflexive: a == a
    #[test]
    fn stream_id_comparison_reflexive(ts in 0u64..u64::MAX, seq in 0u64..1000u64) {
        let id = format!("{}-{}", ts, seq);
        prop_assert_eq!(compare_stream_ids(&id, &id), std::cmp::Ordering::Equal);
    }

    /// Stream ID comparison is antisymmetric: if a < b then b > a
    #[test]
    fn stream_id_comparison_antisymmetric(
        ts1 in 0u64..1_000_000_000_000u64,
        seq1 in 0u64..1000u64,
        ts2 in 0u64..1_000_000_000_000u64,
        seq2 in 0u64..1000u64,
    ) {
        let id1 = format!("{}-{}", ts1, seq1);
        let id2 = format!("{}-{}", ts2, seq2);
        
        let cmp1 = compare_stream_ids(&id1, &id2);
        let cmp2 = compare_stream_ids(&id2, &id1);
        
        prop_assert_eq!(cmp1.reverse(), cmp2);
    }

    /// Stream ID comparison is transitive: if a < b and b < c then a < c
    #[test]
    fn stream_id_comparison_transitive(
        ts1 in 0u64..1_000_000u64,
        seq1 in 0u64..100u64,
        ts2 in 0u64..1_000_000u64,
        seq2 in 0u64..100u64,
        ts3 in 0u64..1_000_000u64,
        seq3 in 0u64..100u64,
    ) {
        let id1 = format!("{}-{}", ts1, seq1);
        let id2 = format!("{}-{}", ts2, seq2);
        let id3 = format!("{}-{}", ts3, seq3);
        
        let cmp12 = compare_stream_ids(&id1, &id2);
        let cmp23 = compare_stream_ids(&id2, &id3);
        let cmp13 = compare_stream_ids(&id1, &id3);
        
        if cmp12 == std::cmp::Ordering::Less && cmp23 == std::cmp::Ordering::Less {
            prop_assert_eq!(cmp13, std::cmp::Ordering::Less);
        }
        if cmp12 == std::cmp::Ordering::Greater && cmp23 == std::cmp::Ordering::Greater {
            prop_assert_eq!(cmp13, std::cmp::Ordering::Greater);
        }
    }

    /// Higher timestamp always means greater ID (regardless of sequence)
    #[test]
    fn stream_id_timestamp_dominates(
        ts1 in 0u64..1_000_000_000_000u64,
        ts2 in 0u64..1_000_000_000_000u64,
        seq1 in 0u64..1000u64,
        seq2 in 0u64..1000u64,
    ) {
        prop_assume!(ts1 != ts2);
        
        let id1 = format!("{}-{}", ts1, seq1);
        let id2 = format!("{}-{}", ts2, seq2);
        
        let cmp = compare_stream_ids(&id1, &id2);
        
        if ts1 < ts2 {
            prop_assert_eq!(cmp, std::cmp::Ordering::Less);
        } else {
            prop_assert_eq!(cmp, std::cmp::Ordering::Greater);
        }
    }

    /// Timestamp parsing extracts correct timestamp
    #[test]
    fn stream_id_timestamp_parsing(ts in 0u64..u64::MAX, seq in 0u64..1000u64) {
        let id = format!("{}-{}", ts, seq);
        let parsed = parse_stream_id_timestamp(&id);
        prop_assert_eq!(parsed, Some(ts));
    }
}

// =============================================================================
// Decompression Properties
// =============================================================================

proptest! {
    /// Uncompressed data passes through unchanged
    #[test]
    fn decompress_passthrough_non_zstd(data in prop::collection::vec(any::<u8>(), 0..1000)) {
        // Ensure data doesn't accidentally start with zstd magic
        let mut safe_data = data;
        if safe_data.len() >= 4 && safe_data[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
            safe_data[0] = 0x00; // Break the magic
        }
        
        let result = maybe_decompress(&safe_data);
        prop_assert!(result.is_ok());
        prop_assert_eq!(result.unwrap(), safe_data);
    }

    /// Valid zstd roundtrips correctly
    #[test]
    fn decompress_zstd_roundtrip(data in prop::collection::vec(any::<u8>(), 1..10000)) {
        // Compress
        let compressed = zstd::encode_all(&data[..], 3);
        prop_assume!(compressed.is_ok());
        let compressed = compressed.unwrap();
        
        // Decompress
        let result = maybe_decompress(&compressed);
        prop_assert!(result.is_ok());
        prop_assert_eq!(result.unwrap(), data);
    }

    /// Corrupted zstd data returns an error (not a panic)
    #[test]
    fn decompress_corrupted_zstd_no_panic(
        corrupt_bytes in prop::collection::vec(any::<u8>(), 4..100)
    ) {
        // Force zstd magic header with random garbage after
        let mut data = vec![0x28, 0xB5, 0x2F, 0xFD];
        data.extend(corrupt_bytes);
        
        // Should not panic - either Ok or Err
        let result = maybe_decompress(&data);
        // We just care it doesn't panic
        let _ = result;
    }
}

// =============================================================================
// AIMD Batch Sizing Properties
// =============================================================================

/// Simulates AIMD (Additive Increase, Multiplicative Decrease) batch sizing.
/// This mirrors the algorithm in hot_path.rs
fn aimd_simulate(
    initial_size: usize,
    min_size: usize,
    max_size: usize,
    additive_increase: usize,
    multiplicative_decrease: f64,
    events: &[(usize, bool)], // (events_received, success)
) -> Vec<usize> {
    let mut sizes = vec![initial_size];
    let mut current = initial_size;
    
    for (received, success) in events {
        if *success {
            // If we got a full batch, increase (we might be behind)
            if *received >= current {
                current = (current + additive_increase).min(max_size);
            }
            // If batch was small, we're caught up - stay same or decrease slowly
        } else {
            // On error, back off multiplicatively
            current = ((current as f64 * multiplicative_decrease) as usize).max(min_size);
        }
        sizes.push(current);
    }
    
    sizes
}

proptest! {
    /// AIMD batch size always stays within bounds
    #[test]
    fn aimd_bounds_invariant(
        initial in 10usize..1000,
        events in prop::collection::vec((0usize..500, any::<bool>()), 1..100),
    ) {
        let min_size = 10;
        let max_size = 1000;
        let additive_increase = 10;
        let multiplicative_decrease = 0.5;
        
        let initial = initial.clamp(min_size, max_size);
        
        let sizes = aimd_simulate(
            initial,
            min_size,
            max_size,
            additive_increase,
            multiplicative_decrease,
            &events,
        );
        
        for size in sizes {
            prop_assert!(size >= min_size, "Size {} below min {}", size, min_size);
            prop_assert!(size <= max_size, "Size {} above max {}", size, max_size);
        }
    }

    /// AIMD converges: after enough failures, size drops to minimum
    #[test]
    fn aimd_converges_to_min_on_failures(initial in 100usize..1000) {
        let min_size = 10;
        let max_size = 1000;
        
        // 20 consecutive failures should bring any size down to min
        let failures: Vec<(usize, bool)> = (0..20).map(|_| (0, false)).collect();
        
        let sizes = aimd_simulate(
            initial.min(max_size),
            min_size,
            max_size,
            10,
            0.5,
            &failures,
        );
        
        prop_assert_eq!(*sizes.last().unwrap(), min_size);
    }

    /// AIMD reaches max after enough successes with full batches
    #[test]
    fn aimd_reaches_max_on_successes(initial in 10usize..500) {
        let min_size = 10;
        let max_size = 1000;
        let additive_increase = 10;
        
        // Enough successes with full batches to reach max
        // (max - initial) / additive_increase iterations needed
        let needed = (max_size - initial) / additive_increase + 1;
        let successes: Vec<(usize, bool)> = (0..needed).map(|i| {
            // Always return "full batch" (received >= current)
            (initial + i * additive_increase, true)
        }).collect();
        
        let sizes = aimd_simulate(
            initial,
            min_size,
            max_size,
            additive_increase,
            0.5,
            &successes,
        );
        
        prop_assert_eq!(*sizes.last().unwrap(), max_size);
    }
}

// =============================================================================
// Content Hash Properties
// =============================================================================

fn compute_sha256(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}

proptest! {
    /// Same data always produces same hash
    #[test]
    fn hash_deterministic(data in prop::collection::vec(any::<u8>(), 0..10000)) {
        let hash1 = compute_sha256(&data);
        let hash2 = compute_sha256(&data);
        prop_assert_eq!(hash1, hash2);
    }

    /// Different data (almost always) produces different hash
    #[test]
    fn hash_collision_resistant(
        data1 in prop::collection::vec(any::<u8>(), 1..1000),
        data2 in prop::collection::vec(any::<u8>(), 1..1000),
    ) {
        prop_assume!(data1 != data2);
        
        let hash1 = compute_sha256(&data1);
        let hash2 = compute_sha256(&data2);
        
        // SHA256 collision probability is astronomically low
        prop_assert_ne!(hash1, hash2);
    }

    /// Hash is always 64 hex characters
    #[test]
    fn hash_length_invariant(data in prop::collection::vec(any::<u8>(), 0..10000)) {
        let hash = compute_sha256(&data);
        prop_assert_eq!(hash.len(), 64);
        prop_assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }
}

// =============================================================================
// Circuit Breaker State Machine Properties
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Simplified circuit breaker state machine for property testing
struct CircuitSim {
    state: CircuitState,
    failures: u32,
    threshold: u32,
}

impl CircuitSim {
    fn new(threshold: u32) -> Self {
        Self {
            state: CircuitState::Closed,
            failures: 0,
            threshold,
        }
    }
    
    fn record_success(&mut self) {
        self.failures = 0;
        self.state = CircuitState::Closed;
    }
    
    fn record_failure(&mut self) {
        self.failures += 1;
        if self.failures >= self.threshold {
            self.state = CircuitState::Open;
        }
    }
    
    fn try_half_open(&mut self) {
        if self.state == CircuitState::Open {
            self.state = CircuitState::HalfOpen;
        }
    }
}

proptest! {
    /// Circuit never opens before threshold failures
    #[test]
    fn circuit_respects_threshold(threshold in 1u32..20) {
        let mut circuit = CircuitSim::new(threshold);
        
        for _ in 0..(threshold - 1) {
            circuit.record_failure();
            prop_assert_ne!(circuit.state, CircuitState::Open);
        }
        
        circuit.record_failure();
        prop_assert_eq!(circuit.state, CircuitState::Open);
    }

    /// Single success always closes circuit
    #[test]
    fn circuit_success_always_closes(threshold in 1u32..20, failures in 0u32..100) {
        let mut circuit = CircuitSim::new(threshold);
        
        for _ in 0..failures {
            circuit.record_failure();
        }
        
        circuit.try_half_open();
        circuit.record_success();
        
        prop_assert_eq!(circuit.state, CircuitState::Closed);
        prop_assert_eq!(circuit.failures, 0);
    }
}
