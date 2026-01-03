//! Fuzz target for lag calculation.
//!
//! This tests that `calculate_lag_ms` and `parse_stream_id_timestamp`
//! never panic on arbitrary input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use replication_engine::stream::{calculate_lag_ms, parse_stream_id_timestamp};

fuzz_target!(|data: (&str, &str)| {
    let (cursor, latest) = data;
    
    // Should never panic
    let _ = parse_stream_id_timestamp(cursor);
    let _ = parse_stream_id_timestamp(latest);
    let _ = calculate_lag_ms(cursor, latest);
});
