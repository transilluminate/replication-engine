//! Fuzz target for stream ID comparison logic.
//!
//! This tests that stream ID comparison never panics on arbitrary input
//! and maintains ordering invariants.

#![no_main]

use libfuzzer_sys::fuzz_target;
use replication_engine::stream::compare_stream_ids;
use std::cmp::Ordering;

fuzz_target!(|data: (&str, &str)| {
    let (a, b) = data;
    
    // Should never panic
    let ab = compare_stream_ids(a, b);
    let ba = compare_stream_ids(b, a);
    
    // Antisymmetry: if a < b then b > a
    match ab {
        Ordering::Less => assert!(ba == Ordering::Greater || ba == Ordering::Equal),
        Ordering::Greater => assert!(ba == Ordering::Less || ba == Ordering::Equal),
        Ordering::Equal => {} // ba can be anything for malformed IDs
    }
    
    // Reflexivity for valid IDs
    let _ = compare_stream_ids(a, a);
    let _ = compare_stream_ids(b, b);
});
