//! Fuzz target for decompression logic.
//!
//! This tests that `maybe_decompress` never panics on arbitrary input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use replication_engine::stream::maybe_decompress;

fuzz_target!(|data: &[u8]| {
    // Just call maybe_decompress - it should never panic
    let _ = maybe_decompress(data);
});
