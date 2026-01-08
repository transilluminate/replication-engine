// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Redis Stream consumer for CDC events.
//!
//! Tails a peer's `cdc` stream and parses events.
//!
//! # Stream Trimming
//!
//! Redis streams can be trimmed via `MAXLEN` or `MINID` to bound memory.
//! If our cursor points to an entry older than the oldest in the stream,
//! we've "fallen behind" and missed events. This module detects this and
//! returns a `StreamTrimmed` result so callers can:
//! - Log a warning (potential data gap)
//! - Reset cursor to oldest available entry
//! - Emit a metric for alerting
//!
//! # Content Hash Validation
//!
//! When a PUT event includes a `hash` field, we verify that the SHA256
//! of the decompressed data matches. This detects corruption from:
//! - Network bit flips
//! - Compression bugs
//! - Malicious peers

use crate::error::{ReplicationError, Result};
use futures::future::join_all;
use redis::aio::ConnectionManager;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;
use tracing::{trace, warn};

/// zstd magic bytes for decompression detection
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// CDC operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOp {
    Put,
    Delete,
}

impl CdcOp {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "PUT" => Some(CdcOp::Put),
            "DEL" | "DELETE" => Some(CdcOp::Delete),
            _ => None,
        }
    }
}

/// Metadata from a CDC PUT event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcMeta {
    pub content_type: String,
    pub version: u64,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_parent: Option<String>,
}

/// A parsed CDC event from the stream.
#[derive(Debug, Clone)]
pub struct CdcEvent {
    /// Stream entry ID (e.g., "1234567890123-0")
    pub stream_id: String,
    /// Operation type
    pub op: CdcOp,
    /// Object ID (key)
    pub key: String,
    /// Content hash for deduplication (only for PUT)
    pub hash: Option<String>,
    /// Decompressed data payload (only for PUT)
    pub data: Option<Vec<u8>>,
    /// Parsed metadata (only for PUT)
    pub meta: Option<CdcMeta>,
}

impl CdcEvent {
    /// Check if this is a PUT operation.
    pub fn is_put(&self) -> bool {
        self.op == CdcOp::Put
    }

    /// Check if this is a DELETE operation.
    pub fn is_delete(&self) -> bool {
        self.op == CdcOp::Delete
    }

    /// Get the trace parent from metadata, if present.
    /// 
    /// Format: W3C Trace Context (e.g., "00-traceid-spanid-flags")
    pub fn trace_parent(&self) -> Option<&str> {
        self.meta.as_ref().and_then(|m| m.trace_parent.as_deref())
    }

    /// Extract trace ID from trace_parent (first 32 hex chars after version).
    pub fn trace_id(&self) -> Option<&str> {
        self.trace_parent().and_then(|tp| {
            let parts: Vec<&str> = tp.split('-').collect();
            if parts.len() >= 2 {
                Some(parts[1])
            } else {
                None
            }
        })
    }

    /// Extract span ID from trace_parent (16 hex chars after trace ID).
    pub fn parent_span_id(&self) -> Option<&str> {
        self.trace_parent().and_then(|tp| {
            let parts: Vec<&str> = tp.split('-').collect();
            if parts.len() >= 3 {
                Some(parts[2])
            } else {
                None
            }
        })
    }
}

/// Result of reading from a stream, accounting for trim scenarios.
#[derive(Debug)]
pub enum ReadResult {
    /// Successfully read events (may be empty if no new events).
    Events(Vec<CdcEvent>),

    /// Stream was trimmed past our cursor.
    ///
    /// This means we've missed events and should:
    /// 1. Log a warning (potential data gap)
    /// 2. Reset cursor to `oldest_id`
    /// 3. Emit a metric for alerting
    ///
    /// The cold path (Merkle repair) will eventually fix any gaps.
    StreamTrimmed {
        /// Our cursor that's now invalid
        cursor: String,
        /// The oldest available entry in the stream
        oldest_id: String,
    },

    /// Stream is empty (no entries at all).
    Empty,
}

impl ReadResult {
    /// Get events if this is a successful read, empty vec otherwise.
    pub fn events(self) -> Vec<CdcEvent> {
        match self {
            ReadResult::Events(events) => events,
            _ => Vec::new(),
        }
    }

    /// Check if the stream was trimmed.
    pub fn is_trimmed(&self) -> bool {
        matches!(self, ReadResult::StreamTrimmed { .. })
    }
}

/// Stream tailer for a single peer's CDC stream.
pub struct StreamTailer {
    /// Peer node ID (for logging)
    peer_id: String,
    /// Stream key to tail
    stream_key: String,
    /// Block timeout for XREAD
    block_timeout: Duration,
    /// Max entries per read
    batch_size: usize,
}

impl StreamTailer {
    /// Create a new stream tailer.
    pub fn new(
        peer_id: String,
        stream_key: String,
        block_timeout: Duration,
        batch_size: usize,
    ) -> Self {
        Self {
            peer_id,
            stream_key,
            block_timeout,
            batch_size,
        }
    }

    /// Get the current batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Update the batch size (for adaptive sizing).
    pub fn set_batch_size(&mut self, size: usize) {
        self.batch_size = size;
    }

    /// Get the oldest entry ID in the stream, if any.
    ///
    /// Returns `None` if the stream is empty or doesn't exist.
    pub async fn get_oldest_id(&self, conn: &mut ConnectionManager) -> Result<Option<String>> {
        // XRANGE key - + COUNT 1 returns the oldest entry
        let result: Vec<(String, HashMap<String, redis::Value>)> = redis::cmd("XRANGE")
            .arg(&self.stream_key)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(1)
            .query_async(conn)
            .await
            .map_err(|e| ReplicationError::redis("XRANGE", e))?;

        Ok(result.first().map(|(id, _)| id.clone()))
    }

    /// Get the latest (newest) entry ID in the stream, if any.
    ///
    /// Returns `None` if the stream is empty or doesn't exist.
    pub async fn get_latest_id(&self, conn: &mut ConnectionManager) -> Result<Option<String>> {
        // XREVRANGE key + - COUNT 1 returns the newest entry
        let result: Vec<(String, HashMap<String, redis::Value>)> = redis::cmd("XREVRANGE")
            .arg(&self.stream_key)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(conn)
            .await
            .map_err(|e| ReplicationError::redis("XREVRANGE", e))?;

        Ok(result.first().map(|(id, _)| id.clone()))
    }

    /// Get the total number of entries in the stream.
    ///
    /// Returns 0 if the stream doesn't exist.
    pub async fn get_stream_length(&self, conn: &mut ConnectionManager) -> Result<u64> {
        let len: u64 = redis::cmd("XLEN")
            .arg(&self.stream_key)
            .query_async(conn)
            .await
            .map_err(|e| ReplicationError::redis("XLEN", e))?;
        Ok(len)
    }

    /// Check if a cursor is still valid (not older than the oldest stream entry).
    ///
    /// Returns the oldest ID if the cursor is invalid, `None` if cursor is valid.
    pub async fn check_cursor_valid(
        &self,
        conn: &mut ConnectionManager,
        cursor: &str,
    ) -> Result<Option<String>> {
        // "0" is always valid (start from beginning)
        if cursor == "0" {
            return Ok(None);
        }

        let oldest = self.get_oldest_id(conn).await?;

        match oldest {
            None => Ok(None), // Empty stream, cursor is valid
            Some(oldest_id) => {
                if compare_stream_ids(cursor, &oldest_id) == std::cmp::Ordering::Less {
                    // Cursor is older than oldest entry - stream was trimmed
                    Ok(Some(oldest_id))
                } else {
                    Ok(None) // Cursor is valid
                }
            }
        }
    }

    /// Read events from the stream with trim detection.
    ///
    /// This is the preferred method for production use. It detects when the
    /// stream has been trimmed past our cursor and returns `ReadResult::StreamTrimmed`
    /// so callers can handle the gap appropriately.
    pub async fn read_events_checked(
        &self,
        conn: &mut ConnectionManager,
        cursor: &str,
    ) -> Result<ReadResult> {
        // First check if cursor is still valid
        if let Some(oldest_id) = self.check_cursor_valid(conn, cursor).await? {
            warn!(
                peer_id = %self.peer_id,
                cursor = %cursor,
                oldest_id = %oldest_id,
                "Stream was trimmed past our cursor - potential data gap!"
            );
            return Ok(ReadResult::StreamTrimmed {
                cursor: cursor.to_string(),
                oldest_id,
            });
        }

        // Check if stream is empty
        if cursor == "0" && self.get_oldest_id(conn).await?.is_none() {
            return Ok(ReadResult::Empty);
        }

        // Normal read
        let events = self.read_events(conn, cursor).await?;
        Ok(ReadResult::Events(events))
    }

    /// Read events from the stream starting after `cursor`.
    ///
    /// Returns a vector of parsed events. Empty vector means no new events
    /// (timeout or stream empty).
    ///
    /// The cursor should be the last successfully processed stream ID,
    /// or "0" to start from the beginning.
    pub async fn read_events(
        &self,
        conn: &mut ConnectionManager,
        cursor: &str,
    ) -> Result<Vec<CdcEvent>> {
        let opts = StreamReadOptions::default()
            .block(self.block_timeout.as_millis() as usize)
            .count(self.batch_size);

        // XREAD BLOCK timeout COUNT batch STREAMS key cursor
        let reply: StreamReadReply = conn
            .xread_options(&[&self.stream_key], &[cursor], &opts)
            .await
            .map_err(|e| ReplicationError::redis("XREAD", e))?;

        let mut events = Vec::new();

        for stream_key in reply.keys {
            for entry in stream_key.ids {
                match self.parse_entry(&entry.id, &entry.map) {
                    Ok(event) => {
                        trace!(
                            peer_id = %self.peer_id,
                            stream_id = %event.stream_id,
                            op = ?event.op,
                            key = %event.key,
                            "Parsed CDC event"
                        );
                        events.push(event);
                    }
                    Err(e) => {
                        warn!(
                            peer_id = %self.peer_id,
                            stream_id = %entry.id,
                            error = %e,
                            "Failed to parse stream entry, skipping"
                        );
                        // Continue processing other entries
                    }
                }
            }
        }

        if !events.is_empty() {
            trace!(
                peer_id = %self.peer_id,
                count = events.len(),
                first_id = %events.first().map(|e| e.stream_id.as_str()).unwrap_or(""),
                last_id = %events.last().map(|e| e.stream_id.as_str()).unwrap_or(""),
                "Read CDC events"
            );
        }

        Ok(events)
    }

    /// Read a range of events using XRANGE (non-blocking).
    ///
    /// This is faster than XREAD for catchup scenarios because:
    /// 1. No blocking timeout overhead
    /// 2. Simpler command (no consumer group semantics)
    /// 3. Better for bulk reads when we know we're behind
    ///
    /// Use this when catching up, then switch to `read_events` (XREAD) when tailing.
    ///
    /// # Arguments
    /// * `start` - Exclusive start ID (use "0" to start from beginning, or last processed ID)
    /// * `count` - Maximum number of entries to fetch
    ///
    /// # Returns
    /// Vector of parsed events. Empty means we've caught up.
    pub async fn read_events_range(
        &self,
        conn: &mut ConnectionManager,
        start: &str,
        count: usize,
    ) -> Result<Vec<CdcEvent>> {
        // XRANGE uses inclusive start, but we want exclusive (after cursor).
        // Use "(" prefix for exclusive range in Redis 6.2+, or increment the sequence.
        let exclusive_start = if start == "0" {
            "-".to_string() // Start from very beginning
        } else {
            // Use exclusive range syntax: (id means "greater than id"
            format!("({}", start)
        };

        let result: Vec<(String, HashMap<String, redis::Value>)> = redis::cmd("XRANGE")
            .arg(&self.stream_key)
            .arg(&exclusive_start)
            .arg("+")
            .arg("COUNT")
            .arg(count)
            .query_async(conn)
            .await
            .map_err(|e| ReplicationError::redis("XRANGE", e))?;

        // Use parallel decompression for catchup batches (typically larger)
        let events = self.parse_entries_parallel(result).await;

        if !events.is_empty() {
            trace!(
                peer_id = %self.peer_id,
                count = events.len(),
                first_id = %events.first().map(|e| e.stream_id.as_str()).unwrap_or(""),
                last_id = %events.last().map(|e| e.stream_id.as_str()).unwrap_or(""),
                "Read CDC events via XRANGE (catchup mode)"
            );
        }

        Ok(events)
    }

    /// Parse a stream entry into a CdcEvent.
    fn parse_entry(
        &self,
        stream_id: &str,
        fields: &HashMap<String, redis::Value>,
    ) -> Result<CdcEvent> {
        // Extract "op" field
        let op_str = get_string_field(fields, "op")?;
        let op = CdcOp::from_str(&op_str).ok_or_else(|| {
            ReplicationError::StreamParse(format!("Unknown op type: {}", op_str))
        })?;

        // Extract "key" field
        let key = get_string_field(fields, "key")?;

        // For DELETE, we're done
        if op == CdcOp::Delete {
            return Ok(CdcEvent {
                stream_id: stream_id.to_string(),
                op,
                key,
                hash: None,
                data: None,
                meta: None,
            });
        }

        // For PUT, extract additional fields
        let hash = get_string_field(fields, "hash").ok();

        // Data is binary, may be compressed
        let raw_data = get_bytes_field(fields, "data")?;
        let data = maybe_decompress(&raw_data)?;

        // Validate content hash if present (detect corruption)
        if let Some(ref expected_hash) = hash {
            let computed = compute_content_hash(&data);
            if &computed != expected_hash {
                return Err(ReplicationError::StreamParse(format!(
                    "Content hash mismatch for key '{}': expected {}, got {}",
                    key, expected_hash, computed
                )));
            }
        }

        // Meta is JSON string
        let meta = if let Ok(meta_str) = get_string_field(fields, "meta") {
            serde_json::from_str(&meta_str).ok()
        } else {
            None
        };

        Ok(CdcEvent {
            stream_id: stream_id.to_string(),
            op,
            key,
            hash,
            data: Some(data),
            meta,
        })
    }

    /// Parse multiple stream entries in parallel.
    ///
    /// Decompression is CPU-bound, so we offload it to blocking tasks.
    /// This significantly improves throughput when processing batches
    /// of compressed events.
    ///
    /// Failed entries are logged and skipped (same as sequential parsing).
    async fn parse_entries_parallel(
        &self,
        entries: Vec<(String, HashMap<String, redis::Value>)>,
    ) -> Vec<CdcEvent> {
        if entries.is_empty() {
            return Vec::new();
        }

        // For small batches, parse sequentially (avoids spawn overhead)
        if entries.len() <= 2 {
            return entries
                .into_iter()
                .filter_map(|(id, fields)| {
                    match self.parse_entry(&id, &fields) {
                        Ok(event) => Some(event),
                        Err(e) => {
                            warn!(
                                peer_id = %self.peer_id,
                                stream_id = %id,
                                error = %e,
                                "Failed to parse stream entry, skipping"
                            );
                            None
                        }
                    }
                })
                .collect();
        }

        // Split into chunks to limit parallelism
        let peer_id = self.peer_id.clone();
        let futures: Vec<_> = entries
            .into_iter()
            .map(|(stream_id, fields)| {
                tokio::task::spawn_blocking(move || {
                    // Extract fields (non-blocking)
                    let op_str = match get_string_field(&fields, "op") {
                        Ok(s) => s,
                        Err(e) => return Err((stream_id, e)),
                    };
                    let op = match CdcOp::from_str(&op_str) {
                        Some(op) => op,
                        None => {
                            return Err((
                                stream_id,
                                ReplicationError::StreamParse(format!("Unknown op: {}", op_str)),
                            ))
                        }
                    };

                    let key = match get_string_field(&fields, "key") {
                        Ok(k) => k,
                        Err(e) => return Err((stream_id, e)),
                    };

                    if op == CdcOp::Delete {
                        return Ok(CdcEvent {
                            stream_id,
                            op,
                            key,
                            hash: None,
                            data: None,
                            meta: None,
                        });
                    }

                    let hash = get_string_field(&fields, "hash").ok();
                    let raw_data = match get_bytes_field(&fields, "data") {
                        Ok(d) => d,
                        Err(e) => return Err((stream_id, e)),
                    };

                    // This is the CPU-intensive part
                    let data = match maybe_decompress(&raw_data) {
                        Ok(d) => d,
                        Err(e) => return Err((stream_id, e)),
                    };

                    // Validate hash
                    if let Some(ref expected) = hash {
                        let computed = compute_content_hash(&data);
                        if &computed != expected {
                            return Err((
                                stream_id.clone(),
                                ReplicationError::StreamParse(format!(
                                    "Hash mismatch for key '{}': expected {}, got {}",
                                    key, expected, computed
                                )),
                            ));
                        }
                    }

                    let meta = if let Ok(meta_str) = get_string_field(&fields, "meta") {
                        serde_json::from_str(&meta_str).ok()
                    } else {
                        None
                    };

                    Ok(CdcEvent {
                        stream_id,
                        op,
                        key,
                        hash,
                        data: Some(data),
                        meta,
                    })
                })
            })
            .collect();

        // Wait for all tasks, filter successes
        let results = join_all(futures).await;
        let mut events = Vec::with_capacity(results.len());

        for result in results {
            match result {
                Ok(Ok(event)) => {
                    trace!(
                        peer_id = %peer_id,
                        stream_id = %event.stream_id,
                        op = ?event.op,
                        key = %event.key,
                        "Parsed CDC event (parallel)"
                    );
                    events.push(event);
                }
                Ok(Err((stream_id, e))) => {
                    warn!(
                        peer_id = %peer_id,
                        stream_id = %stream_id,
                        error = %e,
                        "Failed to parse stream entry, skipping"
                    );
                }
                Err(e) => {
                    warn!(
                        peer_id = %peer_id,
                        error = %e,
                        "Spawn blocking task panicked"
                    );
                }
            }
        }

        events
    }
}

/// Extract a string field from Redis hash.
fn get_string_field(fields: &HashMap<String, redis::Value>, name: &str) -> Result<String> {
    let value = fields
        .get(name)
        .ok_or_else(|| ReplicationError::StreamParse(format!("Missing field: {}", name)))?;

    match value {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone())
            .map_err(|e| ReplicationError::StreamParse(format!("Invalid UTF-8 in {}: {}", name, e))),
        redis::Value::SimpleString(s) => Ok(s.clone()),
        _ => Err(ReplicationError::StreamParse(format!(
            "Unexpected type for field {}: {:?}",
            name, value
        ))),
    }
}

/// Extract a bytes field from Redis hash.
fn get_bytes_field(fields: &HashMap<String, redis::Value>, name: &str) -> Result<Vec<u8>> {
    let value = fields
        .get(name)
        .ok_or_else(|| ReplicationError::StreamParse(format!("Missing field: {}", name)))?;

    match value {
        redis::Value::BulkString(bytes) => Ok(bytes.clone()),
        redis::Value::SimpleString(s) => Ok(s.as_bytes().to_vec()),
        _ => Err(ReplicationError::StreamParse(format!(
            "Unexpected type for field {}: {:?}",
            name, value
        ))),
    }
}

/// Decompress zstd data if it has the magic header, otherwise return as-is.
pub fn maybe_decompress(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() >= 4 && data[..4] == ZSTD_MAGIC {
        let mut decoder = zstd::Decoder::new(data)
            .map_err(|e| ReplicationError::Decompression(format!("zstd init: {}", e)))?;
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| ReplicationError::Decompression(format!("zstd decode: {}", e)))?;
        Ok(decompressed)
    } else {
        Ok(data.to_vec())
    }
}

/// Compute SHA256 content hash as hex string.
fn compute_content_hash(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(data);
    hex::encode(hash)
}

/// Compare two Redis stream IDs.
///
/// Stream IDs are formatted as `{timestamp}-{sequence}` (e.g., "1234567890123-0").
/// This function compares them numerically, not lexicographically.
///
/// Returns `Ordering::Less` if `a < b`, `Equal` if `a == b`, `Greater` if `a > b`.
pub fn compare_stream_ids(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // Parse "timestamp-sequence" format
    let parse = |s: &str| -> (u64, u64) {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() == 2 {
            let ts = parts[0].parse().unwrap_or(0);
            let seq = parts[1].parse().unwrap_or(0);
            (ts, seq)
        } else {
            // Handle special cases like "0" (start) or malformed IDs
            (s.parse().unwrap_or(0), 0)
        }
    };

    let (a_ts, a_seq) = parse(a);
    let (b_ts, b_seq) = parse(b);

    match a_ts.cmp(&b_ts) {
        Ordering::Equal => a_seq.cmp(&b_seq),
        other => other,
    }
}

/// Parse the timestamp (milliseconds since epoch) from a stream ID.
///
/// Stream IDs are formatted as `{timestamp_ms}-{sequence}`.
/// Returns `None` for malformed IDs or special cases like "0".
pub fn parse_stream_id_timestamp(stream_id: &str) -> Option<u64> {
    let parts: Vec<&str> = stream_id.split('-').collect();
    if parts.len() == 2 {
        parts[0].parse().ok()
    } else {
        None
    }
}

/// Calculate the time lag in milliseconds between two stream IDs.
///
/// Returns `None` if either ID can't be parsed.
/// Returns 0 if cursor is ahead of latest (shouldn't happen normally).
pub fn calculate_lag_ms(cursor: &str, latest: &str) -> Option<u64> {
    let cursor_ts = parse_stream_id_timestamp(cursor)?;
    let latest_ts = parse_stream_id_timestamp(latest)?;
    Some(latest_ts.saturating_sub(cursor_ts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_op_from_str() {
        assert_eq!(CdcOp::from_str("PUT"), Some(CdcOp::Put));
        assert_eq!(CdcOp::from_str("put"), Some(CdcOp::Put));
        assert_eq!(CdcOp::from_str("DEL"), Some(CdcOp::Delete));
        assert_eq!(CdcOp::from_str("DELETE"), Some(CdcOp::Delete));
        assert_eq!(CdcOp::from_str("delete"), Some(CdcOp::Delete));
        assert_eq!(CdcOp::from_str("UNKNOWN"), None);
        assert_eq!(CdcOp::from_str(""), None);
    }

    #[test]
    fn test_cdc_event_is_put() {
        let event = CdcEvent {
            stream_id: "1-0".to_string(),
            op: CdcOp::Put,
            key: "test".to_string(),
            hash: None,
            data: None,
            meta: None,
        };
        assert!(event.is_put());
        assert!(!event.is_delete());
    }

    #[test]
    fn test_cdc_event_is_delete() {
        let event = CdcEvent {
            stream_id: "1-0".to_string(),
            op: CdcOp::Delete,
            key: "test".to_string(),
            hash: None,
            data: None,
            meta: None,
        };
        assert!(event.is_delete());
        assert!(!event.is_put());
    }

    #[test]
    fn test_cdc_event_trace_parent() {
        // No meta
        let event = CdcEvent {
            stream_id: "1-0".to_string(),
            op: CdcOp::Put,
            key: "test".to_string(),
            hash: None,
            data: None,
            meta: None,
        };
        assert_eq!(event.trace_parent(), None);
        assert_eq!(event.trace_id(), None);
        assert_eq!(event.parent_span_id(), None);

        // Meta without trace_parent
        let event = CdcEvent {
            stream_id: "1-0".to_string(),
            op: CdcOp::Put,
            key: "test".to_string(),
            hash: None,
            data: None,
            meta: Some(CdcMeta {
                content_type: "application/json".to_string(),
                version: 1,
                updated_at: 12345,
                trace_parent: None,
            }),
        };
        assert_eq!(event.trace_parent(), None);

        // Meta with trace_parent (W3C format)
        let event = CdcEvent {
            stream_id: "1-0".to_string(),
            op: CdcOp::Put,
            key: "test".to_string(),
            hash: None,
            data: None,
            meta: Some(CdcMeta {
                content_type: "application/json".to_string(),
                version: 1,
                updated_at: 12345,
                trace_parent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
            }),
        };
        assert_eq!(event.trace_parent(), Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"));
        assert_eq!(event.trace_id(), Some("0af7651916cd43dd8448eb211c80319c"));
        assert_eq!(event.parent_span_id(), Some("b7ad6b7169203331"));
    }

    #[test]
    fn test_cdc_event_trace_parent_malformed() {
        // Malformed trace_parent (not enough parts)
        let event = CdcEvent {
            stream_id: "1-0".to_string(),
            op: CdcOp::Put,
            key: "test".to_string(),
            hash: None,
            data: None,
            meta: Some(CdcMeta {
                content_type: "application/json".to_string(),
                version: 1,
                updated_at: 12345,
                trace_parent: Some("invalid".to_string()),
            }),
        };
        assert_eq!(event.trace_parent(), Some("invalid"));
        assert_eq!(event.trace_id(), None);
        assert_eq!(event.parent_span_id(), None);

        // Only version
        let event = CdcEvent {
            stream_id: "1-0".to_string(),
            op: CdcOp::Put,
            key: "test".to_string(),
            hash: None,
            data: None,
            meta: Some(CdcMeta {
                content_type: "application/json".to_string(),
                version: 1,
                updated_at: 12345,
                trace_parent: Some("00-traceid".to_string()),
            }),
        };
        assert_eq!(event.trace_id(), Some("traceid"));
        assert_eq!(event.parent_span_id(), None);
    }

    #[test]
    fn test_maybe_decompress_uncompressed() {
        let data = b"hello world";
        let result = maybe_decompress(data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_maybe_decompress_zstd() {
        let original = b"hello world hello world hello world";
        let compressed = zstd::encode_all(&original[..], 3).unwrap();
        
        // Verify it has magic bytes
        assert_eq!(&compressed[..4], &ZSTD_MAGIC);
        
        let result = maybe_decompress(&compressed).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_maybe_decompress_short_data() {
        // Less than 4 bytes should not crash
        let data = b"ab";
        let result = maybe_decompress(data).unwrap();
        assert_eq!(result, data);

        let data = b"";
        let result = maybe_decompress(data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_maybe_decompress_fake_magic() {
        // Data that starts with zstd magic but isn't valid zstd
        let mut data = ZSTD_MAGIC.to_vec();
        data.extend_from_slice(b"not valid zstd data");
        
        let result = maybe_decompress(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_compare_stream_ids() {
        use std::cmp::Ordering;

        // Same timestamp, different sequence
        assert_eq!(
            compare_stream_ids("1234567890123-0", "1234567890123-1"),
            Ordering::Less
        );
        assert_eq!(
            compare_stream_ids("1234567890123-1", "1234567890123-0"),
            Ordering::Greater
        );
        assert_eq!(
            compare_stream_ids("1234567890123-0", "1234567890123-0"),
            Ordering::Equal
        );

        // Different timestamps
        assert_eq!(
            compare_stream_ids("1234567890000-0", "1234567890123-0"),
            Ordering::Less
        );
        assert_eq!(
            compare_stream_ids("1234567890123-0", "1234567890000-0"),
            Ordering::Greater
        );

        // Edge case: "0" start marker
        assert_eq!(
            compare_stream_ids("0", "1234567890123-0"),
            Ordering::Less
        );

        // Both are special "0"
        assert_eq!(compare_stream_ids("0", "0"), Ordering::Equal);
    }

    #[test]
    fn test_compare_stream_ids_high_sequence() {
        use std::cmp::Ordering;

        // High sequence numbers
        assert_eq!(
            compare_stream_ids("1000-999999", "1000-1000000"),
            Ordering::Less
        );
    }

    #[test]
    fn test_read_result_methods() {
        // Events result
        let events = vec![CdcEvent {
            stream_id: "1-0".to_string(),
            op: CdcOp::Put,
            key: "test".to_string(),
            hash: None,
            data: None,
            meta: None,
        }];
        let result = ReadResult::Events(events);
        assert!(!result.is_trimmed());
        assert_eq!(result.events().len(), 1);

        // Trimmed result
        let result = ReadResult::StreamTrimmed {
            cursor: "1-0".to_string(),
            oldest_id: "100-0".to_string(),
        };
        assert!(result.is_trimmed());
        assert_eq!(result.events().len(), 0);

        // Empty result
        let result = ReadResult::Empty;
        assert!(!result.is_trimmed());
        assert_eq!(result.events().len(), 0);
    }

    #[test]
    fn test_read_result_events_empty() {
        let result = ReadResult::Events(vec![]);
        assert!(!result.is_trimmed());
        assert_eq!(result.events().len(), 0);
    }

    #[test]
    fn test_compute_content_hash() {
        let data = b"hello world";
        let hash = compute_content_hash(data);
        // SHA256 of "hello world"
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_compute_content_hash_empty() {
        let data = b"";
        let hash = compute_content_hash(data);
        // SHA256 of empty string
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_content_hash_validation_matches() {
        use sha2::{Digest, Sha256};

        let data = b"test data for hashing";
        let expected_hash = hex::encode(Sha256::digest(data));
        let computed = compute_content_hash(data);
        assert_eq!(computed, expected_hash);
    }

    #[test]
    fn test_parse_stream_id_timestamp() {
        // Normal stream ID
        assert_eq!(
            parse_stream_id_timestamp("1234567890123-0"),
            Some(1234567890123)
        );
        assert_eq!(
            parse_stream_id_timestamp("1234567890123-99"),
            Some(1234567890123)
        );

        // Special "0" marker
        assert_eq!(parse_stream_id_timestamp("0"), None);

        // Malformed
        assert_eq!(parse_stream_id_timestamp("invalid"), None);
        assert_eq!(parse_stream_id_timestamp(""), None);
        assert_eq!(parse_stream_id_timestamp("abc-def"), None);
    }

    #[test]
    fn test_calculate_lag_ms() {
        // Normal case: cursor behind latest
        assert_eq!(
            calculate_lag_ms("1000000000000-0", "1000000005000-0"),
            Some(5000)
        );

        // Caught up (same timestamp)
        assert_eq!(
            calculate_lag_ms("1000000000000-0", "1000000000000-5"),
            Some(0)
        );

        // Cursor ahead (shouldn't happen, but should handle gracefully)
        assert_eq!(
            calculate_lag_ms("1000000005000-0", "1000000000000-0"),
            Some(0) // saturating_sub returns 0
        );

        // Invalid cursor
        assert_eq!(calculate_lag_ms("0", "1000000000000-0"), None);

        // Invalid latest
        assert_eq!(calculate_lag_ms("1000000000000-0", "invalid"), None);

        // Both invalid
        assert_eq!(calculate_lag_ms("abc", "def"), None);
    }

    #[test]
    fn test_get_string_field() {
        let mut fields = HashMap::new();
        fields.insert("key1".to_string(), redis::Value::BulkString(b"value1".to_vec()));
        fields.insert("key2".to_string(), redis::Value::SimpleString("value2".to_string()));
        fields.insert("key3".to_string(), redis::Value::Int(42));

        // BulkString extraction
        assert_eq!(get_string_field(&fields, "key1").unwrap(), "value1");

        // SimpleString extraction
        assert_eq!(get_string_field(&fields, "key2").unwrap(), "value2");

        // Missing field
        assert!(get_string_field(&fields, "missing").is_err());

        // Wrong type (Int)
        assert!(get_string_field(&fields, "key3").is_err());
    }

    #[test]
    fn test_get_string_field_invalid_utf8() {
        let mut fields = HashMap::new();
        // Invalid UTF-8 sequence
        fields.insert("bad".to_string(), redis::Value::BulkString(vec![0xFF, 0xFE]));

        let result = get_string_field(&fields, "bad");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_bytes_field() {
        let mut fields = HashMap::new();
        fields.insert("data".to_string(), redis::Value::BulkString(vec![1, 2, 3, 4]));
        fields.insert("text".to_string(), redis::Value::SimpleString("hello".to_string()));
        fields.insert("num".to_string(), redis::Value::Int(42));

        // BulkString extraction
        assert_eq!(get_bytes_field(&fields, "data").unwrap(), vec![1, 2, 3, 4]);

        // SimpleString extraction (as bytes)
        assert_eq!(get_bytes_field(&fields, "text").unwrap(), b"hello".to_vec());

        // Missing field
        assert!(get_bytes_field(&fields, "missing").is_err());

        // Wrong type (Int)
        assert!(get_bytes_field(&fields, "num").is_err());
    }

    #[test]
    fn test_stream_tailer_new() {
        let tailer = StreamTailer::new(
            "peer-1".to_string(),
            "cdc".to_string(),
            Duration::from_secs(5),
            100,
        );
        assert_eq!(tailer.batch_size(), 100);
    }

    #[test]
    fn test_stream_tailer_set_batch_size() {
        let mut tailer = StreamTailer::new(
            "peer-1".to_string(),
            "cdc".to_string(),
            Duration::from_secs(5),
            100,
        );
        assert_eq!(tailer.batch_size(), 100);
        
        tailer.set_batch_size(500);
        assert_eq!(tailer.batch_size(), 500);
        
        tailer.set_batch_size(0);
        assert_eq!(tailer.batch_size(), 0);
    }

    #[test]
    fn test_cdc_meta_serialization() {
        let meta = CdcMeta {
            content_type: "application/json".to_string(),
            version: 42,
            updated_at: 1234567890123,
            trace_parent: Some("00-traceid-spanid-01".to_string()),
        };
        
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("application/json"));
        assert!(json.contains("42"));
        assert!(json.contains("1234567890123"));
        assert!(json.contains("traceid"));

        let parsed: CdcMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content_type, "application/json");
        assert_eq!(parsed.version, 42);
        assert_eq!(parsed.updated_at, 1234567890123);
        assert_eq!(parsed.trace_parent, Some("00-traceid-spanid-01".to_string()));
    }

    #[test]
    fn test_cdc_meta_serialization_no_trace() {
        let meta = CdcMeta {
            content_type: "text/plain".to_string(),
            version: 1,
            updated_at: 0,
            trace_parent: None,
        };
        
        let json = serde_json::to_string(&meta).unwrap();
        // trace_parent should be skipped when None
        assert!(!json.contains("trace_parent"));
    }
}
