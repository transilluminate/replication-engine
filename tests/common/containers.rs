// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Testcontainers setup for Redis.
//!
//! Provides helpers to spin up Redis containers for integration tests.

use testcontainers::{clients::Cli, Container, GenericImage, core::WaitFor};

/// Create a vanilla Redis container (streams-compatible).
///
/// Uses official redis:7 image. Waits for "Ready to accept connections".
pub fn redis_container(docker: &Cli) -> Container<'_, GenericImage> {
    let image = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379)
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    docker.run(image)
}

/// Get the Redis URL for a container.
pub fn redis_url(container: &Container<'_, GenericImage>) -> String {
    let port = container.get_host_port_ipv4(6379);
    format!("redis://127.0.0.1:{}", port)
}

/// Helper struct for a "peer" Redis with pre-configured stream.
pub struct TestPeer<'a> {
    #[allow(dead_code)] // Kept alive for container lifetime
    container: Container<'a, GenericImage>,
    pub node_id: String,
    pub redis_url: String,
}

impl<'a> TestPeer<'a> {
    /// Create a new test peer with a unique node ID.
    pub fn new(docker: &'a Cli, node_id: &str) -> Self {
        let container = redis_container(docker);
        let redis_url = redis_url(&container);
        Self {
            container,
            node_id: node_id.to_string(),
            redis_url,
        }
    }

    /// Add a CDC event to this peer's stream.
    ///
    /// This simulates what sync-engine would do when data changes.
    pub async fn add_cdc_event(
        &self,
        key: &str,
        op: &str,
        data: Option<&[u8]>,
        hash: Option<&str>,
    ) -> redis::RedisResult<String> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;

        let stream_key = "__local__:cdc";

        use redis::AsyncCommands;

        let mut fields: Vec<(&str, Vec<u8>)> = vec![
            ("op", op.as_bytes().to_vec()),
            ("key", key.as_bytes().to_vec()),
        ];

        if let Some(d) = data {
            fields.push(("data", d.to_vec()));
        }

        if let Some(h) = hash {
            fields.push(("hash", h.as_bytes().to_vec()));
        }

        // Convert to format redis expects
        let field_refs: Vec<(&str, &[u8])> = fields
            .iter()
            .map(|(k, v)| (*k, v.as_slice()))
            .collect();

        let id: String = conn.xadd(stream_key, "*", &field_refs).await?;
        Ok(id)
    }

    /// Add a PUT event with JSON data.
    pub async fn add_put_event(&self, key: &str, json_data: &str) -> redis::RedisResult<String> {
        let hash = format!("{:x}", sha2::Sha256::digest(json_data.as_bytes()));
        self.add_cdc_event(key, "PUT", Some(json_data.as_bytes()), Some(&hash))
            .await
    }

    /// Add a DELETE event.
    pub async fn add_delete_event(&self, key: &str) -> redis::RedisResult<String> {
        self.add_cdc_event(key, "DEL", None, None).await
    }

    /// Get stream length.
    pub async fn stream_len(&self) -> redis::RedisResult<usize> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;
        conn.xlen("__local__:cdc").await
    }

    // =========================================================================
    // Merkle Tree Helpers (for cold path testing)
    // =========================================================================

    /// Set the Merkle root hash for this peer.
    pub async fn set_merkle_root(&self, hash: [u8; 32]) -> redis::RedisResult<()> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;
        let hex_hash = hex::encode(hash);
        conn.set("merkle:hash:", &hex_hash).await
    }

    /// Set Merkle children for a path.
    ///
    /// Children are stored as a sorted set with member format "segment:hex_hash".
    pub async fn set_merkle_children(
        &self,
        path: &str,
        children: &[(&str, [u8; 32])],
    ) -> redis::RedisResult<()> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;

        let key = format!("merkle:children:{}", path);

        // Clear existing children
        conn.del::<_, ()>(&key).await?;

        // Add children as sorted set members
        for (i, (segment, hash)) in children.iter().enumerate() {
            let member = format!("{}:{}", segment, hex::encode(hash));
            conn.zadd::<_, _, _, ()>(&key, &member, i as f64).await?;
        }

        // Also set the hash for this interior node
        let hash_key = format!("merkle:hash:{}", path);
        let combined_hash = compute_interior_hash(children);
        conn.set::<_, _, ()>(&hash_key, hex::encode(combined_hash)).await?;

        Ok(())
    }

    /// Add a Merkle leaf (object hash).
    pub async fn add_merkle_leaf(&self, path: &str, hash: [u8; 32]) -> redis::RedisResult<()> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;

        let key = format!("merkle:hash:{}", path);
        conn.set(&key, hex::encode(hash)).await
    }

    /// Get a Merkle hash from this peer's Redis.
    pub async fn get_merkle_hash(&self, path: &str) -> redis::RedisResult<Option<[u8; 32]>> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;

        let key = if path.is_empty() {
            "merkle:hash:".to_string()
        } else {
            format!("merkle:hash:{}", path)
        };

        let result: Option<String> = conn.get(&key).await?;
        match result {
            Some(hex_str) => {
                let bytes = hex::decode(&hex_str).map_err(|_| {
                    redis::RedisError::from((redis::ErrorKind::TypeError, "Invalid hex"))
                })?;
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&bytes);
                Ok(Some(hash))
            }
            None => Ok(None),
        }
    }

    /// Get Merkle children from this peer's Redis.
    pub async fn get_merkle_children(
        &self,
        path: &str,
    ) -> redis::RedisResult<Vec<(String, [u8; 32])>> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;

        let key = format!("merkle:children:{}", path);
        let members: Vec<String> = conn.zrange(&key, 0, -1).await?;

        let mut children = Vec::new();
        for member in members {
            if let Some((segment, hex_hash)) = member.split_once(':') {
                if let Ok(bytes) = hex::decode(hex_hash) {
                    if bytes.len() == 32 {
                        let mut hash = [0u8; 32];
                        hash.copy_from_slice(&bytes);
                        children.push((segment.to_string(), hash));
                    }
                }
            }
        }
        Ok(children)
    }

    /// Store an item in Redis (for cold path repair to fetch).
    pub async fn store_item(&self, key: &str, data: &[u8]) -> redis::RedisResult<()> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;

        let redis_key = format!("data:{}", key);
        conn.set(&redis_key, data).await
    }

    /// Get an item from Redis.
    pub async fn get_item(&self, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;

        let redis_key = format!("data:{}", key);
        conn.get(&redis_key).await
    }
}

/// Compute interior node hash from children (same algorithm as sync-engine).
fn compute_interior_hash(children: &[(&str, [u8; 32])]) -> [u8; 32] {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    for (segment, hash) in children {
        hasher.update(segment.as_bytes());
        hasher.update(hash);
    }
    hasher.finalize().into()
}

use sha2::Digest;
