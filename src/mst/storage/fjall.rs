use async_trait::async_trait;
use cid::Cid;
use fjall::{Config, PartitionHandle};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::MstStorage;
use crate::error::{AtmosError, Result};
use crate::mst::node::MstNode;

/// Fjall-based persistent MST storage with optional LRU caching
pub struct FjallMstStorage {
    /// Fjall partition for storing MST nodes
    nodes: Arc<PartitionHandle>,
    /// Optional in-memory LRU cache for frequently accessed nodes
    cache: Option<Arc<Mutex<LruCache<Cid, MstNode>>>>,
    /// Serialization format configuration
    config: FjallStorageConfig,
}

impl std::fmt::Debug for FjallMstStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallMstStorage")
            .field("config", &self.config)
            .finish()
    }
}

impl Clone for FjallMstStorage {
    fn clone(&self) -> Self {
        Self {
            nodes: Arc::clone(&self.nodes),
            cache: self.cache.clone(),
            config: self.config.clone(),
        }
    }
}

/// Configuration for Fjall storage behavior
#[derive(Debug, Clone)]
pub struct FjallStorageConfig {
    /// Enable LRU caching
    pub enable_cache: bool,
    /// Maximum number of nodes to cache (if caching enabled)
    pub cache_capacity: usize,
    /// Serialization format
    pub serialization: SerializationFormat,
    /// Compression settings
    pub compression: CompressionConfig,
    /// Sync behavior
    pub sync_on_commit: bool,
}

/// Serialization format options
#[derive(Debug, Clone, Copy)]
pub enum SerializationFormat {
    /// Use bincode for fast, compact serialization
    Bincode,
    /// Use CBOR for IPLD compatibility
    Cbor,
}

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub algorithm: CompressionAlgorithm,
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionAlgorithm {
    Lz4,
    Zstd,
    None,
}

impl Default for FjallStorageConfig {
    fn default() -> Self {
        Self {
            enable_cache: true,
            cache_capacity: 10_000,
            serialization: SerializationFormat::Bincode,
            compression: CompressionConfig {
                enabled: true,
                algorithm: CompressionAlgorithm::Lz4,
            },
            sync_on_commit: false, // Async by default for better performance
        }
    }
}

impl FjallStorageConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cache(mut self, enabled: bool, capacity: usize) -> Self {
        self.enable_cache = enabled;
        self.cache_capacity = capacity;
        self
    }

    pub fn with_serialization(mut self, format: SerializationFormat) -> Self {
        self.serialization = format;
        self
    }

    pub fn with_compression(mut self, enabled: bool, algorithm: CompressionAlgorithm) -> Self {
        self.compression = CompressionConfig { enabled, algorithm };
        self
    }

    pub fn with_sync_on_commit(mut self, sync: bool) -> Self {
        self.sync_on_commit = sync;
        self
    }
}

impl FjallMstStorage {
    /// Create a new Fjall storage at the given path
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, FjallStorageConfig::default()).await
    }

    /// Create a new Fjall storage with custom configuration
    pub async fn open_with_config<P: AsRef<Path>>(
        path: P,
        config: FjallStorageConfig,
    ) -> Result<Self> {
        // Configure Fjall database with basic settings
        let db = Config::new(path)
            .open()
            .map_err(|e| AtmosError::mst(format!("Failed to open Fjall database: {}", e)))?;

        // Create partition for MST nodes
        let nodes = Arc::new(
            db.open_partition("mst_nodes", fjall::PartitionCreateOptions::default())
                .map_err(|e| AtmosError::mst(format!("Failed to create partition: {}", e)))?,
        );

        // Initialize cache if enabled
        let cache = if config.enable_cache {
            let capacity = NonZeroUsize::new(config.cache_capacity)
                .ok_or_else(|| AtmosError::invalid_field("cache_capacity", "must be > 0"))?;
            Some(Arc::new(Mutex::new(LruCache::new(capacity))))
        } else {
            None
        };

        Ok(Self {
            nodes,
            cache,
            config,
        })
    }

    /// Create an in-memory temporary storage (useful for testing)
    pub async fn temporary() -> Result<Self> {
        let temp_dir = std::env::temp_dir().join(format!("fjall_mst_{}", uuid::Uuid::new_v4()));
        Self::open(&temp_dir).await
    }

    /// Compact the underlying database
    pub async fn compact(&self) -> Result<()> {
        // Fjall handles compaction automatically, so this is a no-op
        Ok(())
    }

    /// Get storage statistics
    pub async fn stats(&self) -> Result<FjallStorageStats> {
        let cache_stats = if let Some(cache) = &self.cache {
            let cache_guard = cache.lock().await;
            Some(CacheStats {
                capacity: cache_guard.cap().get(),
                len: cache_guard.len(),
                hit_rate: 0.0, // Would need separate hit/miss counters
            })
        } else {
            None
        };

        // Get actual key count using the len() method
        let total_keys = self.len().await?;

        // For disk usage, we'll estimate based on node count for now
        let estimated_disk_usage = (total_keys * 1024) as u64; // Rough estimate

        Ok(FjallStorageStats {
            disk_usage_bytes: estimated_disk_usage,
            total_keys: total_keys as u64,
            cache_stats,
        })
    }

    /// Serialize a node using the configured format
    fn serialize_node(&self, node: &MstNode) -> Result<Vec<u8>> {
        match self.config.serialization {
            SerializationFormat::Bincode => bincode::serialize(node)
                .map_err(|e| AtmosError::mst(format!("Bincode serialization failed: {}", e))),
            SerializationFormat::Cbor => {
                serde_ipld_dagcbor::to_vec(node).map_err(|e| AtmosError::CborEncodingGeneric(e))
            }
        }
    }

    /// Deserialize a node using the configured format
    fn deserialize_node(&self, data: &[u8]) -> Result<MstNode> {
        match self.config.serialization {
            SerializationFormat::Bincode => bincode::deserialize(data)
                .map_err(|e| AtmosError::mst(format!("Bincode deserialization failed: {}", e))),
            SerializationFormat::Cbor => {
                serde_ipld_dagcbor::from_slice(data).map_err(|e| AtmosError::CborDecodingGeneric(e))
            }
        }
    }
}

#[async_trait]
impl MstStorage for FjallMstStorage {
    async fn get_node(&self, cid: &Cid) -> Result<Option<MstNode>> {
        // Check cache first
        if let Some(cache) = &self.cache {
            let mut cache_guard = cache.lock().await;
            if let Some(node) = cache_guard.get(cid) {
                return Ok(Some(node.clone()));
            }
        }

        // Load from disk
        let cid_bytes = cid.to_bytes();
        if let Ok(Some(data)) = self.nodes.get(cid_bytes) {
            let node = self.deserialize_node(&data)?;

            // Cache for future access
            if let Some(cache) = &self.cache {
                let mut cache_guard = cache.lock().await;
                cache_guard.put(*cid, node.clone());
            }

            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    async fn insert_node(&self, cid: Cid, node: MstNode) -> Result<()> {
        let cid_bytes = cid.to_bytes();
        let data = self.serialize_node(&node)?;

        // Insert into Fjall
        self.nodes
            .insert(cid_bytes, data)
            .map_err(|e| AtmosError::mst(format!("Fjall insert failed: {}", e)))?;

        // Update cache
        if let Some(cache) = &self.cache {
            let mut cache_guard = cache.lock().await;
            cache_guard.put(cid, node);
        }

        Ok(())
    }

    async fn remove_node(&self, cid: &Cid) -> Result<Option<MstNode>> {
        let cid_bytes = cid.to_bytes();

        // Get the node first (for return value)
        let existing = self.get_node(cid).await?;

        // Remove from Fjall
        self.nodes
            .remove(cid_bytes)
            .map_err(|e| AtmosError::mst(format!("Fjall remove failed: {}", e)))?;

        // Remove from cache
        if let Some(cache) = &self.cache {
            let mut cache_guard = cache.lock().await;
            cache_guard.pop(cid);
        }

        Ok(existing)
    }

    async fn contains_node(&self, cid: &Cid) -> Result<bool> {
        // Check cache first
        if let Some(cache) = &self.cache {
            let cache_guard = cache.lock().await;
            if cache_guard.contains(cid) {
                return Ok(true);
            }
        }

        // Check Fjall
        let cid_bytes = cid.to_bytes();
        Ok(self.nodes.contains_key(cid_bytes).unwrap_or(false))
    }

    async fn len(&self) -> Result<usize> {
        // Count entries by iterating through all keys
        let iter = self.nodes.iter();
        let mut count = 0;

        for item in iter {
            match item {
                Ok(_) => count += 1,
                Err(e) => {
                    return Err(AtmosError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to iterate storage: {}", e),
                    )));
                }
            }
        }

        Ok(count)
    }

    async fn clear(&self) -> Result<()> {
        // Clear cache first
        if let Some(cache) = &self.cache {
            let mut cache_guard = cache.lock().await;
            cache_guard.clear();
        }

        // Clear all entries from Fjall storage
        let keys_to_remove: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|item| match item {
                Ok((key, _)) => Some(key),
                Err(_) => None,
            })
            .collect();

        for key in keys_to_remove {
            if let Err(e) = self.nodes.remove(&*key) {
                return Err(AtmosError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to remove key during clear: {}", e),
                )));
            }
        }

        Ok(())
    }

    async fn all_cids(&self) -> Result<Vec<Cid>> {
        let mut cids = Vec::new();
        let iter = self.nodes.iter();

        for item in iter {
            match item {
                Ok((key, _)) => match Cid::try_from(key.as_ref()) {
                    Ok(cid) => cids.push(cid),
                    Err(e) => {
                        return Err(AtmosError::InvalidIpld {
                            message: format!("Failed to parse CID from key: {}", e),
                        });
                    }
                },
                Err(e) => {
                    return Err(AtmosError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to iterate storage: {}", e),
                    )));
                }
            }
        }

        Ok(cids)
    }

    async fn batch_insert(&self, nodes: Vec<(Cid, MstNode)>) -> Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }

        // Fjall batch operations have a different API
        // For now, we'll insert one by one
        // TODO: Implement proper batch operations when Fjall API is stable
        let mut cache_updates = Vec::new();
        for (cid, node) in nodes {
            let cid_bytes = cid.to_bytes();
            let data = self.serialize_node(&node)?;

            self.nodes
                .insert(cid_bytes, data)
                .map_err(|e| AtmosError::mst(format!("Fjall insert failed: {}", e)))?;
            cache_updates.push((cid, node));
        }

        // Update cache after successful commit
        if let Some(cache) = &self.cache {
            let mut cache_guard = cache.lock().await;
            for (cid, node) in cache_updates {
                cache_guard.put(cid, node);
            }
        }

        Ok(())
    }

    async fn batch_remove(&self, cids: Vec<Cid>) -> Result<Vec<Option<MstNode>>> {
        if cids.is_empty() {
            return Ok(Vec::new());
        }

        // Get existing nodes first
        let mut removed_nodes = Vec::new();
        for cid in &cids {
            removed_nodes.push(self.get_node(cid).await?);
        }

        // Fjall batch operations have a different API
        // For now, we'll remove one by one
        for cid in &cids {
            let cid_bytes = cid.to_bytes();
            self.nodes
                .remove(cid_bytes)
                .map_err(|e| AtmosError::mst(format!("Fjall remove failed: {}", e)))?;
        }

        // Update cache after successful commit
        if let Some(cache) = &self.cache {
            let mut cache_guard = cache.lock().await;
            for cid in &cids {
                cache_guard.pop(cid);
            }
        }

        Ok(removed_nodes)
    }

    fn clone_storage(&self) -> Box<dyn MstStorage> {
        Box::new(self.clone())
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct FjallStorageStats {
    pub disk_usage_bytes: u64,
    pub total_keys: u64,
    pub cache_stats: Option<CacheStats>,
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub capacity: usize,
    pub len: usize,
    pub hit_rate: f64,
}

// Compression is handled differently in the current Fjall version
// This function is kept for compatibility but doesn't do anything
fn map_compression(_config: &CompressionConfig) {
    // No-op for now
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Bytes;
    use crate::mst::node::{MstNode, MstNodeLeaf};
    use multihash::Multihash;
    use sha2::Digest;
    use tempfile::TempDir;

    fn create_test_cid(data: &str) -> Cid {
        let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(data.as_bytes())).unwrap();
        Cid::new_v1(0x71, hash)
    }

    fn create_test_node(entries: Vec<(usize, String, Cid, Option<Cid>)>) -> MstNode {
        let e = entries
            .into_iter()
            .map(|(p, k, v, t)| MstNodeLeaf {
                p,
                k: Bytes::from(k.into_bytes()),
                v,
                t,
            })
            .collect();

        MstNode::new(None, e)
    }

    #[tokio::test]
    async fn test_fjall_storage_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FjallMstStorage::open(temp_dir.path()).await.unwrap();

        let cid = create_test_cid("test_node");
        let node = create_test_node(vec![(
            0,
            "key1".to_string(),
            create_test_cid("value1"),
            None,
        )]);

        // Test insert and get
        storage.insert_node(cid, node.clone()).await.unwrap();
        let retrieved = storage.get_node(&cid).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().entries().len(), 1);

        // Test contains
        assert!(storage.contains_node(&cid).await.unwrap());

        // Test len
        assert_eq!(storage.len().await.unwrap(), 1);

        // Test remove
        let removed = storage.remove_node(&cid).await.unwrap();
        assert!(removed.is_some());
        assert!(!storage.contains_node(&cid).await.unwrap());
        assert_eq!(storage.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_fjall_storage_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let cid = create_test_cid("persistent_node");
        let node = create_test_node(vec![(
            0,
            "persistent_key".to_string(),
            create_test_cid("persistent_value"),
            None,
        )]);

        // Insert data and close storage
        {
            let storage = FjallMstStorage::open(&path).await.unwrap();
            storage.insert_node(cid, node.clone()).await.unwrap();
        }

        // Reopen storage and verify data persists
        {
            let storage = FjallMstStorage::open(&path).await.unwrap();
            let retrieved = storage.get_node(&cid).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().entries().len(), 1);
        }
    }

    #[tokio::test]
    async fn test_fjall_storage_batch_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FjallMstStorage::open(temp_dir.path()).await.unwrap();

        let nodes = vec![
            (
                create_test_cid("node1"),
                create_test_node(vec![(
                    0,
                    "key1".to_string(),
                    create_test_cid("value1"),
                    None,
                )]),
            ),
            (
                create_test_cid("node2"),
                create_test_node(vec![(
                    0,
                    "key2".to_string(),
                    create_test_cid("value2"),
                    None,
                )]),
            ),
        ];

        // Test batch insert
        storage.batch_insert(nodes.clone()).await.unwrap();
        assert_eq!(storage.len().await.unwrap(), 2);

        // Test batch remove
        let cids_to_remove: Vec<Cid> = nodes.iter().map(|(cid, _)| *cid).collect();
        let removed = storage.batch_remove(cids_to_remove).await.unwrap();
        assert_eq!(removed.len(), 2);
        assert!(removed[0].is_some());
        assert!(removed[1].is_some());
        assert_eq!(storage.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_fjall_storage_configuration() {
        let temp_dir = TempDir::new().unwrap();

        let config = FjallStorageConfig::new()
            .with_cache(true, 1000)
            .with_serialization(SerializationFormat::Cbor)
            .with_compression(true, CompressionAlgorithm::Zstd)
            .with_sync_on_commit(true);

        let storage = FjallMstStorage::open_with_config(temp_dir.path(), config)
            .await
            .unwrap();

        // Verify cache is enabled
        assert!(storage.cache.is_some());

        // Test basic operations work with custom config
        let cid = create_test_cid("config_test");
        let node = create_test_node(vec![(
            0,
            "config_key".to_string(),
            create_test_cid("config_value"),
            None,
        )]);

        storage.insert_node(cid, node).await.unwrap();
        let retrieved = storage.get_node(&cid).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_fjall_storage_stats() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FjallMstStorage::open(temp_dir.path()).await.unwrap();

        // Insert some data
        for i in 0..5 {
            let cid = create_test_cid(&format!("stats_node_{}", i));
            let node = create_test_node(vec![(
                0,
                format!("stats_key_{}", i),
                create_test_cid(&format!("stats_value_{}", i)),
                None,
            )]);
            storage.insert_node(cid, node).await.unwrap();
        }

        let stats = storage.stats().await.unwrap();
        assert_eq!(stats.total_keys, 5);
        assert!(stats.disk_usage_bytes > 0);
    }
}
