use async_trait::async_trait;
use cid::Cid;
use dashmap::DashMap;
use std::sync::Arc;

use super::node::MstNode;
use crate::error::Result;

#[cfg(feature = "fjall")]
pub mod fjall;

pub mod multi_tenant;

/// Trait for MST node storage backends
#[async_trait]
pub trait MstStorage: Send + Sync {
    /// Get a node by its CID
    async fn get_node(&self, cid: &Cid) -> Result<Option<MstNode>>;

    /// Insert a node with the given CID
    async fn insert_node(&self, cid: Cid, node: MstNode) -> Result<()>;

    /// Remove a node by its CID
    async fn remove_node(&self, cid: &Cid) -> Result<Option<MstNode>>;

    /// Check if a node exists
    async fn contains_node(&self, cid: &Cid) -> Result<bool>;

    /// Get the number of nodes stored
    async fn len(&self) -> Result<usize>;

    /// Check if the storage is empty
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Clear all nodes from storage
    async fn clear(&self) -> Result<()>;

    /// Get all CIDs in storage
    async fn all_cids(&self) -> Result<Vec<Cid>>;

    /// Batch insert multiple nodes atomically
    async fn batch_insert(&self, nodes: Vec<(Cid, MstNode)>) -> Result<()> {
        // Default implementation: insert one by one
        // Implementations can override for better atomicity
        for (cid, node) in nodes {
            self.insert_node(cid, node).await?;
        }
        Ok(())
    }

    /// Batch remove multiple nodes atomically
    async fn batch_remove(&self, cids: Vec<Cid>) -> Result<Vec<Option<MstNode>>> {
        // Default implementation: remove one by one
        // Implementations can override for better atomicity
        let mut removed = Vec::with_capacity(cids.len());
        for cid in cids {
            removed.push(self.remove_node(&cid).await?);
        }
        Ok(removed)
    }

    /// Clone this storage instance
    fn clone_storage(&self) -> Box<dyn MstStorage>;
}

/// Storage wrapper that can hold either concrete or dynamic storage
pub enum StorageWrapper {
    Memory(MemoryMstStorage),
    #[cfg(feature = "fjall")]
    Fjall(crate::mst::storage::fjall::FjallMstStorage),
    Dynamic(Box<dyn MstStorage>),
}

impl std::fmt::Debug for StorageWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageWrapper::Memory(_) => write!(f, "StorageWrapper::Memory(..)"),
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(_) => write!(f, "StorageWrapper::Fjall(..)"),
            StorageWrapper::Dynamic(_) => write!(f, "StorageWrapper::Dynamic(..)"),
        }
    }
}

impl Clone for StorageWrapper {
    fn clone(&self) -> Self {
        match self {
            StorageWrapper::Memory(storage) => StorageWrapper::Memory(storage.clone()),
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => StorageWrapper::Fjall(storage.clone()),
            StorageWrapper::Dynamic(storage) => StorageWrapper::Dynamic(storage.clone_storage()),
        }
    }
}

#[async_trait]
impl MstStorage for StorageWrapper {
    async fn get_node(&self, cid: &Cid) -> Result<Option<MstNode>> {
        match self {
            StorageWrapper::Memory(storage) => storage.get_node(cid).await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.get_node(cid).await,
            StorageWrapper::Dynamic(storage) => storage.get_node(cid).await,
        }
    }

    async fn insert_node(&self, cid: Cid, node: MstNode) -> Result<()> {
        match self {
            StorageWrapper::Memory(storage) => storage.insert_node(cid, node).await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.insert_node(cid, node).await,
            StorageWrapper::Dynamic(storage) => storage.insert_node(cid, node).await,
        }
    }

    async fn remove_node(&self, cid: &Cid) -> Result<Option<MstNode>> {
        match self {
            StorageWrapper::Memory(storage) => storage.remove_node(cid).await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.remove_node(cid).await,
            StorageWrapper::Dynamic(storage) => storage.remove_node(cid).await,
        }
    }

    async fn contains_node(&self, cid: &Cid) -> Result<bool> {
        match self {
            StorageWrapper::Memory(storage) => storage.contains_node(cid).await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.contains_node(cid).await,
            StorageWrapper::Dynamic(storage) => storage.contains_node(cid).await,
        }
    }

    async fn len(&self) -> Result<usize> {
        match self {
            StorageWrapper::Memory(storage) => storage.len().await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.len().await,
            StorageWrapper::Dynamic(storage) => storage.len().await,
        }
    }

    async fn clear(&self) -> Result<()> {
        match self {
            StorageWrapper::Memory(storage) => storage.clear().await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.clear().await,
            StorageWrapper::Dynamic(storage) => storage.clear().await,
        }
    }

    async fn all_cids(&self) -> Result<Vec<Cid>> {
        match self {
            StorageWrapper::Memory(storage) => storage.all_cids().await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.all_cids().await,
            StorageWrapper::Dynamic(storage) => storage.all_cids().await,
        }
    }

    async fn batch_insert(&self, nodes: Vec<(Cid, MstNode)>) -> Result<()> {
        match self {
            StorageWrapper::Memory(storage) => storage.batch_insert(nodes).await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.batch_insert(nodes).await,
            StorageWrapper::Dynamic(storage) => storage.batch_insert(nodes).await,
        }
    }

    async fn batch_remove(&self, cids: Vec<Cid>) -> Result<Vec<Option<MstNode>>> {
        match self {
            StorageWrapper::Memory(storage) => storage.batch_remove(cids).await,
            #[cfg(feature = "fjall")]
            StorageWrapper::Fjall(storage) => storage.batch_remove(cids).await,
            StorageWrapper::Dynamic(storage) => storage.batch_remove(cids).await,
        }
    }

    fn clone_storage(&self) -> Box<dyn MstStorage> {
        Box::new(self.clone())
    }
}

/// In-memory MST storage using DashMap
#[derive(Debug, Clone)]
pub struct MemoryMstStorage {
    nodes: Arc<DashMap<Cid, MstNode>>,
}

impl MemoryMstStorage {
    /// Create a new empty memory storage
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(DashMap::new()),
        }
    }

    /// Create a new memory storage with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Arc::new(DashMap::with_capacity(capacity)),
        }
    }

    /// Get the underlying DashMap for direct access
    pub fn inner(&self) -> &Arc<DashMap<Cid, MstNode>> {
        &self.nodes
    }
}

impl Default for MemoryMstStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MstStorage for MemoryMstStorage {
    async fn get_node(&self, cid: &Cid) -> Result<Option<MstNode>> {
        Ok(self.nodes.get(cid).map(|entry| entry.clone()))
    }

    async fn insert_node(&self, cid: Cid, node: MstNode) -> Result<()> {
        self.nodes.insert(cid, node);
        Ok(())
    }

    async fn remove_node(&self, cid: &Cid) -> Result<Option<MstNode>> {
        Ok(self.nodes.remove(cid).map(|(_, node)| node))
    }

    async fn contains_node(&self, cid: &Cid) -> Result<bool> {
        Ok(self.nodes.contains_key(cid))
    }

    async fn len(&self) -> Result<usize> {
        Ok(self.nodes.len())
    }

    async fn clear(&self) -> Result<()> {
        self.nodes.clear();
        Ok(())
    }

    async fn all_cids(&self) -> Result<Vec<Cid>> {
        Ok(self.nodes.iter().map(|entry| *entry.key()).collect())
    }

    async fn batch_insert(&self, nodes: Vec<(Cid, MstNode)>) -> Result<()> {
        // DashMap allows concurrent inserts, so we can do them all at once
        for (cid, node) in nodes {
            self.nodes.insert(cid, node);
        }
        Ok(())
    }

    async fn batch_remove(&self, cids: Vec<Cid>) -> Result<Vec<Option<MstNode>>> {
        let mut removed = Vec::with_capacity(cids.len());
        for cid in cids {
            removed.push(self.nodes.remove(&cid).map(|(_, node)| node));
        }
        Ok(removed)
    }

    fn clone_storage(&self) -> Box<dyn MstStorage> {
        Box::new(self.clone())
    }
}

/// Configuration for memory storage
#[derive(Debug, Clone)]
pub struct MemoryStorageConfig {
    /// Initial capacity for the underlying DashMap
    pub initial_capacity: Option<usize>,
}

impl Default for MemoryStorageConfig {
    fn default() -> Self {
        Self {
            initial_capacity: None,
        }
    }
}

impl MemoryStorageConfig {
    /// Create a new config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the initial capacity
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.initial_capacity = Some(capacity);
        self
    }

    /// Build the storage with this configuration
    pub fn build(self) -> MemoryMstStorage {
        match self.initial_capacity {
            Some(capacity) => MemoryMstStorage::with_capacity(capacity),
            None => MemoryMstStorage::new(),
        }
    }
}

// Re-export Fjall storage when feature is enabled
#[cfg(feature = "fjall")]
pub use fjall::{
    CompressionAlgorithm, CompressionConfig, FjallMstStorage, FjallStorageConfig,
    SerializationFormat,
};

// Re-export multi-tenant storage
pub use multi_tenant::{
    GlobalStats, MultiTenantConfig, MultiTenantConfigBuilder, MultiTenantMstStorage, PdsStorage,
    RepoId, RepoLifecycleConfig, RepoStats, StorageBackend,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Bytes;
    use crate::mst::node::{MstNode, MstNodeLeaf};
    use multihash::Multihash;
    use sha2::Digest;

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
    async fn test_memory_storage_basic_operations() {
        let storage = MemoryMstStorage::new();

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
        assert!(!storage.is_empty().await.unwrap());

        // Test remove
        let removed = storage.remove_node(&cid).await.unwrap();
        assert!(removed.is_some());
        assert!(!storage.contains_node(&cid).await.unwrap());
        assert_eq!(storage.len().await.unwrap(), 0);
        assert!(storage.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_batch_operations() {
        let storage = MemoryMstStorage::new();

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
            (
                create_test_cid("node3"),
                create_test_node(vec![(
                    0,
                    "key3".to_string(),
                    create_test_cid("value3"),
                    None,
                )]),
            ),
        ];

        // Test batch insert
        storage.batch_insert(nodes.clone()).await.unwrap();
        assert_eq!(storage.len().await.unwrap(), 3);

        // Test all_cids
        let all_cids = storage.all_cids().await.unwrap();
        assert_eq!(all_cids.len(), 3);

        // Test batch remove
        let cids_to_remove: Vec<Cid> = nodes.iter().take(2).map(|(cid, _)| *cid).collect();
        let removed = storage.batch_remove(cids_to_remove).await.unwrap();
        assert_eq!(removed.len(), 2);
        assert!(removed[0].is_some());
        assert!(removed[1].is_some());
        assert_eq!(storage.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_memory_storage_clear() {
        let storage = MemoryMstStorage::new();

        // Add some nodes
        for i in 0..5 {
            let cid = create_test_cid(&format!("node{}", i));
            let node = create_test_node(vec![(
                0,
                format!("key{}", i),
                create_test_cid(&format!("value{}", i)),
                None,
            )]);
            storage.insert_node(cid, node).await.unwrap();
        }

        assert_eq!(storage.len().await.unwrap(), 5);

        // Clear all
        storage.clear().await.unwrap();
        assert_eq!(storage.len().await.unwrap(), 0);
        assert!(storage.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_config() {
        let config = MemoryStorageConfig::new().with_capacity(100);

        let storage = config.build();
        assert_eq!(storage.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_memory_storage_concurrent_access() {
        let storage = MemoryMstStorage::new();
        let storage1 = storage.clone();
        let storage2 = storage.clone();

        // Test concurrent access
        let handle1 = tokio::spawn(async move {
            for i in 0..10 {
                let cid = create_test_cid(&format!("concurrent1_{}", i));
                let node = create_test_node(vec![(
                    0,
                    format!("key{}", i),
                    create_test_cid(&format!("value{}", i)),
                    None,
                )]);
                storage1.insert_node(cid, node).await.unwrap();
            }
        });

        let handle2 = tokio::spawn(async move {
            for i in 0..10 {
                let cid = create_test_cid(&format!("concurrent2_{}", i));
                let node = create_test_node(vec![(
                    0,
                    format!("key{}", i),
                    create_test_cid(&format!("value{}", i)),
                    None,
                )]);
                storage2.insert_node(cid, node).await.unwrap();
            }
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        assert_eq!(storage.len().await.unwrap(), 20);
    }

    #[tokio::test]
    async fn test_memory_storage_nonexistent_operations() {
        let storage = MemoryMstStorage::new();
        let nonexistent_cid = create_test_cid("nonexistent");

        // Test get nonexistent
        assert!(storage.get_node(&nonexistent_cid).await.unwrap().is_none());

        // Test contains nonexistent
        assert!(!storage.contains_node(&nonexistent_cid).await.unwrap());

        // Test remove nonexistent
        assert!(
            storage
                .remove_node(&nonexistent_cid)
                .await
                .unwrap()
                .is_none()
        );
    }
}
