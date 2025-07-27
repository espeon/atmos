//! Multi-tenant MST storage for PDS (Personal Data Server) architecture
//!
//! This module provides storage abstractions for managing multiple user repositories
//! in a single PDS instance. Each user gets their own isolated MST while sharing
//! underlying storage infrastructure for efficiency.

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{MemoryMstStorage, MstStorage, StorageWrapper};
use crate::error::{AtmosError, Result};
use crate::mst::Mst;

#[cfg(feature = "fjall")]
use super::fjall::{FjallMstStorage, FjallStorageConfig};

/// Identifier for a user's repository
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct RepoId(pub String);

impl RepoId {
    pub fn new(did: impl Into<String>) -> Self {
        Self(did.into())
    }

    pub fn from_did(did: &str) -> Self {
        Self(did.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for RepoId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for RepoId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Configuration for multi-tenant storage
#[derive(Debug, Clone)]
pub struct MultiTenantConfig {
    /// Maximum number of repositories to keep in memory simultaneously
    pub max_active_repos: usize,
    /// Storage backend configuration
    pub backend: StorageBackend,
    /// Repository lifecycle settings
    pub lifecycle: RepoLifecycleConfig,
}

pub enum StorageBackend {
    /// In-memory storage (testing/development)
    Memory,
    /// Fjall-based persistent storage
    #[cfg(feature = "fjall")]
    Fjall {
        base_path: std::path::PathBuf,
        config: crate::mst::storage::fjall::FjallStorageConfig,
    },
}

impl std::fmt::Debug for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageBackend::Memory => write!(f, "StorageBackend::Memory"),
            #[cfg(feature = "fjall")]
            StorageBackend::Fjall { base_path, .. } => {
                write!(f, "StorageBackend::Fjall {{ base_path: {:?} }}", base_path)
            }
        }
    }
}

impl Clone for StorageBackend {
    fn clone(&self) -> Self {
        match self {
            StorageBackend::Memory => StorageBackend::Memory,
            #[cfg(feature = "fjall")]
            StorageBackend::Fjall { base_path, config } => StorageBackend::Fjall {
                base_path: base_path.clone(),
                config: config.clone(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct RepoLifecycleConfig {
    /// Automatically evict inactive repositories after this duration
    pub eviction_timeout: std::time::Duration,
    /// Check for inactive repos this often
    pub cleanup_interval: std::time::Duration,
    /// Sync repository changes to disk this often
    pub sync_interval: std::time::Duration,
}

impl Default for MultiTenantConfig {
    fn default() -> Self {
        Self {
            max_active_repos: 1000,
            backend: StorageBackend::Memory,
            lifecycle: RepoLifecycleConfig {
                eviction_timeout: std::time::Duration::from_secs(3600), // 1 hour
                cleanup_interval: std::time::Duration::from_secs(300),  // 5 minutes
                sync_interval: std::time::Duration::from_secs(60),      // 1 minute
            },
        }
    }
}

/// Multi-tenant MST storage manager
pub struct MultiTenantMstStorage {
    /// Configuration
    config: MultiTenantConfig,
    /// Active repositories (in-memory cache)
    active_repos: Arc<RwLock<HashMap<RepoId, StorageWrapper>>>,
    /// Repository access tracking for LRU eviction
    access_tracker: Arc<DashMap<RepoId, std::time::Instant>>,
    /// Cleanup task handle
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MultiTenantMstStorage {
    /// Create a new multi-tenant storage manager
    pub async fn new(config: MultiTenantConfig) -> Result<Self> {
        let storage = Self {
            config,
            active_repos: Arc::new(RwLock::new(HashMap::new())),
            access_tracker: Arc::new(DashMap::new()),
            cleanup_handle: None,
        };

        Ok(storage)
    }

    /// Start background cleanup tasks
    pub fn start_background_tasks(&mut self) {
        let access_tracker = Arc::clone(&self.access_tracker);
        let active_repos = Arc::clone(&self.active_repos);
        let eviction_timeout = self.config.lifecycle.eviction_timeout;
        let cleanup_interval = self.config.lifecycle.cleanup_interval;

        let cleanup_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);

            loop {
                interval.tick().await;

                // Find repositories to evict
                let now = std::time::Instant::now();
                let mut to_evict = Vec::new();

                for entry in access_tracker.iter() {
                    let repo_id = entry.key();
                    let last_access = *entry.value();

                    if now.duration_since(last_access) > eviction_timeout {
                        to_evict.push(repo_id.clone());
                    }
                }

                // Evict inactive repositories
                if !to_evict.is_empty() {
                    let mut repos = active_repos.write().await;
                    for repo_id in to_evict {
                        repos.remove(&repo_id);
                        access_tracker.remove(&repo_id);
                        // Could add logging here if needed
                    }
                }
            }
        });

        self.cleanup_handle = Some(cleanup_handle);
    }

    /// Get or create an MST for a specific repository
    pub async fn get_repo_mst(&self, repo_id: &RepoId) -> Result<Mst<StorageWrapper>> {
        // Update access time
        self.access_tracker
            .insert(repo_id.clone(), std::time::Instant::now());

        // Check if repo is already loaded
        {
            let repos = self.active_repos.read().await;
            if let Some(storage) = repos.get(repo_id) {
                return Ok(Mst::with_storage(storage.clone()));
            }
        }

        // Load or create the repository
        let storage = self.create_storage_for_repo(repo_id).await?;

        // Add to active repos (with capacity management)
        {
            let mut repos = self.active_repos.write().await;

            // Evict oldest repos if at capacity
            if repos.len() >= self.config.max_active_repos {
                self.evict_lru_repo(&mut repos).await;
            }

            repos.insert(repo_id.clone(), storage.clone());
        }

        Ok(Mst::with_storage(storage))
    }

    /// Get repository statistics
    pub async fn get_repo_stats(&self, repo_id: &RepoId) -> Result<RepoStats> {
        let mst = self.get_repo_mst(repo_id).await?;
        let node_count = mst.storage.len().await?;

        let last_access = self
            .access_tracker
            .get(repo_id)
            .map(|entry| *entry.value())
            .unwrap_or_else(std::time::Instant::now);

        Ok(RepoStats {
            repo_id: repo_id.clone(),
            node_count,
            last_access,
            is_active: self.active_repos.read().await.contains_key(repo_id),
        })
    }

    /// List all known repositories
    pub async fn list_repos(&self) -> Result<Vec<RepoId>> {
        match &self.config.backend {
            StorageBackend::Memory => {
                // For memory storage, only return active repos
                let repos = self.active_repos.read().await;
                Ok(repos.keys().cloned().collect())
            }
            #[cfg(feature = "fjall")]
            StorageBackend::Fjall { base_path, .. } => {
                // Scan filesystem for repository directories
                let mut repo_ids = Vec::new();

                if base_path.exists() {
                    if let Ok(mut entries) = tokio::fs::read_dir(base_path).await {
                        while let Ok(Some(entry)) = entries.next_entry().await {
                            if let Ok(file_type) = entry.file_type().await {
                                if file_type.is_dir() {
                                    if let Some(name) = entry.file_name().to_str() {
                                        repo_ids.push(RepoId::new(name));
                                    }
                                }
                            }
                        }
                    }
                }

                Ok(repo_ids)
            }
        }
    }

    /// Get global statistics across all repositories
    pub async fn get_global_stats(&self) -> Result<GlobalStats> {
        let active_count = self.active_repos.read().await.len();
        let total_tracked = self.access_tracker.len();

        // Calculate total nodes across active repos
        let mut total_nodes = 0;
        let repos = self.active_repos.read().await;
        for storage in repos.values() {
            total_nodes += storage.len().await.unwrap_or(0);
        }

        Ok(GlobalStats {
            active_repos: active_count,
            total_tracked_repos: total_tracked,
            total_nodes,
            max_active_repos: self.config.max_active_repos,
        })
    }

    /// Explicitly evict a repository from memory (data remains on disk)
    pub async fn evict_repo(&self, repo_id: &RepoId) -> Result<bool> {
        let mut repos = self.active_repos.write().await;
        let was_active = repos.remove(repo_id).is_some();
        self.access_tracker.remove(repo_id);
        Ok(was_active)
    }

    /// Sync all active repositories to persistent storage
    pub async fn sync_all(&self) -> Result<()> {
        let repos = self.active_repos.read().await;

        for (_repo_id, _storage) in repos.iter() {
            // Note: Individual storages handle their own syncing
            // This is mainly for coordinating sync timing
            // Could add logging here if needed
        }

        Ok(())
    }

    /// Create storage for a specific repository
    // unused variable is allowed here for fjall
    #[allow(unused_variables)]
    async fn create_storage_for_repo(&self, repo_id: &RepoId) -> Result<StorageWrapper> {
        match &self.config.backend {
            StorageBackend::Memory => Ok(StorageWrapper::Memory(MemoryMstStorage::new())),
            #[cfg(feature = "fjall")]
            StorageBackend::Fjall { base_path, config } => {
                let repo_path = base_path.join(repo_id.as_str());

                // Ensure directory exists
                tokio::fs::create_dir_all(&repo_path).await.map_err(|e| {
                    AtmosError::mst(format!("Failed to create repo directory: {}", e))
                })?;

                let storage = crate::mst::storage::fjall::FjallMstStorage::open_with_config(
                    &repo_path,
                    config.clone(),
                )
                .await?;
                Ok(StorageWrapper::Fjall(storage))
            }
        }
    }

    /// Evict the least recently used repository
    async fn evict_lru_repo(&self, repos: &mut HashMap<RepoId, StorageWrapper>) {
        let mut oldest_repo = None;
        let mut oldest_time = std::time::Instant::now();

        // Find the least recently used repo
        for repo_id in repos.keys() {
            if let Some(entry) = self.access_tracker.get(repo_id) {
                let access_time = *entry.value();
                if access_time < oldest_time {
                    oldest_time = access_time;
                    oldest_repo = Some(repo_id.clone());
                }
            }
        }

        // Evict it
        if let Some(repo_id) = oldest_repo {
            repos.remove(&repo_id);
            self.access_tracker.remove(&repo_id);
            // Could add logging here if needed
        }
    }
}

impl Drop for MultiTenantMstStorage {
    fn drop(&mut self) {
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
        }
    }
}

/// Statistics for a specific repository
#[derive(Debug, Clone)]
pub struct RepoStats {
    pub repo_id: RepoId,
    pub node_count: usize,
    pub last_access: std::time::Instant,
    pub is_active: bool,
}

/// Global statistics across all repositories
#[derive(Debug, Clone)]
pub struct GlobalStats {
    pub active_repos: usize,
    pub total_tracked_repos: usize,
    pub total_nodes: usize,
    pub max_active_repos: usize,
}

/// Builder for multi-tenant configuration
pub struct MultiTenantConfigBuilder {
    config: MultiTenantConfig,
}

impl MultiTenantConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: MultiTenantConfig::default(),
        }
    }

    pub fn max_active_repos(mut self, max: usize) -> Self {
        self.config.max_active_repos = max;
        self
    }

    pub fn memory_backend(mut self) -> Self {
        self.config.backend = StorageBackend::Memory;
        self
    }

    #[cfg(feature = "fjall")]
    pub fn fjall_backend<P: AsRef<std::path::Path>>(
        mut self,
        base_path: P,
        config: crate::mst::storage::fjall::FjallStorageConfig,
    ) -> Self {
        self.config.backend = StorageBackend::Fjall {
            base_path: base_path.as_ref().to_path_buf(),
            config,
        };
        self
    }

    pub fn eviction_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config.lifecycle.eviction_timeout = timeout;
        self
    }

    pub fn cleanup_interval(mut self, interval: std::time::Duration) -> Self {
        self.config.lifecycle.cleanup_interval = interval;
        self
    }

    pub fn sync_interval(mut self, interval: std::time::Duration) -> Self {
        self.config.lifecycle.sync_interval = interval;
        self
    }

    pub fn build(self) -> MultiTenantConfig {
        self.config
    }
}

impl Default for MultiTenantConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience wrapper for managing multiple repositories
pub struct PdsStorage {
    multi_tenant: MultiTenantMstStorage,
}

impl PdsStorage {
    /// Create a new PDS storage manager
    pub async fn new(config: MultiTenantConfig) -> Result<Self> {
        let mut multi_tenant = MultiTenantMstStorage::new(config).await?;
        multi_tenant.start_background_tasks();

        Ok(Self { multi_tenant })
    }

    /// Get an MST for a user's repository by their DID
    pub async fn get_user_repo(&self, did: &str) -> Result<Mst<StorageWrapper>> {
        let repo_id = RepoId::from_did(did);
        self.multi_tenant.get_repo_mst(&repo_id).await
    }

    /// Create a new user repository
    pub async fn create_user_repo(&self, did: &str) -> Result<Mst<StorageWrapper>> {
        // For now, this is the same as get_user_repo since we auto-create
        self.get_user_repo(did).await
    }

    /// Delete a user repository (careful!)
    pub async fn delete_user_repo(&self, did: &str) -> Result<()> {
        let repo_id = RepoId::from_did(did);

        // Evict from memory first
        self.multi_tenant.evict_repo(&repo_id).await?;

        // For persistent backends, would need to delete the underlying storage
        // This is intentionally not implemented for safety
        Err(AtmosError::mst(
            "Repository deletion not implemented for safety. Manual filesystem cleanup required."
                .to_string(),
        ))
    }

    /// Get statistics for a user's repository
    pub async fn get_user_stats(&self, did: &str) -> Result<RepoStats> {
        let repo_id = RepoId::from_did(did);
        self.multi_tenant.get_repo_stats(&repo_id).await
    }

    /// List all user repositories
    pub async fn list_users(&self) -> Result<Vec<String>> {
        let repo_ids = self.multi_tenant.list_repos().await?;
        Ok(repo_ids.into_iter().map(|id| id.0).collect())
    }

    /// Get global PDS statistics
    pub async fn get_stats(&self) -> Result<GlobalStats> {
        self.multi_tenant.get_global_stats().await
    }

    /// Sync all user repositories
    pub async fn sync_all_users(&self) -> Result<()> {
        self.multi_tenant.sync_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Bytes;
    use crate::mst::node::{MstNode, MstNodeLeaf};
    use cid::Cid;
    use multihash::Multihash;
    use sha2::Digest;

    fn create_test_cid(data: &str) -> Cid {
        let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(data.as_bytes())).unwrap();
        Cid::new_v1(0x71, hash)
    }

    fn create_test_node(key: &str) -> MstNode {
        let entry = MstNodeLeaf {
            p: 0,
            k: Bytes::from(key.as_bytes().to_vec()),
            v: create_test_cid("value"),
            t: None,
        };
        MstNode::new(None, vec![entry])
    }

    #[tokio::test]
    async fn test_multi_tenant_basic_operations() {
        let config = MultiTenantConfig::default();
        let pds = PdsStorage::new(config).await.unwrap();

        // Create repositories for different users
        let alice_mst = pds.get_user_repo("did:example:alice").await.unwrap();
        let bob_mst = pds.get_user_repo("did:example:bob").await.unwrap();

        // Add data to Alice's repo
        let alice_cid = create_test_cid("alice_node");
        let alice_node = create_test_node("alice_key");
        alice_mst.insert_node(alice_cid, alice_node).await.unwrap();

        // Add data to Bob's repo
        let bob_cid = create_test_cid("bob_node");
        let bob_node = create_test_node("bob_key");
        bob_mst.insert_node(bob_cid, bob_node).await.unwrap();

        // Verify isolation
        assert!(alice_mst.get_node(&alice_cid).await.unwrap().is_some());
        assert!(alice_mst.get_node(&bob_cid).await.unwrap().is_none());

        assert!(bob_mst.get_node(&bob_cid).await.unwrap().is_some());
        assert!(bob_mst.get_node(&alice_cid).await.unwrap().is_none());

        // Check statistics
        let alice_stats = pds.get_user_stats("did:example:alice").await.unwrap();
        assert_eq!(alice_stats.node_count, 1);
        assert!(alice_stats.is_active);

        let global_stats = pds.get_stats().await.unwrap();
        assert_eq!(global_stats.active_repos, 2);
        assert_eq!(global_stats.total_nodes, 2);
    }

    #[tokio::test]
    async fn test_repo_eviction() {
        let config = MultiTenantConfigBuilder::new()
            .max_active_repos(2)
            .eviction_timeout(std::time::Duration::from_millis(100))
            .build();

        let multi_tenant = MultiTenantMstStorage::new(config).await.unwrap();

        // Create 3 repositories (should trigger eviction)
        let _repo1 = multi_tenant
            .get_repo_mst(&RepoId::new("user1"))
            .await
            .unwrap();
        let _repo2 = multi_tenant
            .get_repo_mst(&RepoId::new("user2"))
            .await
            .unwrap();

        // Sleep to make repo1 and repo2 "old"
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let _repo3 = multi_tenant
            .get_repo_mst(&RepoId::new("user3"))
            .await
            .unwrap();

        // Should have evicted one repo due to capacity limit
        let stats = multi_tenant.get_global_stats().await.unwrap();
        assert_eq!(stats.active_repos, 2);
    }

    #[tokio::test]
    async fn test_config_builder() {
        let config = MultiTenantConfigBuilder::new()
            .max_active_repos(500)
            .eviction_timeout(std::time::Duration::from_secs(1800))
            .cleanup_interval(std::time::Duration::from_secs(120))
            .memory_backend()
            .build();

        assert_eq!(config.max_active_repos, 500);
        assert_eq!(
            config.lifecycle.eviction_timeout,
            std::time::Duration::from_secs(1800)
        );
        assert_eq!(
            config.lifecycle.cleanup_interval,
            std::time::Duration::from_secs(120)
        );
        assert!(matches!(config.backend, StorageBackend::Memory));
    }
}
