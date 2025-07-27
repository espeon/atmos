//! Example: Building a Personal Data Server (PDS) with Multi-User MST Storage
//!
//! This example demonstrates how to build a PDS that can handle multiple users,
//! each with their own isolated MST repository. It shows:
//!
//! - Setting up multi-tenant storage
//! - Managing user repositories
//! - Handling concurrent user operations
//! - Repository lifecycle management
//! - Performance monitoring and statistics
//! - Integration with AT Protocol patterns

use atmst::{
    Bytes, Cid, MstStorage, Result,
    mst::{
        node::{MstNode, MstNodeLeaf},
        storage::{MultiTenantConfigBuilder, PdsStorage},
    },
};

#[cfg(feature = "fjall")]
use atmst::{CompressionAlgorithm, FjallStorageConfig, SerializationFormat};

use futures::future::try_join_all;
use multihash::Multihash;
use sha2::Digest;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

// Simulate AT Protocol record types
#[derive(Debug, Clone)]
struct AtProtoRecord {
    collection: String,
    rkey: String,
    content: serde_json::Value,
}

impl AtProtoRecord {
    fn new(collection: &str, rkey: &str, content: serde_json::Value) -> Self {
        Self {
            collection: collection.to_string(),
            rkey: rkey.to_string(),
            content,
        }
    }

    fn to_mst_key(&self) -> String {
        format!("{}/{}", self.collection, self.rkey)
    }

    fn create_cid(&self) -> Cid {
        let data = serde_json::to_string(&self.content).unwrap();
        let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(data.as_bytes())).unwrap();
        Cid::new_v1(0x71, hash)
    }
}

fn create_test_cid(data: &str) -> Cid {
    let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(data.as_bytes())).unwrap();
    Cid::new_v1(0x71, hash)
}

fn create_mst_node_for_record(record: &AtProtoRecord) -> MstNode {
    let entry = MstNodeLeaf {
        p: 0,
        k: Bytes::from(record.to_mst_key().into_bytes()),
        v: record.create_cid(),
        t: None,
    };
    MstNode::new(None, vec![entry])
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🏗️  Personal Data Server (PDS) Multi-Repo Example");
    println!("=================================================\n");

    // Initialize logging
    env_logger::init();

    // Run different scenarios
    println!("1️⃣  Basic Multi-User Setup");
    println!("---------------------------");
    basic_multi_user_example().await?;

    println!("\n2️⃣  High-Scale User Simulation");
    println!("------------------------------");
    high_scale_simulation().await?;

    println!("\n3️⃣  Repository Lifecycle Management");
    println!("-----------------------------------");
    lifecycle_management_example().await?;

    #[cfg(feature = "fjall")]
    {
        println!("\n4️⃣  Persistent Storage with Fjall");
        println!("----------------------------------");
        persistent_storage_example().await?;
    }

    println!("\n5️⃣  Concurrent Operations Stress Test");
    println!("-------------------------------------");
    concurrent_operations_test().await?;

    println!("\n6️⃣  PDS Statistics and Monitoring");
    println!("----------------------------------");
    statistics_and_monitoring().await?;

    println!("\n✅ All PDS examples completed successfully!");

    Ok(())
}

async fn basic_multi_user_example() -> Result<()> {
    // Create PDS with memory storage for quick demo
    let config = MultiTenantConfigBuilder::new()
        .max_active_repos(100)
        .memory_backend()
        .eviction_timeout(Duration::from_secs(300))
        .build();

    let pds = PdsStorage::new(config).await?;

    // Simulate different users creating content
    let users = vec!["did:plc:alice123", "did:plc:bob456", "did:plc:carol789"];

    println!("   👥 Creating repositories for {} users...", users.len());

    for user_did in &users {
        let repo = pds.get_user_repo(user_did).await?;

        // Create some typical AT Proto records
        let records = vec![
            AtProtoRecord::new(
                "app.bsky.actor.profile",
                "self",
                serde_json::json!({
                    "displayName": format!("User {}", &user_did[8..]),
                    "description": "Example user profile"
                }),
            ),
            AtProtoRecord::new(
                "app.bsky.feed.post",
                "3kj5b2c1a9f8",
                serde_json::json!({
                    "text": "Hello from my PDS!",
                    "createdAt": "2024-01-01T12:00:00Z"
                }),
            ),
            AtProtoRecord::new(
                "app.bsky.feed.like",
                "3kj5b2c1a9f9",
                serde_json::json!({
                    "subject": "at://did:plc:someone/app.bsky.feed.post/abc123",
                    "createdAt": "2024-01-01T12:01:00Z"
                }),
            ),
        ];

        // Insert records into the user's MST
        for record in records {
            let cid = create_test_cid(&format!("{}_{}", user_did, record.rkey));
            let node = create_mst_node_for_record(&record);
            repo.insert_node(cid, node).await?;
        }

        println!("      ✅ Created repository for {}", user_did);
    }

    // Verify isolation between users
    println!("   🔒 Verifying user isolation...");

    let alice_repo = pds.get_user_repo("did:plc:alice123").await?;
    let bob_repo = pds.get_user_repo("did:plc:bob456").await?;

    let alice_nodes = alice_repo.storage.len().await?;
    let bob_nodes = bob_repo.storage.len().await?;

    println!("      • Alice's repo: {} nodes", alice_nodes);
    println!("      • Bob's repo: {} nodes", bob_nodes);

    // Check that they don't see each other's data
    let alice_cid = create_test_cid("did:plc:alice123_3kj5b2c1a9f8");
    assert!(alice_repo.get_node(&alice_cid).await?.is_some());
    assert!(bob_repo.get_node(&alice_cid).await?.is_none());

    println!("      ✅ User isolation verified!");

    Ok(())
}

async fn high_scale_simulation() -> Result<()> {
    let config = MultiTenantConfigBuilder::new()
        .max_active_repos(50) // Limit for demo
        .memory_backend()
        .eviction_timeout(Duration::from_secs(30))
        .cleanup_interval(Duration::from_secs(5))
        .build();

    let pds = PdsStorage::new(config).await?;

    let user_count = 100;
    let records_per_user = 10;

    println!(
        "   🚀 Simulating {} users with {} records each...",
        user_count, records_per_user
    );

    let start_time = Instant::now();

    // Create users concurrently (but limit concurrency)
    let semaphore = Arc::new(Semaphore::new(10)); // Max 10 concurrent operations

    let tasks: Vec<_> = (0..user_count)
        .map(|i| {
            let pds = &pds;
            let sem = Arc::clone(&semaphore);

            async move {
                let _permit = sem.acquire().await.unwrap();

                let user_did = format!("did:plc:user{:03}", i);
                let repo = pds.get_user_repo(&user_did).await?;

                // Create records for this user
                for j in 0..records_per_user {
                    let record = AtProtoRecord::new(
                        "app.bsky.feed.post",
                        &format!("post{:03}", j),
                        serde_json::json!({
                            "text": format!("Post {} from user {}", j, i),
                            "createdAt": "2024-01-01T12:00:00Z"
                        }),
                    );

                    let cid = create_test_cid(&format!("{}_{}", user_did, record.rkey));
                    let node = create_mst_node_for_record(&record);
                    repo.insert_node(cid, node).await?;
                }

                Ok::<_, atmst::AtmosError>(user_did)
            }
        })
        .collect();

    let results = try_join_all(tasks).await?;
    let elapsed = start_time.elapsed();

    println!("   📊 High-scale simulation results:");
    println!("      • {} users created in {:?}", results.len(), elapsed);
    println!("      • {} total records", user_count * records_per_user);
    println!(
        "      • {:.2} users/sec",
        user_count as f64 / elapsed.as_secs_f64()
    );

    // Check final statistics
    let stats = pds.get_stats().await?;
    println!(
        "      • Active repos: {}/{}",
        stats.active_repos, stats.max_active_repos
    );
    println!("      • Total tracked: {}", stats.total_tracked_repos);
    println!("      • Total nodes: {}", stats.total_nodes);

    Ok(())
}

async fn lifecycle_management_example() -> Result<()> {
    let config = MultiTenantConfigBuilder::new()
        .max_active_repos(3) // Very small for demo
        .memory_backend()
        .eviction_timeout(Duration::from_millis(500)) // Fast eviction
        .cleanup_interval(Duration::from_millis(200))
        .build();

    let pds = PdsStorage::new(config).await?;

    println!("   ⏰ Testing repository lifecycle management...");

    // Create several repositories
    let users = ["alice", "bob", "charlie", "diana", "eve"];

    for user in &users {
        let repo = pds.get_user_repo(&format!("did:plc:{}", user)).await?;
        let cid = create_test_cid(&format!("{}_data", user));
        let node = create_mst_node_for_record(&AtProtoRecord::new(
            "app.bsky.actor.profile",
            "self",
            serde_json::json!({"displayName": user}),
        ));
        repo.insert_node(cid, node).await?;

        println!("      ✅ Created repo for {}", user);
    }

    // Check initial state
    let stats = pds.get_stats().await?;
    println!("      📊 Active repos: {}", stats.active_repos);

    // Wait for eviction to occur
    println!("      ⏳ Waiting for automatic eviction...");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let stats_after = pds.get_stats().await?;
    println!(
        "      📊 Active repos after eviction: {}",
        stats_after.active_repos
    );
    println!(
        "      📊 Total tracked: {}",
        stats_after.total_tracked_repos
    );

    // Verify that evicted repos can still be accessed (reloaded)
    println!("      🔄 Testing repository reloading...");
    let alice_repo = pds.get_user_repo("did:plc:alice").await?;
    let alice_nodes = alice_repo.storage.len().await?;
    println!("      ✅ Alice's repo reloaded with {} nodes", alice_nodes);

    Ok(())
}

#[cfg(feature = "fjall")]
async fn persistent_storage_example() -> Result<()> {
    use tempfile::TempDir;

    let temp_dir = TempDir::new()
        .map_err(|e| atmst::AtmosError::mst(format!("Failed to create temp dir: {}", e)))?;

    let fjall_config = FjallStorageConfig::new()
        .with_cache(true, 1000)
        .with_serialization(SerializationFormat::Cbor)
        .with_compression(true, CompressionAlgorithm::Lz4)
        .with_sync_on_commit(false);

    let config = MultiTenantConfigBuilder::new()
        .max_active_repos(10)
        .fjall_backend(temp_dir.path(), fjall_config)
        .eviction_timeout(Duration::from_secs(300))
        .build();

    println!("   💾 Testing persistent storage at: {:?}", temp_dir.path());

    // Phase 1: Create data and close PDS
    {
        let pds = PdsStorage::new(config.clone()).await?;

        let users = ["persistent_alice", "persistent_bob"];
        for user in &users {
            let repo = pds.get_user_repo(&format!("did:plc:{}", user)).await?;

            // Create substantial data
            for i in 0..20 {
                let record = AtProtoRecord::new(
                    "app.bsky.feed.post",
                    &format!("post{:03}", i),
                    serde_json::json!({
                        "text": format!("Persistent post {} from {}", i, user),
                        "createdAt": "2024-01-01T12:00:00Z"
                    }),
                );

                let cid = create_test_cid(&format!("{}_{}", user, record.rkey));
                let node = create_mst_node_for_record(&record);
                repo.insert_node(cid, node).await?;
            }

            println!("      ✅ Created persistent data for {}", user);
        }

        let stats = pds.get_stats().await?;
        println!(
            "      📊 Phase 1 - Active repos: {}, Total nodes: {}",
            stats.active_repos, stats.total_nodes
        );
    }

    // Phase 2: Reopen PDS and verify persistence
    {
        println!("      🔄 Reopening PDS to test persistence...");
        let pds = PdsStorage::new(config).await?;

        for user in &["persistent_alice", "persistent_bob"] {
            let repo = pds.get_user_repo(&format!("did:plc:{}", user)).await?;
            let node_count = repo.storage.len().await?;
            println!(
                "      ✅ Restored {}'s repo with {} nodes",
                user, node_count
            );
            assert_eq!(node_count, 20);
        }

        // Verify we can still add new data
        let repo = pds.get_user_repo("did:plc:persistent_alice").await?;
        let new_cid = create_test_cid("new_persistent_data");
        let new_node = create_mst_node_for_record(&AtProtoRecord::new(
            "app.bsky.feed.post",
            "new_post",
            serde_json::json!({"text": "New post after restart"}),
        ));
        repo.insert_node(new_cid, new_node).await?;

        let final_count = repo.storage.len().await?;
        println!("      ✅ Added new data, final count: {}", final_count);
        assert_eq!(final_count, 21);

        println!("      🎉 Persistence test completed successfully!");
    }

    Ok(())
}

async fn concurrent_operations_test() -> Result<()> {
    let config = MultiTenantConfigBuilder::new()
        .max_active_repos(20)
        .memory_backend()
        .build();

    let pds = Arc::new(PdsStorage::new(config).await?);

    println!("   🔄 Testing concurrent operations across multiple users...");

    let num_users = 10;
    let operations_per_user = 50;
    let concurrent_limit = 20;

    let semaphore = Arc::new(Semaphore::new(concurrent_limit));
    let start_time = Instant::now();

    // Spawn concurrent tasks for each user
    let tasks: Vec<_> = (0..num_users)
        .map(|user_id| {
            let pds = Arc::clone(&pds);
            let sem = Arc::clone(&semaphore);

            tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                let user_did = format!("did:plc:concurrent_user_{}", user_id);
                let repo = pds.get_user_repo(&user_did).await?;

                let mut operation_count = 0;

                // Perform mixed read/write operations
                for i in 0_usize..operations_per_user {
                    if i % 3 == 0 {
                        // Write operation
                        let record = AtProtoRecord::new(
                            "app.bsky.feed.post",
                            &format!("concurrent_post_{}", i),
                            serde_json::json!({
                                "text": format!("Concurrent post {} from user {}", i, user_id),
                                "createdAt": "2024-01-01T12:00:00Z"
                            }),
                        );

                        let cid = create_test_cid(&format!("{}_{}", user_did, i));
                        let node = create_mst_node_for_record(&record);
                        repo.insert_node(cid, node).await?;
                        operation_count += 1;
                    } else {
                        // Read operation
                        let cid = create_test_cid(&format!(
                            "{}_{}",
                            user_did,
                            if i > 0 { i - 1 } else { 0 }
                        ));
                        let _node = repo.get_node(&cid).await?;
                        operation_count += 1;
                    }
                }

                Ok::<(String, usize), atmst::AtmosError>((user_did, operation_count))
            })
        })
        .collect();

    // Wait for all tasks to complete
    let mut total_operations = 0;
    for task in tasks {
        let (user_did, ops) = task.await.unwrap()?;
        total_operations += ops;
        println!("      ✅ User {} completed {} operations", user_did, ops);
    }

    let elapsed = start_time.elapsed();
    let ops_per_sec = total_operations as f64 / elapsed.as_secs_f64();

    println!("   📊 Concurrent operations results:");
    println!("      • Total operations: {}", total_operations);
    println!("      • Duration: {:?}", elapsed);
    println!("      • Operations/sec: {:.2}", ops_per_sec);
    println!(
        "      • Avg latency: {:.2}ms",
        elapsed.as_millis() as f64 / total_operations as f64
    );

    let final_stats = pds.get_stats().await?;
    println!("      • Final active repos: {}", final_stats.active_repos);
    println!("      • Total nodes: {}", final_stats.total_nodes);

    Ok(())
}

async fn statistics_and_monitoring() -> Result<()> {
    let config = MultiTenantConfigBuilder::new()
        .max_active_repos(5)
        .memory_backend()
        .eviction_timeout(Duration::from_secs(10))
        .cleanup_interval(Duration::from_secs(2))
        .build();

    let pds = PdsStorage::new(config).await?;

    println!("   📈 Demonstrating PDS monitoring and statistics...");

    // Create diverse user repositories
    let user_data = vec![
        ("alice", 15),  // Heavy user
        ("bob", 8),     // Medium user
        ("charlie", 3), // Light user
        ("diana", 25),  // Very heavy user
        ("eve", 1),     // Minimal user
    ];

    for (user, record_count) in &user_data {
        let repo = pds.get_user_repo(&format!("did:plc:{}", user)).await?;

        for i in 0..*record_count {
            let record = AtProtoRecord::new(
                "app.bsky.feed.post",
                &format!("post_{:03}", i),
                serde_json::json!({
                    "text": format!("Post {} from {}", i, user),
                    "createdAt": "2024-01-01T12:00:00Z"
                }),
            );

            let cid = create_test_cid(&format!("{}_{}", user, i));
            let node = create_mst_node_for_record(&record);
            repo.insert_node(cid, node).await?;
        }

        println!("      ✅ Created {} records for {}", record_count, user);
    }

    // Display detailed statistics
    println!("\n   📊 Global PDS Statistics:");
    let global_stats = pds.get_stats().await?;
    println!(
        "      • Active repositories: {}/{}",
        global_stats.active_repos, global_stats.max_active_repos
    );
    println!(
        "      • Total tracked repositories: {}",
        global_stats.total_tracked_repos
    );
    println!(
        "      • Total nodes across all repos: {}",
        global_stats.total_nodes
    );

    println!("\n   👤 Per-User Statistics:");
    let users = pds.list_users().await?;
    let mut user_stats = Vec::new();

    for user_did in users {
        let stats = pds.get_user_stats(&user_did).await?;
        user_stats.push((user_did.clone(), stats.clone()));

        let status = if stats.is_active {
            "🟢 Active"
        } else {
            "🔘 Inactive"
        };
        println!(
            "      • {}: {} nodes, {} - Last access: {:?} ago",
            user_did,
            stats.node_count,
            status,
            stats.last_access.elapsed()
        );
    }

    // Sort by node count to show top users
    user_stats.sort_by(|a, b| b.1.node_count.cmp(&a.1.node_count));

    println!("\n   🏆 Top Users by Repository Size:");
    for (i, (user_did, stats)) in user_stats.iter().take(3).enumerate() {
        println!("      {}. {}: {} nodes", i + 1, user_did, stats.node_count);
    }

    // Demonstrate memory management
    println!("\n   🧹 Testing Memory Management:");

    // Access only some users to trigger eviction
    for user in &["alice", "diana"] {
        let _repo = pds.get_user_repo(&format!("did:plc:{}", user)).await?;
        println!("      ♻️  Accessed {}'s repository", user);
    }

    // Wait for cleanup cycle
    tokio::time::sleep(Duration::from_secs(3)).await;

    let stats_after_cleanup = pds.get_stats().await?;
    println!(
        "      📊 After cleanup - Active repos: {}/{}",
        stats_after_cleanup.active_repos, stats_after_cleanup.max_active_repos
    );

    // Show that evicted repos can still be accessed
    let eve_repo = pds.get_user_repo("did:plc:eve").await?;
    let eve_nodes = eve_repo.storage.len().await?;
    println!("      🔄 Reloaded eve's repository: {} nodes", eve_nodes);

    println!("\n   ✅ Monitoring and statistics demonstration complete!");

    Ok(())
}
