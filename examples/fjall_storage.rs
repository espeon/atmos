//! Example demonstrating Fjall-based persistent MST storage
//!
//! This example shows how to:
//! - Create a Fjall-backed MST storage
//! - Configure caching and compression
//! - Perform basic MST operations with persistence
//! - Use batch operations for efficiency
//! - Monitor storage statistics

#[cfg(feature = "fjall")]
use atmst::{
    Bytes, CarBuilder, Cid, Result,
    mst::{
        Mst,
        node::{MstNode, MstNodeLeaf},
        storage::MstStorage,
    },
};

#[cfg(feature = "fjall")]
use atmst::{CompressionAlgorithm, FjallMstStorage, FjallStorageConfig, SerializationFormat};

#[cfg(feature = "fjall")]
use multihash::Multihash;
#[cfg(feature = "fjall")]
use sha2::Digest;
#[cfg(feature = "fjall")]
use std::path::Path;
#[cfg(feature = "fjall")]
use tempfile::TempDir;

#[cfg(feature = "fjall")]
fn create_test_cid(data: &str) -> Cid {
    let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(data.as_bytes())).unwrap();
    Cid::new_v1(0x71, hash)
}

#[cfg(feature = "fjall")]
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

#[cfg(feature = "fjall")]
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Fjall MST Storage Example");
    println!("=============================\n");

    // Create temporary directory for this example
    let temp_dir = TempDir::new()
        .map_err(|e| atmst::AtmosError::mst(format!("Failed to create temp dir: {}", e)))?;
    let storage_path = temp_dir.path();

    println!("📁 Storage path: {}\n", storage_path.display());

    // Example 1: Basic Configuration
    println!("1️⃣  Basic Fjall Storage");
    println!("----------------------");
    basic_fjall_example(storage_path).await?;

    // Example 2: Advanced Configuration
    println!("\n2️⃣  Advanced Configuration");
    println!("--------------------------");
    advanced_configuration_example(storage_path).await?;

    // Example 3: Persistence Demonstration
    println!("\n3️⃣  Persistence Example");
    println!("-----------------------");
    persistence_example(storage_path).await?;

    // Example 4: Performance with Batching
    println!("\n4️⃣  Batch Operations");
    println!("-------------------");
    batch_operations_example(storage_path).await?;

    // Example 5: Storage Statistics
    println!("\n5️⃣  Storage Statistics");
    println!("---------------------");
    statistics_example(storage_path).await?;

    println!("\n✅ All examples completed successfully!");
    println!("🗂️  Data persisted to: {}", storage_path.display());

    Ok(())
}

#[cfg(feature = "fjall")]
async fn basic_fjall_example(base_path: &Path) -> Result<()> {
    let storage_path = base_path.join("basic");

    // Create Fjall storage with default configuration
    let storage = FjallMstStorage::open(&storage_path).await?;
    let mst = Mst::with_storage(storage);

    // Create some test data
    let cid1 = create_test_cid("node1");
    let node1 = create_test_node(vec![
        (
            0,
            "app.bsky.feed.post/abc123".to_string(),
            create_test_cid("post1"),
            None,
        ),
        (
            0,
            "app.bsky.feed.post/def456".to_string(),
            create_test_cid("post2"),
            None,
        ),
    ]);

    println!("   📝 Inserting node with 2 entries...");
    mst.insert_node(cid1, node1).await?;

    println!("   🔍 Retrieving node...");
    let retrieved = mst.get_node(&cid1).await?;
    match retrieved {
        Some(node) => println!("   ✅ Retrieved node with {} entries", node.entries().len()),
        None => println!("   ❌ Node not found!"),
    }

    println!("   📊 Storage length: {}", mst.storage.len().await?);

    Ok(())
}

#[cfg(feature = "fjall")]
async fn advanced_configuration_example(base_path: &Path) -> Result<()> {
    let storage_path = base_path.join("advanced");

    // Create advanced configuration
    let config = FjallStorageConfig::new()
        .with_cache(true, 5000) // Enable cache with 5k capacity
        .with_serialization(SerializationFormat::Cbor) // Use CBOR for IPLD compatibility
        .with_compression(true, CompressionAlgorithm::Zstd) // ZSTD compression
        .with_sync_on_commit(true); // Sync writes for durability

    println!("   ⚙️  Configuration:");
    println!("      • Cache: enabled (5,000 entries)");
    println!("      • Serialization: CBOR");
    println!("      • Compression: ZSTD");
    println!("      • Sync on commit: enabled");

    let storage = FjallMstStorage::open_with_config(&storage_path, config).await?;
    let mst = Mst::with_storage(storage);

    // Test with compression-friendly data (repeated patterns)
    let cid = create_test_cid("compressed_node");
    let node = create_test_node(vec![
        (
            0,
            "app.bsky.feed.like/user1_post1".to_string(),
            create_test_cid("like1"),
            None,
        ),
        (
            21,
            "user1_post2".to_string(),
            create_test_cid("like2"),
            None,
        ), // Shared prefix
        (
            21,
            "user1_post3".to_string(),
            create_test_cid("like3"),
            None,
        ), // Shared prefix
        (
            21,
            "user2_post1".to_string(),
            create_test_cid("like4"),
            None,
        ), // Shared prefix
    ]);

    println!("   📝 Inserting compressed node...");
    mst.insert_node(cid, node).await?;

    // Test cache effectiveness
    println!("   🔍 Testing cache (first access)...");
    let start = std::time::Instant::now();
    let _node1 = mst.get_node(&cid).await?;
    let first_access = start.elapsed();

    println!("   🔍 Testing cache (second access)...");
    let start = std::time::Instant::now();
    let _node2 = mst.get_node(&cid).await?;
    let second_access = start.elapsed();

    println!("   ⚡ Cache performance:");
    println!("      • First access: {:?}", first_access);
    println!("      • Second access: {:?}", second_access);
    println!(
        "      • Speedup: {:.2}x",
        first_access.as_nanos() as f64 / second_access.as_nanos() as f64
    );

    Ok(())
}

#[cfg(feature = "fjall")]
async fn persistence_example(base_path: &Path) -> Result<()> {
    let storage_path = base_path.join("persistence");
    let test_cid = create_test_cid("persistent_node");

    // Phase 1: Write data
    println!("   💾 Phase 1: Writing data...");
    {
        let storage = FjallMstStorage::open(&storage_path).await?;
        let mst = Mst::with_storage(storage);

        let node = create_test_node(vec![
            (
                0,
                "persistent.record.1".to_string(),
                create_test_cid("record1"),
                None,
            ),
            (17, "2".to_string(), create_test_cid("record2"), None),
            (17, "3".to_string(), create_test_cid("record3"), None),
        ]);

        mst.insert_node(test_cid, node).await?;
        println!("      ✅ Data written and storage closed");
    }

    // Phase 2: Read data from disk
    println!("   📖 Phase 2: Reading persisted data...");
    {
        let storage = FjallMstStorage::open(&storage_path).await?;
        let mst = Mst::with_storage(storage);

        match mst.get_node(&test_cid).await? {
            Some(node) => {
                println!("      ✅ Data successfully persisted!");
                println!(
                    "      📊 Entries in persisted node: {}",
                    node.entries().len()
                );

                // Display the entries
                for (i, entry) in node.entries().iter().enumerate() {
                    let key = String::from_utf8_lossy(&entry.k);
                    println!("         {}. Key: {}, Value CID: {}", i + 1, key, entry.v);
                }
            }
            None => println!("      ❌ Data not found - persistence failed!"),
        }
    }

    Ok(())
}

#[cfg(feature = "fjall")]
async fn batch_operations_example(base_path: &Path) -> Result<()> {
    let storage_path = base_path.join("batch");
    let storage = FjallMstStorage::open(&storage_path).await?;

    // Prepare batch data
    let batch_size = 1000;
    println!("   📦 Preparing {} nodes for batch insert...", batch_size);

    let mut nodes = Vec::new();
    for i in 0..batch_size {
        let cid = create_test_cid(&format!("batch_node_{}", i));
        let node = create_test_node(vec![(
            0,
            format!("batch.record.{}", i),
            create_test_cid(&format!("value_{}", i)),
            None,
        )]);
        nodes.push((cid, node));
    }

    // Measure batch insert performance
    println!("   ⚡ Performing batch insert...");
    let start = std::time::Instant::now();
    storage.batch_insert(nodes.clone()).await?;
    let batch_insert_time = start.elapsed();

    println!("   📊 Batch insert results:");
    println!(
        "      • {} nodes inserted in {:?}",
        batch_size, batch_insert_time
    );
    println!(
        "      • Average: {:.2} nodes/ms",
        batch_size as f64 / batch_insert_time.as_millis() as f64
    );
    println!("      • Total storage length: {}", storage.len().await?);

    // Test batch removal
    let remove_count = 500;
    let cids_to_remove: Vec<_> = nodes
        .iter()
        .take(remove_count)
        .map(|(cid, _)| *cid)
        .collect();

    println!("   🗑️  Performing batch remove ({} nodes)...", remove_count);
    let start = std::time::Instant::now();
    let removed = storage.batch_remove(cids_to_remove).await?;
    let batch_remove_time = start.elapsed();

    println!("   📊 Batch remove results:");
    println!(
        "      • {} nodes removed in {:?}",
        removed.len(),
        batch_remove_time
    );
    println!("      • Remaining storage length: {}", storage.len().await?);

    Ok(())
}

#[cfg(feature = "fjall")]
async fn statistics_example(base_path: &Path) -> Result<()> {
    let storage_path = base_path.join("stats");
    let storage = FjallMstStorage::open(&storage_path).await?;

    // Add some varied data
    println!("   📊 Adding sample data for statistics...");

    // Small nodes
    for i in 0..100 {
        let cid = create_test_cid(&format!("small_{}", i));
        let node = create_test_node(vec![(
            0,
            format!("small.{}", i),
            create_test_cid(&format!("val_{}", i)),
            None,
        )]);
        storage.insert_node(cid, node).await?;
    }

    // Large nodes with multiple entries
    for i in 0..20 {
        let cid = create_test_cid(&format!("large_{}", i));
        let mut entries = Vec::new();
        for j in 0..10 {
            entries.push((
                0,
                format!("large.{}.entry.{}", i, j),
                create_test_cid(&format!("large_val_{}_{}", i, j)),
                None,
            ));
        }
        let node = create_test_node(entries);
        storage.insert_node(cid, node).await?;
    }

    // Get and display statistics
    let stats = storage.stats().await?;

    println!("   📈 Storage Statistics:");
    println!("      • Total keys: {}", stats.total_keys);
    println!(
        "      • Disk usage: {} bytes ({:.2} KB)",
        stats.disk_usage_bytes,
        stats.disk_usage_bytes as f64 / 1024.0
    );

    if let Some(cache_stats) = &stats.cache_stats {
        println!("      • Cache capacity: {}", cache_stats.capacity);
        println!("      • Cache usage: {}", cache_stats.len);
        println!(
            "      • Cache utilization: {:.1}%",
            (cache_stats.len as f64 / cache_stats.capacity as f64) * 100.0
        );
    }

    // Test compaction
    println!("   🗜️  Running compaction...");
    let start = std::time::Instant::now();
    storage.compact().await?;
    let compact_time = start.elapsed();

    let stats_after = storage.stats().await?;
    println!("   📉 Post-compaction statistics:");
    println!("      • Compaction time: {:?}", compact_time);
    println!(
        "      • Disk usage: {} bytes ({:.2} KB)",
        stats_after.disk_usage_bytes,
        stats_after.disk_usage_bytes as f64 / 1024.0
    );

    let savings = stats
        .disk_usage_bytes
        .saturating_sub(stats_after.disk_usage_bytes);
    if savings > 0 {
        println!(
            "      • Space saved: {} bytes ({:.1}%)",
            savings,
            (savings as f64 / stats.disk_usage_bytes as f64) * 100.0
        );
    }

    Ok(())
}

#[cfg(not(feature = "fjall"))]
fn main() {
    println!("❌ This example requires the 'fjall' feature to be enabled.");
    println!("   Run with: cargo run --example fjall_storage --features fjall");
}
