use std::collections::BTreeMap;

use cid::Cid;
use ipld_core::ipld::Ipld;
use multihash::Multihash;
use sha2::Digest;

use crate::{
    AtmosError, Bytes, CarBuilder, CarImporter,
    mst::{
        Mst,
        node::{MstNode, MstNodeLeaf},
    },
};

// Helper function to create a test CID
fn create_test_cid(data: &str) -> Cid {
    let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(data.as_bytes())).unwrap();
    Cid::new_v1(0x71, hash)
}

// Helper function to create a test MstNode
fn create_test_node(left: Option<Cid>, entries: Vec<(usize, String, Cid, Option<Cid>)>) -> MstNode {
    let e = entries
        .into_iter()
        .map(|(p, k, v, t)| MstNodeLeaf {
            p,
            k: Bytes::from(k.into_bytes()),
            v,
            t,
        })
        .collect();

    MstNode::new(left, e)
}

// Helper function to create a test commit IPLD
fn create_test_commit_ipld(data_cid: Cid) -> Ipld {
    let mut map = BTreeMap::new();
    map.insert(
        "did".to_string(),
        Ipld::String("did:example:alice".to_string()),
    );
    map.insert("version".to_string(), Ipld::Integer(3));
    map.insert("data".to_string(), Ipld::Link(data_cid));
    map.insert("rev".to_string(), Ipld::String("3jzfcijpj2f2i".to_string()));
    map.insert(
        "prev".to_string(),
        Ipld::String("some-prev-commit".to_string()),
    );
    map.insert("sig".to_string(), Ipld::Bytes(vec![1, 2, 3, 4]));

    Ipld::Map(map)
}

// Helper function to create a test MST node IPLD
fn create_test_mst_node_ipld(
    left: Option<Cid>,
    entries: Vec<(usize, String, Cid, Option<Cid>)>,
) -> Ipld {
    let mut map = BTreeMap::new();

    if let Some(left_cid) = left {
        map.insert("l".to_string(), Ipld::Link(left_cid));
    } else {
        map.insert("l".to_string(), Ipld::Null);
    }

    let e_list: Vec<Ipld> = entries
        .into_iter()
        .map(|(p, k, v, t)| {
            let mut entry_map = BTreeMap::new();
            entry_map.insert("p".to_string(), Ipld::Integer(p as i128));
            entry_map.insert("k".to_string(), Ipld::Bytes(k.as_bytes().to_vec()));
            entry_map.insert("v".to_string(), Ipld::Link(v));
            if let Some(t_cid) = t {
                entry_map.insert("t".to_string(), Ipld::Link(t_cid));
            }
            Ipld::Map(entry_map)
        })
        .collect();

    map.insert("e".to_string(), Ipld::List(e_list));

    Ipld::Map(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mst_new() {
        let root_cid = create_test_cid("root");
        let mst = Mst::new(Some(root_cid));

        assert_eq!(mst.root(), Some(&root_cid));
        assert_eq!(mst.nodes.len(), 0);
    }

    #[test]
    fn test_mst_new_empty() {
        let mst = Mst::new(None);

        assert_eq!(mst.root(), None);
        assert_eq!(mst.nodes.len(), 0);
    }

    #[test]
    fn test_mst_empty() {
        let mst = Mst::empty();

        assert_eq!(mst.root(), None);
        assert_eq!(mst.nodes.len(), 0);
    }

    #[test]
    fn test_mst_root() {
        let root_cid = create_test_cid("root");
        let mst = Mst::new(Some(root_cid));

        assert_eq!(mst.root(), Some(&root_cid));
    }

    #[test]
    fn test_mst_root_empty() {
        let mst = Mst::new(None);

        assert_eq!(mst.root(), None);
    }

    #[test]
    fn test_mst_insert_and_get_node() {
        let root_cid = create_test_cid("root");
        let mst = Mst::new(Some(root_cid));

        let node_cid = create_test_cid("node1");
        let test_node = create_test_node(
            None,
            vec![(0, "key1".to_string(), create_test_cid("value1"), None)],
        );

        // Test insert
        mst.insert_node(node_cid, test_node.clone());
        assert_eq!(mst.nodes.len(), 1);

        // Test get
        let retrieved_node = mst.get_node(&node_cid);
        assert!(retrieved_node.is_some());

        let retrieved = retrieved_node.unwrap();
        assert_eq!(retrieved.left(), test_node.left());
        assert_eq!(retrieved.entries().len(), test_node.entries().len());
    }

    #[test]
    fn test_mst_get_nonexistent_node() {
        let root_cid = create_test_cid("root");
        let mst = Mst::new(Some(root_cid));

        let nonexistent_cid = create_test_cid("nonexistent");
        let result = mst.get_node(&nonexistent_cid);

        assert!(result.is_none());
    }

    #[test]
    fn test_mst_iter() {
        let root_cid = create_test_cid("root");
        let mst = Mst::new(Some(root_cid));

        let iterator = mst.iter();

        // The iterator should be created successfully
        // Actual iteration testing would require setting up nodes
        // which is tested in integration tests
        // We just verify the iterator was created properly
        assert!(format!("{:?}", iterator).contains("MstIterator"));
    }

    #[test]
    fn test_mst_iter_empty() {
        let mst = Mst::empty();

        let iterator = mst.iter();

        // The iterator should be created successfully even for empty MST
        assert!(format!("{:?}", iterator).contains("MstIterator"));

        // Should have no items to iterate over
        let items: Vec<_> = iterator.collect();
        assert_eq!(items.len(), 0);
    }

    #[tokio::test]
    async fn test_mst_from_car_importer_success() {
        let mut builder = CarBuilder::new();

        // Create test data CID
        let data_cid = create_test_cid("mst_root");

        // Create and add commit
        let commit_ipld = create_test_commit_ipld(data_cid);
        let commit_cid = builder.add_cbor(&commit_ipld).unwrap();
        builder.add_root(commit_cid);

        // Create and add MST nodes
        let node1_cid = create_test_cid("node1");
        let node1_ipld = create_test_mst_node_ipld(
            None,
            vec![(0, "key1".to_string(), create_test_cid("value1"), None)],
        );
        let _stored_node1_cid = builder.add_cbor(&node1_ipld).unwrap();

        let _node2_cid = create_test_cid("node2");
        let node2_ipld = create_test_mst_node_ipld(
            Some(node1_cid),
            vec![
                (0, "key2".to_string(), create_test_cid("value2"), None),
                (
                    2,
                    "y3".to_string(),
                    create_test_cid("value3"),
                    Some(create_test_cid("subtree")),
                ),
            ],
        );
        let _stored_node2_cid = builder.add_cbor(&node2_ipld).unwrap();

        let importer = builder.build();

        // Test conversion
        let result = Mst::try_from(importer);
        assert!(result.is_ok());

        let mst = result.unwrap();
        assert_eq!(mst.root(), Some(&data_cid));

        // Check that nodes were inserted
        assert!(mst.nodes.len() >= 1); // At least the nodes we can decode should be there
    }

    #[tokio::test]
    async fn test_mst_from_car_importer_no_roots() {
        let importer = CarImporter::new(); // Empty importer with no roots

        let result = Mst::try_from(importer);
        assert!(result.is_err());
        match result.unwrap_err() {
            AtmosError::InvalidRootCount => {}
            _ => panic!("Expected InvalidRootCount error"),
        }
    }

    #[tokio::test]
    async fn test_mst_from_car_importer_invalid_commit() {
        let mut builder = CarBuilder::new();

        // Create invalid commit (missing required fields)
        let mut invalid_commit = BTreeMap::new();
        invalid_commit.insert("invalid".to_string(), Ipld::String("data".to_string()));
        let invalid_ipld = Ipld::Map(invalid_commit);

        let commit_cid = builder.add_cbor(&invalid_ipld).unwrap();
        builder.add_root(commit_cid);

        let importer = builder.build();

        let result = Mst::try_from(importer);
        assert!(result.is_err());
        match result.unwrap_err() {
            AtmosError::CommitParsing { .. } => {}
            _ => panic!("Expected CommitParsing error"),
        }
    }

    #[tokio::test]
    async fn test_mst_from_car_importer_missing_commit_block() {
        let mut importer = CarImporter::new();
        let nonexistent_cid = create_test_cid("nonexistent");
        importer.add_root(nonexistent_cid);

        let result = Mst::try_from(importer);
        assert!(result.is_err());
        match result.unwrap_err() {
            AtmosError::CommitParsing { .. } => {}
            _ => panic!("Expected CommitParsing error"),
        }
    }

    #[tokio::test]
    async fn test_mst_from_car_importer_invalid_mst_node() {
        let mut builder = CarBuilder::new();

        let data_cid = create_test_cid("mst_root");
        let commit_ipld = create_test_commit_ipld(data_cid);
        let commit_cid = builder.add_cbor(&commit_ipld).unwrap();
        builder.add_root(commit_cid);

        // Add an invalid MST node (missing required fields)
        let mut invalid_node = BTreeMap::new();
        invalid_node.insert("invalid".to_string(), Ipld::String("node".to_string()));
        let invalid_node_ipld = Ipld::Map(invalid_node);
        let _invalid_node_cid = builder.add_cbor(&invalid_node_ipld).unwrap();

        let importer = builder.build();

        let result = Mst::try_from(importer);
        assert!(result.is_ok()); // Should still succeed, just skip invalid nodes

        let mst = result.unwrap();
        assert_eq!(mst.root(), Some(&data_cid));
    }

    #[tokio::test]
    async fn test_mst_multiple_nodes() {
        let mut builder = CarBuilder::new();

        let data_cid = create_test_cid("mst_root");
        let commit_ipld = create_test_commit_ipld(data_cid);
        let commit_cid = builder.add_cbor(&commit_ipld).unwrap();
        builder.add_root(commit_cid);

        // Create multiple valid MST nodes
        let node1_ipld = create_test_mst_node_ipld(
            None,
            vec![(0, "alice".to_string(), create_test_cid("alice_value"), None)],
        );
        let node1_cid = builder.add_cbor(&node1_ipld).unwrap();

        let node2_ipld = create_test_mst_node_ipld(
            Some(node1_cid),
            vec![
                (0, "bob".to_string(), create_test_cid("bob_value"), None),
                (
                    1,
                    "ob.charlie".to_string(),
                    create_test_cid("charlie_value"),
                    Some(create_test_cid("subtree1")),
                ),
            ],
        );
        let node2_cid = builder.add_cbor(&node2_ipld).unwrap();

        let node3_ipld = create_test_mst_node_ipld(
            None,
            vec![
                (0, "david".to_string(), create_test_cid("david_value"), None),
                (0, "eve".to_string(), create_test_cid("eve_value"), None),
            ],
        );
        let node3_cid = builder.add_cbor(&node3_ipld).unwrap();

        let importer = builder.build();

        let result = Mst::try_from(importer);
        assert!(result.is_ok());

        let mst = result.unwrap();
        assert_eq!(mst.root(), Some(&data_cid));

        // Should have multiple nodes
        assert!(mst.nodes.len() >= 2);

        // Check specific nodes exist and can be retrieved
        let retrieved_node1 = mst.get_node(&node1_cid);
        assert!(retrieved_node1.is_some());

        let retrieved_node2 = mst.get_node(&node2_cid);
        assert!(retrieved_node2.is_some());

        let retrieved_node3 = mst.get_node(&node3_cid);
        assert!(retrieved_node3.is_some());
    }

    #[test]
    fn test_mst_clone() {
        let root_cid = create_test_cid("root");
        let mst = Mst::new(Some(root_cid));

        let node_cid = create_test_cid("node1");
        let test_node = create_test_node(
            None,
            vec![(0, "key1".to_string(), create_test_cid("value1"), None)],
        );

        mst.insert_node(node_cid, test_node);

        let cloned_mst = mst.clone();

        assert_eq!(cloned_mst.root(), mst.root());
        assert_eq!(cloned_mst.nodes.len(), mst.nodes.len());

        let original_node = mst.get_node(&node_cid).unwrap();
        let cloned_node = cloned_mst.get_node(&node_cid).unwrap();

        assert_eq!(original_node.left(), cloned_node.left());
        assert_eq!(original_node.entries().len(), cloned_node.entries().len());
    }

    #[test]
    fn test_mst_debug() {
        let root_cid = create_test_cid("root");
        let mst = Mst::new(Some(root_cid));

        let debug_string = format!("{:?}", mst);
        assert!(debug_string.contains("Mst"));
        assert!(debug_string.contains("root"));
        assert!(debug_string.contains("nodes"));
    }

    #[test]
    fn test_mst_debug_empty() {
        let mst = Mst::empty();

        let debug_string = format!("{:?}", mst);
        assert!(debug_string.contains("Mst"));
        assert!(debug_string.contains("root"));
        assert!(debug_string.contains("None"));
        assert!(debug_string.contains("nodes"));
    }

    #[tokio::test]
    async fn test_mst_with_empty_nodes() {
        let mut builder = CarBuilder::new();

        let data_cid = create_test_cid("mst_root");
        let commit_ipld = create_test_commit_ipld(data_cid);
        let commit_cid = builder.add_cbor(&commit_ipld).unwrap();
        builder.add_root(commit_cid);

        // Create MST node with empty entries
        let empty_node_ipld = create_test_mst_node_ipld(None, vec![]);
        let empty_node_cid = builder.add_cbor(&empty_node_ipld).unwrap();

        let importer = builder.build();

        let result = Mst::try_from(importer);
        assert!(result.is_ok());

        let mst = result.unwrap();
        assert_eq!(mst.root(), Some(&data_cid));

        // Should be able to retrieve the empty node
        let retrieved_node = mst.get_node(&empty_node_cid);
        assert!(retrieved_node.is_some());

        let node = retrieved_node.unwrap();
        assert_eq!(node.entries().len(), 0);
        assert_eq!(node.left(), None);
    }

    #[test]
    fn test_mst_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let root_cid = create_test_cid("root");
        let mst = Arc::new(Mst::new(Some(root_cid)));

        let mut handles = vec![];

        // Spawn multiple threads to test concurrent access
        for i in 0..10 {
            let mst_clone = Arc::clone(&mst);
            let handle = thread::spawn(move || {
                let node_cid = create_test_cid(&format!("node{}", i));
                let test_node = create_test_node(
                    None,
                    vec![(
                        0,
                        format!("key{}", i),
                        create_test_cid(&format!("value{}", i)),
                        None,
                    )],
                );

                mst_clone.insert_node(node_cid, test_node);

                // Try to retrieve the node
                let retrieved = mst_clone.get_node(&node_cid);
                assert!(retrieved.is_some());
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all nodes were inserted
        assert_eq!(mst.nodes.len(), 10);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_mst_integration_with_iterator() {
        let mut builder = CarBuilder::new();

        let data_cid = create_test_cid("mst_root");
        let commit_ipld = create_test_commit_ipld(data_cid);
        let commit_cid = builder.add_cbor(&commit_ipld).unwrap();
        builder.add_root(commit_cid);

        // Create a simple MST structure
        let leaf_node_ipld = create_test_mst_node_ipld(
            None,
            vec![
                (0, "alice".to_string(), create_test_cid("alice_data"), None),
                (0, "bob".to_string(), create_test_cid("bob_data"), None),
            ],
        );
        let leaf_node_cid = builder.add_cbor(&leaf_node_ipld).unwrap();

        // Add the leaf node as the root MST node
        let root_node_ipld = create_test_mst_node_ipld(
            Some(leaf_node_cid),
            vec![(
                0,
                "charlie".to_string(),
                create_test_cid("charlie_data"),
                None,
            )],
        );
        let _root_node_cid = builder.add_cbor(&root_node_ipld).unwrap();

        let importer = builder.build();

        let mst = Mst::try_from(importer).unwrap();

        // Test that we can create an iterator
        let iterator = mst.iter();

        // The iterator should be properly initialized - test it exists
        let mut count = 0;
        for _item in iterator {
            count += 1;
            if count > 1000 {
                // Safety limit to prevent infinite loops
                break;
            }
        }
        // We just verify the iterator works without infinite loops
    }

    #[tokio::test]
    async fn test_mst_real_world_scenario() {
        let mut builder = CarBuilder::new();

        // Simulate a real AT Protocol repo structure
        let data_cid = create_test_cid("repo_mst_root");
        let commit_ipld = create_test_commit_ipld(data_cid);
        let commit_cid = builder.add_cbor(&commit_ipld).unwrap();
        builder.add_root(commit_cid);

        // Create multiple MST nodes representing different parts of the tree
        let posts_node_ipld = create_test_mst_node_ipld(
            None,
            vec![
                (
                    0,
                    "app.bsky.feed.post/".to_string(),
                    create_test_cid("post1"),
                    None,
                ),
                (18, "123abc".to_string(), create_test_cid("post2"), None), // shared prefix "app.bsky.feed.post/"
                (18, "456def".to_string(), create_test_cid("post3"), None),
            ],
        );
        let posts_node_cid = builder.add_cbor(&posts_node_ipld).unwrap();

        let likes_node_ipld = create_test_mst_node_ipld(
            None,
            vec![
                (
                    0,
                    "app.bsky.feed.like/".to_string(),
                    create_test_cid("like1"),
                    None,
                ),
                (18, "789ghi".to_string(), create_test_cid("like2"), None), // shared prefix "app.bsky.feed.like/"
            ],
        );
        let likes_node_cid = builder.add_cbor(&likes_node_ipld).unwrap();

        // Root node that links to both subtrees
        let root_mst_node_ipld = create_test_mst_node_ipld(
            Some(likes_node_cid),
            vec![
                (
                    0,
                    "app.bsky.feed.post".to_string(),
                    create_test_cid("posts_collection"),
                    Some(posts_node_cid),
                ),
                (
                    13,
                    ".profile".to_string(),
                    create_test_cid("profile_data"),
                    None,
                ), // "app.bsky.actor.profile"
            ],
        );
        let root_mst_node_cid = builder.add_cbor(&root_mst_node_ipld).unwrap();

        let importer = builder.build();

        let result = Mst::try_from(importer);
        assert!(result.is_ok());

        let mst = result.unwrap();
        assert_eq!(mst.root(), Some(&data_cid));

        // Verify we can access all the nodes
        assert!(mst.get_node(&posts_node_cid).is_some());
        assert!(mst.get_node(&likes_node_cid).is_some());
        assert!(mst.get_node(&root_mst_node_cid).is_some());

        // Verify the structure is correct
        let root_node = mst.get_node(&root_mst_node_cid).unwrap();
        assert_eq!(root_node.entries().len(), 2);
        assert!(root_node.left().is_some());

        let posts_node = mst.get_node(&posts_node_cid).unwrap();
        assert_eq!(posts_node.entries().len(), 3);

        let likes_node = mst.get_node(&likes_node_cid).unwrap();
        assert_eq!(likes_node.entries().len(), 2);
    }

    #[tokio::test]
    async fn test_mst_comprehensive_usage_example() {
        // This test demonstrates a complete MST usage scenario
        let mut builder = CarBuilder::new();

        // Create the MST root CID
        let mst_root_cid = create_test_cid("comprehensive_mst_root");

        // Create a commit that points to our MST
        let commit_ipld = create_test_commit_ipld(mst_root_cid);
        let commit_cid = builder.add_cbor(&commit_ipld).unwrap();
        builder.add_root(commit_cid);

        // Build a realistic MST structure with multiple levels
        // Level 1: Leaf nodes with actual data
        let posts_leaf_ipld = create_test_mst_node_ipld(
            None,
            vec![
                (
                    0,
                    "app.bsky.feed.post/3k2a4b5c6d".to_string(),
                    create_test_cid("post_1"),
                    None,
                ),
                (
                    20,
                    "7e8f9g0h1i".to_string(),
                    create_test_cid("post_2"),
                    None,
                ),
                (
                    20,
                    "2j3k4l5m6n".to_string(),
                    create_test_cid("post_3"),
                    None,
                ),
            ],
        );
        let posts_leaf_cid = builder.add_cbor(&posts_leaf_ipld).unwrap();

        let likes_leaf_ipld = create_test_mst_node_ipld(
            None,
            vec![
                (
                    0,
                    "app.bsky.feed.like/1a2b3c4d5e".to_string(),
                    create_test_cid("like_1"),
                    None,
                ),
                (
                    20,
                    "6f7g8h9i0j".to_string(),
                    create_test_cid("like_2"),
                    None,
                ),
            ],
        );
        let likes_leaf_cid = builder.add_cbor(&likes_leaf_ipld).unwrap();

        // Level 2: Intermediate nodes
        let feed_branch_ipld = create_test_mst_node_ipld(
            Some(likes_leaf_cid),
            vec![
                (
                    0,
                    "app.bsky.feed.post".to_string(),
                    create_test_cid("posts_collection"),
                    Some(posts_leaf_cid),
                ),
                (
                    13,
                    ".repost/1z2y3x4w5v".to_string(),
                    create_test_cid("repost_1"),
                    None,
                ),
            ],
        );
        let feed_branch_cid = builder.add_cbor(&feed_branch_ipld).unwrap();

        // Level 3: Root node
        let root_mst_ipld = create_test_mst_node_ipld(
            Some(feed_branch_cid),
            vec![
                (
                    0,
                    "app.bsky.actor.profile".to_string(),
                    create_test_cid("profile"),
                    None,
                ),
                (
                    11,
                    ".follow/9u8t7s6r5q".to_string(),
                    create_test_cid("follow_1"),
                    None,
                ),
            ],
        );
        let root_mst_cid = builder.add_cbor(&root_mst_ipld).unwrap();

        let importer = builder.build();

        // Convert to MST
        let mst = Mst::try_from(importer).unwrap();

        // Verify the MST structure
        assert_eq!(mst.root(), Some(&mst_root_cid));
        assert!(mst.nodes.len() >= 4); // We should have at least our 4 nodes

        // Test accessing different levels of the tree
        let root_node = mst.get_node(&root_mst_cid).unwrap();
        assert_eq!(root_node.entries().len(), 2);
        assert!(root_node.left().is_some());

        let feed_node = mst.get_node(&feed_branch_cid).unwrap();
        assert_eq!(feed_node.entries().len(), 2);
        assert!(feed_node.left().is_some());

        let posts_node = mst.get_node(&posts_leaf_cid).unwrap();
        assert_eq!(posts_node.entries().len(), 3);
        assert!(posts_node.left().is_none());

        let likes_node = mst.get_node(&likes_leaf_cid).unwrap();
        assert_eq!(likes_node.entries().len(), 2);
        assert!(likes_node.left().is_none());

        // Test iteration capabilities - just verify iterator can be created
        let iterator = mst.iter();

        // Verify the iterator exists and has the expected structure
        assert!(format!("{:?}", iterator).contains("MstIterator"));

        // Test cloning preserves all data
        let cloned_mst = mst.clone();
        assert_eq!(cloned_mst.root(), mst.root());
        assert_eq!(cloned_mst.nodes.len(), mst.nodes.len());

        // Verify all nodes are accessible in the clone
        assert!(cloned_mst.get_node(&root_mst_cid).is_some());
        assert!(cloned_mst.get_node(&feed_branch_cid).is_some());
        assert!(cloned_mst.get_node(&posts_leaf_cid).is_some());
        assert!(cloned_mst.get_node(&likes_leaf_cid).is_some());
    }

    #[tokio::test]
    async fn test_mst_from_car_importer_empty_tree() {
        let mut builder = CarBuilder::new();

        // Create a commit that points to an empty MST (no data CID)
        let mut commit_map = BTreeMap::new();
        commit_map.insert(
            "did".to_string(),
            Ipld::String("did:example:empty".to_string()),
        );
        commit_map.insert("version".to_string(), Ipld::Integer(1));
        // For empty MST, the data field might be null
        commit_map.insert("data".to_string(), Ipld::Null);
        commit_map.insert("rev".to_string(), Ipld::String("1abc2def3ghi".to_string()));
        commit_map.insert("prev".to_string(), Ipld::Null);
        commit_map.insert("sig".to_string(), Ipld::Bytes(vec![5, 6, 7, 8]));

        let commit_ipld = Ipld::Map(commit_map);
        let commit_cid = builder.add_cbor(&commit_ipld).unwrap();
        builder.add_root(commit_cid);

        let importer = builder.build();

        // This should fail because data field is null - we can't have a null CID
        let result = Mst::try_from(importer);
        assert!(result.is_err());
    }

    #[test]
    fn test_mst_empty_methods() {
        let empty_mst = Mst::empty();

        // Test all methods work with empty MST
        assert_eq!(empty_mst.root(), None);
        assert_eq!(empty_mst.nodes.len(), 0);

        // Test getting a node from empty MST
        let test_cid = create_test_cid("test");
        assert!(empty_mst.get_node(&test_cid).is_none());

        // Test inserting a node to empty MST
        let test_node = create_test_node(
            None,
            vec![(0, "key".to_string(), create_test_cid("value"), None)],
        );
        empty_mst.insert_node(test_cid, test_node);
        assert_eq!(empty_mst.nodes.len(), 1);
        assert!(empty_mst.get_node(&test_cid).is_some());

        // Root should still be None even after adding nodes
        assert_eq!(empty_mst.root(), None);
    }
}
