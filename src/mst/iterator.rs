use cid::Cid;
use sha2::Digest;

use crate::mst::Mst;

#[derive(Debug)]
pub struct MstIterator<'a> {
    mst: &'a Mst,
    stack: Vec<StackItem>, // (cid, depth, prefix)
}

#[derive(Debug)]
enum StackItem {
    VisitNode { cid: Cid, prefix: String },
    YieldEntry { key: String, cid: Cid },
}

impl<'a> MstIterator<'a> {
    pub fn new(mst: &'a Mst) -> Self {
        let stack = if let Some(root_cid) = mst.root {
            vec![StackItem::VisitNode {
                cid: root_cid,
                prefix: "".to_string(),
            }]
        } else {
            vec![]
        };

        Self { mst, stack }
    }

    pub fn seek(&mut self, target: &str) {
        // Clear the stack and start fresh
        self.stack.clear();

        // Start from root with the target key if root exists
        if let Some(root_cid) = self.mst.root {
            self.seek_in_subtree(root_cid, String::new(), target);
        }
    }

    fn seek_in_subtree(&mut self, cid: Cid, prefix: String, target: &str) {
        let Some(node) = self.mst.get_node(&cid) else {
            return;
        };

        // Pre-compute target depth once
        let target_depth = self.compute_key_depth(target.as_bytes());

        // Check left subtree first if target might be there
        if let Some(left) = &node.l {
            if *target < *prefix {
                self.seek_in_subtree(left.clone(), prefix.clone(), target);
                return;
            }
        }

        // Reconstruct keys to find where target falls
        let mut reconstructed_entries = Vec::new();
        let mut prev_key_suffix_bytes = Vec::new();

        for entry in node.e.iter() {
            let suffix_bytes = &entry.k;

            let key_suffix_bytes = if entry.p == 0 {
                suffix_bytes.to_vec()
            } else {
                let shared_len = std::cmp::min(entry.p, prev_key_suffix_bytes.len());
                let mut key_bytes = Vec::new();
                key_bytes.extend_from_slice(&prev_key_suffix_bytes[..shared_len]);
                key_bytes.extend_from_slice(suffix_bytes);
                key_bytes
            };

            let key_suffix = String::from_utf8_lossy(&key_suffix_bytes);
            let full_key = format!("{}{}", prefix, key_suffix);
            let full_key_bytes = full_key.as_bytes();

            prev_key_suffix_bytes = key_suffix_bytes;
            reconstructed_entries.push((full_key.clone(), full_key_bytes.to_vec(), entry));
        }

        // Find the right position with depth-aware optimizations
        for (i, (full_key, full_key_bytes, entry)) in reconstructed_entries.iter().enumerate() {
            if **full_key >= *target {
                // Found first key >= target

                // Push remaining entries (in reverse for stack)
                for (remaining_key, _, remaining_entry) in reconstructed_entries[i..].iter().rev() {
                    if let Some(subtree_cid) = &remaining_entry.t {
                        self.stack.push(StackItem::VisitNode {
                            cid: subtree_cid.clone(),
                            prefix: remaining_key.clone(),
                        });
                    }

                    self.stack.push(StackItem::YieldEntry {
                        key: remaining_key.clone(),
                        cid: remaining_entry.v.clone(),
                    });
                }
                return;
            } else if let Some(subtree_cid) = &entry.t {
                // Check if we should explore this subtree
                if target.starts_with(full_key) {
                    // Target is definitely in this subtree
                    self.seek_in_subtree(subtree_cid.clone(), full_key.clone(), target);
                    return;
                } else {
                    // Use depth to make pruning decisions
                    let key_depth = self.compute_key_depth(full_key_bytes);

                    // If this key has high depth, it's rare and its subtree is likely small
                    // Worth exploring if we're close lexicographically
                    if key_depth >= target_depth && **full_key < *target {
                        // High-depth keys tend to have smaller, more focused subtrees
                        self.seek_in_subtree(subtree_cid.clone(), full_key.clone(), target);
                    }
                    // If key_depth < target_depth, this subtree likely contains many keys
                    // and we should be more selective about exploring it
                }
            }
        }
    }

    fn compute_key_depth(&self, key: &[u8]) -> usize {
        let mut hasher = sha2::Sha256::new();
        hasher.update(key);
        let digest = hasher.finalize();
        let mut depth = 0;
        for byte in digest {
            for shift in (0..8).step_by(2) {
                let two_bits = (byte >> (6 - shift)) & 0b11;
                if two_bits == 0 {
                    depth += 1;
                } else {
                    return depth;
                }
            }
        }
        depth
    }
}

impl<'a> Into<MstIterator<'a>> for &'a Mst {
    fn into(self) -> MstIterator<'a> {
        MstIterator::new(self)
    }
}

#[derive(Debug)]
pub enum MstIteratorError {
    NodeNotFound(Cid),
    InvalidUtf8,
}

impl<'a> Iterator for MstIterator<'a> {
    type Item = (String, Cid);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.stack.pop() {
            match item {
                StackItem::VisitNode { cid, prefix } => {
                    let node = self.mst.get_node(&cid)?;

                    let mut reconstructed_entries = Vec::new();
                    let mut prev_key_suffix_bytes = Vec::new();

                    for entry in node.e.iter() {
                        let suffix_bytes = &entry.k;

                        let key_suffix_bytes = if entry.p == 0 {
                            suffix_bytes.to_vec()
                        } else {
                            // Share first p bytes from previous entry + new suffix
                            let shared_len = std::cmp::min(entry.p, prev_key_suffix_bytes.len());
                            let mut key_bytes = Vec::new();
                            key_bytes.extend_from_slice(&prev_key_suffix_bytes[..shared_len]);
                            key_bytes.extend_from_slice(suffix_bytes);
                            key_bytes
                        };

                        // Convert to string for the full key
                        let key_suffix = String::from_utf8_lossy(&key_suffix_bytes).to_string();
                        let full_key = format!("{}{}", prefix, key_suffix);

                        prev_key_suffix_bytes = key_suffix_bytes.clone();
                        let suffix = key_suffix.clone();
                        reconstructed_entries.push((full_key, entry, suffix));
                    }

                    for (full_key, entry, suffix) in reconstructed_entries.into_iter().rev() {
                        if let Some(subtree_cid) = &entry.t {
                            self.stack.push(StackItem::VisitNode {
                                cid: subtree_cid.clone(),
                                prefix: full_key.clone(),
                            });
                        }

                        self.stack.push(StackItem::YieldEntry {
                            // the item's key
                            key: suffix,
                            cid: entry.v.clone(),
                        });
                    }

                    if let Some(left) = &node.l {
                        self.stack.push(StackItem::VisitNode {
                            cid: left.clone(),
                            prefix: prefix.clone(),
                        });
                    }
                }
                StackItem::YieldEntry { key, cid } => {
                    return Some((key, cid));
                }
            }
        }
        None
    }
}
