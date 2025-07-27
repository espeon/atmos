use cid::Cid;

use crate::mst::{Mst, storage::MstStorage};

#[derive(Debug)]
pub struct MstIterator<'a, S: MstStorage> {
    mst: &'a Mst<S>,
    stack: Vec<StackItem>, // (cid, depth, prefix)
}

#[derive(Debug)]
enum StackItem {
    VisitNode { cid: Cid, prefix: String },
    YieldEntry { key: String, cid: Cid },
}

impl<'a, S: MstStorage> MstIterator<'a, S> {
    pub fn new(mst: &'a Mst<S>) -> Self {
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

    pub fn seek(&mut self, _target: &str) {
        // TODO: Implement async-compatible seek
        // For now, just clear the stack
        self.stack.clear();
    }
}

impl<'a, S: MstStorage> Into<MstIterator<'a, S>> for &'a Mst<S> {
    fn into(self) -> MstIterator<'a, S> {
        MstIterator::new(self)
    }
}

impl<'a, S: MstStorage> MstIterator<'a, S> {
    /// Convert this iterator to an async stream
    pub fn into_stream(
        self,
    ) -> impl futures::Stream<Item = crate::Result<(String, Cid)>> + 'a + Unpin {
        use futures::stream;

        Box::pin(stream::unfold(self, |mut iter| async move {
            match iter.next_async().await {
                Some(result) => Some((result, iter)),
                None => None,
            }
        }))
    }

    async fn next_async(&mut self) -> Option<crate::Result<(String, Cid)>> {
        while let Some(item) = self.stack.pop() {
            match item {
                StackItem::VisitNode { cid, prefix } => {
                    let node = match self.mst.get_node(&cid).await {
                        Ok(Some(node)) => node,
                        Ok(None) => continue,
                        Err(e) => return Some(Err(e)),
                    };

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
                    return Some(Ok((key, cid)));
                }
            }
        }
        None
    }
}
