use cid::Cid;
use dashmap::DashMap;

use crate::{
    CarImporter,
    commit::ATProtoCommit,
    error::{AtmosError, Result},
    mst::{iterator::MstIterator, node::MstNode},
};

pub mod iterator;
pub mod node;

#[cfg(test)]
pub mod tests;

#[derive(Debug, Clone)]
pub struct Mst {
    pub root: Option<cid::Cid>,
    pub nodes: DashMap<cid::Cid, MstNode>,
}

impl Mst {
    pub fn new(root: Option<Cid>) -> Self {
        Mst {
            root,
            nodes: DashMap::new(),
        }
    }

    pub fn empty() -> Self {
        Mst {
            root: None,
            nodes: DashMap::new(),
        }
    }

    pub fn root(&self) -> Option<&Cid> {
        self.root.as_ref()
    }

    pub fn get_node(&self, cid: &cid::Cid) -> Option<MstNode> {
        self.nodes.get(cid).map(|n| n.clone())
    }

    pub fn insert_node(&self, key: cid::Cid, node: MstNode) {
        self.nodes.insert(key, node);
    }

    pub fn iter(&self) -> MstIterator {
        self.into()
    }
}

impl TryFrom<CarImporter> for Mst {
    type Error = AtmosError;

    fn try_from(importer: CarImporter) -> Result<Self> {
        // car importer must have at least one root
        let root_cid = importer
            .roots()
            .first()
            .ok_or(AtmosError::InvalidRootCount)?
            .clone();

        let commit_cid: ATProtoCommit = importer
            .decode_cbor(&root_cid)
            .map_err(|_| {
                AtmosError::commit_parsing(
                    "couldn't get cbor importer, either invalid cbor or root doesn't exist",
                )
            })?
            .try_into()
            .map_err(|_| AtmosError::commit_parsing("decoding cbor failed for the commit"))?;

        // create a new Mst with the root
        let mst = Mst::new(Some(commit_cid.data));

        for node in importer.get_mst_nodes() {
            // look up the node in the importer
            let block = importer.decode_cbor(&node).map_err(|_| {
                AtmosError::mst(
                    "couldn't get cbor importer, either invalid cbor or node doesn't exist",
                )
            })?;
            mst.insert_node(
                node,
                block.try_into().map_err(|_| {
                    AtmosError::node_conversion("Failed to convert block to MstNode")
                })?,
            );
        }

        Ok(mst)
    }
}
