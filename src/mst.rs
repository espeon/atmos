use cid::Cid;

use crate::{
    CarImporter,
    commit::ATProtoCommit,
    error::{AtmosError, Result},
    mst::{
        iterator::MstIterator,
        node::MstNode,
        storage::{MemoryMstStorage, MstStorage},
    },
};

pub mod iterator;
pub mod node;
pub mod storage;

#[cfg(test)]
pub mod tests;

#[derive(Debug)]
pub struct Mst<S: MstStorage = MemoryMstStorage> {
    pub root: Option<cid::Cid>,
    pub storage: S,
}

// Clone implementation for concrete storage types only
impl Clone for Mst<MemoryMstStorage> {
    fn clone(&self) -> Self {
        Mst {
            root: self.root,
            storage: self.storage.clone(),
        }
    }
}

#[cfg(feature = "fjall")]
impl Clone for Mst<crate::mst::storage::fjall::FjallMstStorage> {
    fn clone(&self) -> Self {
        Mst {
            root: self.root,
            storage: self.storage.clone(),
        }
    }
}

impl Clone for Mst<crate::mst::storage::StorageWrapper> {
    fn clone(&self) -> Self {
        Mst {
            root: self.root,
            storage: self.storage.clone(),
        }
    }
}

impl<S: MstStorage> Mst<S> {
    pub fn new(root: Option<Cid>, storage: S) -> Self {
        Mst { root, storage }
    }

    pub fn with_storage(storage: S) -> Self {
        Mst {
            root: None,
            storage,
        }
    }

    pub fn root(&self) -> Option<&Cid> {
        self.root.as_ref()
    }

    pub async fn get_node(&self, cid: &cid::Cid) -> Result<Option<MstNode>> {
        self.storage.get_node(cid).await
    }

    pub async fn insert_node(&self, key: cid::Cid, node: MstNode) -> Result<()> {
        self.storage.insert_node(key, node).await
    }

    pub fn iter(&self) -> MstIterator<S> {
        MstIterator::new(self)
    }
}

impl Mst<MemoryMstStorage> {
    pub fn empty() -> Self {
        Mst {
            root: None,
            storage: MemoryMstStorage::new(),
        }
    }
}

impl Mst<MemoryMstStorage> {
    pub async fn from_car_importer(importer: CarImporter) -> Result<Self> {
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
        let storage = MemoryMstStorage::new();
        let mst = Mst::new(Some(commit_cid.data), storage);

        for node in importer.get_mst_nodes() {
            // look up the node in the importer
            let block = importer.decode_cbor(&node).map_err(|_| {
                AtmosError::mst(
                    "couldn't get cbor importer, either invalid cbor or node doesn't exist",
                )
            })?;
            let mst_node = block
                .try_into()
                .map_err(|_| AtmosError::node_conversion("Failed to convert block to MstNode"))?;

            mst.insert_node(node, mst_node).await?;
        }

        Ok(mst)
    }
}

impl TryFrom<CarImporter> for Mst<MemoryMstStorage> {
    type Error = AtmosError;

    fn try_from(_importer: CarImporter) -> Result<Self> {
        Err(AtmosError::mst(
            "Use Mst::from_car_importer() async method instead of TryFrom",
        ))
    }
}
