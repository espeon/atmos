use crate::error::{AtmosError, Result};
use bytes::Bytes;
use cid::Cid;
use ipld_core::ipld::Ipld;
use iroh_car::{CarHeader, CarReader, CarWriter};
use multihash::Multihash;
use serde_ipld_dagcbor as dagcbor;
use serde_ipld_dagjson as dagjson;
use sha2::Digest;
use std::collections::HashMap;
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncWrite};

/// A CAR (Content Addressable aRchive) importer and reader for IPLD data
pub struct CarImporter {
    /// Storage for blocks indexed by CID
    pub blocks: HashMap<Cid, Bytes>,
    /// The root CIDs of the CAR file
    pub roots: Vec<Cid>,
}

impl CarImporter {
    /// Create a new empty CAR importer
    pub fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            roots: Vec::new(),
        }
    }

    /// Import a CAR file from a reader
    pub async fn import_from_reader<R: AsyncRead + Unpin>(&mut self, reader: R) -> Result<()> {
        let mut car_reader = CarReader::new(reader).await?;
        let header = car_reader.header().clone();

        // Store the root CIDs
        self.roots = header.roots().to_vec();

        // Read all blocks from the CAR file
        loop {
            match car_reader.next_block().await? {
                Some((cid, data)) => {
                    self.blocks.insert(cid, Bytes::copy_from_slice(&data));
                }
                None => break,
            }
        }

        Ok(())
    }

    /// Import a CAR file from bytes
    pub async fn import_from_bytes(&mut self, data: Bytes) -> Result<()> {
        let cursor = Cursor::new(data);
        self.import_from_reader(cursor).await
    }

    /// Get a block by its CID
    pub fn get_block(&self, cid: &Cid) -> Option<&Bytes> {
        self.blocks.get(cid)
    }

    /// Get all root CIDs
    pub fn roots(&self) -> &[Cid] {
        &self.roots
    }

    /// Get all CIDs in the CAR file
    pub fn cids(&self) -> Vec<Cid> {
        self.blocks.keys().cloned().collect()
    }

    /// Decode a block as IPLD using CBOR codec
    pub fn decode_cbor(&self, cid: &Cid) -> Result<Ipld> {
        let data = self
            .get_block(cid)
            .ok_or_else(|| AtmosError::block_not_found(cid))?;

        let ipld: Ipld = dagcbor::from_slice(data)?;
        Ok(ipld)
    }

    /// Decode a block as IPLD using JSON codec
    pub fn decode_json(&self, cid: &Cid) -> Result<Ipld> {
        let data = self
            .get_block(cid)
            .ok_or_else(|| AtmosError::block_not_found(cid))?;

        let ipld: Ipld = dagjson::from_slice(data)?;
        Ok(ipld)
    }

    /// Export the CAR data to a writer
    pub async fn export_to_writer<W: AsyncWrite + Unpin + Send>(&self, writer: W) -> Result<()> {
        let header = CarHeader::new_v1(self.roots.clone());
        let mut car_writer = CarWriter::new(header, writer);

        // Write all blocks
        for (cid, data) in &self.blocks {
            car_writer.write(*cid, data).await?;
        }

        car_writer.finish().await?;
        Ok(())
    }

    /// Export the CAR data to bytes
    pub async fn export_to_bytes(&self) -> Result<Bytes> {
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        self.export_to_writer(cursor).await?;
        Ok(Bytes::from(buffer))
    }

    /// Add a new block to the CAR
    pub fn add_block(&mut self, cid: Cid, data: Bytes) {
        self.blocks.insert(cid, data);
    }

    /// Add a root CID
    pub fn add_root(&mut self, cid: Cid) {
        if !self.roots.contains(&cid) {
            self.roots.push(cid);
        }
    }

    /// Remove a block by CID
    pub fn remove_block(&mut self, cid: &Cid) -> Option<Bytes> {
        self.blocks.remove(cid)
    }

    /// Check if a CID exists in the CAR
    pub fn contains(&self, cid: &Cid) -> bool {
        self.blocks.contains_key(cid)
    }

    /// Get the number of blocks in the CAR
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Check if the CAR is empty
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Clear all blocks and roots
    pub fn clear(&mut self) {
        self.blocks.clear();
        self.roots.clear();
    }

    /// Check if a block represents an MST node
    pub fn is_mst_node(&self, cid: &Cid) -> bool {
        if let Ok(ipld) = self.decode_cbor(cid) {
            if let Ipld::Map(map) = ipld {
                // MST nodes have 'e' (entries) and 'l' (left) keys
                // and do NOT have a '$type' field
                return map.contains_key("e")
                    && map.contains_key("l")
                    && !map.contains_key("$type");
            }
        }
        false
    }

    /// Get all MST node CIDs
    pub fn get_mst_nodes(&self) -> Vec<Cid> {
        self.cids()
            .into_iter()
            .filter(|cid| self.is_mst_node(cid))
            .collect()
    }

    /// Get all data record CIDs (non-MST nodes)
    pub fn get_data_records(&self) -> Vec<Cid> {
        self.cids()
            .into_iter()
            .filter(|cid| !self.is_mst_node(cid))
            .collect()
    }
}

impl Default for CarImporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating CAR files with IPLD data
pub struct CarBuilder {
    importer: CarImporter,
}

impl CarBuilder {
    /// Create a new CAR builder
    pub fn new() -> Self {
        Self {
            importer: CarImporter::new(),
        }
    }

    /// Add IPLD data encoded with CBOR
    pub fn add_cbor(&mut self, ipld: &Ipld) -> Result<Cid> {
        let data = dagcbor::to_vec(ipld)?;
        let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(&data)).unwrap();
        let cid = Cid::new_v1(0x71, hash); // 0x71 is DAG-CBOR codec
        self.importer.add_block(cid, Bytes::from(data));
        Ok(cid)
    }

    /// Add IPLD data encoded with JSON
    pub fn add_json(&mut self, ipld: &Ipld) -> Result<Cid> {
        let data = dagjson::to_vec(ipld)?;
        let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(&data)).unwrap();
        let cid = Cid::new_v1(0x0129, hash); // 0x0129 is DAG-JSON codec
        self.importer.add_block(cid, Bytes::from(data));
        Ok(cid)
    }

    /// Add a root CID
    pub fn add_root(&mut self, cid: Cid) -> &mut Self {
        self.importer.add_root(cid);
        self
    }

    /// Build and return the CAR importer
    pub fn build(self) -> CarImporter {
        self.importer
    }
}

impl Default for CarBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Utility functions for working with CAR files and IPLD
pub mod utils {
    use super::*;
    use std::path::Path;
    use tokio::fs::File;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Load a CAR file from disk
    pub async fn load_car_file<P: AsRef<Path>>(path: P) -> Result<CarImporter> {
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let mut importer = CarImporter::new();
        importer.import_from_bytes(Bytes::from(buffer)).await?;
        Ok(importer)
    }

    /// Save a CAR file to disk
    pub async fn save_car_file<P: AsRef<Path>>(importer: &CarImporter, path: P) -> Result<()> {
        let data = importer.export_to_bytes().await?;
        let mut file = File::create(path).await?;
        file.write_all(&data).await?;
        Ok(())
    }

    /// Create a simple IPLD object for testing
    pub fn create_test_ipld() -> Ipld {
        use std::collections::BTreeMap;

        let mut map = BTreeMap::new();
        map.insert("hello".to_string(), Ipld::String("world".to_string()));
        map.insert("number".to_string(), Ipld::Integer(42));
        map.insert(
            "array".to_string(),
            Ipld::List(vec![
                Ipld::String("item1".to_string()),
                Ipld::String("item2".to_string()),
            ]),
        );

        Ipld::Map(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_car_importer_basic() {
        let mut importer = CarImporter::new();

        // Create some test data
        let test_data = Bytes::from("hello world");
        let hash = Multihash::wrap(0x12, &sha2::Sha256::digest(&test_data)).unwrap();
        let cid = Cid::new_v1(0x71, hash);

        importer.add_block(cid, test_data.clone());
        importer.add_root(cid);

        assert_eq!(importer.len(), 1);
        assert!(importer.contains(&cid));
        assert_eq!(importer.get_block(&cid), Some(&test_data));
        assert_eq!(importer.roots(), &[cid]);
    }

    #[tokio::test]
    async fn test_car_builder_cbor() -> Result<()> {
        let mut builder = CarBuilder::new();
        let test_ipld = utils::create_test_ipld();

        let cid = builder.add_cbor(&test_ipld)?;
        builder.add_root(cid);

        let importer = builder.build();
        let decoded = importer.decode_cbor(&cid)?;

        assert_eq!(decoded, test_ipld);
        Ok(())
    }

    #[tokio::test]
    async fn test_export_import_roundtrip() -> Result<()> {
        // Create a CAR with test data
        let mut builder = CarBuilder::new();
        let test_ipld = utils::create_test_ipld();

        let cid = builder.add_cbor(&test_ipld)?;
        builder.add_root(cid);

        let original = builder.build();

        // Export to bytes
        let exported_bytes = original.export_to_bytes().await?;

        // Import from bytes
        let mut imported = CarImporter::new();
        imported.import_from_bytes(exported_bytes).await?;

        // Verify the data is the same
        assert_eq!(imported.len(), original.len());
        assert_eq!(imported.roots(), original.roots());

        let decoded = imported.decode_cbor(&cid)?;
        assert_eq!(decoded, test_ipld);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_blocks() -> Result<()> {
        let mut builder = CarBuilder::new();

        // Add multiple IPLD objects
        let ipld1 = utils::create_test_ipld();
        let ipld2 = Ipld::String("another test".to_string());

        let cid1 = builder.add_cbor(&ipld1)?;
        let cid2 = builder.add_cbor(&ipld2)?;

        builder.add_root(cid1);
        builder.add_root(cid2);

        let importer = builder.build();

        assert_eq!(importer.len(), 2);
        assert_eq!(importer.roots().len(), 2);

        let decoded1 = importer.decode_cbor(&cid1)?;
        let decoded2 = importer.decode_cbor(&cid2)?;

        assert_eq!(decoded1, ipld1);
        assert_eq!(decoded2, ipld2);

        Ok(())
    }
}
