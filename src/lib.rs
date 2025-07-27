pub mod car;
pub mod commit;
pub mod error;
pub mod mst;

// Re-export the main types for easier access
pub use car::{CarBuilder, CarImporter};
pub use error::{AtmosError, Result};

// Re-export commonly used types from dependencies
pub use bytes::Bytes;
pub use cid::Cid;
pub use ipld_core::ipld::Ipld;

/// Prelude module for easy importing of commonly used items
pub mod prelude {
    pub use crate::car::utils::{create_test_ipld, load_car_file, save_car_file};
    pub use crate::car::{CarBuilder, CarImporter};
    pub use crate::error::{AtmosError, Result};
    pub use bytes::Bytes;
    pub use cid::Cid;
    pub use ipld_core::ipld::Ipld;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::car::utils;

    #[tokio::test]
    async fn test_library_integration() -> Result<()> {
        // Test the main public interface
        let mut builder = CarBuilder::new();
        let test_ipld = utils::create_test_ipld();

        let cid = builder.add_cbor(&test_ipld)?;
        builder.add_root(cid);

        let importer = builder.build();

        assert_eq!(importer.len(), 1);
        assert!(importer.contains(&cid));

        let decoded = importer.decode_cbor(&cid)?;
        assert_eq!(decoded, test_ipld);

        Ok(())
    }
}
