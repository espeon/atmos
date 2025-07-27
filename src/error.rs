use std::io;
use thiserror::Error;

/// Main error type for the atmos library
#[derive(Error, Debug)]
pub enum AtmosError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("CBOR encoding error: {0}")]
    CborEncodingIo(#[from] serde_ipld_dagcbor::EncodeError<io::Error>),

    #[error("CBOR decoding error: {0}")]
    CborDecodingIo(#[from] serde_ipld_dagcbor::DecodeError<io::Error>),

    #[error("CBOR encoding error: {0}")]
    CborEncodingGeneric(#[from] serde_ipld_dagcbor::EncodeError<std::collections::TryReserveError>),

    #[error("CBOR decoding error: {0}")]
    CborDecodingGeneric(#[from] serde_ipld_dagcbor::DecodeError<std::convert::Infallible>),

    #[error("JSON encoding error: {0}")]
    JsonEncoding(#[from] serde_ipld_dagjson::EncodeError),

    #[error("JSON decoding error: {0}")]
    JsonDecoding(#[from] serde_ipld_dagjson::DecodeError),

    #[error("CAR format error: {0}")]
    Car(#[from] iroh_car::Error),

    #[error("Block not found for CID: {cid}")]
    BlockNotFound { cid: String },

    #[error("Invalid IPLD structure: {message}")]
    InvalidIpld { message: String },

    #[error("MST error: {message}")]
    Mst { message: String },

    #[error("Commit parsing error: {message}")]
    CommitParsing { message: String },

    #[error("Missing required field: {field}")]
    MissingField { field: String },

    #[error("Invalid field value: {field} - {reason}")]
    InvalidField { field: String, reason: String },

    #[error("CAR importer has no roots")]
    NoRoots,

    #[error("CAR importer must have exactly one root for the commit")]
    InvalidRootCount,

    #[error("Node conversion failed: {reason}")]
    NodeConversion { reason: String },
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, AtmosError>;

impl AtmosError {
    /// Create a block not found error
    pub fn block_not_found(cid: impl ToString) -> Self {
        Self::BlockNotFound {
            cid: cid.to_string(),
        }
    }

    /// Create an invalid IPLD error
    pub fn invalid_ipld(message: impl ToString) -> Self {
        Self::InvalidIpld {
            message: message.to_string(),
        }
    }

    /// Create an MST error
    pub fn mst(message: impl ToString) -> Self {
        Self::Mst {
            message: message.to_string(),
        }
    }

    /// Create a commit parsing error
    pub fn commit_parsing(message: impl ToString) -> Self {
        Self::CommitParsing {
            message: message.to_string(),
        }
    }

    /// Create a missing field error
    pub fn missing_field(field: impl ToString) -> Self {
        Self::MissingField {
            field: field.to_string(),
        }
    }

    /// Create an invalid field error
    pub fn invalid_field(field: impl ToString, reason: impl ToString) -> Self {
        Self::InvalidField {
            field: field.to_string(),
            reason: reason.to_string(),
        }
    }

    /// Create a node conversion error
    pub fn node_conversion(reason: impl ToString) -> Self {
        Self::NodeConversion {
            reason: reason.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = AtmosError::block_not_found("bafytest123");
        assert!(err.to_string().contains("bafytest123"));

        let err = AtmosError::invalid_ipld("Expected map");
        assert!(err.to_string().contains("Expected map"));

        let err = AtmosError::missing_field("data");
        assert!(err.to_string().contains("data"));

        let err = AtmosError::invalid_field("version", "must be positive");
        assert!(err.to_string().contains("version"));
        assert!(err.to_string().contains("must be positive"));
    }

    #[test]
    fn test_error_display() {
        let err = AtmosError::NoRoots;
        assert_eq!(err.to_string(), "CAR importer has no roots");

        let err = AtmosError::InvalidRootCount;
        assert_eq!(
            err.to_string(),
            "CAR importer must have exactly one root for the commit"
        );
    }

    #[test]
    fn test_result_alias() {
        fn test_function() -> Result<i32> {
            Ok(42)
        }

        assert_eq!(test_function().unwrap(), 42);
    }
}
