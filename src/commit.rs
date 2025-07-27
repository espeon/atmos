use crate::error::AtmosError;
use ipld_core::ipld::Ipld;

pub struct ATProtoCommit {
    pub did: String,
    pub version: i128,
    // CID link
    pub data: cid::Cid,
    // TID format
    pub rev: String,
    // Previous commit CID link, "nullable"
    pub prev: Option<String>,
    pub sig: crate::Bytes,
}

impl TryFrom<Ipld> for ATProtoCommit {
    type Error = AtmosError;

    fn try_from(value: Ipld) -> Result<Self, Self::Error> {
        // The IPLD value must be a Map
        let map = match value {
            Ipld::Map(m) => m,
            _ => return Err(AtmosError::invalid_ipld("Expected IPLD map")),
        };

        let did = match map.get("did") {
            Some(Ipld::String(s)) => s.clone(),
            _ => return Err(AtmosError::missing_field("did")),
        };

        let version = match map.get("version") {
            Some(Ipld::Integer(i)) => *i,
            _ => return Err(AtmosError::missing_field("version")),
        };

        let data = match map.get("data") {
            Some(Ipld::Link(s)) => s.clone(),
            _ => return Err(AtmosError::missing_field("data")),
        };

        let rev = match map.get("rev") {
            Some(Ipld::String(s)) => s.clone(),
            _ => return Err(AtmosError::missing_field("rev")),
        };

        let prev = match map.get("prev") {
            Some(Ipld::String(s)) => Some(s.clone()),
            Some(Ipld::Null) | None => None,
            _ => return Err(AtmosError::invalid_field("prev", "must be string or null")),
        };

        let sig = match map.get("sig") {
            Some(Ipld::Bytes(b)) => b.clone(),
            _ => return Err(AtmosError::missing_field("sig")),
        };

        Ok(ATProtoCommit {
            did,
            version,
            data,
            rev,
            prev,
            sig: crate::Bytes::from(sig),
        })
    }
}
