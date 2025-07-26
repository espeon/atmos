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
    type Error = String;

    fn try_from(value: Ipld) -> Result<Self, Self::Error> {
        // The IPLD value must be a Map
        let map = match value {
            Ipld::Map(m) => m,
            _ => return Err("Expected IPLD map".to_string()),
        };

        let did = match map.get("did") {
            Some(Ipld::String(s)) => s.clone(),
            _ => return Err("Missing or invalid 'did' field".to_string()),
        };

        let version = match map.get("version") {
            Some(Ipld::Integer(i)) => *i,
            _ => return Err("Missing or invalid 'version' field".to_string()),
        };

        let data = match map.get("data") {
            Some(Ipld::Link(s)) => s.clone(),
            _ => return Err("Missing or invalid 'data' field".to_string()),
        };

        let rev = match map.get("rev") {
            Some(Ipld::String(s)) => s.clone(),
            _ => return Err("Missing or invalid 'rev' field".to_string()),
        };

        let prev = match map.get("prev") {
            Some(Ipld::String(s)) => Some(s.clone()),
            Some(Ipld::Null) | None => None,
            _ => return Err("Invalid 'prev' field".to_string()),
        };

        let sig = match map.get("sig") {
            Some(Ipld::Bytes(b)) => b.clone(),
            _ => return Err("Missing or invalid 'sig' field".to_string()),
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
