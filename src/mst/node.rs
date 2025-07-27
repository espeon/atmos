use crate::error::AtmosError;
use ipld_core::ipld::Ipld;

#[derive(Clone, Debug)]
pub struct MstNode {
    pub l: Option<cid::Cid>,
    pub e: Vec<MstNodeLeaf>,
}

#[derive(Clone, Debug)]
pub struct MstNodeLeaf {
    /// prefix len
    pub p: usize,
    /// key suffix
    pub k: crate::Bytes,
    /// value: cid link to the value in the outer CAR
    pub v: cid::Cid,
    /// tree: optional link to the next tree node
    pub t: Option<cid::Cid>,
}

impl MstNode {
    pub fn new(l: Option<cid::Cid>, e: Vec<MstNodeLeaf>) -> Self {
        MstNode { l, e }
    }

    pub fn left(&self) -> Option<cid::Cid> {
        self.l
    }

    pub fn entries(&self) -> &Vec<MstNodeLeaf> {
        &self.e
    }
}

impl TryFrom<Ipld> for MstNode {
    type Error = AtmosError;
    fn try_from(node: Ipld) -> Result<Self, AtmosError> {
        match node {
            Ipld::Map(map) => {
                let l = if let Some(Ipld::Link(cid)) = map.get("l") {
                    Some(cid.clone())
                } else {
                    None
                };

                let e = if let Some(Ipld::List(entries)) = map.get("e") {
                    entries
                        .iter()
                        .map(|entry| MstNodeLeaf::try_from(entry))
                        .collect::<Result<Vec<MstNodeLeaf>, _>>()?
                } else {
                    return Err(AtmosError::missing_field("e"));
                };

                Ok(MstNode { l, e })
            }
            _ => Err(AtmosError::invalid_ipld("Expected IPLD map for MstNode")),
        }
    }
}

impl TryFrom<&Ipld> for MstNodeLeaf {
    type Error = AtmosError;
    fn try_from(entry: &Ipld) -> Result<Self, Self::Error> {
        match entry {
            Ipld::Map(map) => {
                let v = if let Some(Ipld::Link(v_cid)) = map.get("v") {
                    *v_cid
                } else {
                    return Err(AtmosError::missing_field("v"));
                };

                let t = if let Some(Ipld::Link(t_cid)) = map.get("t") {
                    Some(*t_cid)
                } else {
                    None
                };

                let p = map
                    .get("p")
                    .ok_or_else(|| AtmosError::missing_field("p"))?
                    .to_owned()
                    .try_into()
                    .map_err(|_| AtmosError::invalid_field("p", "must be valid integer"))?;

                let k = map
                    .get("k")
                    .ok_or_else(|| AtmosError::missing_field("k"))?
                    .to_owned();
                let k: crate::Bytes = <Ipld as TryInto<Vec<u8>>>::try_into(k)
                    .map(|b| crate::Bytes::copy_from_slice(&b))
                    .map_err(|_| AtmosError::invalid_field("k", "must be valid bytes"))?;

                Ok(MstNodeLeaf { p, k, v, t })
            }
            _ => Err(AtmosError::invalid_ipld(
                "Expected IPLD map for MstNodeLeaf",
            )),
        }
    }
}
