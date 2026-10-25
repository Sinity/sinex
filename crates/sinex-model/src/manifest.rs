//! A material's manifest: what its bytes are and how to read them again. The
//! fields code acts on are typed; what a driver records only to describe its
//! input stays in `details`.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Whether stored material contributes to the original input byte stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MaterialRole {
    /// The input's own bytes.
    #[default]
    Input,
    /// An export archive kept whole; its records are read from record parts.
    OriginalArchive,
    /// Records decoded from an archive or read from a database.
    Records,
}

impl MaterialRole {
    fn is_input(&self) -> bool {
        *self == Self::Input
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(default, skip_serializing_if = "MaterialRole::is_input")]
    pub role: MaterialRole,
    /// The file the bytes were read from; a member's own path is in `members`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// How much of the part before this one was admitted when this one was
    /// read; replay reads that part only that far.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_through: Option<u64>,
    /// The container an original archive was stored as (`zip`, `gzip`, `tar`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// The record ranges the driver read, when it read only some of the
    /// framed records (a rewrite's new records).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<Vec<(usize, usize)>>,
    /// The members the bytes are made of, in stream order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<Member>,
    /// Reading order: a base plus the record's offset (a database snapshot),
    /// one read time for every record, or the file offset plus the record's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_base: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_at_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_offset: Option<u64>,
    /// What the driver records to describe its input (host, table, query).
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

/// One member of a material made of members: an archive member or a file of
/// a directory bundle.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub path: String,
    /// Where the member starts in the material's bytes.
    #[serde(default)]
    pub stream_offset: u64,
    #[serde(default)]
    pub length: u64,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

impl Manifest {
    /// A manifest from a JSON object a driver writes, its known fields typed.
    /// Drivers write these objects themselves, so a known field of the wrong
    /// type is a bug in the driver.
    pub fn from_json(value: Value) -> Self {
        serde_json::from_value(value).expect("a driver writes its manifest's known fields typed")
    }

    /// A detail value, or `Null`.
    pub fn detail(&self, key: &str) -> &Value {
        self.details.get(key).unwrap_or(&Value::Null)
    }

    /// The member whose stream range holds byte `anchor`. Members are in
    /// stream order and do not overlap.
    pub fn member_at(&self, anchor: usize) -> Option<&Member> {
        let members = &self.members;
        let i = members.partition_point(|m| m.stream_offset as usize <= anchor);
        let member = members[..i].last()?;
        let start = member.stream_offset as usize;
        if member.length == 0 && anchor == start {
            // Equal zero-length members at one byte offset have no unique context.
            if members[..i - 1].last().is_some_and(|previous| {
                previous.stream_offset as usize == anchor && previous.length == 0
            }) {
                return None;
            }
            return Some(member);
        }
        (anchor < start + member.length as usize).then_some(member)
    }
}

#[cfg(test)]
mod tests;
