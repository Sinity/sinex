//! Owner-installed native source declarations. Transport credentials authorize
//! their use; source labels in an event payload never confer that authority.
use crate::{KindDeclaration, SourceRef};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDeclaration {
    pub source: SourceRef,
    pub family: String,
    /// Full immutable emitted declarations bind both version and schema.
    pub kinds: Vec<KindDeclaration>,
}

impl SourceDeclaration {
    pub fn fingerprint(&self) -> String {
        let mut normalized = self.clone();
        normalized.kinds.sort_by(|a, b| a.kind.cmp(&b.kind));
        crate::json_identity::hash(&serde_json::to_value(normalized).expect("source declarations serialize"))
    }

    pub fn allows(&self, kind: &crate::KindRef, hash: &str) -> bool {
        self.kinds.iter().any(|decl| &decl.kind == kind && decl.definition_hash() == hash)
    }
}

/// Owner-installed cache projection contract. Its exact kind definitions are
/// read requirements, independently of any credential's write authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionCatalog {
    pub name: String,
    pub version: u32,
    pub kinds: Vec<KindDeclaration>,
}

impl ProjectionCatalog {
    pub fn fingerprint(&self) -> String {
        let mut normalized = self.clone();
        normalized.kinds.sort_by(|a, b| a.kind.cmp(&b.kind));
        crate::json_identity::hash(&serde_json::to_value(normalized).expect("projection catalogs serialize"))
    }
}
