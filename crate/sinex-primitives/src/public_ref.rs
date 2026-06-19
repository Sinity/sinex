//! Public Sinex object-reference grammar.

use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::JsonValue;
use crate::views::{ActionAvailability, SinexObjectKind, SinexObjectRef};

pub const RESOLVED_OBJECT_VIEW_SCHEMA_VERSION: &str = "sinex.resolved-object/v1";

#[derive(Debug, Clone, PartialEq)]
pub struct PublicSinexRef {
    pub kind: SinexObjectKind,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicSinexRefParseError {
    MissingSeparator,
    EmptyKind,
    EmptyId,
    UnknownKind(String),
}

impl fmt::Display for PublicSinexRefParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSeparator => write!(f, "Sinex ref must use '<kind>:<id>'"),
            Self::EmptyKind => write!(f, "Sinex ref kind must not be empty"),
            Self::EmptyId => write!(f, "Sinex ref id must not be empty"),
            Self::UnknownKind(kind) => write!(f, "unknown Sinex ref kind '{kind}'"),
        }
    }
}

impl std::error::Error for PublicSinexRefParseError {}

impl PublicSinexRef {
    #[must_use]
    pub fn new(kind: SinexObjectKind, id: impl Into<String>) -> Self {
        Self {
            kind,
            id: id.into(),
        }
    }

    #[must_use]
    pub fn into_object_ref(self) -> SinexObjectRef {
        SinexObjectRef::new(self.kind, self.id)
    }
}

impl fmt::Display for PublicSinexRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", public_kind_name(&self.kind), self.id)
    }
}

impl FromStr for PublicSinexRef {
    type Err = PublicSinexRefParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let Some((kind, id)) = input.split_once(':') else {
            return Err(PublicSinexRefParseError::MissingSeparator);
        };
        if kind.is_empty() {
            return Err(PublicSinexRefParseError::EmptyKind);
        }
        if id.is_empty() {
            return Err(PublicSinexRefParseError::EmptyId);
        }
        let kind = parse_public_kind(kind)
            .ok_or_else(|| PublicSinexRefParseError::UnknownKind(kind.to_string()))?;
        Ok(Self::new(kind, id))
    }
}

impl fmt::Display for SinexObjectRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        PublicSinexRef::new(self.kind.clone(), self.id.clone()).fmt(f)
    }
}

impl FromStr for SinexObjectRef {
    type Err = PublicSinexRefParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        input
            .parse::<PublicSinexRef>()
            .map(PublicSinexRef::into_object_ref)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedObjectStatus {
    Resolved,
    NotFound,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedObjectView {
    pub schema_version: String,
    #[serde(rename = "ref")]
    pub ref_: SinexObjectRef,
    pub public_ref: String,
    pub status: ResolvedObjectStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_surface: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub payload: JsonValue,
}

impl ResolvedObjectView {
    #[must_use]
    pub fn resolved(
        ref_: SinexObjectRef,
        source_surface: impl Into<String>,
        payload: JsonValue,
    ) -> Self {
        let public_ref = ref_.to_string();
        Self {
            schema_version: RESOLVED_OBJECT_VIEW_SCHEMA_VERSION.to_string(),
            ref_,
            public_ref,
            status: ResolvedObjectStatus::Resolved,
            source_surface: Some(source_surface.into()),
            message: None,
            actions: Vec::new(),
            payload,
        }
    }

    #[must_use]
    pub fn unsupported(ref_: SinexObjectRef) -> Self {
        let public_ref = ref_.to_string();
        Self {
            schema_version: RESOLVED_OBJECT_VIEW_SCHEMA_VERSION.to_string(),
            ref_,
            public_ref,
            status: ResolvedObjectStatus::Unsupported,
            source_surface: None,
            message: Some("this ref kind parses but has no show resolver yet".to_string()),
            actions: Vec::new(),
            payload: JsonValue::Null,
        }
    }

    #[must_use]
    pub fn not_found(ref_: SinexObjectRef, source_surface: impl Into<String>) -> Self {
        let public_ref = ref_.to_string();
        Self {
            schema_version: RESOLVED_OBJECT_VIEW_SCHEMA_VERSION.to_string(),
            ref_,
            public_ref,
            status: ResolvedObjectStatus::NotFound,
            source_surface: Some(source_surface.into()),
            message: Some("this ref kind has a resolver, but no object matched the id".to_string()),
            actions: Vec::new(),
            payload: JsonValue::Null,
        }
    }
}

#[must_use]
pub fn public_kind_name(kind: &SinexObjectKind) -> &'static str {
    match kind {
        SinexObjectKind::Event => "event",
        SinexObjectKind::SourceDriver => "source-driver",
        SinexObjectKind::SourceMaterial => "source-material",
        SinexObjectKind::MaterialAnchor => "material-anchor",
        SinexObjectKind::Document => "document",
        SinexObjectKind::DocumentChunk => "document-chunk",
        SinexObjectKind::Task => "task",
        SinexObjectKind::SemanticLane => "semantic-lane",
        SinexObjectKind::SemanticEntity => "semantic-entity",
        SinexObjectKind::SemanticRelation => "semantic-relation",
        SinexObjectKind::Operation => "operation",
        SinexObjectKind::Projection => "projection",
        SinexObjectKind::Artifact => "artifact",
        SinexObjectKind::QueryRun => "query-run",
        SinexObjectKind::AdmissionOutcome => "admission-outcome",
        SinexObjectKind::DebtRow => "debt-row",
        SinexObjectKind::Proposal => "proposal",
        SinexObjectKind::Judgment => "judgment",
        SinexObjectKind::ExternalRef => "external-ref",
        SinexObjectKind::Policy => "policy",
        SinexObjectKind::ReplayRun => "replay-run",
        SinexObjectKind::Snapshot => "snapshot",
        SinexObjectKind::DlqMessage => "dlq-message",
        SinexObjectKind::ContextPack => "context-pack",
        SinexObjectKind::MomentCandidate => "moment-candidate",
        SinexObjectKind::PrivacySession => "privacy-session",
        SinexObjectKind::Caveat => "caveat",
        SinexObjectKind::RpcMethod => "rpc-method",
        SinexObjectKind::RuntimeModule => "runtime-module",
        SinexObjectKind::Command => "command",
    }
}

#[must_use]
pub fn parse_public_kind(kind: &str) -> Option<SinexObjectKind> {
    Some(match kind {
        "event" => SinexObjectKind::Event,
        "source-driver" => SinexObjectKind::SourceDriver,
        "source-material" => SinexObjectKind::SourceMaterial,
        "material-anchor" => SinexObjectKind::MaterialAnchor,
        "document" => SinexObjectKind::Document,
        "document-chunk" => SinexObjectKind::DocumentChunk,
        "task" => SinexObjectKind::Task,
        "semantic-lane" => SinexObjectKind::SemanticLane,
        "semantic-entity" => SinexObjectKind::SemanticEntity,
        "semantic-relation" => SinexObjectKind::SemanticRelation,
        "operation" => SinexObjectKind::Operation,
        "projection" => SinexObjectKind::Projection,
        "artifact" => SinexObjectKind::Artifact,
        "query-run" => SinexObjectKind::QueryRun,
        "admission-outcome" => SinexObjectKind::AdmissionOutcome,
        "debt-row" => SinexObjectKind::DebtRow,
        "proposal" => SinexObjectKind::Proposal,
        "judgment" => SinexObjectKind::Judgment,
        "external-ref" => SinexObjectKind::ExternalRef,
        "policy" => SinexObjectKind::Policy,
        "replay-run" => SinexObjectKind::ReplayRun,
        "snapshot" => SinexObjectKind::Snapshot,
        "dlq-message" => SinexObjectKind::DlqMessage,
        "context-pack" => SinexObjectKind::ContextPack,
        "moment-candidate" => SinexObjectKind::MomentCandidate,
        "privacy-session" => SinexObjectKind::PrivacySession,
        "caveat" => SinexObjectKind::Caveat,
        "rpc-method" => SinexObjectKind::RpcMethod,
        "runtime-module" => SinexObjectKind::RuntimeModule,
        "command" => SinexObjectKind::Command,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_ref_roundtrips_punctuated_ids() {
        let parsed: PublicSinexRef = "source-material:0199:abc/def".parse().unwrap();
        assert_eq!(parsed.kind, SinexObjectKind::SourceMaterial);
        assert_eq!(parsed.id, "0199:abc/def");
        assert_eq!(parsed.to_string(), "source-material:0199:abc/def");
    }

    #[test]
    fn public_ref_rejects_invalid_forms() {
        assert_eq!(
            "event".parse::<PublicSinexRef>().unwrap_err(),
            PublicSinexRefParseError::MissingSeparator
        );
        assert_eq!(
            ":id".parse::<PublicSinexRef>().unwrap_err(),
            PublicSinexRefParseError::EmptyKind
        );
        assert_eq!(
            "event:".parse::<PublicSinexRef>().unwrap_err(),
            PublicSinexRefParseError::EmptyId
        );
        assert_eq!(
            "source_material:id".parse::<PublicSinexRef>().unwrap_err(),
            PublicSinexRefParseError::UnknownKind("source_material".to_string())
        );
    }

    #[test]
    fn resolved_object_view_distinguishes_not_found_and_unsupported() {
        let ref_ = SinexObjectRef::new(SinexObjectKind::SourceDriver, "terminal.fish-history");

        let missing = ResolvedObjectView::not_found(ref_.clone(), "sinexctl.sources.status");
        assert_eq!(missing.public_ref, "source-driver:terminal.fish-history");
        assert_eq!(missing.status, ResolvedObjectStatus::NotFound);
        assert_eq!(
            missing.source_surface.as_deref(),
            Some("sinexctl.sources.status")
        );

        let unsupported = ResolvedObjectView::unsupported(ref_);
        assert_eq!(unsupported.status, ResolvedObjectStatus::Unsupported);
        assert!(unsupported.source_surface.is_none());
    }
}
