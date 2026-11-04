//! The fusion families as data: `families.json`, read once and checked
//! before any family runs.
//!
//! A family is defined only once its definition is written against its real
//! sources, so the file may hold only the families named in [`DEFINED`], each
//! with the fused kind its definition names. Each family declares, over the
//! closed set of strategies (identifier, window, fallback, absorb):
//!
//! - its members in preference order: an event type and, where several
//!   sources write that type, the one source it reads, with the payload
//!   fields each member supplies and the identifier namespaces it carries;
//! - its scope fields, which partition matching (two hosts or two accounts
//!   never associate), and its match fields, which must be equal for a time
//!   match;
//! - its window, fallback and absorb spans, any of which may be absent;
//! - or, for a family whose identity alone decides whatever the records'
//!   times (mail by Message-ID within its account), the fields its keys are
//!   made of and how far apart in time its copies are expected to be;
//! - whether agents' duplicate decisions govern or are only recorded.
//!
//! The file is checked against the kind registry: every member and output
//! type must be registered, and every field the family writes must be a
//! field of its fused kind. A family's version covers its declared version
//! and its declaration's text, so a changed rule is a new version.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::EventPayload;
use std::collections::BTreeMap;
use std::sync::LazyLock;

/// The families whose definition is written, each with its fused kind.
pub const DEFINED: &[(&str, &str)] = &[("command", "command.fused"), ("mail", "mail.fused")];

/// The author whose duplicate decisions always govern.
pub const OPERATOR: &str = "operator";

const FILE: &str = include_str!("families.json");

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileText {
    families: Vec<FamilyText>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FamilyText {
    name: String,
    derivation: String,
    version: u32,
    output: String,
    start_field: String,
    scope: Vec<String>,
    #[serde(rename = "match", default, skip_serializing_if = "Vec::is_empty")]
    match_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    window_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fallback_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    absorb_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    keyed_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spread_ms: Option<u64>,
    /// Identifier namespaces whose values are UUIDv7 tokens minted when the
    /// occurrence started, carried in a top-level field of the same name by
    /// every member that has them. Their partners are read wherever they
    /// fall in time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tokens: Vec<String>,
    /// The family field, carried in a top-level field of the same name,
    /// holding how long an occurrence lasted in nanoseconds: with the mint
    /// time, it bounds where a token's partners can lie.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token_length: Option<String>,
    agent_decisions: AgentDecisions,
    members: Vec<MemberText>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberText {
    event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    /// Payload fields a record must hold exactly these texts in to be read.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    only: BTreeMap<String, String>,
    /// It joins only by identifier, never by time (its records' times are
    /// not the occurrence's start).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    identifier_only: bool,
    /// Its records are spans, so one without an end is still open.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    spans: bool,
    fields: BTreeMap<String, FieldText>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    identifiers: BTreeMap<String, FieldText>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum FieldText {
    Pointer(String),
    Normalized {
        pointer: String,
        normalize: Normalize,
    },
}

/// Whether agents' duplicate decisions decide pairs, or are recorded and shown only.
/// The operator's decisions always govern.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentDecisions {
    Govern,
    Record,
}

/// The closed set of normalisations a field reading may apply. Neither
/// merges different meanings: inner whitespace is kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Normalize {
    /// Outer whitespace removed; an empty value is absent.
    Trim,
    /// The text before the first `:` (Atuin's `host:user`).
    BeforeColon,
    /// Text kept as it is; empty or whitespace-only text is absent.
    NonEmpty,
}

/// Where a member reads one value from.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub pointer: &'static str,
    pub normalize: Option<Normalize>,
}

impl Field {
    /// The value at this field, normalised; `None` when absent or null.
    pub fn read(&self, payload: &Value) -> Option<Value> {
        let value = payload.pointer(self.pointer)?;
        if value.is_null() {
            return None;
        }
        match self.normalize {
            None => Some(value.clone()),
            Some(Normalize::Trim) => {
                let text = value.as_str()?.trim();
                (!text.is_empty()).then(|| Value::String(text.to_owned()))
            }
            Some(Normalize::NonEmpty) => {
                let text = value.as_str()?;
                (!text.trim().is_empty()).then(|| value.clone())
            }
            Some(Normalize::BeforeColon) => {
                let text = value.as_str()?;
                let host = text.split(':').next().unwrap_or(text);
                (!host.is_empty()).then(|| Value::String(host.to_owned()))
            }
        }
    }

    /// The value as identifier text: strings as they are, other values as
    /// JSON.
    pub fn read_text(&self, payload: &Value) -> Option<String> {
        match self.read(payload)? {
            Value::String(s) => Some(s),
            other => Some(other.to_string()),
        }
    }
}

/// One member of a family: the records of one event type (from one source,
/// where named).
#[derive(Clone, Debug)]
pub struct Member {
    pub event_type: &'static str,
    pub source: Option<&'static str>,
    /// Payload fields a record must hold exactly these texts in.
    pub only: Vec<(&'static str, &'static str)>,
    pub identifier_only: bool,
    pub spans: bool,
    /// The fused kind's fields this member supplies.
    pub fields: Vec<(&'static str, Field)>,
    /// The identifier namespaces this member carries.
    pub identifiers: Vec<(&'static str, Field)>,
}

impl Member {
    pub fn admits(&self, event_type: &str, source: &str, payload: &Value) -> bool {
        self.event_type == event_type
            && self.source.is_none_or(|s| s == source)
            && self
                .only
                .iter()
                .all(|(field, text)| payload.get(*field).and_then(Value::as_str) == Some(*text))
    }

    /// The kind a query reads this member's records with.
    pub fn kind(&self) -> crate::Kind {
        crate::Kind::only(self.event_type, &self.only)
    }

    /// The value of the family field `name` in `payload`.
    pub fn field(&self, name: &str, payload: &Value) -> Option<Value> {
        self.fields
            .iter()
            .find(|(field, _)| *field == name)
            .and_then(|(_, field)| field.read(payload))
    }
}

/// One defined fusion family, as declared.
#[derive(Clone, Debug)]
pub struct Family {
    pub name: &'static str,
    pub derivation: &'static str,
    /// The family's version: its declared version and its declaration's text.
    pub version: u32,
    pub output: &'static str,
    pub start_field: &'static str,
    pub scope: Vec<&'static str>,
    pub match_fields: Vec<&'static str>,
    pub window_ns: Option<i128>,
    pub fallback_ns: Option<i128>,
    pub absorb_ns: Option<i128>,
    /// For a family whose identity alone decides: the fields its keys are
    /// made of. Empty for a family keyed by hour.
    pub keyed_by: Vec<&'static str>,
    /// For a family keyed by identity: how far apart in time copies of one
    /// occurrence are expected to be. Consumers that read a period read this
    /// far beyond it; copies further apart can count once in each period.
    pub spread_ns: Option<i128>,
    /// Identifier namespaces carrying start-minted UUIDv7 tokens, each the
    /// name of the top-level field that carries it.
    pub tokens: Vec<&'static str>,
    /// The family field holding an occurrence's length, which bounds a
    /// token's lifetime.
    pub token_length: Option<&'static str>,
    pub agent_decisions: AgentDecisions,
    /// In preference order.
    pub members: Vec<Member>,
    /// The members' event types and the duplicate decision kind, once each.
    pub input_types: &'static [&'static str],
    /// The fused kind and the ambiguous-pair kind.
    pub output_types: &'static [&'static str],
}

/// Why the declaration file was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused {
    pub family: Option<String>,
    pub reason: String,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.family {
            Some(family) => write!(f, "fusion family {family}: {}", self.reason),
            None => write!(f, "fusion families: {}", self.reason),
        }
    }
}

fn leak(text: &str) -> &'static str {
    Box::leak(text.to_owned().into_boxed_str())
}

fn field(text: &FieldText) -> Result<Field, String> {
    let (pointer, normalize) = match text {
        FieldText::Pointer(pointer) => (pointer, None),
        FieldText::Normalized { pointer, normalize } => (pointer, Some(*normalize)),
    };
    if !pointer.starts_with('/') || pointer.len() < 2 {
        return Err(format!("field pointer {pointer:?} is not a payload field"));
    }
    Ok(Field {
        pointer: leak(pointer),
        normalize,
    })
}

const MS: i128 = 1_000_000;

/// The fields a registered kind's payload declares.
fn payload_fields(event_type: &str) -> Option<Vec<String>> {
    let registered = sinex_model::event_types::find(event_type)?;
    let schema = (registered.declaration)().payload_schema?;
    Some(
        schema
            .get("properties")?
            .as_object()?
            .keys()
            .cloned()
            .collect(),
    )
}

fn family(text: &FamilyText) -> Result<Family, String> {
    let Some((_, defined_output)) = DEFINED.iter().find(|(name, _)| *name == text.name) else {
        return Err("no written definition; only defined families fuse".into());
    };
    if text.output != *defined_output {
        return Err(format!(
            "its definition writes {defined_output}, not {}",
            text.output
        ));
    }
    let output_fields = payload_fields(&text.output)
        .ok_or_else(|| format!("output {} is not a registered kind", text.output))?;
    if !output_fields.contains(&text.start_field) {
        return Err(format!(
            "start field {} is not a field of {}",
            text.start_field, text.output
        ));
    }
    if text.members.len() < 2 {
        return Err("a family needs at least two members".into());
    }
    if text.scope.is_empty() {
        return Err("a family needs scope fields; matching never crosses scopes".into());
    }
    let window_ns = text.window_ms.map(|ms| i128::from(ms) * MS);
    let fallback_ns = text.fallback_ms.map(|ms| i128::from(ms) * MS);
    let absorb_ns = text.absorb_ms.map(|ms| i128::from(ms) * MS);
    if fallback_ns.is_some() && fallback_ns < window_ns {
        return Err("the fallback window is narrower than the window".into());
    }
    if (fallback_ns.is_some() || absorb_ns.is_some()) && window_ns.is_none() {
        return Err("fallback and absorb need a window".into());
    }
    let spread_ns = text.spread_ms.map(|ms| i128::from(ms) * MS);
    if text.keyed_by.is_empty() != spread_ns.is_none() {
        return Err("keys by identity and the spread are declared together".into());
    }
    if !text.keyed_by.is_empty() && window_ns.is_some() {
        return Err("a family keyed by identity matches by identity alone, not time".into());
    }
    if window_ns.is_some() && text.match_fields.is_empty() {
        return Err("time matching needs match fields".into());
    }
    let mut members = Vec::new();
    let mut namespaces: BTreeMap<&str, usize> = BTreeMap::new();
    for m in &text.members {
        if sinex_model::event_types::find(&m.event_type).is_none() {
            return Err(format!("member {} is not a registered kind", m.event_type));
        }
        if members.iter().any(|other: &Member| {
            other.event_type == m.event_type
                && other.source == m.source.as_deref()
                && other
                    .only
                    .iter()
                    .map(|(f, v)| (f.to_string(), v.to_string()))
                    .collect::<BTreeMap<_, _>>()
                    == m.only
        }) {
            return Err(format!("member {} is declared twice", m.event_type));
        }
        if m.spans && absorb_ns.is_none() {
            return Err(format!(
                "member {} has spans but nothing absorbs",
                m.event_type
            ));
        }
        let member_fields = payload_fields(&m.event_type).unwrap_or_default();
        let mut fields = Vec::new();
        for (name, text_field) in &m.fields {
            if !output_fields.contains(name) {
                return Err(format!("{name} is not a field of {}", text.output));
            }
            let f = field(text_field)?;
            let top = f.pointer[1..].split('/').next().unwrap_or_default();
            if !member_fields.iter().any(|known| known == top) {
                return Err(format!("{} has no field {top}", m.event_type));
            }
            fields.push((leak(name), f));
        }
        let time_fields: &[String] = if m.identifier_only {
            &[]
        } else {
            &text.match_fields
        };
        if m.identifier_only && m.identifiers.is_empty() {
            return Err(format!(
                "member {} joins only by identifier but carries none",
                m.event_type
            ));
        }
        for required in text.scope.iter().chain(time_fields).chain(&text.keyed_by) {
            if !m.fields.contains_key(required) {
                return Err(format!(
                    "member {} does not supply {required}",
                    m.event_type
                ));
            }
        }
        // A key field is read by equality on a top-level payload field, so
        // the stored value must be the key's value.
        for name in &text.keyed_by {
            let f = field(&m.fields[name])?;
            if f.pointer[1..].contains('/')
                || !matches!(f.normalize, None | Some(Normalize::NonEmpty))
            {
                return Err(format!(
                    "key field {name} of {} must be a top-level field kept as it is",
                    m.event_type
                ));
            }
        }
        let mut identifiers = Vec::new();
        for (namespace, text_field) in &m.identifiers {
            let f = field(text_field)?;
            let top = f.pointer[1..].split('/').next().unwrap_or_default();
            if !member_fields.iter().any(|known| known == top) {
                return Err(format!("{} has no field {top}", m.event_type));
            }
            *namespaces.entry(namespace).or_default() += 1;
            identifiers.push((leak(namespace), f));
        }
        for (field, _) in &m.only {
            if !member_fields.iter().any(|known| known == field) {
                return Err(format!("{} has no field {field}", m.event_type));
            }
        }
        for token in &text.tokens {
            if let Some(f) = m.identifiers.get(token)
                && field(f)?.pointer != format!("/{token}")
            {
                return Err(format!(
                    "token {token} of {} is not carried in its top-level field {token}",
                    m.event_type
                ));
            }
        }
        if let Some(length) = &text.token_length
            && let Some(f) = m.fields.get(length)
            && field(f)?.pointer != format!("/{length}")
        {
            return Err(format!(
                "token length {length} of {} is not carried in its top-level field {length}",
                m.event_type
            ));
        }
        members.push(Member {
            event_type: leak(&m.event_type),
            source: m.source.as_deref().map(leak),
            only: m.only.iter().map(|(f, v)| (leak(f), leak(v))).collect(),
            identifier_only: m.identifier_only,
            spans: m.spans,
            fields,
            identifiers,
        });
    }
    if let Some((namespace, _)) = namespaces.iter().find(|(_, n)| **n < 2) {
        return Err(format!(
            "identifier namespace {namespace} is carried by one member; it can join nothing"
        ));
    }
    if window_ns.is_none() && namespaces.is_empty() {
        return Err("a family needs a window or a shared identifier namespace".into());
    }
    for token in &text.tokens {
        if namespaces.get(token.as_str()).is_none_or(|n| *n < 2) {
            return Err(format!(
                "token {token} is not an identifier two members carry"
            ));
        }
    }
    if let Some(length) = &text.token_length {
        if text.tokens.is_empty() {
            return Err("a token length needs a token".into());
        }
        if !output_fields.contains(length) {
            return Err(format!(
                "token length {length} is not a field of {}",
                text.output
            ));
        }
    }
    for name in &text.scope {
        if !output_fields.contains(name) {
            return Err(format!(
                "scope field {name} is not a field of {}",
                text.output
            ));
        }
    }
    let mut input_types: Vec<&'static str> = Vec::new();
    for m in &members {
        if !input_types.contains(&m.event_type) {
            input_types.push(m.event_type);
        }
    }
    input_types.push(sinex_model::event_types::duplicate::Duplicate::EVENT_TYPE);
    let canonical = serde_json::to_vec(text).map_err(|e| e.to_string())?;
    let mut hash = blake3::Hasher::new();
    hash.update(&text.version.to_le_bytes());
    hash.update(&canonical);
    let version = 1
        + (u32::from_le_bytes(
            hash.finalize().as_bytes()[..4]
                .try_into()
                .expect("four bytes"),
        ) >> 2);
    let output = leak(&text.output);
    Ok(Family {
        name: leak(&text.name),
        derivation: leak(&text.derivation),
        version,
        output,
        start_field: leak(&text.start_field),
        scope: text.scope.iter().map(|s| leak(s)).collect(),
        match_fields: text.match_fields.iter().map(|s| leak(s)).collect(),
        window_ns,
        fallback_ns,
        absorb_ns,
        keyed_by: text.keyed_by.iter().map(|s| leak(s)).collect(),
        tokens: text.tokens.iter().map(|s| leak(s)).collect(),
        token_length: text.token_length.as_deref().map(leak),
        spread_ns,
        agent_decisions: text.agent_decisions,
        members,
        input_types: Box::leak(input_types.into_boxed_slice()),
        output_types: Box::leak(
            vec![
                output,
                sinex_model::event_types::fused::ConsideredPair::EVENT_TYPE,
            ]
            .into_boxed_slice(),
        ),
    })
}

/// Read and check a declaration file. Any refused family refuses the file.
pub fn parse(text: &str) -> Result<Vec<Family>, Refused> {
    let file: FileText = serde_json::from_str(text).map_err(|e| Refused {
        family: None,
        reason: e.to_string(),
    })?;
    let mut families: Vec<Family> = Vec::new();
    for text in &file.families {
        let refused = |reason: String| Refused {
            family: Some(text.name.clone()),
            reason,
        };
        if families.iter().any(|f| f.name == text.name) {
            return Err(refused("declared twice".into()));
        }
        let family = family(text).map_err(refused)?;
        if families.iter().any(|f| f.derivation == family.derivation) {
            return Err(refused(format!(
                "derivation {} is declared twice",
                family.derivation
            )));
        }
        families.push(family);
    }
    Ok(families)
}

static FAMILIES: LazyLock<Result<Vec<Family>, Refused>> = LazyLock::new(|| parse(FILE));

/// Check the shipped declaration; fusion refuses to start when it fails.
pub fn check() -> Result<(), Refused> {
    FAMILIES.as_ref().map(|_| ()).map_err(Clone::clone)
}

/// The shipped families. Start-up runs [`check`] first, so this is reached
/// only with a valid file; a test checks the shipped one.
pub fn families() -> &'static [Family] {
    match FAMILIES.as_ref() {
        Ok(families) => families,
        Err(refused) => panic!("{refused}"),
    }
}

/// The shipped family `name`.
pub fn family_named(name: &str) -> &'static Family {
    families()
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("fusion family {name} is declared"))
}

#[cfg(test)]
mod tests;
