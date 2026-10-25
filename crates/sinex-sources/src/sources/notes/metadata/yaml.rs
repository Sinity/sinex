//! YAML syntax belongs to serde-saphyr. This boundary admits only values whose
//! types JSON can hold, without converting mapping keys or large integers.
use super::*;
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_saphyr::granit_parser::{Event, Parser, ScalarStyle};
use std::fmt;

fn invalid(kind: NoteFrontMatterError, line: Option<u64>, column: Option<u64>) -> NoteFrontMatter {
    NoteFrontMatter::Invalid { kind, line, column }
}

pub(super) fn parse(text: &str) -> NoteFrontMatter {
    // Front matter of any size is read. The limits left are real ones: nesting
    // depth bounds the parser's stack, and the alias limits stop a few lines
    // of aliases from expanding into an unbounded document.
    let mut options = serde_saphyr::options! {
        with_snippet: false,
        reject_unsupported_tags: true,
        strict_booleans: true,
        budget: serde_saphyr::budget! {
            max_depth: 64,
            max_nodes: usize::MAX,
            max_events: usize::MAX,
            max_total_scalar_bytes: usize::MAX,
            max_aliases: 64,
        },
    };
    options.alias_limits.max_total_replayed_events = 4096;
    options.alias_limits.max_replay_stack_depth = 16;
    options.alias_limits.max_alias_expansions_per_anchor = 32;
    let value = match serde_saphyr::from_str_with_options::<Json>(text, options) {
        Ok(Json(value)) => value,
        Err(error) => {
            let kind = problem(&error);
            let location = error.location();
            // YAML starts on line two of the note. Diagnostics carry no source
            // values, snippets or third-party error messages.
            return invalid(
                kind,
                location.map(|l| l.line() + 1),
                location.map(|l| l.column()),
            );
        }
    };
    // deserialize_any falls back to string/float for some oversized integers.
    // Check the library's actual scalar styles, so quoted numeric strings and
    // explicitly tagged strings remain strings. Run only after budget checks.
    static INTEGER: OnceLock<Regex> = OnceLock::new();
    let integer = INTEGER.get_or_init(|| {
        Regex::new(r"^[+-]?(?:[0-9][0-9_]*|0x[0-9a-fA-F_]+|0o[0-7_]+|0b[01_]+)$")
            .expect("integer shape")
    });
    for event in Parser::new_from_str(text) {
        let Ok((event, span)) = event else {
            return invalid(NoteFrontMatterError::Yaml, None, None);
        };
        let Event::Scalar(value, style, _, tag) = event else {
            continue;
        };
        if tag.as_ref().is_some_and(|tag| tag.core_suffix().is_none()) {
            return invalid(
                NoteFrontMatterError::UnsupportedValue,
                Some(span.start.line() as u64 + 1),
                Some(span.start.col() as u64 + 1),
            );
        }
        let integer_tag = tag.as_ref().and_then(|t| t.core_suffix()) == Some("int");
        if !integer_tag && (style != ScalarStyle::Plain || tag.is_some()) {
            continue;
        }
        if !integer.is_match(&value) {
            continue;
        }
        if serde_saphyr::from_str::<i64>(&value).is_err()
            && serde_saphyr::from_str::<u64>(&value).is_err()
        {
            return invalid(
                NoteFrontMatterError::UnsupportedValue,
                Some(span.start.line() as u64 + 1),
                Some(span.start.col() as u64 + 1),
            );
        }
    }
    NoteFrontMatter::Parsed { value }
}

struct Json(Value);

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(JsonVisitor)
    }
}
struct JsonVisitor;
impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON-representable YAML value with string mapping keys")
    }
    fn visit_unit<E: de::Error>(self) -> Result<Json, E> {
        Ok(Json(Value::Null))
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Json, E> {
        Ok(Json(Value::Bool(v)))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Json, E> {
        Ok(Json(v.into()))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Json, E> {
        Ok(Json(v.into()))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Json, E> {
        serde_json::Number::from_f64(v)
            .map(|n| Json(Value::Number(n)))
            .ok_or_else(|| E::custom("non-finite number"))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Json, E> {
        Ok(Json(Value::String(v.into())))
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<Json, E> {
        Ok(Json(Value::String(v)))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Json, A::Error> {
        let mut values = Vec::new();
        while let Some(Json(v)) = a.next_element()? {
            values.push(v);
        }
        Ok(Json(Value::Array(values)))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Json, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some((Json(k), Json(v))) = a.next_entry::<Json, Json>()? {
            let Value::String(key) = k else {
                return Err(de::Error::custom("non-string mapping key"));
            };
            if values.insert(key, v).is_some() {
                return Err(de::Error::custom("duplicate mapping key"));
            }
        }
        Ok(Json(Value::Object(values)))
    }
}

fn problem(error: &serde_saphyr::Error) -> NoteFrontMatterError {
    use serde_saphyr::Error;
    match error {
        Error::WithSnippet { error, .. } => problem(error),
        Error::Budget { .. }
        | Error::AliasReplayLimitExceeded { .. }
        | Error::AliasExpansionLimitExceeded { .. }
        | Error::AliasReplayStackDepthExceeded { .. }
        | Error::AliasReplayCounterOverflow { .. } => NoteFrontMatterError::Limit,
        Error::UnsupportedTag { .. }
        | Error::InvalidScalar { .. }
        | Error::TaggedScalarCannotDeserializeIntoString { .. }
        | Error::InvalidBinaryBase64 { .. }
        | Error::BinaryNotUtf8 { .. }
        | Error::NonFiniteFloat { .. }
        | Error::MultipleDocuments { .. } => NoteFrontMatterError::UnsupportedValue,
        Error::Message { msg, .. }
            if matches!(msg.as_str(), "non-string mapping key" | "non-finite number") =>
        {
            NoteFrontMatterError::UnsupportedValue
        }
        _ => NoteFrontMatterError::Yaml,
    }
}
