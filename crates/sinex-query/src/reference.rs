//! Citations in the shared reference grammar of common `references`: a bare
//! event ID, or `sinex:<kind>/<identity>` for the kinds `ev`, `record`, `key`,
//! `at` and `window`.
use super::{QueryError, Reference};
use jiff::Timestamp;
use references::{Product, Target};
use uuid::Uuid;

pub(super) fn parse_id(raw: &str) -> Option<Uuid> {
    let raw = raw.strip_prefix("sinex:ev/").unwrap_or(raw);
    Uuid::parse_str(raw).ok()
}

/// The global citation `sinex:<kind>/<identity>`, with the identity escaped
/// by the shared grammar so it parses back unchanged whatever it holds.
pub fn citation(kind: &str, identity: &str) -> String {
    references::Reference::object(
        references::Object::new(Product::Sinex, kind, identity, None)
            .expect("Sinex citation kinds are kind names and identities are nonempty"),
    )
    .global()
}

/// A citation: `sinex:ev/<id>`, a bare event ID, `sinex:record/...`,
/// `sinex:key/...`, `sinex:at/...` or `sinex:window/...`.
pub fn parse_reference(raw: &str) -> Result<Reference, QueryError> {
    if let Ok(id) = Uuid::parse_str(raw) {
        return Ok(Reference::Event(id));
    }
    let unknown = || {
        QueryError::new(format!(
            "`{raw}` is not a reference; use an event ID, sinex:ev/<id>, sinex:record/..., sinex:key/..., sinex:at/... or sinex:window/..."
        ))
    };
    let parsed = references::Reference::parse(raw).map_err(|_| unknown())?;
    let Target::Object(object) = parsed.target() else {
        return Err(unknown());
    };
    if object.product() != Product::Sinex {
        return Err(unknown());
    }
    if object.revision().is_some() || parsed.selector().is_some() {
        return Err(QueryError::new(format!(
            "`{raw}` names a revision or a place; a Sinex citation names a whole reading"
        )));
    }
    let identity = object.id();
    match object.kind() {
        "ev" => Uuid::parse_str(identity)
            .map(Reference::Event)
            .map_err(|_| QueryError::new("the identity in sinex:ev is not an event ID")),
        "record" => {
            let parts: Vec<&str> = identity.split('/').collect();
            if !(2..=4).contains(&parts.len()) {
                return Err(QueryError::new(
                    "sinex:record/<material>/<offset>[/<event type>[/<output>]]",
                ));
            }
            let material = Uuid::parse_str(parts[0])
                .map_err(|_| QueryError::new("the material in sinex:record is not an ID"))?;
            let offset = parts[1]
                .parse()
                .map_err(|_| QueryError::new("the offset in sinex:record is not a byte offset"))?;
            let output = parts
                .get(3)
                .map(|o| {
                    o.parse()
                        .map_err(|_| QueryError::new("the output in sinex:record is not a number"))
                })
                .transpose()?;
            Ok(Reference::Record {
                material,
                offset,
                event_type: parts.get(2).map(|s| s.to_string()),
                output,
            })
        }
        "key" => {
            let mut parts = identity.splitn(3, '/');
            let (Some(source), Some(event_type), Some(key)) =
                (parts.next(), parts.next(), parts.next())
            else {
                return Err(QueryError::new(
                    "sinex:key/<source>/<event type>/<equivalence key>",
                ));
            };
            Ok(Reference::Key {
                source: source.to_string(),
                event_type: event_type.to_string(),
                key: key.to_string(),
            })
        }
        "at" => {
            let (time, event_type) = identity
                .split_once('/')
                .ok_or_else(|| QueryError::new("sinex:at/<RFC3339 time>/<event type>"))?;
            let at = time.parse::<Timestamp>().map_err(|_| {
                QueryError::new("sinex:at requires an absolute RFC3339 time with offset")
            })?;
            if event_type.is_empty() || event_type.contains('/') {
                return Err(QueryError::new("sinex:at requires one event type"));
            }
            Ok(Reference::At {
                at,
                event_type: event_type.into(),
            })
        }
        "window" => {
            let (from, to) = identity
                .split_once('/')
                .ok_or_else(|| QueryError::new("sinex:window/<from>/<to>"))?;
            let from = from
                .parse::<Timestamp>()
                .map_err(|_| QueryError::new("window start requires an absolute RFC3339 time"))?;
            let to = to
                .parse::<Timestamp>()
                .map_err(|_| QueryError::new("window end requires an absolute RFC3339 time"))?;
            if from >= to {
                return Err(QueryError::new("window end must follow its start"));
            }
            Ok(Reference::Window { from, to })
        }
        _ => Err(unknown()),
    }
}
