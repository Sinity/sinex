//! Citations: an event ID, `sinex:ev/<id>`, `sinex:record/...` or
//! `sinex:key/...`.
use super::{QueryError, Reference};
use jiff::Timestamp;
use uuid::Uuid;

pub(super) fn parse_id(raw: &str) -> Option<Uuid> {
    let raw = raw.strip_prefix("sinex:ev/").unwrap_or(raw);
    Uuid::parse_str(raw).ok()
}

/// A citation: `sinex:ev/<id>`, a bare event ID, `sinex:record/...` or
/// `sinex:key/...`.
pub fn parse_reference(raw: &str) -> Result<Reference, QueryError> {
    if let Some(id) = parse_id(raw) {
        return Ok(Reference::Event(id));
    }
    if let Some(rest) = raw.strip_prefix("sinex:record/") {
        let parts: Vec<&str> = rest.split('/').collect();
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
        return Ok(Reference::Record {
            material,
            offset,
            event_type: parts.get(2).map(|s| s.to_string()),
            output,
        });
    }
    if let Some(rest) = raw.strip_prefix("sinex:key/") {
        let mut parts = rest.splitn(3, '/');
        let (Some(source), Some(event_type), Some(key)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(QueryError::new(
                "sinex:key/<source>/<event type>/<equivalence key>",
            ));
        };
        return Ok(Reference::Key {
            source: source.to_string(),
            event_type: event_type.to_string(),
            key: key.to_string(),
        });
    }
    if let Some(rest) = raw.strip_prefix("sinex:at/") {
        let (time, event_type) = rest
            .split_once('/')
            .ok_or_else(|| QueryError::new("sinex:at/<RFC3339 time>/<event type>"))?;
        let at = time.parse::<Timestamp>().map_err(|_| {
            QueryError::new("sinex:at requires an absolute RFC3339 time with offset")
        })?;
        if event_type.is_empty() || event_type.contains('/') {
            return Err(QueryError::new("sinex:at requires one event type"));
        }
        return Ok(Reference::At {
            at,
            event_type: event_type.into(),
        });
    }
    if let Some(rest) = raw.strip_prefix("sinex:window/") {
        let (from, to) = rest
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
        return Ok(Reference::Window { from, to });
    }
    Err(QueryError::new(format!(
        "`{raw}` is not a reference; use an event ID, sinex:ev/<id>, sinex:record/..., sinex:key/..., sinex:at/... or sinex:window/..."
    )))
}
