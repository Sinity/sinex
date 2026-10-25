//! Citations: an event ID, `sinex:ev/<id>`, `sinex:record/...` or
//! `sinex:key/...`.
use super::*;

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
    if raw.starts_with("sinex:at/") {
        return Err(QueryError {
            not_built: true,
            ..QueryError::new("sinex:at citations need spanning outputs, which are not built yet")
        });
    }
    Err(QueryError::new(format!(
        "`{raw}` is not a reference; use an event ID, sinex:ev/<id>, sinex:record/... or sinex:key/..."
    )))
}
