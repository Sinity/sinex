//! Freedom's daily domain-seconds export. Each `stat_YYYY-MM-DD` entry is
//! one record and yields one `domain.usage` per domain. The export gives a
//! civil date without its zone, so it does not supply an event instant.
use crate::drivers::append_file::AppendFile;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::desktop::DomainUsage;

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.freedom",
    driver: Driver::AppendFile,
    excisable: false,
    purpose: "domain usage reported by Freedom, for attention analysis",
    class: Class::Guarded,
    parser: "freedom-daily-domains",
    parser_version: 1,
    event_types: &["domain.usage"],
    framer: crate::Framer::incremental(frame),
};

pub struct FreedomParser;

impl Parser for FreedomParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = DomainUsage;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        _: &RecordContext<'_>,
    ) -> Result<(Vec<DomainUsage>, ()), RecordError> {
        let mut object = Vec::with_capacity(record.len() + 2);
        object.push(b'{');
        object.extend_from_slice(record);
        object.push(b'}');
        let entries: Map<String, Value> = serde_json::from_slice(&object)
            .map_err(|error| RecordError::new(format!("Freedom day: {error}")))?;
        if entries.len() != 1 {
            return Err(RecordError::new("Freedom record must name one day"));
        }
        let (key, domains) = entries.into_iter().next().expect("one entry");
        let day = key
            .strip_prefix("stat_")
            .ok_or_else(|| RecordError::new("Freedom day must start with stat_"))?;
        jiff::civil::Date::strptime("%Y-%m-%d", day)
            .map_err(|_| RecordError::new(format!("Freedom day {day:?} is not a date")))?;
        let domains = domains
            .as_object()
            .ok_or_else(|| RecordError::new("Freedom day is not a domain-seconds object"))?;
        let events = domains
            .iter()
            .map(|(domain, seconds)| {
                if domain.is_empty() {
                    return Err(RecordError::new("Freedom domain is empty"));
                }
                let seconds = seconds.as_u64().ok_or_else(|| {
                    RecordError::new("Freedom seconds is not an unsigned integer")
                })?;
                Ok(DomainUsage {
                    day: day.to_owned(),
                    domain: domain.clone(),
                    seconds,
                })
            })
            .collect::<Result<Vec<_>, RecordError>>()?;
        Ok((events, ()))
    }
}

/// Read one JSON value, leaving the following separator for the caller.
fn value_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut values = serde_json::Deserializer::from_slice(&bytes[start..])
        .into_iter::<&serde_json::value::RawValue>();
    values.next()?.ok()?;
    Some(start + values.byte_offset())
}

fn whitespace(bytes: &[u8], position: &mut usize) {
    while bytes.get(*position).is_some_and(u8::is_ascii_whitespace) {
        *position += 1;
    }
}

fn frame(bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut records = Vec::new();
    let mut position = 0;
    whitespace(bytes, &mut position);
    if bytes.get(position) == Some(&b'{') {
        position += 1;
    }
    loop {
        whitespace(bytes, &mut position);
        if bytes.get(position) == Some(&b',') {
            position += 1;
            whitespace(bytes, &mut position);
        }
        if bytes.get(position) == Some(&b'}') {
            position += 1;
            whitespace(bytes, &mut position);
        }
        if position == bytes.len() {
            return (records, position);
        }
        let start = position;
        let end = (|| {
            position = value_end(bytes, position)?;
            whitespace(bytes, &mut position);
            if bytes.get(position) != Some(&b':') {
                return None;
            }
            position += 1;
            whitespace(bytes, &mut position);
            value_end(bytes, position)
        })();
        match end {
            Some(end) => {
                records.push((start, end));
                position = end;
            }
            None if eof => {
                records.push((start, bytes.len()));
                return (records, bytes.len());
            }
            None => return (records, start),
        }
    }
}

pub fn source() -> DeclaredSource<FreedomParser> {
    DeclaredSource {
        spec: SPEC,
        parser: FreedomParser,
    }
}

pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "freedom",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(crate::binding::single(&SPEC, input, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|binding| {
        Ok(crate::binding::appended(AppendFile {
            path: binding.input.clone(),
            host: binding.settings.host.clone(),
            framer: SPEC.framer,
        }))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
