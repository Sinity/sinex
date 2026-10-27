//! Staging a batch's events with one `COPY` into `core.admission_stage`, in
//! PostgreSQL's text format. One statement per batch instead of several round
//! trips per row is most of admission's speed.
use crate::{Error, Result};
use sinex_model::{DbCheck, Event, Origin};
use sqlx::PgConnection;
use std::fmt::Write;

pub(crate) const COLUMNS: &str = "ord, id, event_type, source, material_id, anchor, output_index, parents, ts_orig, ts_quality, payload, equivalence_key, replace_rule, reading_order, record_hash, record_state, class, kind_version, validation";

/// Copy `events` (with their batch-order numbers) into the stage table, which
/// the caller has truncated in the same transaction.
pub(crate) async fn copy(connection: &mut PgConnection, events: &[(i32, &Event)]) -> Result<()> {
    let mut buf = String::with_capacity(events.len() * 512);
    for (ord, event) in events {
        row(&mut buf, *ord, event)?;
    }
    let mut copy = connection
        .copy_in_raw(&format!("COPY core.admission_stage ({COLUMNS}) FROM STDIN"))
        .await?;
    copy.send(buf.into_bytes()).await?;
    copy.finish().await?;
    Ok(())
}

fn row(buf: &mut String, ord: i32, e: &Event) -> Result<()> {
    let (material, anchor, output_index, parents) = match &e.origin {
        Origin::Record {
            location,
            output_index,
        } => (
            Some(location.material.0.to_string()),
            Some(
                i64::try_from(location.anchor)
                    .map_err(|_| Error::Invalid("anchor beyond i64".into()))?
                    .to_string(),
            ),
            *output_index,
            None,
        ),
        Origin::Derived { parents } => (
            None,
            None,
            0,
            Some(format!(
                "{{{}}}",
                parents
                    .iter()
                    .map(|p| p.0.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )),
        ),
    };
    let ts_orig = match e.ts_orig.at {
        Some(t) => Some(
            t.0.format(&time::format_description::well_known::Rfc3339)
                .map_err(|_| Error::Invalid("unformattable ts_orig".into()))?,
        ),
        None => None,
    };
    let mut hash = String::from("\\\\x");
    for b in e.record_hash {
        let _ = write!(hash, "{b:02x}");
    }
    let fields: [Option<String>; 19] = [
        Some(ord.to_string()),
        Some(e.id.0.to_string()),
        Some(e.event_type.to_owned()),
        Some(e.source.clone()),
        material,
        anchor,
        Some(output_index.to_string()),
        parents,
        ts_orig,
        Some(e.ts_orig.quality.as_db().to_owned()),
        Some(e.payload.to_string()),
        e.equivalence_key.as_ref().map(|k| k.0.clone()),
        Some(
            e.replace
                .ok_or_else(|| {
                    Error::Invalid("historical declaration has no replacement rule".into())
                })?
                .as_db()
                .to_owned(),
        ),
        Some(e.reading_order.0.to_string()),
        None, // record_hash, written pre-escaped below
        Some(e.record_state.as_db().to_owned()),
        Some(e.class.as_db().to_owned()),
        Some(
            e.kind_ref()
                .database_version()
                .map_err(Error::Invalid)?
                .to_string(),
        ),
        Some(e.validation.as_db().to_owned()),
    ];
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            buf.push('\t');
        }
        if i == 14 {
            buf.push_str(&hash);
            continue;
        }
        match field {
            None => buf.push_str("\\N"),
            Some(text) => escape(buf, text),
        }
    }
    buf.push('\n');
    Ok(())
}

/// Text-format escaping: backslash, tab, newline and carriage return.
fn escape(buf: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '\\' => buf.push_str("\\\\"),
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            c => buf.push(c),
        }
    }
}

#[cfg(test)]
mod tests;
