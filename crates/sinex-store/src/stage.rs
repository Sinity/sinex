//! One streaming text COPY stages a batch in its admission transaction.
#[cfg(test)]
use crate::copy_text::{COPY_BYTES, JSON_STRING_BYTES};
use crate::{Error, Result, copy_text::CopyText};
use sinex_model::{DbCheck, Event, Origin};
use sqlx::PgConnection;

pub(crate) const COLUMNS: &str = "ord, id, event_type, source, material_id, anchor, output_index, parents, ts_orig, ts_quality, payload, equivalence_key, replace_rule, reading_order, record_hash, record_state, class, kind_version, validation";

impl CopyText<'_> {
    async fn row(&mut self, ord: i64, e: &Event) -> Result<()> {
        enum Field<'a> {
            Null,
            Text(&'a str),
            Small(String),
            Json(&'a serde_json::Value),
            Parents(&'a [sinex_model::EventId]),
        }
        let (material, anchor, output, parents) = match &e.origin {
            Origin::Record {
                location,
                output_index,
            } => (
                Field::Small(location.material.0.to_string()),
                Field::Small(
                    i64::try_from(location.anchor)
                        .map_err(|_| Error::Invalid("anchor beyond i64".into()))?
                        .to_string(),
                ),
                i64::try_from(*output_index)
                    .map_err(|_| Error::Invalid("output index beyond i64".into()))?,
                Field::Null,
            ),
            Origin::Derived { parents } => (Field::Null, Field::Null, 0, Field::Parents(parents)),
        };
        let timestamp = match e.ts_orig.at {
            Some(t) => Field::Small(
                t.0.format(&time::format_description::well_known::Rfc3339)
                    .map_err(|_| Error::Invalid("unformattable ts_orig".into()))?,
            ),
            None => Field::Null,
        };
        use std::fmt::Write;
        let mut hash = String::from("\\x");
        for byte in e.record_hash {
            let _ = write!(hash, "{byte:02x}");
        }
        let fields = [
            Field::Small(ord.to_string()),
            Field::Small(e.id.0.to_string()),
            Field::Text(&e.event_type),
            Field::Text(&e.source),
            material,
            anchor,
            Field::Small(output.to_string()),
            parents,
            timestamp,
            Field::Text(e.ts_orig.quality.as_db()),
            Field::Json(&e.payload),
            e.equivalence_key
                .as_ref()
                .map_or(Field::Null, |key| Field::Text(&key.0)),
            Field::Text(
                e.replace
                    .ok_or_else(|| {
                        Error::Invalid("historical declaration has no replacement rule".into())
                    })?
                    .as_db(),
            ),
            Field::Small(e.reading_order.0.to_string()),
            Field::Small(hash),
            Field::Text(e.record_state.as_db()),
            Field::Text(e.class.as_db()),
            Field::Small(
                i64::try_from(e.kind_version)
                    .ok()
                    .filter(|v| *v > 0)
                    .ok_or_else(|| Error::Invalid("kind version must be a positive bigint".into()))?
                    .to_string(),
            ),
            Field::Text(e.validation.as_db()),
        ];
        for (i, field) in fields.into_iter().enumerate() {
            if i > 0 {
                self.raw(b"\t").await?;
            }
            match field {
                Field::Null => self.raw(b"\\N").await?,
                Field::Text(text) => self.text(text).await?,
                Field::Small(text) => self.text(&text).await?,
                Field::Json(value) => self.json(value).await?,
                Field::Parents(parents) => {
                    self.text("{").await?;
                    for (i, id) in parents.iter().enumerate() {
                        if i > 0 {
                            self.text(",").await?;
                        }
                        self.text(&id.0.to_string()).await?;
                    }
                    self.text("}").await?;
                }
            }
        }
        self.raw(b"\n").await
    }
}

/// Copy rows in batch order, within the caller's existing transaction.
pub(crate) async fn copy(connection: &mut PgConnection, events: &[(i64, &Event)]) -> Result<()> {
    let copy = connection
        .copy_in_raw(&format!("COPY core.admission_stage ({COLUMNS}) FROM STDIN"))
        .await?;
    let mut encoder = CopyText::new(copy);
    let result = async {
        for (ord, event) in events {
            encoder.row(*ord, event).await?;
        }
        encoder.flush().await
    }
    .await;
    match result {
        Ok(()) => {
            encoder.finish().await?;
            Ok(())
        }
        Err(error) => {
            // Encoding can now fail after COPY began. Restore the protocol
            // before returning the original content error; abort failures remain transient.
            encoder.copy.abort("admission COPY encoding failed").await?;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests;
