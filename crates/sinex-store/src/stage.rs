//! One streaming text COPY stages a batch in its admission transaction.
use crate::{Error, Result};
use sinex_model::{DbCheck, Event, Origin};
use sqlx::{PgConnection, postgres::PgCopyIn};

pub(crate) const COLUMNS: &str = "ord, id, event_type, source, material_id, anchor, output_index, parents, ts_orig, ts_quality, payload, equivalence_key, replace_rule, reading_order, record_hash, record_state, class, kind_version, validation";
const COPY_BYTES: usize = 64 * 1024;
const JSON_STRING_BYTES: usize = 4096;

/// COPY messages and JSON string fragments stay bounded even for one large field.
/// The events remain borrowed; there is no second batch or background encoder.
struct Encoder<'c> {
    copy: PgCopyIn<&'c mut PgConnection>,
    bytes: Vec<u8>,
    #[cfg(test)]
    peak: usize,
    #[cfg(test)]
    messages: usize,
    #[cfg(test)]
    captured: Option<Vec<u8>>,
    #[cfg(test)]
    fragment_peak: usize,
}

impl<'c> Encoder<'c> {
    fn new(copy: PgCopyIn<&'c mut PgConnection>) -> Self {
        Self {
            copy,
            bytes: Vec::with_capacity(COPY_BYTES),
            #[cfg(test)]
            peak: 0,
            #[cfg(test)]
            messages: 0,
            #[cfg(test)]
            captured: None,
            #[cfg(test)]
            fragment_peak: 0,
        }
    }

    async fn flush(&mut self) -> Result<()> {
        if !self.bytes.is_empty() {
            #[cfg(test)]
            if let Some(captured) = &mut self.captured {
                captured.extend_from_slice(&self.bytes);
            }
            self.copy.send(self.bytes.as_slice()).await?;
            self.bytes.clear();
            #[cfg(test)]
            {
                self.messages += 1;
            }
        }
        Ok(())
    }

    async fn raw(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            let n = bytes.len().min(COPY_BYTES - self.bytes.len());
            self.bytes.extend_from_slice(&bytes[..n]);
            #[cfg(test)]
            {
                self.peak = self.peak.max(self.bytes.len());
            }
            bytes = &bytes[n..];
            if self.bytes.len() == COPY_BYTES {
                self.flush().await?;
            }
        }
        Ok(())
    }

    /// Escaping COPY's ASCII metacharacters leaves all other UTF-8 bytes intact.
    async fn text(&mut self, text: &str) -> Result<()> {
        for bytes in text.as_bytes().chunks(JSON_STRING_BYTES) {
            let mut start = 0;
            for (i, byte) in bytes.iter().enumerate() {
                let escaped: &[u8] = match byte {
                    b'\\' => b"\\\\",
                    b'\t' => b"\\t",
                    b'\n' => b"\\n",
                    b'\r' => b"\\r",
                    _ => continue,
                };
                self.raw(&bytes[start..i]).await?;
                self.raw(escaped).await?;
                start = i + 1;
            }
            self.raw(&bytes[start..]).await?;
        }
        Ok(())
    }

    async fn string(&mut self, mut text: &str) -> Result<()> {
        self.text("\"").await?;
        while !text.is_empty() {
            let mut end = text.len().min(JSON_STRING_BYTES);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            // String escaping has no state across Unicode scalar boundaries.
            // Reuse serde_json's exact spelling, without another whole-field String.
            let quoted =
                serde_json::to_string(&text[..end]).map_err(|e| Error::Invalid(e.to_string()))?;
            #[cfg(test)]
            {
                self.fragment_peak = self.fragment_peak.max(quoted.len());
            }
            self.text(&quoted[1..quoted.len() - 1]).await?;
            text = &text[end..];
        }
        self.text("\"").await
    }

    async fn number(&mut self, value: &serde_json::Number) -> Result<()> {
        use std::fmt::Write;
        // Number's Display writes a borrowed digit string when arbitrary
        // precision is enabled, and fixed-size primitive tokens otherwise.
        // A formatting window handles both without a whole-number allocation.
        struct Window {
            skip: usize,
            text: String,
        }
        impl std::fmt::Write for Window {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                if self.skip >= text.len() {
                    self.skip -= text.len();
                    return Ok(());
                }
                let text = &text[self.skip..];
                self.skip = 0;
                let n = text.len().min(JSON_STRING_BYTES - self.text.len());
                self.text.push_str(&text[..n]);
                if n == text.len() {
                    Ok(())
                } else {
                    Err(std::fmt::Error)
                }
            }
        }
        let mut offset = 0;
        loop {
            let mut window = Window {
                skip: offset,
                text: String::with_capacity(JSON_STRING_BYTES),
            };
            let complete = write!(&mut window, "{value}").is_ok();
            if window.text.is_empty() && !complete {
                return Err(Error::Invalid("number formatting made no progress".into()));
            }
            self.text(&window.text).await?;
            offset += window.text.len();
            if complete {
                return Ok(());
            }
        }
    }

    async fn json(&mut self, value: &serde_json::Value) -> Result<()> {
        enum Part<'a> {
            Value(&'a serde_json::Value),
            Array(std::slice::Iter<'a, serde_json::Value>, bool),
            Object(serde_json::map::Iter<'a>, bool),
        }
        let mut pending = vec![Part::Value(value)];
        while let Some(part) = pending.pop() {
            match part {
                Part::Value(serde_json::Value::Null) => self.text("null").await?,
                Part::Value(serde_json::Value::Bool(v)) => {
                    self.text(if *v { "true" } else { "false" }).await?
                }
                Part::Value(serde_json::Value::Number(v)) => self.number(v).await?,
                Part::Value(serde_json::Value::String(v)) => self.string(v).await?,
                Part::Value(serde_json::Value::Array(v)) => {
                    self.text("[").await?;
                    pending.push(Part::Array(v.iter(), true));
                }
                Part::Value(serde_json::Value::Object(v)) => {
                    self.text("{").await?;
                    pending.push(Part::Object(v.iter(), true));
                }
                Part::Array(mut values, first) => match values.next() {
                    Some(value) => {
                        if !first {
                            self.text(",").await?;
                        }
                        pending.push(Part::Array(values, false));
                        pending.push(Part::Value(value));
                    }
                    None => self.text("]").await?,
                },
                Part::Object(mut entries, first) => match entries.next() {
                    Some((key, value)) => {
                        if !first {
                            self.text(",").await?;
                        }
                        self.string(key).await?;
                        self.text(":").await?;
                        pending.push(Part::Object(entries, false));
                        pending.push(Part::Value(value));
                    }
                    None => self.text("}").await?,
                },
            }
        }
        Ok(())
    }

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
    let mut encoder = Encoder::new(copy);
    let result = async {
        for (ord, event) in events {
            encoder.row(*ord, event).await?;
        }
        encoder.flush().await
    }
    .await;
    match result {
        Ok(()) => {
            encoder.copy.finish().await?;
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
