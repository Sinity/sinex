//! Bounded text COPY encoding shared by admission and its PostgreSQL fuzzer.
use crate::{Error, Result};
use sqlx::{PgConnection, postgres::PgCopyIn};

pub(crate) const COPY_BYTES: usize = 64 * 1024;
pub(crate) const JSON_STRING_BYTES: usize = 4096;

/// COPY messages and JSON string fragments stay bounded even for one large field.
/// The events remain borrowed; there is no second batch or background encoder.
pub struct CopyText<'c> {
    pub(crate) copy: PgCopyIn<&'c mut PgConnection>,
    pub(crate) bytes: Vec<u8>,
    #[cfg(test)]
    pub(crate) peak: usize,
    #[cfg(test)]
    pub(crate) messages: usize,
    #[cfg(test)]
    pub(crate) captured: Option<Vec<u8>>,
    #[cfg(test)]
    pub(crate) fragment_peak: usize,
}

impl<'c> CopyText<'c> {
    /// Continue a COPY already opened on the caller's transaction.
    pub fn new(copy: PgCopyIn<&'c mut PgConnection>) -> Self {
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

    pub(crate) async fn flush(&mut self) -> Result<()> {
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

    pub(crate) async fn raw(&mut self, mut bytes: &[u8]) -> Result<()> {
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
    pub async fn text(&mut self, text: &str) -> Result<()> {
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

    pub(crate) async fn json(&mut self, value: &serde_json::Value) -> Result<()> {
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

    /// End a row after its fields have been written.
    pub async fn end_row(&mut self) -> Result<()> {
        self.raw(b"\n").await
    }

    /// Flush the final bounded fragment and complete this COPY.
    pub async fn finish(mut self) -> Result<u64> {
        self.flush().await?;
        Ok(self.copy.finish().await?)
    }
}
