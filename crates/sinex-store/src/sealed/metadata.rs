//! Class-owned replay and current carry survive data-key retirement. Neither
//! representation is a material secret or a public manifest/cursor value.
use super::{Identity, Recipient};
use crate::{Error, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::MaterialId;
use std::io::{BufRead, BufReader, Read, Write};

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "purpose", deny_unknown_fields)]
pub enum Scope {
    #[serde(rename = "erased-record-replay")]
    Replay {
        source: String,
        material: MaterialId,
        parser: String,
        parser_version: i32,
        length: u64,
        whole: bool,
    },
    #[serde(rename = "current-carry")]
    Carry {
        source: String,
        binding: String,
        parser: String,
        parser_version: i32,
        cursor: Value,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format: String,
    version: u32,
    scope: Scope,
}

/// Coordinates and irreversible matching evidence, never original record text.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErasedRecord {
    pub start: u64,
    pub end: u64,
    pub raw_digest: [u8; 32],
    pub transition: Option<Value>,
}

/// Only ciphertext reaches `output`. Memory is one serialized record/value plus
/// age's stream buffer; callers own the existing record-coordinate collection.
pub fn seal_replay(
    output: impl Write,
    recipient: &Recipient,
    scope: &Scope,
    records: impl IntoIterator<Item = ErasedRecord>,
    mut check: impl FnMut() -> Result<()>,
) -> Result<()> {
    let Scope::Replay { length, .. } = scope else {
        return Err(invalid());
    };
    let mut out = begin(output, recipient, scope)?;
    let mut previous = 0;
    for record in records {
        check()?;
        if record.start < previous || record.start >= record.end || record.end > *length {
            return Err(invalid());
        }
        previous = record.end;
        line(&mut out, &record)?;
    }
    check()?;
    out.finish().map_err(io)?;
    check()
}

/// Fully authenticate the age stream before returning any transitions. One
/// decoded record may be large; total metadata is proportional to erased spans.
pub fn open_replay(
    input: impl Read,
    identity: &Identity,
    expected: &Scope,
    mut check: impl FnMut() -> Result<()>,
) -> Result<Vec<ErasedRecord>> {
    let Scope::Replay { length, .. } = expected else {
        return Err(invalid());
    };
    check()?;
    let mut input = open(input, identity, expected)?;
    let mut records = Vec::new();
    let mut previous = 0;
    while let Some(record) = next::<ErasedRecord>(&mut input)? {
        check()?;
        if record.start < previous || record.start >= record.end || record.end > *length {
            return Err(invalid());
        }
        previous = record.end;
        records.push(record);
    }
    check()?;
    Ok(records)
}

/// Current carry retains only the state returned by the format's erase hook.
/// It is one value, as in ordinary capture; it has no wrapped-key size limit.
pub fn seal_carry(value: &Value, recipient: &Recipient, scope: &Scope) -> Result<Vec<u8>> {
    if !matches!(scope, Scope::Carry { .. }) {
        return Err(invalid());
    }
    let mut out = begin(Vec::new(), recipient, scope)?;
    line(&mut out, value)?;
    out.finish().map_err(io)
}
pub fn open_carry(input: impl Read, identity: &Identity, expected: &Scope) -> Result<Value> {
    if !matches!(expected, Scope::Carry { .. }) {
        return Err(invalid());
    }
    let mut input = open(input, identity, expected)?;
    let value = next(&mut input)?.ok_or_else(invalid)?;
    // Reaching authenticated EOF is mandatory, including the final age chunk.
    if next::<Value>(&mut input)?.is_some() {
        return Err(invalid());
    }
    Ok(value)
}

fn begin<W: Write>(
    output: W,
    recipient: &Recipient,
    scope: &Scope,
) -> Result<age::stream::StreamWriter<W>> {
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(recipient as &dyn age::Recipient))
            .map_err(|_| invalid())?;
    let mut out = encryptor.wrap_output(output).map_err(io)?;
    line(
        &mut out,
        &Header {
            format: "sinex-class-metadata".into(),
            version: 1,
            scope: scope.clone(),
        },
    )?;
    Ok(out)
}
fn open<R: Read>(
    input: R,
    identity: &Identity,
    expected: &Scope,
) -> Result<BufReader<age::stream::StreamReader<R>>> {
    let decryptor = age::Decryptor::new(input).map_err(|_| invalid())?;
    let mut input = BufReader::new(
        decryptor
            .decrypt(std::iter::once(identity as &dyn age::Identity))
            .map_err(|_| invalid())?,
    );
    let header: Header = next(&mut input)?.ok_or_else(invalid)?;
    if header.format != "sinex-class-metadata" || header.version != 1 || header.scope != *expected {
        return Err(invalid());
    }
    Ok(input)
}
fn next<T: serde::de::DeserializeOwned>(input: &mut impl BufRead) -> Result<Option<T>> {
    let mut line = Vec::new();
    if input.read_until(b'\n', &mut line).map_err(io)? == 0 {
        return Ok(None);
    }
    if line.last() != Some(&b'\n') {
        return Err(invalid());
    }
    serde_json::from_slice(&line)
        .map(Some)
        .map_err(|_| invalid())
}
fn line(out: &mut impl Write, value: &impl Serialize) -> Result<()> {
    serde_json::to_writer(&mut *out, value).map_err(|_| invalid())?;
    out.write_all(b"\n").map_err(io)
}
fn invalid() -> Error {
    Error::Invalid("invalid sealed metadata purpose, version, scope or contents".into())
}
fn io(_: std::io::Error) -> Error {
    invalid()
}

#[cfg(test)]
mod tests;

/// The database position contains only ciphertext. This is not a payload or key.
pub fn carry_envelope(ciphertext: &[u8]) -> Value {
    serde_json::json!({"sealed_carry": base64::engine::general_purpose::STANDARD.encode(ciphertext)})
}
pub fn is_carry(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|o| o.len() == 1 && o.get("sealed_carry").is_some_and(Value::is_string))
}
pub fn carry_ciphertext(value: &Value) -> Result<Vec<u8>> {
    if !is_carry(value) {
        return Err(invalid());
    }
    base64::engine::general_purpose::STANDARD
        .decode(value["sealed_carry"].as_str().expect("checked string"))
        .map_err(|_| invalid())
}
