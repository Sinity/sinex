//! Opaque page cursors use hexadecimal JSON so credential masking preserves them.
use serde::{Serialize, de::DeserializeOwned};

pub fn encode<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    use std::fmt::Write;
    let bytes = serde_json::to_vec(value)?;
    let mut text = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(text, "{byte:02x}").expect("writing into a String does not fail");
    }
    Ok(text)
}

pub fn decode<T: DeserializeOwned>(text: &str) -> Result<T, &'static str> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return Err("invalid cursor");
    }
    let bytes = (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| "invalid cursor"))
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::from_slice(&bytes).map_err(|_| "invalid cursor")
}
