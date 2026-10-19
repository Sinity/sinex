//! Masking on every output (architecture §3.5): each string in a result passes
//! through the shared credential masker, which masks rather than refuses, so a
//! long token or URL password never reaches a caller in the clear.
use credential_masking::Masker;
use serde_json::Value;

pub fn mask_str(text: &str) -> String {
    let mut out = Vec::with_capacity(text.len());
    let mut masker = Masker::new();
    // Writing to a Vec cannot fail, and the masker refuses no input.
    masker
        .push(text, &mut out)
        .and_then(|()| masker.finish(&mut out))
        .expect("masking into memory does not fail");
    String::from_utf8(out).expect("the masker writes UTF-8")
}

pub fn mask_json(value: Value) -> Value {
    match value {
        Value::String(s) => Value::String(mask_str(&s)),
        Value::Array(items) => Value::Array(items.into_iter().map(mask_json).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, mask_json(v))).collect())
        }
        other => other,
    }
}

#[cfg(test)]
mod tests;
