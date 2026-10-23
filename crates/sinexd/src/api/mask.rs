//! Masking on every output (architecture §3.5): each string in a result passes
//! through the shared credential masker. Credential-shaped object keys refuse
//! the result because renaming could change its meaning.
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

pub fn mask_json(value: Value) -> Result<Value, &'static str> {
    Ok(match value {
        Value::String(s) => Value::String(mask_str(&s)),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(mask_json).collect::<Result<_, _>>()?)
        }
        Value::Object(map) => {
            let mut output = serde_json::Map::new();
            for (key, value) in map {
                if mask_str(&key) != key {
                    return Err("result contains a credential-shaped object key");
                }
                output.insert(key, mask_json(value)?);
            }
            Value::Object(output)
        }
        other => other,
    })
}

#[cfg(test)]
mod tests;
