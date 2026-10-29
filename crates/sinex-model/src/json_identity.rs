//! SJSON1: compact serde_json serialization with recursively sorted UTF-8
//! object keys. Arrays retain order and finite number spelling is serde_json's.
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::Value;

pub fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(&Canonical(value)).expect("JSON values serialize")
}

pub fn hash(value: &Value) -> String {
    let mut hasher = blake3::Hasher::new();
    serde_json::to_writer(&mut hasher, &Canonical(value)).expect("JSON values serialize");
    hasher.finalize().to_hex().to_string()
}

struct Canonical<'a>(&'a Value);
impl Serialize for Canonical<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Object(object) => {
                let mut keys = object.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                let mut map = serializer.serialize_map(Some(keys.len()))?;
                for key in keys {
                    map.serialize_entry(key, &Canonical(&object[key]))?;
                }
                map.end()
            }
            Value::Array(array) => {
                let mut sequence = serializer.serialize_seq(Some(array.len()))?;
                for value in array {
                    sequence.serialize_element(&Canonical(value))?;
                }
                sequence.end()
            }
            value => value.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests;
