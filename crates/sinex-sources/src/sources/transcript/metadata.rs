//! Project supplied file metadata before reading segments, including fields after them.
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde_json::{Map, Value};
use std::{fmt, io::Read};

pub fn project(reader: &mut dyn Read) -> Result<Map<String, Value>, String> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let metadata = Metadata
        .deserialize(&mut deserializer)
        .map_err(|e| format!("import metadata: {e}"))?;
    deserializer
        .end()
        .map_err(|e| format!("import metadata: {e}"))?;
    Ok(metadata)
}
struct Metadata;
impl<'de> DeserializeSeed<'de> for Metadata {
    type Value = Map<String, Value>;
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Metadata {
    type Value = Map<String, Value>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an imported text document")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut metadata = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if key == "segments" {
                map.next_value::<IgnoredAny>()?;
            } else {
                if metadata.contains_key(&key) {
                    return Err(de::Error::custom("duplicate file metadata field"));
                }
                metadata.insert(key, map.next_value::<Value>()?);
            }
        }
        Ok(metadata)
    }
}
#[cfg(test)]
mod tests;
