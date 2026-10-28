//! Frame decoded archive members one record at a time.
use super::{DriverError, Framing, io};
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;
use std::{fmt, io::Read};

pub(super) fn visit(
    reader: &mut dyn Read,
    framing: Framing,
    part_bytes: usize,
    emit: &mut dyn FnMut(Vec<u8>) -> Result<(), DriverError>,
) -> Result<(), DriverError> {
    if let Framing::JsonArray(pointer) = framing {
        let path: Vec<String> = pointer
            .split('/')
            .skip(1)
            .map(|p| p.replace("~1", "/").replace("~0", "~"))
            .collect();
        let mut sink = JsonSink {
            emit,
            stopped: None,
        };
        let mut buffered = std::io::BufReader::with_capacity(65536, reader);
        let mut deserializer = serde_json::Deserializer::from_reader(&mut buffered);
        let parsed = ArrayAt {
            path: &path,
            sink: &mut sink,
        }
        .deserialize(&mut deserializer)
        .and_then(|()| deserializer.end());
        if let Some(error) = sink.stopped {
            return Err(error);
        }
        return parsed.map_err(|error| DriverError::Io(format!("JSON member: {error}")));
    }
    if framing == Framing::Whole {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).map_err(io)?;
        if !bytes.is_empty() {
            emit(bytes)?;
        }
        return Ok(());
    }
    let mut buffer = Vec::new();
    let mut want = part_bytes.max(1);
    loop {
        let before = buffer.len();
        (&mut *reader)
            .take(want as u64)
            .read_to_end(&mut buffer)
            .map_err(io)?;
        let eof = buffer.len() - before < want;
        let (records, consumed) = framing.frame(&buffer, eof);
        for (start, end) in &records {
            emit(buffer[*start..*end].to_vec())?;
        }
        if consumed > 0 {
            buffer.drain(..consumed);
        }
        if eof {
            return Ok(());
        }
        // One valid record may be larger than a part. Grow geometrically so framing it
        // does not rescan every prefix; the drain admits such a record alone.
        want = if records.is_empty() && consumed == 0 {
            want.saturating_mul(2)
        } else {
            part_bytes.max(1)
        };
    }
}

struct JsonSink<'a> {
    emit: &'a mut dyn FnMut(Vec<u8>) -> Result<(), DriverError>,
    stopped: Option<DriverError>,
}

struct ArrayAt<'a, 'b> {
    path: &'a [String],
    sink: &'a mut JsonSink<'b>,
}

impl<'de> DeserializeSeed<'de> for ArrayAt<'_, '_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.path.is_empty() {
            deserializer.deserialize_seq(self)
        } else {
            deserializer.deserialize_map(self)
        }
    }
}

impl<'de> Visitor<'de> for ArrayAt<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the selected JSON array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        while let Some(raw) = sequence.next_element::<Box<RawValue>>()? {
            if let Err(error) = (self.sink.emit)(raw.get().as_bytes().to_vec()) {
                self.sink.stopped = Some(error);
                return Err(de::Error::custom("record consumer stopped"));
            }
        }
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut found = false;
        while let Some(key) = map.next_key::<String>()? {
            if key == self.path[0] {
                if found {
                    return Err(de::Error::custom("duplicate selected array field"));
                }
                found = true;
                map.next_value_seed(ArrayAt {
                    path: &self.path[1..],
                    sink: self.sink,
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if found {
            Ok(())
        } else {
            Err(de::Error::custom("selected JSON array field is absent"))
        }
    }
}

#[cfg(test)]
mod tests;
