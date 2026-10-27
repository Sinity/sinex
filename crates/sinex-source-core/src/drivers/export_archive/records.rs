//! Record framing over a member reader. JSON arrays use Serde's streaming
//! visitor; other formats hold only a part and the current incomplete record.
use super::Framing;
use serde::Deserialize;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;
use std::fmt;
use std::io::Read;

/// Frame a stored original stream without changing its byte coordinates. A JSON
/// syntax failure makes the whole stream one failed record; no valid prefix
/// escapes it.
pub(crate) fn ranges(
    reader: &mut dyn Read,
    framing: Framing,
) -> std::io::Result<Vec<(usize, usize)>> {
    if let Framing::JsonArray(pointer) = framing {
        return json_ranges(reader, pointer);
    }
    if framing == Framing::Whole {
        let length = std::io::copy(reader, &mut std::io::sink())?;
        let length = usize::try_from(length)
            .map_err(|_| std::io::Error::other("material exceeds address space"))?;
        return Ok(if length == 0 {
            vec![]
        } else {
            vec![(0, length)]
        });
    }
    let mut buffer = Vec::new();
    let mut base = 0usize;
    let mut out = Vec::new();
    let mut block = [0; 65536];
    loop {
        let n = reader.read(&mut block)?;
        buffer.extend_from_slice(&block[..n]);
        let eof = n == 0;
        let (records, consumed) = framing.frame(&buffer, eof);
        out.extend(
            records
                .into_iter()
                .map(|(start, end)| (base + start, base + end)),
        );
        buffer.drain(..consumed);
        base += consumed;
        if eof {
            return Ok(out);
        }
    }
}

#[derive(Default)]
struct Position {
    bytes: std::cell::Cell<usize>,
    last: std::cell::Cell<u8>,
    error: std::cell::RefCell<Option<std::io::Error>>,
}
type Input<'a> = std::rc::Rc<std::cell::RefCell<std::io::BufReader<&'a mut dyn Read>>>;
struct Counted<'a> {
    reader: Input<'a>,
    position: std::rc::Rc<Position>,
}
impl Read for Counted<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        match self.reader.borrow_mut().read(out) {
            Ok(n) => {
                self.position.bytes.set(self.position.bytes.get() + n);
                if n > 0 {
                    self.position.last.set(out[n - 1]);
                }
                Ok(n)
            }
            Err(error) => {
                let message = error.to_string();
                *self.position.error.borrow_mut() = Some(error);
                Err(std::io::Error::other(message))
            }
        }
    }
}

fn json_ranges(reader: &mut dyn Read, pointer: &str) -> std::io::Result<Vec<(usize, usize)>> {
    let position = std::rc::Rc::new(Position::default());
    let input = std::rc::Rc::new(std::cell::RefCell::new(std::io::BufReader::with_capacity(
        65536, reader,
    )));
    let mut counted = Counted {
        reader: input.clone(),
        position: position.clone(),
    };
    let path: Vec<_> = pointer.split('/').skip(1).collect();
    let mut selection = Selection::default();
    let mut de = serde_json::Deserializer::from_reader(&mut counted);
    let parsed = RangesAt {
        path: &path,
        position: &position,
        input: &input,
        selection: &mut selection,
    }
    .deserialize(&mut de)
    .and_then(|()| de.end());
    if let Some(error) = position.error.borrow_mut().take() {
        return Err(error);
    }
    if parsed.is_err() || !selection.valid {
        // Finish the same reader so malformed source syntax still covers all original bytes.
        std::io::copy(&mut counted, &mut std::io::sink())?;
        let length = position.bytes.get();
        return Ok(if length == 0 {
            vec![]
        } else {
            vec![(0, length)]
        });
    }
    Ok(selection.records)
}

fn peek_value(input: &Input<'_>, position: &Position) -> std::io::Result<Option<u8>> {
    use std::io::BufRead;
    loop {
        let mut reader = input.borrow_mut();
        let (spaces, next, last) = match reader.fill_buf() {
            Ok(bytes) => {
                let spaces = bytes
                    .iter()
                    .take_while(|b| matches!(**b, b' ' | b'\n' | b'\r' | b'\t'))
                    .count();
                (
                    spaces,
                    bytes.get(spaces).copied(),
                    spaces.checked_sub(1).map(|i| bytes[i]),
                )
            }
            Err(error) => {
                let message = error.to_string();
                *position.error.borrow_mut() = Some(error);
                return Err(std::io::Error::other(message));
            }
        };
        reader.consume(spaces);
        position.bytes.set(position.bytes.get() + spaces);
        if let Some(last) = last {
            position.last.set(last);
        }
        if next.is_some() || spaces == 0 {
            return Ok(next);
        }
    }
}

#[derive(Default)]
struct Selection {
    records: Vec<(usize, usize)>,
    valid: bool,
}

struct RangesAt<'a, 'b, 'r> {
    path: &'a [&'a str],
    position: &'a Position,
    input: &'a Input<'r>,
    selection: &'b mut Selection,
}
impl<'de> DeserializeSeed<'de> for RangesAt<'_, '_, '_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, de: D) -> Result<(), D::Error> {
        self.selection.records.clear();
        self.selection.valid = false;
        // A map value seed follows the consumed colon; the root seed precedes any
        // lookahead. Peek through the shared buffer before selecting a typed visitor.
        let head = peek_value(self.input, self.position).map_err(de::Error::custom)?;
        let expected = if self.path.is_empty() { b'[' } else { b'{' };
        if head != Some(expected) {
            IgnoredAny::deserialize(de).map(|_| ())
        } else if self.path.is_empty() {
            de.deserialize_seq(self)
        } else {
            de.deserialize_map(self)
        }
    }
}
impl<'de> Visitor<'de> for RangesAt<'_, '_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        self.selection.valid = true;
        while let Some(raw) = sequence.next_element::<Box<RawValue>>()? {
            // Serde's reader may have peeked one delimiter past a scalar. The raw value
            // excludes that lookahead; structured/string values end at their closing byte.
            let end = self.position.bytes.get()
                - usize::from(
                    self.position.last.get()
                        != *raw.get().as_bytes().last().expect("nonempty JSON value"),
                );
            self.selection.records.push((end - raw.get().len(), end));
        }
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<String>()? {
            if key == self.path[0] {
                // The final duplicate selected key wins, matching slice framing,
                // including when an earlier value has a different JSON shape.
                map.next_value_seed(RangesAt {
                    path: &self.path[1..],
                    position: self.position,
                    input: self.input,
                    selection: self.selection,
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(())
    }
}
