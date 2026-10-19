//! arbtt's capture log (`capture.log`), on `AppendFile`: arbtt appends one
//! sample per interval while it runs.
//!
//! The file is `arbtt-timelog-v1\n` followed by samples in Haskell's
//! `Data.Binary` encoding:
//!
//! - a tag byte (1);
//! - the time as a Modified Julian Day and a day time, both `Integer`, the day
//!   time a `Rational` (numerator and denominator);
//! - the sampling rate in milliseconds (`Integer`);
//! - the capture data's version byte (3 in the logs read so far), the windows
//!   (a 64-bit count, then per window an active flag, a title and a program),
//!   the milliseconds since the last input (`Integer`) and the desktop name.
//!
//! An `Integer` is either a 0 byte and a big-endian `i32`, or a 1 byte, a sign
//! byte (1 for positive), a 64-bit count and that many little-endian base-256
//! digits. A string is a reference byte: 0 for a literal (a 64-bit count of
//! characters, then the characters in UTF-8), or n for the n-th string of the
//! previous sample (titles and programs in window order, then the desktop),
//! which the parser carries.
//!
//! Times are UTC: Lynchpin's materialized view read them as local time and is
//! two hours late in summer.
use crate::drivers::append_file::AppendFile;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use sinex_model::kinds::desktop::{SampledWindow, WindowSampled};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

const MAGIC: &[u8] = b"arbtt-timelog-v1\n";

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.arbtt",
    driver: Driver::AppendFile,
    consumer: "which window was in use, sampled every interval",
    class: Class::Open,
    parser: "arbtt-timelog-v1",
    parser_version: 1,
    event_types: &["window.sampled"],
    framer: frame,
};

/// The previous sample's strings, which a reference byte indexes.
pub type Carry = Vec<String>;

pub struct ArbttParser;

impl Parser for ArbttParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = WindowSampled;

    fn parse(
        &self,
        record: &&[u8],
        previous: &Carry,
    ) -> Result<(Vec<WindowSampled>, Carry), RecordError> {
        let mut r = Reader {
            bytes: record,
            pos: 0,
        };
        let (sample, strings) = r.sample(Some(previous))?;
        if r.pos != record.len() {
            return Err(RecordError::new(format!(
                "{} bytes after the sample",
                record.len() - r.pos
            )));
        }
        Ok((vec![sample.expect("decoding with strings builds")], strings))
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

type Decoded = (Option<WindowSampled>, Vec<String>);

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], RecordError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.bytes.len())
            .ok_or_else(|| RecordError::new("truncated sample"))?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, RecordError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, RecordError> {
        let b = self.take(8)?;
        Ok(u64::from_be_bytes(b.try_into().expect("eight bytes")))
    }

    /// A Haskell `Integer`; values beyond `i128` are refused by name.
    fn integer(&mut self) -> Result<i128, RecordError> {
        match self.u8()? {
            0 => {
                let b = self.take(4)?;
                Ok(i128::from(i32::from_be_bytes(
                    b.try_into().expect("four bytes"),
                )))
            }
            1 => {
                let sign = self.u8()?;
                let n =
                    usize::try_from(self.u64()?).map_err(|_| RecordError::new("huge Integer"))?;
                if n > 15 {
                    return Err(RecordError::new(format!("an Integer of {n} digits")));
                }
                let digits = self.take(n)?;
                let mut v: i128 = 0;
                for (i, d) in digits.iter().enumerate() {
                    v |= i128::from(*d) << (8 * i);
                }
                Ok(if sign == 1 { v } else { -v })
            }
            t => Err(RecordError::new(format!("Integer tag {t}"))),
        }
    }

    /// A literal string: a count of characters, then their UTF-8 bytes.
    fn literal(&mut self) -> Result<String, RecordError> {
        let chars = usize::try_from(self.u64()?).map_err(|_| RecordError::new("huge string"))?;
        let mut out = String::new();
        for _ in 0..chars {
            let first = *self
                .bytes
                .get(self.pos)
                .ok_or_else(|| RecordError::new("truncated string"))?;
            let len = match first {
                0x00..=0x7f => 1,
                0xc0..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf7 => 4,
                _ => return Err(RecordError::new("invalid UTF-8 in a string")),
            };
            let c = std::str::from_utf8(self.take(len)?)
                .map_err(|_| RecordError::new("invalid UTF-8 in a string"))?;
            out.push_str(c);
        }
        Ok(out)
    }

    /// A string, literal or a reference into the previous sample's strings.
    /// Without `previous` (framing), a reference is only skipped.
    fn string(&mut self, previous: Option<&Carry>) -> Result<String, RecordError> {
        match self.u8()? {
            0 => self.literal(),
            i => match previous {
                Some(strings) => strings.get(usize::from(i) - 1).cloned().ok_or_else(|| {
                    RecordError::new(format!(
                        "reference {i} to the previous sample, which has {} strings",
                        strings.len()
                    ))
                }),
                None => Ok(String::new()),
            },
        }
    }

    /// One sample. With `previous` it is decoded; without, only its length is
    /// found (the framer's job).
    fn sample(&mut self, previous: Option<&Carry>) -> Result<Decoded, RecordError> {
        let tag = self.u8()?;
        if tag != 1 {
            return Err(RecordError::new(format!("sample tag {tag}")));
        }
        let day = self.integer()?;
        let num = self.integer()?;
        let den = self.integer()?;
        let rate = self.integer()?;
        let version = self.u8()?;
        if version != 3 {
            return Err(RecordError::new(format!(
                "capture data version {version}; only version 3 is read"
            )));
        }
        let count = self.u64()?;
        let mut windows = Vec::new();
        let mut strings = Vec::new();
        for _ in 0..count {
            let active = self.u8()? != 0;
            let title = self.string(previous)?;
            let program = self.string(previous)?;
            strings.push(title.clone());
            strings.push(program.clone());
            windows.push(SampledWindow {
                active,
                title,
                program,
            });
        }
        let idle = self.integer()?;
        let desktop = self.string(previous)?;
        strings.push(desktop.clone());
        if previous.is_none() {
            return Ok((None, strings));
        }
        if den <= 0 {
            return Err(RecordError::new("a day time with a zero denominator"));
        }
        // Modified Julian Day 40587 is 1970-01-01.
        let secs_ns = (day - 40_587) * 86_400 * 1_000_000_000 + num * 1_000_000_000 / den;
        let sampled_at = Timestamp::from_unix_ns(secs_ns)
            .map_err(|_| RecordError::new("sample time out of range"))?;
        let active = windows.iter().find(|w| w.active);
        let sample = WindowSampled {
            sampled_at,
            rate_ms: i64::try_from(rate).map_err(|_| RecordError::new("rate out of range"))?,
            idle_ms: i64::try_from(idle).map_err(|_| RecordError::new("idle out of range"))?,
            active_title: active.map(|w| w.title.clone()),
            active_program: active.map(|w| w.program.clone()),
            desktop: Some(desktop).filter(|d| !d.is_empty()),
            windows,
        };
        Ok((Some(sample), strings))
    }
}

/// Samples, each one record; the file header is not a record. A sample cut
/// off at the end waits for the rest.
pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut pos = if bytes.starts_with(MAGIC) {
        MAGIC.len()
    } else {
        0
    };
    let mut records = Vec::new();
    while pos < bytes.len() {
        let mut r = Reader {
            bytes: &bytes[pos..],
            pos: 0,
        };
        match r.sample(None) {
            Ok(_) => {
                records.push((pos, pos + r.pos));
                pos += r.pos;
            }
            Err(_) => break,
        }
    }
    (records, pos)
}

pub fn source() -> Declared<ArbttParser> {
    Declared {
        spec: SPEC,
        parser: ArbttParser,
    }
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: frame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer(v: i32) -> Vec<u8> {
        let mut out = vec![0];
        out.extend(v.to_be_bytes());
        out
    }

    fn big(v: u64) -> Vec<u8> {
        let digits: Vec<u8> = v
            .to_le_bytes()
            .into_iter()
            .rev()
            .skip_while(|b| *b == 0)
            .collect();
        let digits: Vec<u8> = digits.into_iter().rev().collect();
        let mut out = vec![1, 1];
        out.extend((digits.len() as u64).to_be_bytes());
        out.extend(digits);
        out
    }

    fn literal(s: &str) -> Vec<u8> {
        let mut out = vec![0];
        out.extend((s.chars().count() as u64).to_be_bytes());
        out.extend(s.as_bytes());
        out
    }

    fn sample(windows: &[(bool, Vec<u8>, Vec<u8>)], desktop: Vec<u8>) -> Vec<u8> {
        let mut out = vec![1];
        out.extend(integer(59_772)); // 2022-07-12
        out.extend(big(154_965_627_369)); // numerator
        out.extend(integer(5_000_000)); // denominator: 30993.125 s = 08:36:33.125
        out.extend(integer(60_000));
        out.push(3);
        out.extend((windows.len() as u64).to_be_bytes());
        for (active, t, p) in windows {
            out.push(u8::from(*active));
            out.extend(t);
            out.extend(p);
        }
        out.extend(integer(2_390));
        out.extend(desktop);
        out
    }

    #[test]
    fn samples_decode_with_back_references_and_utc_times() {
        let mut file = MAGIC.to_vec();
        file.extend(sample(
            &[
                (false, literal("Program Manager"), literal("explorer.exe")),
                (true, literal("Ustawienia – Zażółć"), literal("app.exe")),
            ],
            literal(""),
        ));
        // The second sample refers back: title 3 and program 4 of the first.
        file.extend(sample(&[(true, vec![3], vec![4])], vec![5]));
        let (records, covered) = frame(&file);
        assert_eq!(records.len(), 2);
        assert_eq!(covered, file.len());
        let (a, b) = records[0];
        let (out, carry) = ArbttParser.parse(&&file[a..b], &vec![]).unwrap();
        let s = &out[0];
        assert_eq!(
            s.sampled_at.unix_ns(),
            1_657_614_993_125_473_800,
            "2022-07-12T08:36:33.1254738Z"
        );
        assert_eq!((s.rate_ms, s.idle_ms), (60_000, 2_390));
        assert_eq!(s.active_title.as_deref(), Some("Ustawienia – Zażółć"));
        assert_eq!(s.desktop, None);
        let (a, b) = records[1];
        let (out, _) = ArbttParser.parse(&&file[a..b], &carry).unwrap();
        assert_eq!(out[0].active_program.as_deref(), Some("app.exe"));
        assert_eq!(out[0].windows.len(), 1);
    }

    #[test]
    fn a_cut_sample_waits_and_a_bad_reference_fails() {
        let mut file = MAGIC.to_vec();
        file.extend(sample(&[(true, literal("t"), literal("p"))], literal("")));
        let cut = &file[..file.len() - 3];
        let (records, covered) = frame(cut);
        assert!(records.is_empty());
        assert_eq!(covered, MAGIC.len());
        let one = sample(&[(true, vec![9], literal("p"))], literal(""));
        assert!(ArbttParser.parse(&one.as_slice(), &vec![]).is_err());
    }
}
