//! What the shell history files (zsh, bash, fish) share: numbering records
//! that have no IDs.
//!
//! A timed history record is identified by host, start second, command and
//! its number among identical (start, command) pairs in file order.
//! The parser carries the counts; a re-read from the start (a lost position, a
//! replay) begins with no carry and numbers the same records the same way.

/// Records seen per (start second, command). Identical pairs share a start
/// second, so only starts within a day of the latest are kept: a pair needs a
/// second record with that exact start, and history lines are written within
/// minutes of their start. The window decides numbering, never what is kept.
/// Keys hold a digest of the command, never its text, so the carry does not
/// copy what the material holds.
/// The map is persistent: the drain hands each record the carry and takes a
/// new one back, so a clone must not copy the map.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Seen {
    latest: i64,
    /// Stored as a list: JSON object keys are strings only.
    #[serde(with = "pairs")]
    counts: imbl::OrdMap<(i64, [u8; 32]), u32>,
}

mod pairs {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    type Counts = imbl::OrdMap<(i64, [u8; 32]), u32>;

    pub(crate) fn serialize<S: Serializer>(
        m: &imbl::OrdMap<(i64, [u8; 32]), u32>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        m.iter()
            .map(|((start, key), n)| (start, key, n))
            .collect::<Vec<_>>()
            .serialize(s)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Counts, D::Error> {
        let list = Vec::<(i64, [u8; 32], u32)>::deserialize(d)?;
        Ok(list
            .into_iter()
            .map(|(start, key, n)| ((start, key), n))
            .collect())
    }
}

/// The context string is fixed: saved carries hold digests made with it.
fn command_key(command: &str) -> [u8; 32] {
    *blake3::Hasher::new_derive_key("Sinex zsh command occurrence v1")
        .update(command.as_bytes())
        .finalize()
        .as_bytes()
}

const SEEN_WINDOW_SECS: i64 = 86_400;

impl Seen {
    pub(crate) fn next(&self, start: i64, command: &str) -> (u32, Self) {
        let mut seen = self.clone();
        let key = (start, command_key(command));
        let n = seen.counts.get(&key).copied().unwrap_or(0);
        seen.counts.insert(key, n + 1);
        if start > seen.latest {
            seen.latest = start;
            // Keys sort by start, so the expired ones are at the front.
            let floor = start - SEEN_WINDOW_SECS;
            while let Some(((oldest, _), _)) = seen.counts.get_min() {
                if *oldest >= floor {
                    break;
                }
                let first = seen.counts.get_min().map(|(k, _)| *k).expect("checked");
                seen.counts.remove(&first);
            }
        }
        (n, seen)
    }
}

