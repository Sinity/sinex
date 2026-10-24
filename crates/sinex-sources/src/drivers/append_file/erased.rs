//! Erased occurrences remain counted members of the bounded alignment tail.
use super::*;
use std::sync::Arc;

pub type RawMatcher = Arc<dyn Fn(&[u8]) -> Result<[u8; 32], DriverError> + Send + Sync>;
type Key = (MaterialId, [u8; 32]);

/// Only the archive owner constructs this after authenticating original
/// coordinates and opening its scoped naming-only matching capability.
#[derive(Clone)]
pub struct ErasedMember {
    location: RecordLocation,
    length: usize,
    digest: [u8; 32],
    matcher: RawMatcher,
}
impl ErasedMember {
    pub fn location(&self) -> RecordLocation {
        self.location
    }
    pub fn from_archive(
        location: RecordLocation,
        length: usize,
        digest: [u8; 32],
        matcher: RawMatcher,
    ) -> Result<Self, DriverError> {
        if length == 0 || location.anchor.checked_add(length as u64).is_none() {
            return Err(DriverError::InvalidMaterial(
                "invalid erased append member coordinates".into(),
            ));
        }
        Ok(Self {
            location,
            length,
            digest,
            matcher,
        })
    }
    pub(crate) fn coordinates(&self) -> (RecordLocation, usize) {
        (self.location, self.length)
    }
}

#[derive(Clone)]
pub(super) struct Group {
    matcher: RawMatcher,
    locations: std::collections::VecDeque<RecordLocation>,
}
impl PartialEq for Group {
    fn eq(&self, other: &Self) -> bool {
        self.locations == other.locations
    }
}
impl Eq for Group {}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Tail {
    Raw([u8; 32], RecordLocation, usize),
    Erased(Key, RecordLocation, usize),
}
impl Tail {
    pub(super) fn length(&self) -> usize {
        match self {
            Self::Raw(_, _, n) | Self::Erased(_, _, n) => *n,
        }
    }
}
pub(super) struct Alignment {
    matches: HashMap<[u8; 32], Vec<Key>>,
}

impl std::fmt::Debug for AppendState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendState")
            .field("records", &self.order.len())
            .field("erased_domains", &self.erased.len())
            .field("bytes", &self.bytes)
            .finish()
    }
}
impl AppendState {
    pub(crate) fn remember_erased(&mut self, member: ErasedMember) -> Result<(), DriverError> {
        let key = (member.location.material, member.digest);
        if let Some(entry) = self.order.iter_mut().find(|entry| match entry {
            Tail::Raw(_, l, _) | Tail::Erased(_, l, _) => *l == member.location,
        }) {
            if entry.length() != member.length {
                return Err(DriverError::InvalidMaterial(
                    "erased append member length changed".into(),
                ));
            }
            match entry {
                Tail::Raw(hash, location, length) => {
                    if let Some(held) = self.records.get_mut(hash) {
                        held.retain(|l| l != location);
                        if held.is_empty() {
                            self.records.remove(hash);
                        }
                    }
                    *entry = Tail::Erased(key, member.location, *length);
                }
                Tail::Erased(existing, _, _) if *existing != key => {
                    return Err(DriverError::InvalidMaterial(
                        "erased append member digest changed".into(),
                    ));
                }
                _ => {}
            }
        } else {
            self.bytes = self.bytes.saturating_add(member.length);
            self.order
                .push_back(Tail::Erased(key, member.location, member.length));
        }
        let group = self.erased.entry(key).or_insert_with(|| Group {
            matcher: member.matcher.clone(),
            locations: std::collections::VecDeque::new(),
        });
        group.matcher = member.matcher;
        if !group.locations.contains(&member.location) {
            group.locations.push_back(member.location);
        }
        self.trim();
        Ok(())
    }
    pub(crate) fn remembers_material(&self, material: MaterialId) -> bool {
        self.order.iter().any(|entry| match entry {
            Tail::Raw(_, location, _) | Tail::Erased(_, location, _) => {
                location.material == material
            }
        })
    }
    pub(crate) fn append_erased(&mut self, member: ErasedMember) {
        let key = (member.location.material, member.digest);
        self.order
            .push_back(Tail::Erased(key, member.location, member.length));
        self.bytes = self.bytes.saturating_add(member.length);
        let group = self.erased.entry(key).or_insert_with(|| Group {
            matcher: member.matcher.clone(),
            locations: std::collections::VecDeque::new(),
        });
        group.matcher = member.matcher;
        group.locations.push_back(member.location);
        self.trim();
    }
    pub(crate) fn erased_locations(&self) -> std::collections::HashSet<RecordLocation> {
        self.order
            .iter()
            .filter_map(|entry| match entry {
                Tail::Erased(_, location, _) => Some(*location),
                _ => None,
            })
            .collect()
    }
    pub(super) fn trim(&mut self) {
        while self.order.len() > STATE_RECORDS || (self.bytes > STATE_TAIL && self.order.len() > 1)
        {
            let old = self.order.pop_front().expect("not empty");
            self.bytes -= old.length();
            match old {
                Tail::Raw(hash, location, _) => {
                    if let Some(held) = self.records.get_mut(&hash) {
                        if held.front() == Some(&location) {
                            held.pop_front();
                        } else {
                            held.retain(|l| *l != location);
                        }
                        if held.is_empty() {
                            self.records.remove(&hash);
                        }
                    }
                }
                Tail::Erased(key, location, _) => {
                    if let Some(held) = self.erased.get_mut(&key) {
                        if held.locations.front() == Some(&location) {
                            held.locations.pop_front();
                        } else {
                            held.locations.retain(|l| *l != location);
                        }
                        if held.locations.is_empty() {
                            self.erased.remove(&key);
                        }
                    }
                }
            }
        }
    }
    pub(super) fn prepare_alignment(
        &self,
        bytes: &[u8],
        records: &[(usize, usize)],
    ) -> Result<Alignment, DriverError> {
        let mut matches = HashMap::<[u8; 32], Vec<Key>>::new();
        let mut counts = HashMap::<[u8; 32], usize>::new();
        // This preflight performs every fallible scoped match before consuming
        // old membership. Its map holds only candidates matching the old tail.
        for &(start, end) in records {
            let raw = &bytes[start..end];
            let hash = *blake3::hash(raw).as_bytes();
            if let Some(count) = counts.get_mut(&hash) {
                *count = count.saturating_add(1);
                continue;
            }
            let mut found = Vec::new();
            for (key, group) in &self.erased {
                if !group.locations.is_empty() && (group.matcher)(raw)? == key.1 {
                    found.push(*key);
                }
            }
            if !found.is_empty() {
                counts.insert(hash, 1);
                matches.insert(hash, found);
            }
        }
        for (hash, keys) in &matches {
            let plain = self
                .records
                .get(hash)
                .map_or(0, |locations| locations.len());
            let erased = keys
                .iter()
                .map(|key| self.erased.get(key).map_or(0, |g| g.locations.len()))
                .sum::<usize>();
            if plain > 0 && counts[hash] < plain.saturating_add(erased) {
                return Err(DriverError::InvalidMaterial("rewrite cannot distinguish retained and erased equal occurrences after multiplicity decreased".into()));
            }
        }
        Ok(Alignment { matches })
    }
    pub(super) fn take_aligned(&mut self, record: &[u8], alignment: &Alignment) -> bool {
        if self.take(record) {
            return true;
        }
        if let Some(keys) = alignment.matches.get(blake3::hash(record).as_bytes()) {
            for key in keys {
                if let Some(group) = self.erased.get_mut(key)
                    && !group.locations.is_empty()
                {
                    group.locations.pop_front();
                    return true;
                }
            }
        }
        false
    }
    pub(super) fn erased_remaining(&self) -> impl Iterator<Item = RecordLocation> + '_ {
        self.erased
            .values()
            .flat_map(|g| g.locations.iter().copied())
    }
    pub(super) fn has_erased(&self) -> bool {
        self.erased.values().any(|g| !g.locations.is_empty())
    }
}

#[cfg(test)]
mod tests;
