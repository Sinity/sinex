//! Reconstruct native identities with the same parser predecessors as replay.
use super::*;
use sinex_model::{Class, Event, RecordOutcome};
use sinex_sources::{DrainInput, Limits, Source, drain::RecordBytes, drivers};

pub(super) struct Record<'a> {
    pub candidate: &'a Candidate,
    pub source: &'a dyn Source,
    pub start: usize,
    pub end: usize,
    pub raw: &'a [u8],
    pub after: &'a Value,
    pub events: &'a [Event],
    pub failed: bool,
}

pub(super) struct Archive {
    pub candidates: Vec<Candidate>,
    stored: Vec<sinex_store::replay::Stored>,
    parsers: Vec<Box<dyn Source>>,
}
impl Archive {
    pub async fn load(conn: &mut PgConnection, source: &str, parser: &str) -> Result<Self, String> {
        let another_parser:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.materials m LEFT JOIN raw.inputs i ON i.id=m.input_id WHERE m.parser<>$2 AND (EXISTS(SELECT 1 FROM raw.bindings b WHERE b.binding=i.binding AND b.source=$1) OR EXISTS(SELECT 1 FROM core.events e WHERE e.material_id=m.id AND e.source=$1)))")
            .bind(source).bind(parser).fetch_one(&mut *conn).await.map_err(|e|e.to_string())?;
        if another_parser {
            return Err("sealed source contains unsupported parser epochs".into());
        }
        let candidates = excision::candidate_materials(conn, source, parser)
            .await
            .map_err(|e| e.to_string())?;
        if candidates.is_empty() || candidates.iter().any(|c| c.sources.as_slice() != [source]) {
            return Err("sealed excision needs complete native source ownership".into());
        }
        let ids = candidates.iter().map(|c| c.material.0).collect::<Vec<_>>();
        let stored = sinex_store::replay::chains(conn, &[], &ids)
            .await
            .map_err(|e| e.to_string())?;
        let parsers = crate::replay::sources_of(conn, &stored).await?;
        for (material, parser) in stored.iter().zip(&parsers) {
            if parser.spec().driver == sinex_sources::Driver::AppendFile
                && material.previous.is_none()
                && (material.part_index != 0
                    || material.manifest.get("records").is_some()
                    || material.input.is_some_and(|input| {
                        stored
                            .iter()
                            .filter(|other| other.input == Some(input) && other.previous.is_none())
                            .count()
                            != 1
                    }))
            {
                return Err(
                    "append material lacks unambiguous retained predecessor evidence".into(),
                );
            }
            if parser.spec().driver == sinex_sources::Driver::SqliteRows
                && material
                    .manifest
                    .get("row_format")
                    .is_some_and(|v| *v != 1 && *v != 2)
            {
                return Err("sealed excision found an unsupported native row encoding".into());
            }
            if material.parser_version != parser.spec().parser_version as i32 {
                return Err("sealed excision needs the recorded parser version".into());
            }
            if material
                .manifest
                .get("container")
                .and_then(Value::as_str)
                .is_some_and(|s| s != "plain")
                || matches!(
                    sinex_model::MaterialRole::of(&material.manifest).map_err(|e| e.to_string())?,
                    sinex_model::MaterialRole::OriginalArchive
                        | sinex_model::MaterialRole::SqliteImage
                )
            {
                return Err("sealed excision needs decoded logical record materials".into());
            }
        }
        Ok(Self {
            candidates,
            stored,
            parsers,
        })
    }

    pub fn source(&self, material: MaterialId) -> Result<&dyn Source, String> {
        let index = self
            .stored
            .iter()
            .position(|m| m.id == material.0)
            .ok_or("material has no recorded parser")?;
        Ok(&*self.parsers[index])
    }
    pub fn predecessors(&self, material: MaterialId) -> Result<BTreeSet<MaterialId>, String> {
        let mut result = BTreeSet::new();
        let mut at = material;
        loop {
            let current = self
                .stored
                .iter()
                .find(|m| m.id == at.0)
                .ok_or("missing retained parser predecessor")?;
            let Some(previous) = current.previous else {
                return Ok(result);
            };
            at = MaterialId(previous);
            if at == material || !result.insert(at) {
                return Err("parser predecessor cycle".into());
            }
        }
    }

    /// Reconstruct each requested predecessor path independently. This keeps one
    /// decrypted record/body reader and one carry, rather than caching private
    /// carry for every branch. Long shared prefixes are consequently reread.
    pub async fn walk(
        &self,
        conn: &mut PgConnection,
        blobs: &Blobs,
        material: MaterialId,
        authority: &Authority,
        mut visit: impl FnMut(Record<'_>) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut chain = Vec::new();
        let mut next = Some(material.0);
        let mut seen = BTreeSet::new();
        while let Some(id) = next {
            if !seen.insert(id) {
                return Err("sealed excision found a parser predecessor cycle".into());
            }
            let index = self
                .stored
                .iter()
                .position(|m| m.id == id)
                .ok_or("sealed excision lacks its actual parser predecessor")?;
            chain.push(index);
            next = self.stored[index].previous;
        }
        let mut carry = Value::Null;
        for index in chain.into_iter().rev() {
            authority.check()?;
            let stored = &self.stored[index];
            let candidate = self
                .candidates
                .iter()
                .find(|c| c.material.0 == stored.id)
                .ok_or("sealed excision candidate disappeared")?;
            let source = &*self.parsers[index];
            let rows: Vec<(i64, String, Option<String>, i16, Vec<u8>)> = if candidate.material
                == material
            {
                sqlx::query_as("SELECT DISTINCT anchor,event_type,equivalence_key,ordinal,record_hash FROM core.events WHERE material_id=$1 ORDER BY anchor,event_type,ordinal")
                    .bind(material.0).fetch_all(&mut *conn).await.map_err(|e|e.to_string())?
            } else {
                Vec::new()
            };
            let mut expected: BTreeMap<i64, Vec<(String, Option<String>, i16, Vec<u8>)>> =
                BTreeMap::new();
            for (anchor, kind, key, ordinal, hash) in rows {
                expected
                    .entry(anchor)
                    .or_default()
                    .push((kind, key, ordinal, hash));
            }
            let mut verified_anchors = BTreeSet::new();
            let opened = crate::retained::open(
                conn,
                blobs,
                &candidate.sources[0],
                candidate.material,
                Some(authority),
            )
            .await?;
            let records = match &opened.body {
                Some(body) => body.records(&candidate.manifest, source.spec().framer)?,
                None => Vec::new(),
            };
            let erased = sinex_sources::drain::ErasedTransitions::from_archive(
                opened
                    .metadata
                    .iter()
                    .filter(|r| r.is_active())
                    .map(|r| {
                        Ok((
                            usize::try_from(r.start)
                                .map_err(|_| "erased range exceeds address space")?,
                            usize::try_from(r.end)
                                .map_err(|_| "erased range exceeds address space")?,
                            r.transition.clone(),
                        ))
                    })
                    .collect::<Result<Vec<_>, &str>>()?,
                candidate.length,
                opened.body.is_none(),
            )
            .map_err(|e| e.to_string())?;
            let order = drivers::reading_order(&candidate.manifest);
            let mut after = carry.clone();
            let chunks = source
                .drainer(DrainInput {
                    erased: Some(&erased),
                    source: source.spec().name,
                    class: Class::Sealed,
                    material: candidate.material,
                    bytes: match &opened.body {
                        Some(body) => RecordBytes::Reader(&**body),
                        None => RecordBytes::Erased(candidate.length),
                    },
                    records: &records,
                    carry,
                    reading_order: &*order,
                    limits: Limits {
                        max_events: 1,
                        max_bytes: 1,
                    },
                    manifest: &candidate.manifest,
                })
                .map_err(|e| e.to_string())?;
            for chunk in chunks {
                authority.check()?;
                let chunk = chunk.map_err(|e| e.to_string())?;
                after = chunk.carry;
                if candidate.material != material {
                    continue;
                }
                for outcome in chunk.outcomes {
                    let (location, events, failed) = match outcome {
                        RecordOutcome::Events {
                            location: Some(location),
                            events,
                        } => (location, events, false),
                        RecordOutcome::Failure { location, .. } => (location, Vec::new(), true),
                        _ => {
                            return Err(
                                "sealed excision parser has no native record location".into()
                            );
                        }
                    };
                    let start = usize::try_from(location.anchor)
                        .map_err(|_| "record anchor exceeds address space")?;
                    // Erased transitions have no plaintext and are retained as
                    // authenticated tokens, never interpreted as fresh records.
                    if opened.metadata.iter().any(|r| r.start == start as u64) {
                        continue;
                    }
                    let end = records
                        .iter()
                        .find(|(s, _)| *s == start)
                        .ok_or("sealed excision outcome lacks a recorded range")?
                        .1;
                    let raw =
                        RecordBytes::Reader(&**opened.body.as_ref().ok_or("missing logical body")?)
                            .record(start, end)
                            .map_err(|_| "sealed record read failed")?;
                    let names = opened.secrets.as_ref().map(|(_, names)| names);
                    for (kind, key, ordinal, hash) in
                        expected.get(&(start as i64)).into_iter().flatten()
                    {
                        if !events.iter().any(|event| {
                            let actual_key =
                                event.equivalence_key.as_ref().map(|key| match names {
                                    Some(names) => names.name_equivalence(&key.0).0,
                                    None => key.0.clone(),
                                });
                            let actual_hash = match names {
                                Some(names) => names.name_record_hash(&event.record_hash),
                                None => event.record_hash,
                            };
                            event.event_type == kind.as_str()
                                && event
                                    .location()
                                    .is_some_and(|(_, n)| i32::from(n) == i32::from(*ordinal))
                                && actual_key == *key
                                && actual_hash.as_slice() == hash
                        }) {
                            let matching_hash = events.iter().any(|event| {
                                let actual = names.map_or(event.record_hash, |n| {
                                    n.name_record_hash(&event.record_hash)
                                });
                                actual.as_slice() == hash
                            });
                            let matching_key = events.iter().any(|event| {
                                let actual = event.equivalence_key.as_ref().map(|k| {
                                    names
                                        .map_or_else(|| k.0.clone(), |n| n.name_equivalence(&k.0).0)
                                });
                                actual == *key
                            });
                            return Err(format!(
                                "native record identity does not match its retained predecessor evidence (hash matches: {matching_hash}, key matches: {matching_key})"
                            ));
                        }
                        verified_anchors.insert(start as i64);
                    }
                    visit(Record {
                        candidate,
                        source,
                        start,
                        end,
                        raw: &raw,
                        after: &after,
                        events: &events,
                        failed,
                    })?;
                }
            }
            if expected
                .keys()
                .any(|anchor| !verified_anchors.contains(anchor))
            {
                return Err("retained readings lack reconstructible native record evidence".into());
            }
            carry = after;
        }
        Ok(())
    }
}
