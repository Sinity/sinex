//! Source acquisition applies permanent exclusions before retaining readable bytes.
use crate::sealed_access::Authority;
use sinex_model::{Event, ExcisionBasis, MaterialId};
use sinex_store::{
    Blobs, excision,
    sealed::{MaterialSecrets, metadata},
};
use sqlx::PgConnection;
use std::collections::BTreeSet;
use uuid::Uuid;

struct Domain {
    names: Option<MaterialSecrets>,
    raw: BTreeSet<[u8; 32]>,
}

pub(crate) struct AcquisitionPolicy {
    pub basis: ExcisionBasis,
    domains: Vec<Domain>,
    keys: BTreeSet<(String, String)>,
    hashes: BTreeSet<(String, [u8; 32])>,
    locations: BTreeSet<(MaterialId, u64)>,
    authority: Option<Authority>,
}
impl AcquisitionPolicy {
    pub async fn load(
        conn: &mut PgConnection,
        blobs: &Blobs,
        source: &str,
        authority: Option<&Authority>,
    ) -> Result<Self, String> {
        let (basis, operations) = excision::operation_set(conn, source)
            .await
            .map_err(|e| e.to_string())?;
        let mut policy = Self {
            basis,
            domains: Vec::new(),
            keys: BTreeSet::new(),
            hashes: BTreeSet::new(),
            locations: BTreeSet::new(),
            authority: authority.cloned(),
        };
        if operations.is_empty() {
            return Ok(policy);
        }
        let image:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.sqlite_images x JOIN raw.materials m ON m.id=x.material_id JOIN raw.inputs i ON i.id=m.input_id JOIN raw.bindings b ON b.binding=i.binding WHERE b.source=$1)")
            .bind(source).fetch_one(&mut *conn).await.map_err(|e|e.to_string())?;
        if image {
            return Err("source has permanent exclusions and physical images; image-safe acquisition is unavailable".into());
        }
        let keys:Vec<(String,Option<String>,Option<Uuid>,Option<i64>)>=sqlx::query_as("SELECT event_type,equivalence_key,material_id,anchor FROM core.hidden_occurrences WHERE kind='excise' AND source=$1 AND operation_id=ANY($2)")
            .bind(source).bind(&operations).fetch_all(&mut *conn).await.map_err(|e|e.to_string())?;
        for (kind, key, material, anchor) in keys {
            if let Some(key) = key {
                policy.keys.insert((kind, key));
            } else if let (Some(material), Some(anchor)) = (material, anchor) {
                policy.locations.insert((
                    MaterialId(material),
                    u64::try_from(anchor).map_err(|_| "negative excluded record anchor")?,
                ));
            } else {
                return Err("excluded keyless record lacks exact location evidence".into());
            }
        }
        let hashes:Vec<(String,Vec<u8>)>=sqlx::query_as("SELECT event_type,record_hash FROM core.excised_digests WHERE source=$1 AND operation_id=ANY($2)")
            .bind(source).bind(&operations).fetch_all(&mut *conn).await.map_err(|e|e.to_string())?;
        for (kind, hash) in hashes {
            policy.hashes.insert((
                kind,
                hash.try_into()
                    .map_err(|_| "invalid excluded record digest")?,
            ));
        }
        let scopes: Vec<(Uuid, serde_json::Value)> =
            sqlx::query_as("SELECT id,scope FROM core.operations WHERE id=ANY($1) ORDER BY id")
                .bind(&operations)
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
        let mut materials = BTreeSet::new();
        let mut plain = false;
        for (operation, scope) in scopes {
            if scope["record_domains_version"] == 1 {
                let domains = scope["record_domains"]
                    .as_array()
                    .ok_or("exclusion lacks record naming domains")?;
                for domain in domains {
                    let material = serde_json::from_value::<MaterialId>(domain["material"].clone())
                        .map_err(|_| "invalid exclusion material identity")?;
                    match domain["naming"].as_str() {
                        Some("sealed") if domain["source"] == source => {
                            materials.insert(material);
                        }
                        Some("plain") if domain["source"] == source => {
                            verify_plain(conn, blobs, source, Some(material), &scope, operation)
                                .await?;
                            plain = true;
                        }
                        _ => {
                            return Err(
                                "exclusion lacks a supported source-owned naming domain".into()
                            );
                        }
                    }
                }
            } else {
                verify_plain(conn, blobs, source, None, &scope, operation).await?;
                plain = true;
            }
        }
        if plain {
            policy.domains.push(Domain {
                names: None,
                raw: BTreeSet::new(),
            });
        }
        for material in materials {
            let authority = authority
                .ok_or("permanent sealed exclusions require explicit scoped unlock authority")?;
            authority.check()?;
            let (_, names) = authority.material_secrets(conn, material).await?;
            let (parser,version,length,blob):(String,i32,i64,Option<String>)=sqlx::query_as("SELECT parser,parser_version,durable_bytes,erased_replay_blob FROM raw.materials WHERE id=$1")
                .bind(material.0).fetch_one(&mut *conn).await.map_err(|e|e.to_string())?;
            let blob =
                blob.ok_or("exclusion lacks authenticated exact raw-byte matching evidence")?;
            let scope = metadata::Scope::Replay {
                source: source.into(),
                material,
                parser,
                parser_version: version,
                length: u64::try_from(length).map_err(|_| "negative excluded material length")?,
                whole: names.role() == sinex_store::sealed::SecretRole::RecordNamingOnly,
            };
            let check = authority.clone();
            let reader = blobs
                .read_plain(&blob, None, move || {
                    check
                        .check()
                        .map_err(std::io::Error::other)
                        .map_err(Into::into)
                })
                .map_err(|e| e.to_string())?;
            let records = authority.open_replay_metadata(reader, &scope)?;
            policy.domains.push(Domain {
                names: Some(names),
                raw: records.into_iter().map(|r| r.raw_digest).collect(),
            });
        }
        if policy.domains.is_empty() && (!policy.keys.is_empty() || !policy.hashes.is_empty()) {
            return Err("exclusions have no verified record naming domain".into());
        }
        policy.revalidate(conn).await?;
        Ok(policy)
    }
    pub async fn revalidate(&self, conn: &mut PgConnection) -> Result<(), String> {
        if let Some(authority) = &self.authority {
            authority.check()?;
        }
        let current = excision::operations_basis(conn, &self.basis.source)
            .await
            .map_err(|e| e.to_string())?;
        if current != self.basis {
            return Err("source excision operations changed; retry acquisition".into());
        }
        Ok(())
    }
    pub fn excludes(
        &self,
        material: MaterialId,
        anchor: u64,
        raw: &[u8],
        hash: [u8; 32],
        events: &[Event],
    ) -> Result<bool, String> {
        if let Some(authority) = &self.authority {
            authority.check()?;
        }
        if self.locations.contains(&(material, anchor)) {
            return Ok(true);
        }
        for domain in &self.domains {
            let named_hash = domain
                .names
                .as_ref()
                .map_or(hash, |names| names.name_record_hash(&hash));
            if self.hashes.iter().any(|(_, stored)| *stored == named_hash) {
                return Ok(true);
            }
            if domain
                .names
                .as_ref()
                .is_some_and(|names| domain.raw.contains(&names.name_raw_record(raw)))
            {
                return Ok(true);
            }
            for event in events {
                if event.source != self.basis.source {
                    return Err("parser output changed source exclusion namespace".into());
                }
                let key = event.equivalence_key.as_ref().map(|key| {
                    domain
                        .names
                        .as_ref()
                        .map_or_else(|| key.0.clone(), |names| names.name_equivalence(&key.0).0)
                });
                if key.is_some_and(|key| self.keys.contains(&(event.event_type.into(), key))) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

// Legacy inference requires a registered native parser, retained plaintext source
// custody, canonical native keys and actual stored digest rows. The current
// privacy floor and an arbitrary key prefix are not naming-domain evidence.
async fn verify_plain(
    conn: &mut PgConnection,
    blobs: &Blobs,
    source: &str,
    material: Option<MaterialId>,
    scope: &serde_json::Value,
    operation: Uuid,
) -> Result<(), String> {
    let spec = sinex_sources::sources::spec(source)
        .ok_or("legacy exclusion has no declared native source")?;
    if !matches!(
        spec.driver,
        sinex_sources::Driver::AppendFile | sinex_sources::Driver::SqliteRows
    ) || excision::unsupported_ingress(conn, source)
        .await
        .map_err(|e| e.to_string())?
    {
        return Err("legacy source exclusion has unsupported or producer custody".into());
    }
    let candidates = excision::candidate_materials(conn, source, spec.parser)
        .await
        .map_err(|e| e.to_string())?;
    let selected = candidates
        .iter()
        .filter(|c| material.is_none_or(|id| id == c.material))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Err("plain exclusion lacks retained native source evidence".into());
    }
    for candidate in &selected {
        if candidate.sources != [source]
            || candidate.key.is_some()
            || candidate.parser_version != spec.parser_version as i32
            || blobs
                .is_sealed(&candidate.blob)
                .map_err(|e| e.to_string())?
        {
            return Err("legacy exclusion naming domain is ambiguous".into());
        }
    }
    let sealed_domains = scope["record_domains"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|d| d["source"] == source && d["naming"] == "sealed")
        .filter_map(|d| serde_json::from_value::<MaterialId>(d["material"].clone()).ok())
        .collect::<BTreeSet<_>>();
    let keys:Vec<(String,Option<String>,Option<Uuid>)>=sqlx::query_as("SELECT event_type,equivalence_key,material_id FROM core.hidden_occurrences WHERE source=$1 AND operation_id=$2 AND kind='excise'")
        .bind(source).bind(operation).fetch_all(&mut *conn).await.map_err(|e|e.to_string())?;
    for (kind, key, location) in keys {
        if !spec.event_types.contains(&kind.as_str())
            || key.as_ref().is_some_and(|key| {
                let key = if !sealed_domains.is_empty() {
                    key.strip_prefix("sealed:").unwrap_or(key)
                } else {
                    key
                };
                key.len() != 64
                    || !key
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            || location.is_some_and(|id| {
                !selected.iter().any(|c| c.material.0 == id)
                    && !sealed_domains.contains(&MaterialId(id))
            })
        {
            return Err("legacy exclusion is not verified native plaintext naming".into());
        }
    }
    let hashes:Vec<(String,Vec<u8>)>=sqlx::query_as("SELECT event_type,record_hash FROM core.excised_digests WHERE source=$1 AND operation_id=$2")
        .bind(source).bind(operation).fetch_all(&mut *conn).await.map_err(|e|e.to_string())?;
    if hashes
        .iter()
        .any(|(kind, hash)| !spec.event_types.contains(&kind.as_str()) || hash.len() != 32)
    {
        return Err("plain exclusion lacks declared record digest evidence".into());
    }
    if scope.get("material").is_some() {
        let id: MaterialId = serde_json::from_value(scope["material"].clone())
            .map_err(|_| "invalid whole exclusion material")?;
        if selected.iter().any(|c| c.material == id && c.length == 0) {
            return Ok(());
        }
        return Err("legacy whole exclusion lacks exact raw/no-event matching evidence".into());
    }
    if hashes.is_empty() && scope["record_domains_version"] != 1 {
        return Err("legacy exclusion lacks actual native record digests".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;

pub(crate) struct Filtered {
    pub acquired: sinex_sources::drivers::Acquired,
    pub erased: Option<sinex_sources::drain::ErasedTransitions>,
    pub publication: Option<sinex_model::ErasedReplayPublication>,
    pub registered: bool,
    pub bodyless: bool,
}
impl AcquisitionPolicy {
    /// Replace stale pending custody only after the current archive body and
    /// minimal replay envelope have both been authenticated.
    pub async fn reconcile(
        &self,
        conn: &mut PgConnection,
        blobs: &Blobs,
        source: &dyn sinex_sources::Source,
        acquired: sinex_sources::drivers::Acquired,
        cursor: &serde_json::Value,
        sealing: Option<&crate::one_shot::SealingSink>,
    ) -> Result<Filtered, String> {
        let mut result = Filtered {
            acquired,
            erased: None,
            publication: None,
            registered: false,
            bodyless: false,
        };
        let Some(material) = &mut result.acquired.material else {
            return Ok(result);
        };
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.materials WHERE id=$1)")
                .bind(material.id.0)
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
        if !exists {
            return Ok(result);
        }
        let opened: crate::retained::Opened = crate::retained::open(
            conn,
            blobs,
            source.spec().name,
            material.id,
            self.authority.as_ref(),
        )
        .await?;
        if opened.reference.parser != source.spec().parser
            || opened.reference.parser_version != source.spec().parser_version
        {
            return Err("pending material uses an unsupported recorded parser version".into());
        }
        let through = if cursor["material"].as_str() == Some(material.id.to_string().as_str()) {
            usize::try_from(
                cursor["through"]
                    .as_u64()
                    .ok_or("pending cursor lacks its exact admitted byte boundary")?,
            )
            .map_err(|_| "pending cursor exceeds address space")?
        } else {
            0
        };
        if opened.body.is_none()
            && material.records.iter().any(|&(start, end)| {
                !opened
                    .metadata
                    .iter()
                    .any(|r| r.start <= start as u64 && end as u64 <= r.end)
            })
        {
            return Err(
                "pending records differ from authenticated whole-erasure coordinates".into(),
            );
        }
        if let Some(sealing) = sealing {
            if let Some((reference, names)) = &opened.secrets {
                if names.role() == sinex_store::sealed::SecretRole::DataAndRecordNaming {
                    sealing.restore_current(
                        material.id,
                        opened.reference.blob.clone(),
                        reference.clone(),
                        names.clone(),
                    )?;
                }
            }
        }
        let backing = opened.records();
        result.bodyless = backing.bodyless;
        material.stored = Some(sinex_sources::drivers::StoredBytes::FramedReader(backing));
        material.bytes.clear();
        material.manifest = opened.reference.manifest.clone();
        result.erased = opened.transitions(through)?;
        result.registered = true;
        self.revalidate(conn).await?;
        Ok(result)
    }

    /// The caller holds acquisition custody through storage and the first
    /// durable material admission. No provider manifest supplies replay tokens.
    pub fn filter(
        &self,
        blobs: &Blobs,
        source: &dyn sinex_sources::Source,
        mut input: Filtered,
        carry: &serde_json::Value,
        class: sinex_model::Class,
        sealing: Option<&crate::one_shot::SealingSink>,
    ) -> Result<Filtered, String> {
        use sinex_model::RecordOutcome;
        use sinex_sources::{DrainInput, Limits, RecordContext, drivers};
        if self.basis.count == 0 {
            return Ok(input);
        }
        if input.acquired.image.is_some()
            || input
                .acquired
                .material
                .as_ref()
                .is_some_and(|m| !m.manifest["image_revision"].is_null())
        {
            return Err(
                "physical image acquisition with permanent exclusions is unavailable".into(),
            );
        }
        if !matches!(
            source.spec().driver,
            sinex_sources::Driver::AppendFile | sinex_sources::Driver::SqliteRows
        ) {
            return Err(
                "this source acquisition shape cannot yet enforce permanent exclusions".into(),
            );
        }
        let Some(material) = &mut input.acquired.material else {
            return Ok(input);
        };
        if source.spec().driver == sinex_sources::Driver::SqliteRows
            && !material.manifest["row_format"].is_null()
            && material.manifest["row_format"] != 1
            && material.manifest["row_format"] != 2
        {
            return Err("unsupported SQLite row encoding for exclusion matching".into());
        }
        if input.bodyless {
            return Ok(input);
        }
        if matches!(
            material.stored,
            Some(drivers::StoredBytes::File(_) | drivers::StoredBytes::Blob { .. })
        ) {
            return Err("container-backed source exclusion matching is unavailable".into());
        }
        let bytes = material.record_bytes();
        let length = byte_length(&bytes)?;
        let mut selected = Vec::new();
        let chunks = source
            .drainer(DrainInput {
                erased: input.erased.as_ref(),
                source: source.spec().name,
                class,
                material: material.id,
                bytes: material.record_bytes(),
                records: &material.records,
                carry: carry.clone(),
                reading_order: &*material.reading_order,
                limits: Limits {
                    max_events: 1,
                    max_bytes: 1,
                },
                manifest: &material.manifest,
            })
            .map_err(|e| e.to_string())?;
        for chunk in chunks {
            let chunk = chunk.map_err(|e| e.to_string())?;
            for outcome in chunk.outcomes {
                let (location, events, failed) = match outcome {
                    RecordOutcome::Events {
                        location: Some(location),
                        events,
                    } => (location, events, false),
                    RecordOutcome::Failure { location, .. } => (location, Vec::new(), true),
                    _ => return Err("native parser outcome lacks its exact record location".into()),
                };
                let anchor = usize::try_from(location.anchor)
                    .map_err(|_| "record anchor exceeds address space")?;
                let index = material
                    .records
                    .binary_search_by_key(&anchor, |r| r.0)
                    .map_err(|_| "parser outcome has no acquired record")?;
                let (start, end) = material.records[index];
                let raw = bytes.record(start, end).map_err(|e| e.to_string())?;
                let context = RecordContext {
                    manifest: &material.manifest,
                    member: sinex_sources::drain::member_at(&material.manifest, start),
                };
                let hash = source
                    .record_hash(&raw, &context)
                    .map_err(|e| e.to_string())?;
                if self.excludes(material.id, location.anchor, &raw, hash, &events)? {
                    if input.registered {
                        return Err("retained pending material still needs its authoritative exclusion replacement".into());
                    }
                    // A failed parse does not advance carry in the drain.
                    let token = if failed {
                        None
                    } else {
                        source
                            .erase_record(&raw, &chunk.carry, &context)
                            .map_err(|e| e.to_string())?
                            .replay
                    };
                    selected.push((start, end, token));
                }
            }
        }
        // An aligned rewrite retains old records too, even though only its
        // newly interpreted ranges enter the drain. Match their actual bytes
        // and declared hash without inventing a second interpretation/counter.
        let active = material.records.iter().copied().collect::<BTreeSet<_>>();
        let mut full = BorrowedBytes {
            bytes: &bytes,
            length,
            position: 0,
        };
        for (start, end) in source
            .spec()
            .framer
            .ranges(&mut full)
            .map_err(|e| e.to_string())?
        {
            if active.contains(&(start, end)) {
                continue;
            }
            let raw = bytes.record(start, end).map_err(|e| e.to_string())?;
            let context = RecordContext {
                manifest: &material.manifest,
                member: sinex_sources::drain::member_at(&material.manifest, start),
            };
            let hash = source
                .record_hash(&raw, &context)
                .map_err(|e| e.to_string())?;
            if self.excludes(material.id, start as u64, &raw, hash, &[])? {
                if input.registered {
                    return Err(
                        "retained material still contains excluded bytes outside its pending drain"
                            .into(),
                    );
                }
                selected.push((start, end, None));
            }
        }
        selected.sort_by_key(|(start, _, _)| *start);
        if selected.is_empty() {
            return Ok(input);
        }
        let spans = selected
            .iter()
            .map(|(s, e, _)| (*s, *e))
            .collect::<Vec<_>>();
        let reader = crate::retained::Blanked::new(
            BorrowedBytes {
                bytes: &bytes,
                length,
                position: 0,
            },
            spans,
        );
        let blob = if class == sinex_model::Class::Sealed {
            sealing
                .ok_or("sealed exclusion filtering requires its class recipient")?
                .put_reader(reader)?
        } else {
            if selected.iter().any(|(_, _, token)| token.is_some()) {
                return Err("stateful plain exclusion replay metadata is not available".into());
            }
            if let Some(drivers::StoredBytes::FramedReader(retained)) = &material.stored {
                if blobs.is_sealed(&retained.blob).map_err(|e| e.to_string())? {
                    return Err("sealed retained input cannot stage plaintext".into());
                }
            }
            blobs.put_reader(reader).map_err(|e| e.to_string())?
        };
        let backing = if class == sinex_model::Class::Sealed {
            let sealing = sealing.expect("checked sealing");
            let authority = self
                .authority
                .as_ref()
                .ok_or("sealed exclusion filtering requires scoped authority")?;
            if sealing.recipient().to_string() != authority.recipient().to_string() {
                return Err("source recipient differs from scoped class authority".into());
            }
            let key = sealing.key(&blob)?;
            let names = MaterialSecrets::from_data_key(&key);
            let records = selected
                .into_iter()
                .map(|(start, end, transition)| {
                    let raw = bytes.record(start, end).map_err(|e| e.to_string())?;
                    Ok(metadata::ErasedRecord {
                        start: start as u64,
                        end: end as u64,
                        raw_digest: names.name_raw_record(&raw),
                        transition,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let scope = metadata::Scope::Replay {
                source: source.spec().name.into(),
                material: material.id,
                parser: source.spec().parser.into(),
                parser_version: i32::try_from(source.spec().parser_version)
                    .map_err(|_| "parser version exceeds i32")?,
                length,
                whole: false,
            };
            let mut ciphertext = tempfile::tempfile().map_err(|e| e.to_string())?;
            metadata::seal_replay(
                &mut ciphertext,
                sealing.recipient(),
                &scope,
                records,
                || authority.check().map_err(sinex_store::Error::Invalid),
            )
            .map_err(|e| e.to_string())?;
            use std::io::Seek;
            let ciphertext_length = ciphertext.stream_position().map_err(|e| e.to_string())?;
            ciphertext.rewind().map_err(|e| e.to_string())?;
            let replay_blob = blobs
                .put_reader(&mut ciphertext)
                .map_err(|e| e.to_string())?;
            let check = authority.clone();
            let replay_reader = blobs
                .read_plain(&replay_blob, Some(ciphertext_length), move || {
                    check
                        .check()
                        .map_err(std::io::Error::other)
                        .map_err(Into::into)
                })
                .map_err(|e| e.to_string())?;
            let records = authority.open_replay_metadata(replay_reader, &scope)?;
            input.erased = Some(
                sinex_sources::drain::ErasedTransitions::from_archive(
                    records
                        .into_iter()
                        .map(|r| (r.start as usize, r.end as usize, r.transition)),
                    length,
                    false,
                )
                .map_err(|e| e.to_string())?,
            );
            input.publication = Some(sinex_model::ErasedReplayPublication {
                material: material.id,
                blob: replay_blob,
                length: ciphertext_length,
            });
            let check = authority.clone();
            let mut reader = blobs
                .read_sealed(
                    &blob,
                    Some(length),
                    &key,
                    crate::sealed_access::read_limits(length)?,
                    move || {
                        check
                            .check()
                            .map_err(std::io::Error::other)
                            .map_err(Into::into)
                    },
                )
                .map_err(|e| e.to_string())?;
            std::io::copy(&mut reader, &mut std::io::sink()).map_err(|e| e.to_string())?;
            reader.rewind().map_err(|e| e.to_string())?;
            std::sync::Arc::new(crate::retained::Retained::new(reader, length))
        } else {
            let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
            let mut reader = blobs
                .read_plain(&blob, Some(length), || Ok(()))
                .map_err(|e| e.to_string())?;
            std::io::copy(&mut reader, &mut file).map_err(|e| e.to_string())?;
            std::sync::Arc::new(crate::retained::Retained::new(file, length))
        };
        material.stored = Some(drivers::StoredBytes::FramedReader(
            drivers::RetainedRecords {
                bodyless: false,
                blob,
                bytes: backing,
            },
        ));
        material.bytes.clear();
        Ok(input)
    }
}

fn byte_length(bytes: &sinex_sources::drain::RecordBytes<'_>) -> Result<u64, String> {
    use sinex_sources::drain::RecordBytes;
    match bytes {
        RecordBytes::Slice(bytes) => Ok(bytes.len() as u64),
        RecordBytes::File(file) => file.metadata().map(|m| m.len()).map_err(|e| e.to_string()),
        RecordBytes::Reader(reader) => Ok(reader.length()),
        RecordBytes::Erased(length) => Ok(*length),
    }
}
struct BorrowedBytes<'a, 'b> {
    bytes: &'a sinex_sources::drain::RecordBytes<'b>,
    length: u64,
    position: u64,
}
impl std::io::Read for BorrowedBytes<'_, '_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = (self.length.saturating_sub(self.position)).min(out.len() as u64) as usize;
        if n == 0 {
            return Ok(0);
        }
        let start = usize::try_from(self.position)
            .map_err(|_| std::io::Error::other("material exceeds address space"))?;
        let raw = self.bytes.record(
            start,
            start
                .checked_add(n)
                .ok_or_else(|| std::io::Error::other("record range overflow"))?,
        )?;
        out[..n].copy_from_slice(&raw);
        self.position += n as u64;
        Ok(n)
    }
}
impl std::io::Seek for BorrowedBytes<'_, '_> {
    fn seek(&mut self, from: std::io::SeekFrom) -> std::io::Result<u64> {
        let position = match from {
            std::io::SeekFrom::Start(n) => i128::from(n),
            std::io::SeekFrom::Current(n) => i128::from(self.position) + i128::from(n),
            std::io::SeekFrom::End(n) => i128::from(self.length) + i128::from(n),
        };
        self.position =
            u64::try_from(position).map_err(|_| std::io::Error::other("invalid retained seek"))?;
        Ok(self.position)
    }
}
