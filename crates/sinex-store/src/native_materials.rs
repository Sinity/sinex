//! Native publication custody precedes CAS installation. Ready materials and
//! interpretations never cite this table; failed first admission leaves its
//! original version available for exact retry or an explicit missing state.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::{MaterialId, MaterialRef, SourceProgress};
use sqlx::PgConnection;
use std::collections::BTreeMap;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum State {
    Reserved,
    Installed,
}

#[derive(Clone)]
pub struct Pending {
    pub material: MaterialId,
    pub source: String,
    pub binding: String,
    pub progress: SourceProgress,
    pub removed: Vec<sinex_model::RecordLocation>,
    pub cited: Vec<sinex_model::Cited>,
    pub image: Option<PendingImage>,
    pub base_cursor: Value,
    pub base_carry: Value,
    pub start_cursor: Value,
    pub end_cursor: Value,
    pub state: State,
}

/// A physical image's content/layout is fixed before CAS installation; the
/// dedup winner's actual stored costs are known only after verification.
#[derive(Clone)]
pub enum PendingImage {
    AwaitingCosts(sinex_model::SqliteImagePublication),
}
impl PendingImage {
    pub fn awaiting(mut image: sinex_model::SqliteImagePublication) -> Result<Self> {
        image.validate().map_err(Error::Invalid)?;
        // These placeholders never enter ready SourceProgress or durable JSON.
        image.metadata.base_stored_bytes = 0;
        image.metadata.patch_stored_bytes = 0;
        image.metadata.read_stored_bytes = 0;
        image.material.manifest["sqlite_image"] = serde_json::to_value(&image.metadata)
            .map_err(|_| invalid("image plan cannot encode"))?;
        Ok(Self::AwaitingCosts(image))
    }
}

impl Pending {
    /// A restarted driver supplies decoded records and chunk progress, while
    /// the reservation supplies the original observation/publication facts.
    /// Re-polling the edited provider cannot replace those frozen facts.
    pub fn first_archive(&self) -> Vec<sinex_model::ArchiveRequest> {
        if self.removed.is_empty() {
            Vec::new()
        } else {
            vec![sinex_model::ArchiveRequest::RemovedAtSource {
                locations: self.removed.clone(),
            }]
        }
    }

    pub fn first_progress(&self, cursor: Value, carry: Value) -> Result<SourceProgress> {
        if self.image.is_some() {
            return Err(invalid("native image costs are unresolved"));
        }
        let mut progress = self.progress.clone();
        progress.cursor = cursor;
        progress.carry = carry;
        progress.native_pending = Some(self.material);
        Ok(progress)
    }
}

// Serialization stays private to storage; no event bodies or decoded sealed
// carry/keys enter this publication representation or a public command DTO.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    version: u32,
    input: sinex_model::InputRef,
    observation: sinex_model::ObservationFacts,
    revision: sinex_model::RevisionChange,
    material: Option<Material>,
    image: Option<Image>,
    image_revision: Option<sinex_model::RevisionId>,
    keys: Vec<Key>,
    erased_replay: Option<Replay>,
    basis: Basis,
    removed: Vec<sinex_model::RecordLocation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    cited: Vec<sinex_model::Cited>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Material {
    id: MaterialId,
    input: Option<sinex_model::InputId>,
    blob: String,
    length: u64,
    part_index: u32,
    previous_part: Option<MaterialId>,
    parser: String,
    parser_version: u32,
    manifest: Value,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Key {
    material: MaterialId,
    key_ref: String,
    wrapped: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Image {
    metadata: Value,
    costs: Option<Costs>,
    material: Material,
    extents: Vec<sinex_model::ImageExtent>,
    key: Option<Key>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Costs {
    base_stored_bytes: u64,
    patch_stored_bytes: u64,
    read_stored_bytes: u64,
}

fn image(i: &sinex_model::SqliteImagePublication, resolved: bool) -> Result<Image> {
    let mut metadata =
        serde_json::to_value(&i.metadata).map_err(|_| invalid("image plan cannot encode"))?;
    let object = metadata
        .as_object_mut()
        .ok_or_else(|| invalid("image metadata is not an object"))?;
    for field in [
        "base_stored_bytes",
        "patch_stored_bytes",
        "read_stored_bytes",
    ] {
        object.remove(field);
    }
    let mut m = material(&i.material);
    m.manifest["sqlite_image"] = metadata.clone();
    Ok(Image {
        metadata,
        costs: resolved.then_some(Costs {
            base_stored_bytes: i.metadata.base_stored_bytes,
            patch_stored_bytes: i.metadata.patch_stored_bytes,
            read_stored_bytes: i.metadata.read_stored_bytes,
        }),
        material: m,
        extents: i.extents.clone(),
        key: i.key.as_ref().map(key),
    })
}
fn unimage(i: Image) -> Result<(sinex_model::SqliteImagePublication, bool)> {
    let resolved = i.costs.is_some();
    let costs = i.costs.unwrap_or(Costs {
        base_stored_bytes: 0,
        patch_stored_bytes: 0,
        read_stored_bytes: 0,
    });
    if i.material.manifest["sqlite_image"] != i.metadata {
        return Err(invalid("pending image manifest differs from its facts"));
    }
    let mut metadata = i.metadata;
    let fields = metadata
        .as_object_mut()
        .ok_or_else(|| invalid("invalid image metadata"))?;
    for (name, value) in [
        ("base_stored_bytes", costs.base_stored_bytes),
        ("patch_stored_bytes", costs.patch_stored_bytes),
        ("read_stored_bytes", costs.read_stored_bytes),
    ] {
        if fields.insert(name.into(), Value::from(value)).is_some() {
            return Err(invalid("image plan duplicates storage costs"));
        }
    }
    let metadata =
        serde_json::from_value(metadata).map_err(|_| invalid("invalid image metadata"))?;
    let mut material = unmaterial(i.material);
    material.manifest["sqlite_image"] =
        serde_json::to_value(&metadata).map_err(|_| invalid("invalid image metadata"))?;
    let image = sinex_model::SqliteImagePublication {
        metadata,
        material,
        extents: i.extents,
        key: i.key.map(unkey),
    };
    image.validate().map_err(Error::Invalid)?;
    Ok((image, resolved))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Replay {
    material: MaterialId,
    blob: String,
    length: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Basis {
    source: String,
    operations: [u8; 32],
    count: u64,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
fn material(m: &MaterialRef) -> Material {
    Material {
        id: m.id,
        input: m.input,
        blob: m.blob.clone(),
        length: m.length,
        part_index: m.part_index,
        previous_part: m.previous_part,
        parser: m.parser.clone(),
        parser_version: m.parser_version,
        manifest: m.manifest.clone(),
    }
}
fn unmaterial(m: Material) -> MaterialRef {
    MaterialRef {
        id: m.id,
        input: m.input,
        blob: m.blob,
        length: m.length,
        part_index: m.part_index,
        previous_part: m.previous_part,
        parser: m.parser,
        parser_version: m.parser_version,
        manifest: m.manifest,
    }
}
fn key(k: &sinex_model::WrappedMaterialKey) -> Key {
    Key {
        material: k.material,
        key_ref: k.key_ref.clone(),
        wrapped: k.wrapped.clone(),
    }
}
fn unkey(k: Key) -> sinex_model::WrappedMaterialKey {
    sinex_model::WrappedMaterialKey {
        material: k.material,
        key_ref: k.key_ref,
        wrapped: k.wrapped,
    }
}
fn encode(p: &SourceProgress) -> Result<Value> {
    let b = p
        .excision_basis
        .as_ref()
        .ok_or_else(|| invalid("native publication needs its source excision basis"))?;
    let publication = Publication {
        version: 1,
        removed: Vec::new(),
        cited: Vec::new(),
        input: p.input.clone(),
        observation: p.observation.clone(),
        revision: p.revision,
        material: p.material.as_ref().map(material),
        image: p.image.as_ref().map(|i| image(i, true)).transpose()?,
        image_revision: p.image_revision,
        keys: p.material_keys.iter().map(key).collect(),
        erased_replay: p.erased_replay.as_ref().map(|r| Replay {
            material: r.material,
            blob: r.blob.clone(),
            length: r.length,
        }),
        basis: Basis {
            source: b.source.clone(),
            operations: b.operations,
            count: b.count,
        },
    };
    serde_json::to_value(publication).map_err(|_| invalid("native publication cannot encode"))
}
struct Decoded {
    progress: SourceProgress,
    image: Option<PendingImage>,
    removed: Vec<sinex_model::RecordLocation>,
    cited: Vec<sinex_model::Cited>,
}
fn decode(v: Value, binding: &str, cursor: &Value, carry: &Value) -> Result<Decoded> {
    let p: Publication =
        serde_json::from_value(v).map_err(|_| invalid("invalid native publication format"))?;
    if !matches!(p.version, 1 | 2) || (p.version == 1 && !p.cited.is_empty()) {
        return Err(invalid("unsupported native publication version"));
    }
    citation_set(&p.cited)?;
    let image = p.image.map(unimage).transpose()?;
    let pending_image = image
        .as_ref()
        .filter(|(_, resolved)| !*resolved)
        .map(|(i, _)| PendingImage::AwaitingCosts(i.clone()));
    let image = image.filter(|(_, resolved)| *resolved).map(|(i, _)| i);
    Ok(Decoded {
        progress: SourceProgress {
            native_pending: None,
            erased_replay: p
                .erased_replay
                .map(|r| sinex_model::ErasedReplayPublication {
                    material: r.material,
                    blob: r.blob,
                    length: r.length,
                }),
            excision_basis: Some(sinex_model::ExcisionBasis {
                source: p.basis.source,
                operations: p.basis.operations,
                count: p.basis.count,
            }),
            binding: binding.into(),
            image,
            image_revision: p.image_revision,
            material_keys: p.keys.into_iter().map(unkey).collect(),
            input: p.input,
            observation: p.observation,
            revision: p.revision,
            material: p.material.map(unmaterial),
            cursor: cursor.clone(),
            carry: carry.clone(),
        },
        image: pending_image,
        removed: p.removed,
        cited: p.cited,
    })
}
fn encode_pending(p: &Pending) -> Result<Value> {
    let mut encoded = encode(&p.progress)?;
    if !p.cited.is_empty() {
        encoded["version"] = serde_json::json!(2);
        encoded["cited"] = serde_json::to_value(&p.cited)
            .map_err(|_| invalid("native citations cannot encode"))?;
    }
    encoded["removed"] = serde_json::to_value(&p.removed)
        .map_err(|_| invalid("native removal plan cannot encode"))?;
    if let Some(PendingImage::AwaitingCosts(i)) = &p.image {
        if p.progress.image.is_some() {
            return Err(invalid(
                "native publication has both pending and ready image",
            ));
        }
        encoded["image"] = serde_json::to_value(image(i, false)?)
            .map_err(|_| invalid("image plan cannot encode"))?;
    }
    Ok(encoded)
}
fn pending_primary(p: &Pending) -> Option<MaterialId> {
    primary(&p.progress).or_else(|| {
        p.image
            .as_ref()
            .map(|PendingImage::AwaitingCosts(i)| i.material.id)
    })
}
fn primary(p: &SourceProgress) -> Option<MaterialId> {
    p.material
        .as_ref()
        .map(|m| m.id)
        .or_else(|| p.image.as_ref().map(|i| i.material.id))
}

/// Native first admission supports only the driver's vanished-record plan.
/// The same extraction is used before reservation and when validating admission.
pub fn removal_plan(
    archive: &[sinex_model::ArchiveRequest],
) -> Result<Vec<sinex_model::RecordLocation>> {
    let mut locations = Vec::new();
    for request in archive {
        match request {
            sinex_model::ArchiveRequest::RemovedAtSource { locations: removed } => {
                locations.extend_from_slice(removed)
            }
            _ => {
                return Err(invalid(
                    "native publication contains an unsupported archive request",
                ));
            }
        }
    }
    Ok(locations)
}

// The projection is derived exclusively from the frozen publication codec.
// Extents may refer to an older full blob whose total length is not in the map.
fn blob_references(publication: Value) -> Result<BTreeMap<String, Option<i64>>> {
    let p: Publication =
        serde_json::from_value(publication).map_err(|_| invalid("invalid native publication"))?;
    let mut refs = BTreeMap::new();
    let mut add = |name: String, length: Option<u64>| -> Result<()> {
        let length = length
            .map(i64::try_from)
            .transpose()
            .map_err(|_| invalid("native blob length beyond i64"))?;
        match refs.entry(name) {
            std::collections::btree_map::Entry::Vacant(e) => {
                e.insert(length);
            }
            std::collections::btree_map::Entry::Occupied(mut e) => {
                if let (Some(old), Some(new)) = (*e.get(), length)
                    && old != new
                {
                    return Err(invalid("native blob has conflicting lengths"));
                }
                if length.is_some() {
                    e.insert(length);
                }
            }
        }
        Ok(())
    };
    if let Some(m) = p.material {
        add(m.blob, Some(m.length))?;
    }
    if let Some(i) = p.image {
        add(i.material.blob, Some(i.material.length))?;
        for extent in i.extents {
            add(extent.blob, None)?;
        }
    }
    if let Some(replay) = p.erased_replay {
        add(replay.blob, Some(replay.length))?;
    }
    if !matches!(p.version, 1 | 2) || (p.version == 1 && !p.cited.is_empty()) {
        return Err(invalid("unsupported native publication citation format"));
    }
    for citation in p.cited {
        add(citation.blob, Some(citation.size))?;
    }
    Ok(refs)
}

async fn reserve_blobs(
    conn: &mut PgConnection,
    material: MaterialId,
    publication: Value,
) -> Result<()> {
    for (name, length) in blob_references(publication)? {
        if let Some(length) = length {
            sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'reserved') ON CONFLICT(hash) DO NOTHING")
                .bind(&name).bind(length).execute(&mut *conn).await?;
        }
        // Reservation and deletion take the same row lock. If deletion won,
        // the INSERT above reserved a new missing name before installation.
        let actual: Option<i64> =
            sqlx::query_scalar("SELECT size FROM core.blobs WHERE hash=$1 FOR UPDATE")
                .bind(&name)
                .fetch_optional(&mut *conn)
                .await?;
        if actual.is_none() || length.is_some_and(|length| actual != Some(length)) {
            return Err(invalid(
                "native blob reservation has missing or inconsistent length",
            ));
        }
        sqlx::query("INSERT INTO raw.pending_native_blobs(material_id,blob) VALUES($1,$2) ON CONFLICT DO NOTHING")
            .bind(material.0).bind(&name).execute(&mut *conn).await?;
    }
    Ok(())
}

async fn validate_blob_references(conn: &mut PgConnection, pending: &Pending) -> Result<()> {
    let expected = blob_references(encode_pending(pending)?)?;
    let actual: Vec<(String,i64)> = sqlx::query_as("SELECT p.blob,b.size FROM raw.pending_native_blobs p JOIN core.blobs b ON b.hash=p.blob WHERE p.material_id=$1 ORDER BY p.blob")
        .bind(pending.material.0).fetch_all(conn).await?;
    if actual.len() != expected.len()
        || actual.iter().any(|(name, size)| {
            expected
                .get(name)
                .is_none_or(|length| length.is_some_and(|length| length != *size))
        })
    {
        return Err(invalid(
            "native blob reference projection differs from frozen publication",
        ));
    }
    Ok(())
}

/// Caller holds the source shared guard. Commit this reservation before the
/// prepared plain handles install any public CAS names.
pub async fn reserve(conn: &mut PgConnection, pending: &Pending) -> Result<()> {
    sinex_schema::require_current(conn).await?;
    citation_set(&pending.cited)?;
    if pending_primary(pending) != Some(pending.material)
        || pending.progress.binding != pending.binding
        || pending
            .progress
            .excision_basis
            .as_ref()
            .is_none_or(|b| b.source != pending.source)
    {
        return Err(invalid(
            "native publication owner differs from its progress",
        ));
    }
    let image = pending
        .image
        .as_ref()
        .map(|PendingImage::AwaitingCosts(i)| &i.metadata)
        .or_else(|| pending.progress.image.as_ref().map(|i| &i.metadata));
    if let Some(image) = image {
        crate::sqlite_images::guard_acquisition(
            conn,
            &pending.progress.input,
            &pending.progress.observation,
            image,
        )
        .await?;
    }
    let source: Option<String> =
        sqlx::query_scalar("SELECT source FROM raw.bindings WHERE binding=$1 FOR UPDATE")
            .bind(&pending.binding)
            .fetch_optional(&mut *conn)
            .await?;
    if source.as_deref() != Some(pending.source.as_str()) {
        return Err(invalid("native publication has no matching source binding"));
    }
    let current = crate::positions::load(conn, &pending.binding)
        .await?
        .unwrap_or((Value::Null, Value::Null));
    if current != (pending.base_cursor.clone(), pending.base_carry.clone()) {
        return Err(invalid(
            "native publication base position changed before reservation",
        ));
    }
    if binding_pending(conn, &pending.binding)
        .await?
        .iter()
        .any(|id| *id != pending.material)
    {
        return Err(invalid(
            "binding already owns another pending native version",
        ));
    }
    if let Some(m) = &pending.progress.material
        && pending
            .progress
            .material_keys
            .iter()
            .any(|k| k.material == m.id)
    {
        for carry in [&pending.progress.carry, &pending.base_carry] {
            if !(carry.is_null()
                || crate::sealed::is_sealed(carry)
                || crate::sealed::metadata::is_carry(carry))
            {
                return Err(invalid(
                    "sealed pending publication contains unprotected carry",
                ));
            }
        }
    }
    let publication = encode_pending(pending)?;
    sqlx::query("INSERT INTO raw.pending_native_materials(material_id,source,binding,publication,base_cursor,base_carry,start_cursor,end_cursor,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'reserved') ON CONFLICT(material_id) DO NOTHING")
        .bind(pending.material.0).bind(&pending.source).bind(&pending.binding).bind(&publication).bind(&pending.base_cursor).bind(&pending.base_carry).bind(&pending.start_cursor).bind(&pending.end_cursor).execute(&mut *conn).await?;
    reserve_blobs(conn, pending.material, publication.clone()).await?;
    let actual = load(conn, pending.material)
        .await?
        .ok_or_else(|| invalid("native publication reservation disappeared"))?;
    if actual.source != pending.source
        || actual.binding != pending.binding
        || encode_pending(&actual)? != publication
        || actual.base_cursor != pending.base_cursor
        || actual.base_carry != pending.base_carry
        || actual.start_cursor != pending.start_cursor
        || actual.end_cursor != pending.end_cursor
    {
        return Err(invalid(
            "native material already has a different frozen publication",
        ));
    }
    Ok(())
}

pub async fn load(conn: &mut PgConnection, material: MaterialId) -> Result<Option<Pending>> {
    type Row = (String, String, Value, Value, Value, Value, Value, String);
    let row: Option<Row> =sqlx::query_as("SELECT source,binding,publication,base_cursor,base_carry,start_cursor,end_cursor,state FROM raw.pending_native_materials WHERE material_id=$1")
        .bind(material.0).fetch_optional(&mut *conn).await?;
    let pending = row
        .map(
            |(
                source,
                binding,
                publication,
                base_cursor,
                base_carry,
                start_cursor,
                end_cursor,
                state,
            )| {
                let Decoded {
                    progress,
                    image,
                    removed,
                    cited,
                } = decode(publication, &binding, &start_cursor, &base_carry)?;
                if primary(&progress).or_else(|| {
                    image
                        .as_ref()
                        .map(|PendingImage::AwaitingCosts(i)| i.material.id)
                }) != Some(material)
                    || progress
                        .excision_basis
                        .as_ref()
                        .is_none_or(|b| b.source != source)
                {
                    return Err(invalid("native publication ownership metadata differs"));
                }
                Ok(Pending {
                    material,
                    source,
                    binding,
                    progress,
                    removed,
                    cited,
                    image,
                    base_cursor,
                    base_carry,
                    start_cursor,
                    end_cursor,
                    state: match state.as_str() {
                        "reserved" => State::Reserved,
                        "installed" => State::Installed,
                        _ => return Err(invalid("unknown native publication state")),
                    },
                })
            },
        )
        .transpose()?;
    if let Some(pending) = &pending {
        validate_blob_references(conn, pending).await?;
    }
    Ok(pending)
}
pub async fn binding_pending(conn: &mut PgConnection, binding: &str) -> Result<Vec<MaterialId>> {
    Ok(sqlx::query_scalar::<_,uuid::Uuid>("SELECT material_id FROM raw.pending_native_materials WHERE binding=$1 ORDER BY reserved_at,material_id").bind(binding).fetch_all(conn).await?.into_iter().map(MaterialId).collect())
}
/// Image ownership includes unresolved and finalized reservations. Decode through
/// the publication owner before using any input/layout facts for a lineage guard.
pub async fn pending_images(conn: &mut PgConnection) -> Result<Vec<Pending>> {
    let ids: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT material_id FROM raw.pending_native_materials WHERE publication->'image' IS NOT NULL AND publication->'image'<>'null'::jsonb ORDER BY material_id")
        .fetch_all(&mut *conn).await?;
    let mut images = Vec::new();
    for id in ids {
        let pending = load(conn, MaterialId(id))
            .await?
            .ok_or_else(|| invalid("pending image disappeared"))?;
        images.push(pending);
    }
    Ok(images)
}
pub async fn source_pending(conn: &mut PgConnection, source: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM raw.pending_native_materials WHERE source=$1)",
    )
    .bind(source)
    .fetch_one(conn)
    .await?)
}
/// Finish only the actual installed costs. Content, layout, keys and original
/// source facts are compared with the reserved unpriced plan before replacement.
pub async fn finalize_image(
    conn: &mut PgConnection,
    material: MaterialId,
    completed: sinex_model::SqliteImagePublication,
) -> Result<()> {
    completed.validate().map_err(Error::Invalid)?;
    let mut pending = load(conn, material)
        .await?
        .ok_or_else(|| invalid("image finalization has no native owner"))?;
    let Some(PendingImage::AwaitingCosts(expected)) = pending.image.take() else {
        return Err(invalid("native image costs already finalized or absent"));
    };
    if serde_json::to_value(image(&expected, false)?)
        .map_err(|_| invalid("image plan cannot encode"))?
        != serde_json::to_value(image(&completed, false)?)
            .map_err(|_| invalid("image plan cannot encode"))?
    {
        return Err(invalid(
            "installed image differs from reserved content/layout",
        ));
    }
    let old = encode_pending(&Pending {
        image: Some(PendingImage::AwaitingCosts(expected)),
        ..pending.clone()
    })?;
    pending.progress.image = Some(completed);
    let changed=sqlx::query("UPDATE raw.pending_native_materials SET publication=$1 WHERE material_id=$2 AND state='reserved' AND publication=$3")
        .bind(encode_pending(&pending)?).bind(material.0).bind(old).execute(conn).await?;
    if changed.rows_affected() != 1 {
        return Err(invalid("native image plan changed before finalization"));
    }
    Ok(())
}
pub async fn installed(conn: &mut PgConnection, material: MaterialId) -> Result<()> {
    if load(conn, material)
        .await?
        .is_none_or(|p| p.image.is_some())
    {
        return Err(invalid(
            "native installation needs finalized image storage costs",
        ));
    }
    sqlx::query("UPDATE core.blobs b SET status='present' FROM raw.pending_native_blobs p WHERE p.material_id=$1 AND p.blob=b.hash AND b.status IN ('reserved','deleting')")
        .bind(material.0).execute(&mut *conn).await?;
    let changed = sqlx::query(
        "UPDATE raw.pending_native_materials SET state='installed' WHERE material_id=$1",
    )
    .bind(material.0)
    .execute(conn)
    .await?;
    if changed.rows_affected() != 1 {
        return Err(invalid("native installation has no reserved owner"));
    }
    Ok(())
}

pub(crate) async fn validate(
    conn: &mut PgConnection,
    p: &SourceProgress,
    archive: &[sinex_model::ArchiveRequest],
    cited: &[sinex_model::Cited],
) -> Result<()> {
    let Some(id) = p.native_pending else {
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM raw.pending_native_materials WHERE binding=$1)",
        )
        .bind(&p.binding)
        .fetch_one(&mut *conn)
        .await?
        {
            return Err(invalid("binding has unresolved native publication custody"));
        }
        return Ok(());
    };
    let pending = load(conn, id)
        .await?
        .ok_or_else(|| invalid("native publication reservation is missing"))?;
    let current = crate::positions::load(conn, &pending.binding)
        .await?
        .unwrap_or((Value::Null, Value::Null));
    if current != (pending.base_cursor.clone(), pending.base_carry.clone()) {
        return Err(invalid(
            "native publication base position changed before first admission",
        ));
    }
    if pending.state != State::Installed
        || pending.binding != p.binding
        || primary(p) != Some(id)
        || pending.removed != removal_plan(archive)?
        || citation_set(&pending.cited)? != citation_set(cited)?
        || encode(&pending.progress)? != encode(p)?
    {
        return Err(invalid(
            "native first admission differs from its frozen installed publication",
        ));
    }
    Ok(())
}
pub(crate) async fn complete(
    conn: &mut PgConnection,
    p: &SourceProgress,
    events: &[sinex_model::Event],
    resolved: &[Option<uuid::Uuid>],
) -> Result<()> {
    if let Some(id) = p.native_pending {
        let pending = load(conn, id)
            .await?
            .ok_or_else(|| invalid("native completion lost its publication"))?;
        let expected = citation_set(&pending.cited)?;
        if !expected.is_empty() {
            if events.len() != resolved.len() {
                return Err(invalid("cited media has no complete admission resolution"));
            }
            let (mut ids, mut blobs, mut sizes) = (Vec::new(), Vec::new(), Vec::new());
            let mut covered = BTreeMap::new();
            for (event, reading) in events.iter().zip(resolved) {
                for citation in &event.cites {
                    let id = reading.ok_or_else(|| invalid("cited media lacks a resolved ready ownership reading; retain pending custody"))?;
                    covered.insert(citation.blob.clone(), citation.size);
                    ids.push(id);
                    blobs.push(citation.blob.clone());
                    sizes.push(
                        i64::try_from(citation.size)
                            .map_err(|_| invalid("citation length beyond i64"))?,
                    );
                }
            }
            if covered != expected {
                return Err(invalid(
                    "resolved readings do not cover frozen media citations",
                ));
            }
            // A different record/source may own the same hash only temporarily.
            // Resolve every incoming citation to its actual admitted or repeated
            // reading before releasing this publication's pending ownership.
            let ready:bool=sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM unnest($1::uuid[],$2::text[],$3::bigint[]) wanted(event_id,blob,size) LEFT JOIN core.event_blobs e ON e.event_id=wanted.event_id AND e.blob=wanted.blob LEFT JOIN core.blobs b ON b.hash=e.blob WHERE e.event_id IS NULL OR b.size IS DISTINCT FROM wanted.size)")
                .bind(&ids).bind(&blobs).bind(&sizes).fetch_one(&mut *conn).await?;
            if !ready {
                return Err(invalid(
                    "cited media lacks ready ownership on its resolved reading; retain pending custody",
                ));
            }
        }
        let rows=sqlx::query("DELETE FROM raw.pending_native_materials WHERE material_id=$1 AND binding=$2 AND state='installed'").bind(id.0).bind(&p.binding).execute(conn).await?;
        if rows.rows_affected() != 1 {
            return Err(invalid("native publication completion lost its owner"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Citation custody freezes unique names and exact decoded lengths before install.
pub fn citation_set(cited: &[sinex_model::Cited]) -> Result<BTreeMap<String, u64>> {
    let mut result = BTreeMap::new();
    for citation in cited {
        if i64::try_from(citation.size).is_err() || crate::blobs::parse(&citation.blob).is_err() {
            return Err(invalid("invalid native cited blob"));
        }
        if result
            .insert(citation.blob.clone(), citation.size)
            .is_some_and(|old| old != citation.size)
        {
            return Err(invalid("native citation lengths conflict"));
        }
    }
    Ok(result)
}
