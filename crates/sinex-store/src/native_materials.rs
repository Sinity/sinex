//! Native publication custody precedes CAS installation. Ready materials and
//! interpretations never cite this table; failed first admission leaves its
//! original version available for exact retry or an explicit missing state.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::{MaterialId, MaterialRef, SourceProgress};
use sqlx::PgConnection;

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
    pub base_cursor: Value,
    pub base_carry: Value,
    pub start_cursor: Value,
    pub end_cursor: Value,
    pub state: State,
}

impl Pending {
    /// A restarted driver supplies decoded records and chunk progress, while
    /// the reservation supplies the original observation/publication facts.
    /// Re-polling the edited provider cannot replace those frozen facts.
    pub fn first_progress(&self, cursor: Value, carry: Value) -> SourceProgress {
        let mut progress = self.progress.clone();
        progress.cursor = cursor;
        progress.carry = carry;
        progress.native_pending = Some(self.material);
        progress
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
    metadata: sinex_model::SqliteImageMeta,
    material: Material,
    extents: Vec<sinex_model::ImageExtent>,
    key: Option<Key>,
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
        input: p.input.clone(),
        observation: p.observation.clone(),
        revision: p.revision,
        material: p.material.as_ref().map(material),
        image: p.image.as_ref().map(|i| Image {
            metadata: i.metadata.clone(),
            material: material(&i.material),
            extents: i.extents.clone(),
            key: i.key.as_ref().map(key),
        }),
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
fn decode(v: Value, binding: &str, cursor: &Value, carry: &Value) -> Result<SourceProgress> {
    let p: Publication =
        serde_json::from_value(v).map_err(|_| invalid("invalid native publication format"))?;
    if p.version != 1 {
        return Err(invalid("unsupported native publication version"));
    }
    let image = p.image.map(|i| sinex_model::SqliteImagePublication {
        metadata: i.metadata,
        material: unmaterial(i.material),
        extents: i.extents,
        key: i.key.map(unkey),
    });
    if let Some(image) = &image {
        image.validate().map_err(Error::Invalid)?;
    }
    Ok(SourceProgress {
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
    })
}
fn primary(p: &SourceProgress) -> Option<MaterialId> {
    p.material
        .as_ref()
        .map(|m| m.id)
        .or_else(|| p.image.as_ref().map(|i| i.material.id))
}

/// Caller holds the source shared guard. Commit this reservation before the
/// prepared plain handles install any public CAS names.
pub async fn reserve(conn: &mut PgConnection, pending: &Pending) -> Result<()> {
    sinex_schema::require_current(conn).await?;
    if primary(&pending.progress) != Some(pending.material)
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
    if let Some(m) = &pending.progress.material {
        if pending
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
    }
    let publication = encode(&pending.progress)?;
    sqlx::query("INSERT INTO raw.pending_native_materials(material_id,source,binding,publication,base_cursor,base_carry,start_cursor,end_cursor,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'reserved') ON CONFLICT(material_id) DO NOTHING")
        .bind(pending.material.0).bind(&pending.source).bind(&pending.binding).bind(&publication).bind(&pending.base_cursor).bind(&pending.base_carry).bind(&pending.start_cursor).bind(&pending.end_cursor).execute(&mut *conn).await?;
    let actual = load(conn, pending.material)
        .await?
        .ok_or_else(|| invalid("native publication reservation disappeared"))?;
    if actual.source != pending.source
        || actual.binding != pending.binding
        || encode(&actual.progress)? != publication
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
    let row:Option<(String,String,Value,Value,Value,Value,Value,String)>=sqlx::query_as("SELECT source,binding,publication,base_cursor,base_carry,start_cursor,end_cursor,state FROM raw.pending_native_materials WHERE material_id=$1")
        .bind(material.0).fetch_optional(conn).await?;
    row.map(
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
            let progress = decode(publication, &binding, &start_cursor, &base_carry)?;
            if primary(&progress) != Some(material)
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
    .transpose()
}
pub async fn binding_pending(conn: &mut PgConnection, binding: &str) -> Result<Vec<MaterialId>> {
    Ok(sqlx::query_scalar::<_,uuid::Uuid>("SELECT material_id FROM raw.pending_native_materials WHERE binding=$1 ORDER BY reserved_at,material_id").bind(binding).fetch_all(conn).await?.into_iter().map(MaterialId).collect())
}
pub async fn source_pending(conn: &mut PgConnection, source: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM raw.pending_native_materials WHERE source=$1)",
    )
    .bind(source)
    .fetch_one(conn)
    .await?)
}
pub async fn installed(conn: &mut PgConnection, material: MaterialId) -> Result<()> {
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

pub(crate) async fn validate(conn: &mut PgConnection, p: &SourceProgress) -> Result<()> {
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
        || encode(&pending.progress)? != encode(p)?
    {
        return Err(invalid(
            "native first admission differs from its frozen installed publication",
        ));
    }
    Ok(())
}
pub(crate) async fn complete(conn: &mut PgConnection, p: &SourceProgress) -> Result<()> {
    if let Some(id) = p.native_pending {
        let rows=sqlx::query("DELETE FROM raw.pending_native_materials WHERE material_id=$1 AND binding=$2 AND state='installed'").bind(id.0).bind(&p.binding).execute(conn).await?;
        if rows.rows_affected() != 1 {
            return Err(invalid("native publication completion lost its owner"));
        }
    }
    Ok(())
}
