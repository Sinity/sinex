//! Source-owned query products are projected after their captured contracts are checked.
//! Captured events, source declarations and payload identities remain immutable.
use super::*;
use sinex_model::event_types::{analysis::AnalysisRequest, query::Saved, work::ContextPack};
use sinex_model::{EventId, EventPayload, KindDeclaration, KindRef, SourceRef};

pub struct CheckedAnalysisRequest {
    event: EventId,
    request: AnalysisRequest,
}
impl CheckedAnalysisRequest {
    pub fn event(&self) -> EventId {
        self.event
    }
    pub fn request(&self) -> &AnalysisRequest {
        &self.request
    }
    pub fn into_request(self) -> AnalysisRequest {
        self.request
    }
}

#[derive(sqlx::FromRow)]
struct Captured {
    event_type: String,
    kind_version: i64,
    source: String,
    source_version: Option<i64>,
    source_fingerprint: Option<String>,
    producer_family: Option<String>,
    payload: Option<Json>,
    material_id: Option<uuid::Uuid>,
}

/// Only this owning read path can construct a checked request. The value passed
/// to a pure derivation is separate from its original immutable input event.
pub async fn analysis_request(
    conn: &mut PgConnection,
    event: EventId,
) -> Result<CheckedAnalysisRequest> {
    let payload = read(conn, event, AnalysisRequest::EVENT_TYPE).await?;
    let request = serde_json::from_value(payload.current)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(CheckedAnalysisRequest { event, request })
}

pub(super) async fn saved(conn: &mut PgConnection, event: EventId) -> Result<Saved> {
    serde_json::from_value(read(conn, event, Saved::EVENT_TYPE).await?.current)
        .map_err(|error| Error::Invalid(error.to_string()))
}

pub async fn context_pack(conn: &mut PgConnection, event: EventId) -> Result<Json> {
    Ok(read(conn, event, ContextPack::EVENT_TYPE).await?.current)
}

struct ReadProduct {
    original: Json,
    current: Json,
}
async fn read(conn: &mut PgConnection, event: EventId, expected: &str) -> Result<ReadProduct> {
    let captured:Captured=sqlx::query_as("SELECT e.event_type,e.kind_version,e.source,e.source_version,e.source_fingerprint,e.producer_family,e.material_id,e.payload FROM core.visible_events e WHERE e.id=$1 AND e.event_type=$2")
        .bind(event.0).bind(expected).fetch_optional(&mut *conn).await?
        .ok_or_else(||Error::Invalid("captured query product is unavailable".into()))?;
    let payload = captured
        .payload
        .clone()
        .filter(|payload| payload.is_object() && payload.get("withheld").is_none())
        .ok_or_else(|| {
            Error::Invalid("captured query product is unavailable to this reader".into())
        })?;
    let reference = KindRef {
        name: captured.event_type.clone(),
        version: u64::try_from(captured.kind_version)
            .map_err(|_| Error::Invalid("captured query kind version is invalid".into()))?,
    };
    let historical = sinex_model::query::historical::original_declaration(&reference);
    let declaration = match &historical {
        Some(declaration) => declaration.clone(),
        None => match expected {
            Saved::EVENT_TYPE if reference.version == Saved::VERSION => Saved::declaration(),
            AnalysisRequest::EVENT_TYPE if reference.version == AnalysisRequest::VERSION => {
                AnalysisRequest::declaration()
            }
            ContextPack::EVENT_TYPE if reference.version == ContextPack::VERSION => {
                ContextPack::declaration()
            }
            _ => {
                return Err(Error::Invalid(
                    "captured query product has an unknown owner format".into(),
                ));
            }
        },
    };
    check_kind(conn, &reference, &declaration).await?;
    if historical.is_some() {
        sinex_model::query::historical::validate_original_payload(&reference, &payload)
            .map_err(Error::Query)?;
    }
    // Current admitted payloads already have their typed representation. A
    // historical format is read only with its exact captured source authority.
    let legacy = historical.is_some() && expected != ContextPack::EVENT_TYPE
        || expected == ContextPack::EVENT_TYPE
            && payload
                .pointer("/analysis/selected")
                .is_some_and(|selection| {
                    selection.is_object() && selection.get("template").is_none()
                });
    if !legacy {
        return Ok(ReadProduct {
            original: payload.clone(),
            current: payload,
        });
    }
    if expected == ContextPack::EVENT_TYPE {
        let current = project_pack(conn, event, &captured, &declaration, &payload).await?;
        return Ok(ReadProduct {
            original: payload,
            current,
        });
    }
    check_source(conn, &captured, &declaration).await?;
    let mut ctx = Context::utc(jiff::Timestamp::UNIX_EPOCH);
    ctx.catalog = Some(sinex_model::query::QueryCatalog::new(
        sinex_schema::kinds::all(conn).await?.into_values(),
    ));
    let mut projected = payload.clone();
    sinex_model::query::historical::upgrade_payload(&mut projected, &ctx).map_err(Error::Query)?;
    Ok(ReadProduct {
        original: payload,
        current: projected,
    })
}

async fn check_kind(
    conn: &mut PgConnection,
    reference: &KindRef,
    expected: &KindDeclaration,
) -> Result<()> {
    let registered = sinex_schema::kinds::load(conn, std::slice::from_ref(reference)).await?;
    if registered
        .get(reference)
        .is_none_or(|declaration| declaration.definition_hash() != expected.definition_hash())
    {
        return Err(Error::Invalid(
            "captured query kind declaration differs from its exact owner format".into(),
        ));
    }
    Ok(())
}

async fn check_source(
    conn: &mut PgConnection,
    captured: &Captured,
    kind: &KindDeclaration,
) -> Result<()> {
    if captured
        .producer_family
        .as_deref()
        .is_some_and(|family| family != kind.declaring_producer)
    {
        return Err(Error::Invalid(
            "historical query belongs to a different producer family".into(),
        ));
    }
    if captured.source_version.is_none() && captured.source_fingerprint.is_none() {
        return check_material(conn, captured, kind).await;
    }
    let source = SourceRef {
        name: captured.source.clone(),
        version: u32::try_from(captured.source_version.ok_or_else(|| {
            Error::Invalid("historical query source authority is unavailable".into())
        })?)
        .map_err(|_| Error::Invalid("historical query source version is invalid".into()))?,
    };
    let declaration = crate::source_declarations::load(conn, &source)
        .await?
        .ok_or_else(|| {
            Error::Invalid("historical query source declaration is unavailable".into())
        })?;
    if captured.source_fingerprint.as_deref() != Some(declaration.fingerprint().as_str())
        || captured.producer_family.as_deref() != Some(kind.declaring_producer.as_str())
        || declaration.family != kind.declaring_producer
        || !declaration.allows(&kind.kind, &kind.definition_hash())
    {
        return Err(Error::Invalid(
            "captured query source contract differs from its retained owner declaration".into(),
        ));
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct MaterialContract {
    parser: Option<String>,
    parser_version: Option<i32>,
    status: String,
    retained: bool,
    manifest: Json,
}
async fn check_material(
    conn: &mut PgConnection,
    captured: &Captured,
    kind: &KindDeclaration,
) -> Result<()> {
    let format =
        sinex_model::query::historical::original_material_format(&kind.kind).ok_or_else(|| {
            Error::Invalid(
                "historical query has no captured native or material source proof".into(),
            )
        })?;
    let material = captured
        .material_id
        .ok_or_else(|| Error::Invalid("historical query source material is unavailable".into()))?;
    let contract:MaterialContract=sqlx::query_as("SELECT m.parser,m.parser_version,m.status,b.status='present' AS retained,m.manifest FROM raw.materials m JOIN core.blobs b ON b.hash=m.blob WHERE m.id=$1").bind(material).fetch_optional(conn).await?
        .ok_or_else(||Error::Invalid("historical query source material is unavailable".into()))?;
    if contract.parser.as_deref() != Some(format.parser)
        || contract.parser_version != Some(format.version)
        || contract.status == "tombstoned"
        || !contract.retained
        || contract.manifest.get("path").and_then(Json::as_str) != Some(format.path)
        || contract.manifest.get("media_type").and_then(Json::as_str) != Some(format.media_type)
        || contract
            .manifest
            .get("format_version")
            .is_some_and(|version| version.as_u64() != Some(1))
    {
        return Err(Error::Invalid(
            "historical query material does not prove its exact owner input format".into(),
        ));
    }
    let by = captured
        .payload
        .as_ref()
        .and_then(|payload| payload.get("by"))
        .and_then(Json::as_str)
        .filter(|by| !by.is_empty());
    if by.is_none() || contract.manifest.get("actor").and_then(Json::as_str) != by {
        return Err(Error::Invalid(
            "historical query material authorship differs from its admitted payload".into(),
        ));
    }
    Ok(())
}

async fn project_pack(
    conn: &mut PgConnection,
    event: EventId,
    captured: &Captured,
    kind: &KindDeclaration,
    original: &Json,
) -> Result<Json> {
    if captured.source_version.is_some() || captured.source_fingerprint.is_some() {
        check_source(conn, captured, kind).await?;
    } else if captured
        .producer_family
        .as_deref()
        .is_some_and(|family| family != kind.declaring_producer)
    {
        return Err(Error::Invalid(
            "historical context pack belongs to a different producer family".into(),
        ));
    }
    let parents:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT p.parent FROM core.event_parents_of($1) p JOIN core.visible_events e ON e.id=p.parent WHERE e.event_type='analysis.request' ORDER BY p.parent").bind(event.0).fetch_all(&mut *conn).await?;
    let selection = original.pointer("/analysis/selected").ok_or_else(|| {
        Error::Invalid("historical context pack has no retained selection".into())
    })?;
    let mut matched = None;
    for parent in parents {
        let request = Box::pin(read(conn, EventId(parent), AnalysisRequest::EVENT_TYPE)).await?;
        let request_id = request
            .original
            .get("request")
            .and_then(Json::as_str)
            .ok_or_else(|| Error::Invalid("context pack parent request has no identity".into()))?;
        if original.get("pack").and_then(Json::as_str)
            != Some(format!("query:{request_id}").as_str())
        {
            continue;
        }
        let parent_selection = request.original.get("selection").ok_or_else(|| {
            Error::Invalid("context pack parent has no original selected question".into())
        })?;
        let normalize = |value: &Json| {
            let mut value = value.clone();
            if let Some(selected) = value.get_mut("selected").and_then(Json::as_object_mut) {
                selected.remove("_fingerprint");
            }
            value
        };
        if normalize(selection) != normalize(parent_selection) {
            return Err(Error::Invalid(
                "historical context pack selection differs from its immutable request parent"
                    .into(),
            ));
        }
        let mut projected = request.current.get("selection").cloned().ok_or_else(|| {
            Error::Invalid("checked context pack parent has no selected question".into())
        })?;
        if let Some(fingerprint) = selection.pointer("/selected/_fingerprint") {
            projected["selected"]["_fingerprint"] = fingerprint.clone();
        }
        if matched.replace(projected).is_some() {
            return Err(Error::Invalid(
                "historical context pack has ambiguous request parents".into(),
            ));
        }
    }
    let projected = matched.ok_or_else(|| {
        Error::Invalid(
            "historical context pack has no authenticated immutable request parent".into(),
        )
    })?;
    let mut payload = original.clone();
    payload["analysis"]["selected"] = projected;
    Ok(payload)
}

#[cfg(test)]
mod tests;
