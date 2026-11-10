//! Checked storage reads for authenticated producer orchestration.
use crate::Result;
use sinex_model::InputRef;
use sqlx::PgConnection;
use uuid::Uuid;

pub async fn material_owners(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<Vec<(Option<Uuid>, Option<String>)>> {
    Ok(sqlx::query!("SELECT material_id,family FROM core.ingress_frames WHERE material_id=ANY($1) AND refusal IS NULL",ids).fetch_all(conn).await?.into_iter().map(|row|(row.material_id,row.family)).collect())
}
pub async fn family_unknown(
    conn: &mut PgConnection,
    producer: &str,
    ids: &[Uuid],
    family: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar!(r#"SELECT EXISTS(SELECT 1 FROM core.ingress_frames WHERE producer=$1 AND material_id=ANY($2) AND family IS DISTINCT FROM $3) AS "unknown!""#,producer,ids,family).fetch_one(conn).await?)
}
/// Each held material's length and descriptors, one per extent in order.
pub async fn material_extents(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<Vec<(Uuid, i64, Vec<Vec<u8>>)>> {
    Ok(sqlx::query!(r#"SELECT m.id,m.length,core.blob_descriptors(m.blob) AS "descriptors!" FROM raw.materials m WHERE m.id=ANY($1) AND core.blob_descriptors(m.blob) IS NOT NULL"#,ids).fetch_all(conn).await?.into_iter().map(|row|(row.id,row.length,row.descriptors)).collect())
}
pub async fn material_family(
    conn: &mut PgConnection,
    id: Uuid,
) -> Result<Option<(Option<String>, Option<Vec<String>>)>> {
    Ok(sqlx::query!("SELECT family,write_kind_prefixes FROM core.ingress_frames WHERE material_id=$1 AND refusal IS NULL ORDER BY number LIMIT 1",id).fetch_optional(conn).await?.map(|row|(row.family,row.write_kind_prefixes)))
}
pub struct MaterialOutput {
    pub kind: String,
    pub class: String,
}
pub async fn event_output(conn: &mut PgConnection, id: Uuid) -> Result<Option<MaterialOutput>> {
    Ok(sqlx::query_as!(
        MaterialOutput,
        "SELECT e.event_type AS kind,e.class FROM core.events e WHERE e.id=$1",
        id
    )
    .fetch_optional(conn)
    .await?)
}
pub async fn material_classes(conn: &mut PgConnection, id: Uuid) -> Result<Vec<MaterialOutput>> {
    Ok(sqlx::query_as!(
        MaterialOutput,
        "SELECT DISTINCT e.event_type AS kind,e.class FROM core.events e WHERE e.material_id=$1",
        id
    )
    .fetch_all(conn)
    .await?)
}
pub async fn material_input(conn: &mut PgConnection, id: Uuid) -> Result<Option<InputRef>> {
    Ok(sqlx::query!("SELECT i.kind,i.locator,i.host FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE m.id=$1",id).fetch_optional(conn).await?.map(|row|InputRef{kind:row.kind,locator:row.locator,host:row.host}))
}
/// The read authority of held bytes: one descriptor per extent, in order.
pub async fn descriptors(conn: &mut PgConnection, hash: &str) -> Result<Option<Vec<Vec<u8>>>> {
    Ok(
        sqlx::query_scalar!("SELECT core.blob_descriptors($1) AS descriptors", hash)
            .fetch_one(conn)
            .await?,
    )
}

/// Source authority captured with the original material receipt, never inferred
/// from a current parser or a producer-controlled input label.
pub async fn material_sources(
    conn: &mut PgConnection,
    ids: &[uuid::Uuid],
) -> crate::Result<Vec<(uuid::Uuid, Option<String>, Option<i64>)>> {
    Ok(sqlx::query_as("SELECT material_id,source_name,source_version FROM core.ingress_frames WHERE material_id=ANY($1)")
        .bind(ids).fetch_all(conn).await?)
}

pub async fn event_source(
    conn: &mut PgConnection,
    id: Uuid,
) -> Result<
    Option<(
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
    )>,
> {
    Ok(sqlx::query_as("SELECT source,source_version,source_fingerprint,producer_family,producer_identity FROM core.events WHERE id=$1").bind(id).fetch_optional(conn).await?)
}

pub async fn materials_registered_by(
    conn: &mut PgConnection,
    producer: &str,
    ids: &[Uuid],
) -> Result<Vec<Uuid>> {
    Ok(sqlx::query_scalar("SELECT material_id FROM core.ingress_frames WHERE producer=$1 AND material_id=ANY($2) AND refusal IS NULL")
        .bind(producer).bind(ids).fetch_all(conn).await?)
}
