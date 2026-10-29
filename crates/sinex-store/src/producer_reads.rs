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
pub async fn material_extents(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<Vec<(Uuid, i64, Vec<u8>)>> {
    Ok(sqlx::query!("SELECT m.id,m.length,x.descriptor FROM raw.materials m JOIN core.blob_extents x ON x.hash=m.blob AND NOT x.deleting WHERE m.id=ANY($1)",ids).fetch_all(conn).await?.into_iter().map(|row|(row.id,row.length,row.descriptor)).collect())
}
pub async fn material_family(
    conn: &mut PgConnection,
    id: Uuid,
) -> Result<Option<(Option<String>, Option<Vec<String>>)>> {
    Ok(sqlx::query!("SELECT family,write_kind_prefixes FROM core.ingress_frames WHERE material_id=$1 AND refusal IS NULL ORDER BY number LIMIT 1",id).fetch_optional(conn).await?.map(|row|(row.family,row.write_kind_prefixes)))
}
pub struct MaterialOutput {
    pub kind: String,
    pub class: Option<String>,
    pub epoch: Option<i64>,
    pub changed: Option<bool>,
    pub state: Option<String>,
}
pub async fn event_output(conn: &mut PgConnection, id: Uuid) -> Result<Option<MaterialOutput>> {
    Ok(sqlx::query_as!(MaterialOutput,r#"SELECT e.event_type AS "kind!",o.class AS "class?",o.epoch AS "epoch?",o.changed AS "changed?",o.state AS "state?" FROM core.events e LEFT JOIN privacy.event_outputs o ON o.event_id=e.id WHERE e.id=$1"#,id).fetch_optional(conn).await?)
}
pub async fn material_classes(conn: &mut PgConnection, id: Uuid) -> Result<Vec<MaterialOutput>> {
    Ok(sqlx::query_as!(MaterialOutput,r#"SELECT DISTINCT e.event_type AS "kind!",o.class AS "class?",o.epoch AS "epoch?",o.changed AS "changed?",o.state AS "state?" FROM core.events e LEFT JOIN privacy.event_outputs o ON o.event_id=e.id WHERE e.material_id=$1"#,id).fetch_all(conn).await?)
}
pub async fn material_input(conn: &mut PgConnection, id: Uuid) -> Result<Option<InputRef>> {
    Ok(sqlx::query!("SELECT i.kind,i.locator,i.host FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE m.id=$1",id).fetch_optional(conn).await?.map(|row|InputRef{kind:row.kind,locator:row.locator,host:row.host}))
}
pub async fn extent(conn: &mut PgConnection, hash: &str) -> Result<Option<Vec<u8>>> {
    Ok(sqlx::query_scalar!(
        "SELECT descriptor FROM core.blob_extents WHERE hash=$1 AND NOT deleting",
        hash
    )
    .fetch_optional(conn)
    .await?)
}
