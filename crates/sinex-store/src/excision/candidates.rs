use crate::{Error, Result};
use sinex_model::MaterialId;
use sqlx::PgConnection;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyEnvelope {
    pub reference: String,
    pub wrapped: Vec<u8>,
    pub role: String,
    pub class: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub material: MaterialId,
    pub blob: String,
    pub length: u64,
    pub manifest: serde_json::Value,
    pub parser: String,
    pub parser_version: i32,
    pub host: String,
    pub sources: Vec<String>,
    pub key: Option<KeyEnvelope>,
}

/// Include unknown ownership so preparation refuses missing evidence rather
/// than silently excluding a potentially related retained body.
pub async fn candidate_materials(
    conn: &mut PgConnection,
    source: &str,
    parser: &str,
) -> Result<Vec<Candidate>> {
    let rows = sqlx::query!(
        r#"WITH candidates AS (
             SELECT m.id,m.blob,m.durable_bytes,m.manifest,m.parser,m.parser_version,i.host,
               k.key_ref,k.wrapped_key,k.role,k.class,
               ARRAY(SELECT DISTINCT s FROM (
                 SELECT e.source AS s FROM core.events e WHERE e.material_id=m.id
                 UNION SELECT b.source FROM raw.bindings b WHERE b.binding=i.binding
               ) owned ORDER BY s) AS sources
             FROM raw.materials m LEFT JOIN raw.inputs i ON i.id=m.input_id
             LEFT JOIN privacy.material_keys k ON k.material_id=m.id WHERE m.parser=$2)
           SELECT id,blob,durable_bytes,manifest,parser,parser_version,host,key_ref,wrapped_key,role,class,
             sources AS "sources!" FROM candidates WHERE cardinality(sources)=0 OR $1=ANY(sources) ORDER BY id"#,
        source,parser
    ).fetch_all(conn).await?;
    rows.into_iter()
        .map(|row| {
            let key = match (row.key_ref, row.wrapped_key, row.role, row.class) {
                (Some(reference), Some(wrapped), Some(role), Some(class)) => Some(KeyEnvelope {
                    reference,
                    wrapped,
                    role,
                    class,
                }),
                (None, None, None, None) => None,
                _ => {
                    return Err(Error::Invalid(
                        "material key ownership is incomplete".into(),
                    ));
                }
            };
            Ok(Candidate {
                material: MaterialId(row.id),
                blob: row.blob,
                length: u64::try_from(row.durable_bytes)
                    .map_err(|_| Error::Invalid("negative material length".into()))?,
                manifest: row.manifest,
                parser: row.parser,
                parser_version: row.parser_version,
                host: row.host.unwrap_or_default(),
                sources: row.sources,
                key,
            })
        })
        .collect()
}
