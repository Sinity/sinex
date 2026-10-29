use super::*;
use crate::commit_log::{self,Committed};
use serde_json::Value;

pub async fn apply(conn:&mut PgConnection,events:&[Committed])->Result<()> {
    let entries:Vec<_>=events.iter().filter(|event|event.event_type=="text.embedding").collect();
    if entries.is_empty(){return Ok(())}
    sqlx::query("LOCK TABLE core.embedding_vectors IN SHARE ROW EXCLUSIVE MODE").execute(&mut *conn).await?;
    for event in entries {apply_one(conn,event.id,&event.payload,&event.class).await?;}
    Ok(())
}
async fn apply_one(conn:&mut PgConnection,id:Uuid,payload:&Value,class:&str)->Result<()> {
    let parsed=if class=="sealed"{None}else{serde_json::from_value::<TextEmbedding>(payload.clone()).ok()};
    let (target,space,dimensions,projected,coverage,reason)=if let Some(embedding)=parsed {
        match embedding.validate(){
            Ok(())=>{
                let space=embedding.space.id();let projection=project(&embedding.vector);
                let dimensions=i64::try_from(embedding.space.dimensions).ok();
                if let Some(dimensions)=dimensions {
                    sqlx::query!("INSERT INTO core.embedding_spaces(space,provider,model,revision,settings_hash,dimensions) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
                        space,embedding.space.provider,embedding.space.model,embedding.space.revision,embedding.space.settings_hash,dimensions).execute(&mut *conn).await?;
                }
                if projection.coverage=="indexed"{ensure_index(conn,&space,embedding.vector.len()).await?;}
                let text=projection.values.as_ref().map(|values|format!("[{}]",values.iter().map(f32::to_string).collect::<Vec<_>>().join(",")));
                (Some(embedding.target.0),Some(space),dimensions,text,projection.coverage,projection.reason)
            },
            Err(_)=>(Some(embedding.target.0),None,None,None,"unavailable",Some("invalid_stored_embedding")),
        }
    }else{(None,None,None,None,"unavailable",Some(if class=="sealed"{"sealed_vector_not_indexed"}else{"invalid_stored_embedding"}))};
    // Excision takes the same table lock and deletes before deleting events.
    // Rechecking the event prevents an already-read page resurrecting its vector.
    sqlx::query!(r#"INSERT INTO core.embedding_vectors(embedding_id,target_id,space,dimensions,projected,coverage,reason)
        SELECT e.id,$2,$3,$4,$5::text::vector,$6,$7 FROM core.events e WHERE e.id=$1
        ON CONFLICT(embedding_id) DO UPDATE SET target_id=EXCLUDED.target_id,space=EXCLUDED.space,
        dimensions=EXCLUDED.dimensions,projected=EXCLUDED.projected,coverage=EXCLUDED.coverage,reason=EXCLUDED.reason"#,
        id,target,space,dimensions,projected,coverage,reason).execute(conn).await?;
    Ok(())
}
async fn ensure_index(conn:&mut PgConnection,space:&str,dimensions:usize)->Result<()> {
    // Both interpolations are internal: a BLAKE3 hex digest and a checked
    // dimension supported by the extension, never producer-supplied SQL.
    assert!(space.len()==64&&space.bytes().all(|value|value.is_ascii_hexdigit()));
    assert!((1..=INDEX_DIMENSIONS).contains(&dimensions));
    use base64::Engine;
    let digest=blake3::Hash::from_hex(space).expect("space is a digest");
    let name=base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.as_bytes());
    let statement=format!(r#"CREATE INDEX IF NOT EXISTS "embedding_space_{name}" ON core.embedding_vectors USING hnsw ((projected::vector({dimensions})) vector_cosine_ops) WHERE space='{space}' AND coverage='indexed'"#);
    sqlx::query(&statement).execute(conn).await?;
    Ok(())
}

/// New consumers and explicit rebuilds read retained events, so pruned commit
/// log rows cannot skip historical embeddings. Each transfer page is bounded;
/// the projection and its scalar checkpoint become visible together.
pub async fn rebuild(conn:&mut PgConnection)->Result<()> {
    let mut tx=conn.begin().await?;
    sqlx::query("LOCK TABLE core.embedding_vectors IN SHARE ROW EXCLUSIVE MODE").execute(&mut *tx).await?;
    let head=sqlx::query_scalar!("SELECT value FROM core.counters WHERE name='commit_seq'").fetch_one(&mut *tx).await?;
    sqlx::query("DELETE FROM core.embedding_vectors").execute(&mut *tx).await?;
    sqlx::query("DELETE FROM core.embedding_spaces").execute(&mut *tx).await?;
    let mut after=0;
    loop {
        let rows=sqlx::query!(r#"WITH selected AS MATERIALIZED (
            SELECT id,commit_seq,payload,class,octet_length(payload::text)::bigint AS size FROM core.events
            WHERE event_type='text.embedding' AND commit_seq>$1 AND commit_seq<=$2 ORDER BY commit_seq LIMIT 1000
          ), bounded AS(SELECT s.*,sum(size) OVER(ORDER BY commit_seq)-size AS before FROM selected s)
          SELECT id AS "id!",commit_seq AS "commit_seq!",payload AS "payload!",class AS "class!" FROM bounded WHERE before<$3::bigint ORDER BY commit_seq"#,
          after,head,crate::PAGE_BYTES).fetch_all(&mut *tx).await?;
        if rows.is_empty(){break}
        for row in rows {apply_one(&mut tx,row.id,&row.payload,&row.class).await?;after=row.commit_seq;}
    }
    commit_log::set_consumer_position(&mut tx,CONSUMER,head,0).await?;
    tx.commit().await?;Ok(())
}
pub async fn initialize(conn:&mut PgConnection)->Result<()> {
    if !commit_log::consumer_exists(conn,CONSUMER).await?{rebuild(conn).await?}
    Ok(())
}
