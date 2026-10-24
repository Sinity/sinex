//! Reclaim superseded material references without deleting their descriptors.
//! The application supplies validated ownership/selection into transaction-local
//! scratch tables. This module knows material identities, never source names.
use crate::{Blobs, Error, Result};
use serde_json::Value;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

pub async fn begin(conn: &mut PgConnection) -> Result<()> {
    for statement in [
        "DROP TABLE IF EXISTS retirement_units,retirement_current,retirement_candidates,retirement_covered,retirement_hashes,retirement_references",
        "CREATE TEMP TABLE retirement_units(identity text PRIMARY KEY, position bigint UNIQUE, rank bigint, subrank text COLLATE \"C\")",
        "CREATE TEMP TABLE retirement_current(identity text PRIMARY KEY, generation bigint, owner text, hash text, refs jsonb)",
        "CREATE TEMP TABLE retirement_candidates(identity text, hash text, material uuid, event uuid, PRIMARY KEY(identity,hash,material))",
        "CREATE TEMP TABLE retirement_covered(identity text PRIMARY KEY)",
        "CREATE TEMP TABLE retirement_hashes(hash text PRIMARY KEY)",
        "CREATE TEMP TABLE retirement_references(namespace text,identity text,generation bigint,owner text,hash text,refs jsonb,PRIMARY KEY(namespace,identity))",
    ] {
        sqlx::query(statement).execute(&mut *conn).await?;
    }
    Ok(())
}

/// Page immutable admitted descriptors, including archived readings. A producer
/// cannot claim another producer's event merely by citing its material.
pub async fn descriptors(
    conn: &mut PgConnection,
    producer: &str,
    kind: &str,
    field: &str,
    value: &str,
    after: i64,
) -> Result<Vec<(i64, Uuid, Option<Uuid>, Value)>> {
    Ok(sqlx::query("SELECT c.commit_seq,e.id,e.material_id,e.payload FROM core.commit_log c JOIN core.events e ON e.id=c.event_id WHERE c.commit_seq>$1 AND e.source=$2 AND e.event_type=$3 AND e.payload->>$4=$5 AND EXISTS(SELECT 1 FROM core.ingress_frames f WHERE f.producer=$2 AND f.refusal IS NULL AND e.id=ANY(f.admitted)) ORDER BY c.commit_seq LIMIT 500")
        .bind(after).bind(producer).bind(kind).bind(field).bind(value).fetch_all(conn).await?.into_iter().map(|r|(r.get(0),r.get(1),r.get(2),r.get(3))).collect())
}

/// Page producer-authenticated descriptors across namespaces. Shared content
/// cannot be reclaimed using only one archive's current reference set.
pub async fn all_descriptors(
    conn: &mut PgConnection,
    kind: &str,
    after: i64,
) -> Result<Vec<(i64, String, Value)>> {
    Ok(sqlx::query("SELECT c.commit_seq,e.source,e.payload FROM core.commit_log c JOIN core.events e ON e.id=c.event_id WHERE c.commit_seq>$1 AND e.event_type=$2 AND EXISTS(SELECT 1 FROM core.ingress_frames f WHERE f.producer=e.source AND f.refusal IS NULL AND e.id=ANY(f.admitted)) ORDER BY c.commit_seq LIMIT 500")
        .bind(after).bind(kind).fetch_all(conn).await?.into_iter().map(|r|(r.get(0),r.get(1),r.get(2))).collect())
}

/// Keep the latest references per immutable identity within its namespace.
pub async fn protect(
    conn: &mut PgConnection,
    namespace: &str,
    identity: &str,
    generation: i64,
    owner: &str,
    hash: Option<&str>,
    refs: &[String],
) -> Result<()> {
    sqlx::query("INSERT INTO retirement_references VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(namespace,identity) DO UPDATE SET generation=excluded.generation,owner=excluded.owner,hash=excluded.hash,refs=excluded.refs WHERE (excluded.generation,excluded.owner COLLATE \"C\")>(retirement_references.generation,retirement_references.owner COLLATE \"C\")")
        .bind(namespace).bind(identity).bind(generation).bind(owner).bind(hash).bind(serde_json::json!(refs)).execute(conn).await?;
    Ok(())
}

pub async fn original(
    conn: &mut PgConnection,
    identity: &str,
    position: i64,
    rank: i64,
    subrank: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO retirement_units VALUES($1,$2,$3,$4)")
        .bind(identity)
        .bind(position)
        .bind(rank)
        .bind(subrank)
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn select(
    conn: &mut PgConnection,
    identity: &str,
    generation: i64,
    owner: &str,
    hash: Option<&str>,
    refs: &[String],
    replacement: bool,
) -> Result<()> {
    if replacement {
        sqlx::query("INSERT INTO retirement_covered VALUES($1) ON CONFLICT DO NOTHING")
            .bind(identity)
            .execute(&mut *conn)
            .await?;
    }
    sqlx::query("INSERT INTO retirement_current VALUES($1,$2,$3,$4,$5) ON CONFLICT(identity) DO UPDATE SET generation=excluded.generation,owner=excluded.owner,hash=excluded.hash,refs=excluded.refs WHERE (excluded.generation,excluded.owner COLLATE \"C\")>(retirement_current.generation,retirement_current.owner COLLATE \"C\")")
        .bind(identity).bind(generation).bind(owner).bind(hash).bind(serde_json::json!(refs)).execute(conn).await?;
    Ok(())
}

/// Resolve a registered material by its producer frame, rather than trusting
/// an incoming content hash as authority. Unpublished historical refs are absent.
pub async fn candidate(
    conn: &mut PgConnection,
    producer: &str,
    identity: &str,
    frame: &str,
    hash: &str,
    event: Option<Uuid>,
    required: bool,
) -> Result<()> {
    let material: Option<Uuid> = sqlx::query_scalar("SELECT f.material_id FROM core.ingress_frames f JOIN raw.materials m ON m.id=f.material_id JOIN raw.inputs i ON i.id=m.input_id WHERE f.producer=$1 AND f.frame=$2 AND f.refusal IS NULL AND i.binding='ingress:'||$1 AND i.locator=$2")
        .bind(producer).bind(frame).fetch_optional(&mut *conn).await?;
    if let Some(material) = material {
        if let Some(event) = event {
            let owns: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM core.events WHERE id=$1 AND material_id=$2)",
            )
            .bind(event)
            .bind(material)
            .fetch_one(&mut *conn)
            .await?;
            if !owns {
                return Err(Error::Invalid(
                    "descriptor does not cite its registered batch material".into(),
                ));
            }
        }
        sqlx::query("INSERT INTO retirement_candidates VALUES($1,$2,$3,$4) ON CONFLICT(identity,hash,material) DO UPDATE SET event=COALESCE(retirement_candidates.event,excluded.event)").bind(identity).bind(hash).bind(material).bind(event).execute(conn).await?;
    } else if required {
        return Err(Error::Invalid(
            "selected batch material was not confirmed by this producer".into(),
        ));
    }
    Ok(())
}

/// Current backing of a material named by its producer-owned input locator.
/// Other naming conventions remain ordinary historical frame acknowledgments.
pub struct Backing {
    pub frame: String,
    pub hash: String,
    pub present: bool,
    pub kind: String,
    pub parser: String,
    pub parser_version: i32,
}

pub async fn backings(
    conn: &mut PgConnection,
    producer: &str,
    frames: &[String],
) -> Result<Vec<Backing>> {
    if frames.is_empty() {
        return Ok(vec![]);
    }
    Ok(sqlx::query("SELECT f.frame,m.blob,b.status='present',i.kind,m.parser,m.parser_version FROM core.ingress_frames f JOIN raw.materials m ON m.id=f.material_id JOIN raw.inputs i ON i.id=m.input_id JOIN core.blobs b ON b.hash=m.blob WHERE f.producer=$1 AND f.frame=ANY($2) AND f.refusal IS NULL AND i.binding='ingress:'||$1 AND i.locator=f.frame")
        .bind(producer).bind(frames).fetch_all(conn).await?.into_iter().map(|r|Backing { frame:r.get(0),hash:r.get(1),present:r.get(2),kind:r.get(3),parser:r.get(4),parser_version:r.get(5) }).collect())
}

pub async fn validate(conn: &mut PgConnection) -> Result<()> {
    let (count, max): (i64, i64) =
        sqlx::query_as("SELECT count(*),COALESCE(max(position),0) FROM retirement_units")
            .fetch_one(&mut *conn)
            .await?;
    let unknown:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM (SELECT position,row_number() OVER(ORDER BY rank,subrank) AS expected FROM retirement_units) u WHERE position<>expected) OR EXISTS(SELECT 1 FROM retirement_covered c WHERE NOT EXISTS(SELECT 1 FROM retirement_units u WHERE u.identity=c.identity))").fetch_one(conn).await?;
    if count != max || unknown {
        return Err(Error::Invalid(
            "retirement needs a complete confirmed unit prefix and replacement coverage".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct Applied {
    pub retired: u64,
    pub retained: u64,
}

/// The writer holds its transaction and is the only source of persistent
/// mutations. Same-revision aliases are retained, before touching any extents.
pub async fn apply(conn: &mut PgConnection, blobs: &Blobs) -> Result<Applied> {
    sqlx::query("INSERT INTO retirement_hashes SELECT DISTINCT c.hash FROM retirement_candidates c JOIN retirement_current s ON s.identity=c.identity WHERE c.hash IS DISTINCT FROM s.hash").execute(&mut *conn).await?;
    // Verify before unlinking unreferenced old bytes, including pending retries.
    // Completed requests need no repeated scan of the live batch contents.
    let mut verified: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM retirement_hashes h JOIN core.blobs b ON b.hash=h.hash
             WHERE NOT EXISTS(SELECT 1 FROM raw.materials m WHERE m.blob=b.hash OR m.erased_replay_blob=b.hash)
               AND NOT EXISTS(SELECT 1 FROM raw.revision_extents x WHERE x.blob=b.hash)
               AND NOT EXISTS(SELECT 1 FROM core.event_blobs e WHERE e.blob=b.hash)
         )",
    )
    .fetch_one(&mut *conn)
    .await?;
    if verified {
        verify_selected(conn, blobs).await?;
    }
    let mut done = Applied::default();
    let mut after = Uuid::nil();
    loop {
        let rows=sqlx::query("SELECT DISTINCT c.material,c.hash,m.blob,m.durable_bytes,m.revision_id FROM retirement_candidates c JOIN retirement_hashes h ON h.hash=c.hash JOIN raw.materials m ON m.id=c.material WHERE c.material>$1 AND NOT EXISTS(SELECT 1 FROM retirement_current s WHERE s.hash=c.hash OR s.refs ? c.hash) AND NOT EXISTS(SELECT 1 FROM retirement_references s WHERE s.hash=c.hash OR s.refs ? c.hash) ORDER BY c.material LIMIT 500").bind(after).fetch_all(&mut *conn).await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let material: Uuid = row.get(0);
            after = material;
            let old: String = row.get(1);
            let current: String = row.get(2);
            let length: i64 = row.get(3);
            let revision: Option<Uuid> = row.get(4);
            // A retry must not recreate filler or overwrite newer backing.
            // The old hash still participates in the reference checks below.
            if current != old {
                continue;
            }
            let shared:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.materials WHERE revision_id=$1 AND id<>$2) OR EXISTS(SELECT 1 FROM core.events e WHERE material_id=$2 AND NOT EXISTS(SELECT 1 FROM retirement_candidates c WHERE c.event=e.id)) OR EXISTS(SELECT 1 FROM privacy.material_keys WHERE material_id=$2) OR EXISTS(SELECT 1 FROM raw.materials WHERE previous_part_id=$2)").bind(revision).bind(material).fetch_one(&mut *conn).await?;
            if shared {
                done.retained += 1;
                continue;
            }
            if !verified {
                verify_selected(conn, blobs).await?;
                verified = true;
            }
            let blank =
                blobs.put_reader(std::io::Read::take(std::io::repeat(b' '), length as u64))?;
            sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present') ON CONFLICT(hash) DO UPDATE SET status='present' WHERE core.blobs.status IN ('reserved','deleting')").bind(&blank).bind(length).execute(&mut *conn).await?;
            sqlx::query("UPDATE raw.revision_extents SET blob=$2 WHERE revision_id=$1 AND blob=$3")
                .bind(revision)
                .bind(&blank)
                .bind(&old)
                .execute(&mut *conn)
                .await?;
            sqlx::query("UPDATE raw.materials SET blob=$2 WHERE id=$1")
                .bind(material)
                .bind(&blank)
                .execute(&mut *conn)
                .await?;
            done.retired += 1;
        }
    }
    sqlx::query("UPDATE core.blobs b SET status='deleting' FROM retirement_hashes h WHERE b.hash=h.hash AND NOT EXISTS(SELECT 1 FROM raw.materials m WHERE m.blob=b.hash OR m.erased_replay_blob=b.hash) AND NOT EXISTS(SELECT 1 FROM raw.revision_extents x WHERE x.blob=b.hash) AND NOT EXISTS(SELECT 1 FROM core.event_blobs e WHERE e.blob=b.hash) AND NOT EXISTS(SELECT 1 FROM raw.pending_native_blobs p WHERE p.blob=b.hash) AND NOT EXISTS(SELECT 1 FROM raw.pending_producer_blobs p WHERE p.blob=b.hash)").execute(conn).await?;
    Ok(done)
}

/// Reconstruct the exact requested hash set on every retry, including after a
/// restart. Cleanup itself rechecks all live references and deleting status.
pub async fn pending(conn: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM retirement_hashes h JOIN core.blobs b ON b.hash=h.hash WHERE b.status='deleting'").fetch_one(conn).await?)
}
pub async fn retained(conn: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM retirement_hashes h JOIN core.blobs b ON b.hash=h.hash WHERE b.status<>'deleting'").fetch_one(conn).await?)
}
pub async fn record(
    conn: &mut PgConnection,
    operation: Uuid,
    actor: &str,
    scope: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'retire_materials',$2,$3) ON CONFLICT(id) DO NOTHING").bind(operation).bind(actor).bind(scope).execute(conn).await?;
    Ok(())
}
async fn verify_selected(conn: &mut PgConnection, blobs: &Blobs) -> Result<()> {
    let mut after = String::new();
    loop {
        let names: Vec<(String, bool)> = sqlx::query_as(
            "SELECT hash, bool_or(batch)
             FROM (
                 SELECT hash, true AS batch FROM retirement_current WHERE hash IS NOT NULL
                 UNION ALL
                 SELECT jsonb_array_elements_text(refs), false FROM retirement_current
             ) r
             WHERE hash > $1 GROUP BY hash ORDER BY hash LIMIT 500",
        )
        .bind(&after)
        .fetch_all(&mut *conn)
        .await?;
        if names.is_empty() {
            break;
        }
        for (name, batch) in names {
            if batch {
                // These bytes replace backing we are about to remove. A
                // registered pathname alone does not prove they survived.
                if blobs.verify(&name)? != hashed_blobs::Verified::Intact {
                    return Err(Error::Invalid(format!(
                        "selected row batch {name} failed content verification"
                    )));
                }
            } else if !blobs.contains(&name)? {
                return Err(Error::Invalid(
                    "selected source bytes are unavailable".into(),
                ));
            }
            after = name;
        }
    }
    let missing:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM retirement_current s WHERE s.hash IS NOT NULL AND NOT EXISTS(SELECT 1 FROM retirement_candidates c WHERE c.hash=s.hash))").fetch_one(conn).await?;
    if missing {
        return Err(Error::Invalid("selected batch registration missing".into()));
    }
    Ok(())
}
