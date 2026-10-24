//! A lock per source, shared by acquisitions and held alone by an excision.
use crate::Result;
use sqlx::PgConnection;

pub async fn lock_source(conn: &mut PgConnection, source: &str, shared: bool) -> Result<()> {
    let lock = source_lock(source);
    if shared {
        sqlx::query("SELECT pg_advisory_xact_lock_shared($1)")
            .bind(lock)
            .execute(conn)
            .await?;
    } else {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock)
            .execute(conn)
            .await?;
    }
    Ok(())
}

fn source_lock(source: &str) -> i64 {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinex source acquisition 1\0");
    hash.update(source.as_bytes());
    i64::from_le_bytes(
        hash.finalize().as_bytes()[..8]
            .try_into()
            .expect("eight bytes"),
    )
}

/// Only for an owned connection that is closed at the acquisition boundary.
/// Session ownership permits a custody reservation transaction to commit while
/// this same source remains protected through the queued first admission.
pub async fn lock_source_session(
    conn: &mut PgConnection,
    source: &str,
    shared: bool,
) -> Result<()> {
    sqlx::query(if shared {
        "SELECT pg_advisory_lock_shared($1)"
    } else {
        "SELECT pg_advisory_lock($1)"
    })
    .bind(source_lock(source))
    .execute(conn)
    .await?;
    Ok(())
}

/// Whether `source` names an outside producer.
pub async fn is_producer(conn: &mut PgConnection, source: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT EXISTS(SELECT 1 FROM core.manifests WHERE name=$1 AND kind='producer')
             OR EXISTS(SELECT 1 FROM core.ingress_frames WHERE producer=$1)
             OR EXISTS(SELECT 1 FROM raw.pending_producer_bytes WHERE producer=$1) AS "present!""#,
        source
    )
    .fetch_one(conn)
    .await?)
}
