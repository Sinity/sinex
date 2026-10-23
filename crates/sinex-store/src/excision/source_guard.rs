//! Direct source-namespace serialization for ingress custody and sealed excision.
use crate::Result;
use sqlx::PgConnection;

pub async fn lock_source(conn: &mut PgConnection, source: &str, shared: bool) -> Result<()> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinex source acquisition 1\0");
    hash.update(source.as_bytes());
    let lock = i64::from_le_bytes(
        hash.finalize().as_bytes()[..8]
            .try_into()
            .expect("eight bytes"),
    );
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

pub async fn unsupported_ingress(conn: &mut PgConnection, source: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT EXISTS(SELECT 1 FROM core.manifests WHERE name=$1 AND kind='producer')
             OR EXISTS(SELECT 1 FROM core.ingress_frames WHERE producer=$1) AS "present!""#,
        source
    )
    .fetch_one(conn)
    .await?)
}
