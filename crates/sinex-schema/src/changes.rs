//! Ordered, explicit archive upgrades. Old declarations were independently
//! compared with the version-0 native schema binary, including views, trigger
//! state and hypertable settings. Unknown shapes are refused before mutation.
use crate::{Applied, CHANGES, Error, VERSION, WRITER_LOCK, catalog, grants, tables};
use sqlx::{Connection, Executor, PgConnection};

pub(super) async fn apply(conn: &mut PgConnection) -> Result<Applied, Error> {
    let mut tx = conn.begin().await?;
    // This API returns the borrowed connection to its caller. Transaction Drop
    // only queues rollback; await the end even for Current and refused changes
    // so an idle caller cannot retain schema locks against the next writer.
    match apply_locked(&mut tx).await {
        Ok(applied) => {
            tx.commit().await?;
            Ok(applied)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn apply_locked(conn: &mut PgConnection) -> Result<Applied, Error> {
    let available: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(WRITER_LOCK)
        .fetch_one(&mut *conn)
        .await?;
    if !available {
        return Err(Error::ActiveWriter);
    }
    if let Err(error) = conn
        .execute("LOCK TABLE sinex_schema.applied IN EXCLUSIVE MODE NOWAIT")
        .await
    {
        if error
            .as_database_error()
            .is_some_and(|error| error.code().as_deref() == Some("55P03"))
        {
            return Err(Error::ActiveWriter);
        }
        return Err(error.into());
    }
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM sinex_schema.applied ORDER BY version")
            .fetch_all(&mut *conn)
            .await?;
    let found = versions.last().copied().ok_or(Error::Unversioned)?;
    if versions != (0..=found).collect::<Vec<_>>() || found > VERSION {
        return Err(Error::Upgrade {
            found,
            declared: VERSION,
        });
    }
    if found == VERSION {
        return Ok(Applied::Current { version: VERSION });
    }
    for change in CHANGES.iter().filter(|change| change.version > found) {
        match change.version {
            1 => archival_history(conn).await?,
            _ => {
                return Err(Error::Upgrade {
                    found,
                    declared: VERSION,
                });
            }
        }
        sqlx::query("INSERT INTO sinex_schema.applied(version) VALUES($1)")
            .bind(change.version)
            .execute(&mut *conn)
            .await?;
    }
    Ok(Applied::Upgraded {
        from: found,
        version: VERSION,
    })
}

async fn archival_history(conn: &mut PgConnection) -> Result<(), Error> {
    // BLAKE3 of JSON-encoded sorted catalog rows from the independently
    // checked version-0 declarations (tests/v0-declarations.json). This checks
    // exact shape without embedding a second copy of the old declarations.
    const VERSION_ZERO_SHAPE: &str =
        "7beeef675953bb844406dd4df877a4673f2115da2161d32e68704235ee6a94c6";
    let shape = serde_json::to_vec(&catalog(conn).await?).expect("catalog strings serialize");
    if blake3::hash(&shape).to_hex().as_str() != VERSION_ZERO_SHAPE {
        return Err(Error::Shape(0));
    }
    // Retain old rows, sequence ownership and original archival metadata.
    conn.execute("ALTER TABLE core.commit_log ALTER COLUMN event_id DROP NOT NULL, ALTER COLUMN event_type DROP NOT NULL, ADD COLUMN persisted_at timestamptz DEFAULT now(), ADD CHECK ((event_id IS NULL)=(event_type IS NULL))").await?;
    conn.execute("UPDATE core.commit_log c SET persisted_at=e.ts_persisted FROM core.events e WHERE e.id=c.event_id").await?;
    conn.execute("UPDATE core.commit_log c SET persisted_at=NULL WHERE NOT EXISTS(SELECT 1 FROM core.events e WHERE e.id=c.event_id)").await?;
    for statement in [
        tables::archive_history_table(),
        tables::archive_history_index(),
        tables::archive_history_function(),
    ] {
        conn.execute(statement).await?;
    }
    // The old projection cannot reveal erased unhide transitions. Its state is
    // known from this cutover, not proof of visibility at an earlier position.
    let cutover: i64 = sqlx::query_scalar(
        "UPDATE core.counters SET value=value+1 WHERE name='commit_seq' RETURNING value",
    )
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,event_type) VALUES($1,NULL,NULL)")
        .bind(cutover)
        .execute(&mut *conn)
        .await?;
    sqlx::query("INSERT INTO audit.archive_changes(notice_seq,event_id,commit_seq,archived,reason,replaced_by,operation_id,recorded_at,archived_seq,archived_at,baseline) SELECT notice_seq,event_id,$1,true,reason,replaced_by,operation_id,now(),archived_seq,archived_at,true FROM audit.archived")
        .bind(cutover).execute(&mut *conn).await?;
    sqlx::query("INSERT INTO audit.archive_changes(notice_seq,event_id,commit_seq,archived,reason,archived_seq,archived_at,baseline) VALUES(nextval(pg_get_serial_sequence('audit.archived','notice_seq')),NULL,$1,false,'baseline',$1,now(),true)")
        .bind(cutover).execute(&mut *conn).await?;
    for statement in grants() {
        conn.execute(statement.as_str()).await?;
    }
    Ok(())
}
