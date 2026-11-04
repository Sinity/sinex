//! Work an upgrade can do before the daemon stops: `sinexd admin upgrade
//! --prepare` runs from the new build while the old daemon still serves.
//!
//! The equivalence-key table changes its key from text to the compact bytes
//! admission reads from its first write, so the change cannot wait until
//! after start, and rewriting the table inside the upgrade takes minutes.
//! Instead a copy in its final shape is built beside it: triggers mirror every
//! change admission makes, and a backfill copies the stored rows in key order.
//! Each backfill batch holds a share lock on the table, so a change either
//! committed before the batch read the table or waits for the batch and is
//! then mirrored onto the copied row; a table lock, unlike row locks taken in
//! key order, cannot deadlock with admission. Once the backfill reaches the
//! end the copy is marked complete, and the triggers keep it equal to the
//! table. The
//! upgrade then swaps the copy in; without a complete copy it converts the
//! table in place. `--unprepare` removes everything prepared.
//!
//! The mirror runs inside the old daemon's admission, with the rights of the
//! role that prepared it, so the copy needs no grant beyond a fresh table's.
//! Its statements cannot fail on their own: the encoding is defined for every
//! text, and the copy has no constraint the table it mirrors does not have.
use crate::Error;
use sqlx::{Connection, PgConnection};

/// Key rows copied per backfill step, each step one short transaction.
pub const BATCH: i64 = 5_000;

const COPY: &str = "sinex_schema.live_equivalence_next";

/// The backfill's position: every stored key up to and including this one is
/// in the copy. `complete` once the backfill reached the end.
const PROGRESS: &str = "CREATE TABLE sinex_schema.live_equivalence_copy (
  id boolean PRIMARY KEY CHECK (id),
  source text,
  event_type text,
  equivalence_key text,
  complete boolean NOT NULL DEFAULT false
)";

const MIRROR: &str = r#"CREATE FUNCTION sinex_schema.mirror_live_equivalence() RETURNS trigger LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF TG_OP = 'TRUNCATE' THEN
    TRUNCATE sinex_schema.live_equivalence_next;
    RETURN NULL;
  END IF;
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    DELETE FROM sinex_schema.live_equivalence_next
    WHERE source = OLD.source AND event_type = OLD.event_type
      AND equivalence_key = sinex_schema.encode_equivalence_key(OLD.equivalence_key);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO sinex_schema.live_equivalence_next (source, event_type, equivalence_key, event_id, reading_order, record_hash)
    VALUES (NEW.source, NEW.event_type, sinex_schema.encode_equivalence_key(NEW.equivalence_key),
            NEW.event_id, NEW.reading_order, NEW.record_hash)
    ON CONFLICT (source, event_type, equivalence_key) DO UPDATE
      SET event_id = EXCLUDED.event_id, reading_order = EXCLUDED.reading_order, record_hash = EXCLUDED.record_hash;
  END IF;
  RETURN NULL;
END $$"#;

/// What `prepare` found or did.
#[derive(Debug, PartialEq, Eq)]
pub enum Prepared {
    /// The copy is complete; `rows` key rows were copied by this run.
    Complete { rows: u64 },
    /// The store's keys are already in their final form; nothing to prepare.
    NotNeeded,
}

impl std::fmt::Display for Prepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Prepared::Complete { rows } => write!(
                f,
                "prepared the equivalence-key copy ({rows} rows copied by this run); the upgrade swaps it in"
            ),
            Prepared::NotNeeded => write!(f, "nothing to prepare: the keys are already compact"),
        }
    }
}

/// Whether the store still keeps its equivalence keys as text.
async fn keys_are_text(conn: &mut PgConnection) -> Result<bool, Error> {
    Ok(sqlx::query_scalar(
        "SELECT coalesce((SELECT data_type = 'text' FROM information_schema.columns
           WHERE table_schema = 'core' AND table_name = 'live_equivalence' AND column_name = 'equivalence_key'), false)",
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// The statements that create the copy, its encoding, the mirror and the
/// backfill position. Each is skipped when it already exists, so a prepare
/// interrupted at any point runs again from where it stopped.
fn objects() -> Vec<String> {
    vec![
        crate::tables::ENCODE_EQUIVALENCE_KEY
            .replacen(
                "CREATE FUNCTION core.encode_equivalence_key",
                "CREATE OR REPLACE FUNCTION sinex_schema.encode_equivalence_key",
                1,
            ),
        copy_table().replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1),
        crate::tables::LIVE_EQUIVALENCE_EVENT
            .replacen(
                "CREATE INDEX live_equivalence_event ON core.live_equivalence",
                "CREATE INDEX IF NOT EXISTS live_equivalence_next_event ON sinex_schema.live_equivalence_next",
                1,
            ),
        PROGRESS.replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1),
        "INSERT INTO sinex_schema.live_equivalence_copy (id) VALUES (true) ON CONFLICT DO NOTHING".into(),
        MIRROR.replacen("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1),
        "CREATE OR REPLACE TRIGGER live_equivalence_mirror AFTER INSERT OR UPDATE OR DELETE ON core.live_equivalence
         FOR EACH ROW EXECUTE FUNCTION sinex_schema.mirror_live_equivalence()".into(),
        "CREATE OR REPLACE TRIGGER live_equivalence_mirror_truncate AFTER TRUNCATE ON core.live_equivalence
         FOR EACH STATEMENT EXECUTE FUNCTION sinex_schema.mirror_live_equivalence()".into(),
    ]
}

fn copy_table() -> String {
    crate::tables::LIVE_EQUIVALENCE.replacen(
        "CREATE TABLE core.live_equivalence (",
        &format!("CREATE TABLE {COPY} ("),
        1,
    )
}

/// The statements that remove everything prepared: what `--unprepare` runs,
/// and what the upgrade runs when it converts the table in place.
pub(crate) fn removal() -> Vec<String> {
    vec![
        "DO $$ BEGIN
           IF to_regclass('core.live_equivalence') IS NOT NULL THEN
             DROP TRIGGER IF EXISTS live_equivalence_mirror ON core.live_equivalence;
             DROP TRIGGER IF EXISTS live_equivalence_mirror_truncate ON core.live_equivalence;
           END IF;
         END $$"
            .into(),
        "DROP FUNCTION IF EXISTS sinex_schema.mirror_live_equivalence()".into(),
        "DROP TABLE IF EXISTS sinex_schema.live_equivalence_next, sinex_schema.live_equivalence_copy".into(),
        "DROP FUNCTION IF EXISTS sinex_schema.encode_equivalence_key(text)".into(),
    ]
}

/// The upgrade's statements that put the complete copy in place of the
/// table, under the fresh names, and remove the rest of the preparation. The
/// key functions are created as the in-place conversion creates them.
pub(crate) fn swap() -> Vec<String> {
    vec![
        crate::tables::ENCODE_EQUIVALENCE_KEY.into(),
        crate::tables::DECODE_EQUIVALENCE_KEY.into(),
        "DO $$ BEGIN
           IF (SELECT count(*) FROM core.live_equivalence) <> (SELECT count(*) FROM sinex_schema.live_equivalence_next) THEN
             RAISE EXCEPTION 'the prepared equivalence-key copy does not hold the same number of rows; run sinexd admin upgrade --unprepare';
           END IF;
         END $$"
            .into(),
        "DROP TABLE core.live_equivalence".into(),
        "ALTER TABLE sinex_schema.live_equivalence_next SET SCHEMA core".into(),
        "ALTER TABLE core.live_equivalence_next RENAME TO live_equivalence".into(),
        "ALTER INDEX core.live_equivalence_next_event RENAME TO live_equivalence_event".into(),
        // Constraint names were derived from the copy's name; renaming the
        // primary key renames its index too.
        "DO $$ DECLARE c record; BEGIN
           FOR c IN SELECT conname FROM pg_constraint
             WHERE conrelid = 'core.live_equivalence'::regclass AND conname LIKE 'live\\_equivalence\\_next\\_%' LOOP
             EXECUTE format('ALTER TABLE core.live_equivalence RENAME CONSTRAINT %I TO %I',
               c.conname, 'live_equivalence_' || substr(c.conname, length('live_equivalence_next_') + 1));
           END LOOP;
         END $$"
            .into(),
        "DROP FUNCTION sinex_schema.mirror_live_equivalence()".into(),
        "DROP TABLE sinex_schema.live_equivalence_copy".into(),
        "DROP FUNCTION sinex_schema.encode_equivalence_key(text)".into(),
    ]
}

/// Whether the upgrade can swap the copy in: it is complete and the mirror is
/// still in place, so it equals the table.
pub(crate) async fn ready(conn: &mut PgConnection) -> Result<bool, Error> {
    Ok(sqlx::query_scalar(
        "SELECT to_regclass('sinex_schema.live_equivalence_copy') IS NOT NULL
           AND to_regclass('sinex_schema.live_equivalence_next') IS NOT NULL
           AND (SELECT count(*) = 2 FROM pg_trigger
                WHERE tgrelid = to_regclass('core.live_equivalence')
                  AND tgname IN ('live_equivalence_mirror', 'live_equivalence_mirror_truncate')
                  AND tgenabled <> 'D')",
    )
    .fetch_one(&mut *conn)
    .await?
        && sqlx::query_scalar("SELECT complete FROM sinex_schema.live_equivalence_copy")
            .fetch_one(&mut *conn)
            .await?)
}

/// Build the copy and run the backfill to the end, in steps of `batch` rows.
/// Runs beside the serving daemon: it takes no writer lock.
pub async fn prepare(conn: &mut PgConnection, batch: i64) -> Result<Prepared, Error> {
    if !keys_are_text(conn).await? {
        return Ok(Prepared::NotNeeded);
    }
    let mut tx = conn.begin().await?;
    for statement in objects() {
        sqlx::query(&statement)
            .persistent(false)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    let mut rows = 0;
    while let Some(copied) = copy_batch(conn, batch).await? {
        rows += copied;
    }
    Ok(Prepared::Complete { rows })
}

/// Copy the next rows after the position and move it, in one transaction.
/// `None` once no row follows, after marking the copy complete.
pub async fn copy_batch(conn: &mut PgConnection, batch: i64) -> Result<Option<u64>, Error> {
    let mut tx = conn.begin().await?;
    let (source, event_type, key): (Option<String>, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT source, event_type, equivalence_key FROM sinex_schema.live_equivalence_copy FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("LOCK TABLE core.live_equivalence IN SHARE MODE")
        .execute(&mut *tx)
        .await?;
    let after = if source.is_some() {
        "(l.source, l.event_type, l.equivalence_key) > ($1, $2, $3)"
    } else {
        "$1::text IS NULL AND $2::text IS NULL AND $3::text IS NULL"
    };
    let (last_source, last_type, last_key, copied): (
        Option<String>,
        Option<String>,
        Option<String>,
        i64,
    ) = sqlx::query_as(&format!(
        "WITH batch AS (
           SELECT l.source, l.event_type, l.equivalence_key, l.event_id, l.reading_order, l.record_hash
           FROM core.live_equivalence l WHERE {after}
           ORDER BY l.source, l.event_type, l.equivalence_key LIMIT $4),
         copied AS (
           INSERT INTO sinex_schema.live_equivalence_next (source, event_type, equivalence_key, event_id, reading_order, record_hash)
           SELECT source, event_type, sinex_schema.encode_equivalence_key(equivalence_key), event_id, reading_order, record_hash
           FROM batch ON CONFLICT DO NOTHING RETURNING 1),
         last AS (
           SELECT source, event_type, equivalence_key FROM batch
           ORDER BY source DESC, event_type DESC, equivalence_key DESC LIMIT 1)
         SELECT (SELECT source FROM last), (SELECT event_type FROM last), (SELECT equivalence_key FROM last),
                (SELECT count(*) FROM copied)"
    ))
    .persistent(false)
    .bind(&source)
    .bind(&event_type)
    .bind(&key)
    .bind(batch.max(1))
    .fetch_one(&mut *tx)
    .await?;
    let more = last_source.is_some();
    if more {
        sqlx::query("UPDATE sinex_schema.live_equivalence_copy SET source = $1, event_type = $2, equivalence_key = $3")
            .bind(&last_source)
            .bind(&last_type)
            .bind(&last_key)
            .execute(&mut *tx)
            .await?;
    } else {
        sqlx::query("UPDATE sinex_schema.live_equivalence_copy SET complete = true")
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(more.then_some(u64::try_from(copied).expect("a count is not negative")))
}

/// Remove the copy, the mirror and the encoding.
pub async fn unprepare(conn: &mut PgConnection) -> Result<(), Error> {
    let mut tx = conn.begin().await?;
    for statement in removal() {
        sqlx::query(&statement)
            .persistent(false)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
