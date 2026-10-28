//! Writing what admission admitted: events, their source's and run's counts,
//! the commit log, the live side tables, archived readings, dirty keys and citations; then failures and the
//! source position.
use super::classify::Classified;
use super::derived::{mark_archived_dirty, mark_inputs_dirty};
use super::{DirtyKeys, KeyInput, Outcome};
use crate::Error;
use crate::Result;
use sinex_model::{Batch, Event, RecordOutcome, SourceProgress};
use sqlx::PgConnection;
use uuid::Uuid;

/// Allocate the commit sequence range and write every admitted event. The
/// live side tables are written in key order: equivalence keys are hashes, so
/// a batch in arrival order would touch the key index at random.
pub(super) async fn insert_admitted(
    conn: &mut PgConnection,
    batch: &Batch,
    events: &[Event],
    classified: &Classified,
    keys: &dyn DirtyKeys,
    outcome: &mut Outcome,
) -> Result<()> {
    let admit_ords = &classified.admit;
    let replaced = &classified.replaced;
    let n = i64::try_from(admit_ords.len()).expect("count fits i64");
    let last = sqlx::query_scalar!(
        "UPDATE core.counters SET value = value + $1 WHERE name = 'commit_seq' RETURNING value",
        n
    )
    .fetch_one(&mut *conn)
    .await?;
    let first = last - n + 1;
    outcome.commit_seq = Some((first, last));

    sqlx::query!(
        r#"WITH a AS (
             SELECT s.*, $2::bigint + row_number() OVER (ORDER BY s.ord) - 1 AS seq
             FROM core.admission_stage s WHERE s.ord = ANY($1))
           INSERT INTO core.events (id, event_type, source, run_id, material_id, anchor, output_index,
             parents, ts_orig, ts_quality, commit_seq, payload, equivalence_key, reading_order,
             record_hash, record_state, class)
           SELECT id, event_type, source, $3, material_id, anchor, output_index, parents, ts_orig,
             ts_quality, seq, payload, equivalence_key, reading_order, record_hash, record_state,
             class
           FROM a"#,
        admit_ords,
        first,
        batch.run.0,
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query!(
        r#"INSERT INTO core.event_children (parent, child)
           SELECT DISTINCT unnest(s.parents), s.id FROM core.admission_stage s
           WHERE s.ord = ANY($1) AND s.parents IS NOT NULL"#,
        admit_ords,
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query!(
        r#"INSERT INTO core.source_counts AS c (source, events, first_event, last_event, last_seen)
           SELECT source, count(*), min(ts_orig), max(ts_orig), max(uuid_extract_timestamp(id))
           FROM core.admission_stage WHERE ord = ANY($1) GROUP BY source
           ON CONFLICT (source) DO UPDATE SET events = c.events + EXCLUDED.events,
             first_event = least(c.first_event, EXCLUDED.first_event),
             last_event = greatest(c.last_event, EXCLUDED.last_event),
             last_seen = greatest(c.last_seen, EXCLUDED.last_seen)"#,
        admit_ords,
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query!(
        "UPDATE core.runs SET events = events + $2 WHERE id = $1",
        batch.run.0,
        n,
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query!(
        r#"INSERT INTO core.commit_log (commit_seq, event_id, event_type)
           SELECT $2::bigint + row_number() OVER (ORDER BY s.ord) - 1, s.id, s.event_type
           FROM core.admission_stage s WHERE s.ord = ANY($1)"#,
        admit_ords,
        first,
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query!(
        r#"INSERT INTO core.live_equivalence (source, event_type, equivalence_key, event_id, reading_order, record_hash)
           SELECT s.source, s.event_type, s.equivalence_key, s.id, s.reading_order, s.record_hash
           FROM core.admission_stage s
           WHERE s.ord = ANY($1) AND s.equivalence_key IS NOT NULL
           ORDER BY s.source, s.event_type, s.equivalence_key
           ON CONFLICT (source, event_type, equivalence_key) DO UPDATE
           SET event_id = EXCLUDED.event_id, reading_order = EXCLUDED.reading_order,
               record_hash = EXCLUDED.record_hash"#,
        admit_ords,
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query!(
        r#"INSERT INTO core.live_locations (material_id, anchor, event_type, output_index, event_id, reading_order, record_hash)
           SELECT s.material_id, s.anchor, s.event_type, s.output_index, s.id, s.reading_order, s.record_hash
           FROM core.admission_stage s
           WHERE s.ord = ANY($1) AND s.equivalence_key IS NULL AND s.material_id IS NOT NULL
           ORDER BY s.material_id, s.anchor, s.event_type, s.output_index
           ON CONFLICT (material_id, anchor, event_type, output_index) DO UPDATE
           SET event_id = EXCLUDED.event_id, reading_order = EXCLUDED.reading_order,
               record_hash = EXCLUDED.record_hash"#,
        admit_ords,
    )
    .execute(&mut *conn)
    .await?;

    if !replaced.is_empty() {
        let (ords, live): (Vec<i32>, Vec<Uuid>) = replaced.iter().copied().unzip();
        let mut replacements = Vec::with_capacity(ords.len());
        for ord in &ords {
            replacements.push(
                events[usize::try_from(*ord).expect("non-negative event output_index")]
                    .id
                    .0,
            );
        }
        super::archive::archive_at(
            conn,
            &live,
            last,
            sinex_model::ArchiveReason::NewerReading,
            Some(&replacements),
            None,
        )
        .await?;
        outcome.replaced = replaced.len();
        outcome.dirty_keys += mark_archived_dirty(conn, &live, last, keys, Some(batch)).await?;
    }

    outcome.admitted = admit_ords.len();
    let mut dirty = Vec::new();
    for ord in admit_ords {
        let event = &events[usize::try_from(*ord).expect("ord is non-negative")];
        outcome.admitted_ids.push(event.id);
        let input = KeyInput {
            event_type: event.event_type,
            ts_orig: event.ts_orig.at.map(|t| t.0),
            payload: &event.payload,
        };
        dirty.push(input);
    }
    outcome.dirty_keys += mark_inputs_dirty(conn, &dirty, last, keys).await?;
    record_citations(conn, events, admit_ords).await?;
    Ok(())
}

/// Record the batch's failed records; returns how many.
pub(super) async fn record_failures(conn: &mut PgConnection, batch: &Batch) -> Result<usize> {
    let failures: Vec<(Uuid, i64, &str)> = batch
        .records
        .iter()
        .filter_map(|r| match r {
            RecordOutcome::Failure { location, reason } => Some((
                location.material.0,
                i64::try_from(location.anchor).unwrap_or(i64::MAX),
                reason.as_str(),
            )),
            RecordOutcome::Events { .. } => None,
        })
        .collect();
    if !failures.is_empty() {
        let materials: Vec<Uuid> = failures.iter().map(|f| f.0).collect();
        let anchors: Vec<i64> = failures.iter().map(|f| f.1).collect();
        let reasons: Vec<String> = failures.iter().map(|f| f.2.to_owned()).collect();
        sqlx::query!(
            r#"INSERT INTO raw.failures (material_id, anchor, reason, run_id)
               SELECT * , $4::uuid FROM unnest($1::uuid[], $2::bigint[], $3::text[])"#,
            &materials,
            &anchors,
            &reasons,
            batch.run.0,
        )
        .execute(&mut *conn)
        .await?;
        return Ok(failures.len());
    }
    Ok(0)
}

/// Save the source's cursor and carried parser state with the events they
/// cover, and the reader's memory when the batch carries it.
pub(super) async fn save_position(
    conn: &mut PgConnection,
    progress: &SourceProgress,
) -> Result<()> {
    let committed =
        sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
            .fetch_one(&mut *conn)
            .await?;
    sqlx::query!(
        r#"INSERT INTO raw.positions (binding, cursor, carry, committed_seq) VALUES ($1, $2, $3, $4)
           ON CONFLICT (binding) DO UPDATE SET cursor = EXCLUDED.cursor, carry = EXCLUDED.carry,
             committed_seq = EXCLUDED.committed_seq, updated_at = now()"#,
        progress.binding,
        progress.cursor,
        progress.carry,
        committed,
    )
    .execute(&mut *conn)
    .await?;
    if let Some(memory) = &progress.memory {
        sqlx::query!(
            r#"INSERT INTO raw.reader_memory (binding, material_id, memory) VALUES ($1, $2, $3)
               ON CONFLICT (binding) DO UPDATE SET material_id = EXCLUDED.material_id,
                 memory = EXCLUDED.memory, updated_at = now()"#,
            progress.binding,
            memory.material.0,
            &memory.bytes[..],
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Register the stored bytes admitted events own beyond their materials, so
/// excision can find and delete them (`core.event_blobs`).
async fn record_citations(conn: &mut PgConnection, events: &[Event], ords: &[i32]) -> Result<()> {
    let (mut ids, mut blobs, mut sizes) = (Vec::new(), Vec::new(), Vec::new());
    for ord in ords {
        let event = &events[usize::try_from(*ord).expect("ord is non-negative")];
        for c in &event.owned_blobs {
            ids.push(event.id.0);
            blobs.push(c.blob.clone());
            sizes.push(
                i64::try_from(c.size).map_err(|_| Error::Invalid("blob size beyond i64".into()))?,
            );
        }
    }
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query!(
        r#"INSERT INTO core.blobs (hash, size, status)
           SELECT DISTINCT ON (b) b, s, 'present' FROM unnest($1::text[], $2::bigint[]) AS c(b, s)
           ON CONFLICT (hash) DO UPDATE SET status = 'present' WHERE core.blobs.status = 'deleting'"#,
        &blobs,
        &sizes,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        r#"INSERT INTO core.event_blobs (event_id, blob)
           SELECT * FROM unnest($1::uuid[], $2::text[]) ON CONFLICT DO NOTHING"#,
        &ids,
        &blobs,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}
