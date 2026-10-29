//! Excision: every reading of the one record and its descendants go, a
//! tombstone stays, each material the record was read from goes whole with
//! only its digest kept, and nothing that is not the record changes. Every record
//! here is alone in its material, as the caller requires.
use super::*;
use crate::Blobs;
use crate::admission::NoDerivations;
use sinex_model::event_types::{CommandExecuted, CommandFused};
use sinex_model::{
    Batch, BatchKind, Class, Event, Origin, OwnedBlob, ReadingOrder, RecordLocation, RecordOutcome,
    RunId, Timestamp,
};
use sqlx::Connection;

struct Store {
    blobs: Blobs,
    dir: std::path::PathBuf,
}

impl Store {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("sinex-excise-{}", Uuid::now_v7()));
        Self {
            blobs: Blobs::open(&dir).unwrap(),
            dir,
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Register `bytes` as a material read by a binding of `source`, the way
/// admission does for a source batch.
async fn material(
    conn: &mut PgConnection,
    store: &Store,
    source: &str,
    bytes: &[u8],
) -> (MaterialId, String) {
    let blob = store.blobs.put(bytes).unwrap();
    let mut tx = conn.begin().await.unwrap();
    let id = MaterialId::new();
    let binding = format!("{source}:synthetic@h");
    sqlx::query!(
        "INSERT INTO core.blobs (hash, size, status) VALUES ($1, $2, 'present') ON CONFLICT(hash) DO UPDATE SET status='present'",
        blob,
        bytes.len() as i64
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query!(
        "INSERT INTO raw.bindings (binding, source, input, settings) VALUES ($1, $2, 'synthetic', '{}') ON CONFLICT DO NOTHING",
        binding,
        source
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    let input = sqlx::query_scalar!(
        r#"INSERT INTO raw.inputs (id, binding, kind, locator, host) VALUES ($1, $2, 'file', 'synthetic', 'h')
           ON CONFLICT (binding, locator, host) DO UPDATE SET kind = EXCLUDED.kind RETURNING id"#,
        Uuid::now_v7(),
        binding
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    sqlx::query!(
        r#"INSERT INTO raw.materials (id, input_id, blob, length, manifest, parser, parser_version)
           VALUES ($1, $2, $3, $4, '{}', 'lines', 1)"#,
        id.0,
        input,
        blob,
        bytes.len() as i64
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    let receipt = store.blobs.commit_extents(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    store.blobs.acknowledge(receipt).unwrap();
    (id, blob)
}

fn line_at(bytes: &[u8], anchor: usize) -> &[u8] {
    let end = bytes[anchor..].iter().position(|b| *b == b'\n').unwrap() + anchor + 1;
    &bytes[anchor..end]
}

fn keyed(material: MaterialId, bytes: &[u8], anchor: usize, key: &str, order: i64) -> Event {
    let raw = line_at(bytes, anchor);
    Event::build(
        &CommandExecuted {
            native_id: key.into(),
            command: String::from_utf8_lossy(raw).trim().into(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            duration_ns: Some(1),
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "s",
        Origin::Record {
            location: RecordLocation {
                material,
                anchor: anchor as u64,
            },
            output_index: 0,
        },
        ReadingOrder(order),
        *blake3::hash(raw).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn keyless(material: MaterialId, bytes: &[u8], anchor: usize) -> Event {
    let raw = line_at(bytes, anchor);
    Event::build(
        // A location-keyed type: the record's place is its identity.
        &sinex_model::event_types::machine::FileChanged {
            received_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            change: String::from_utf8_lossy(raw).trim().into(),
            path: None,
            path_hex: None,
            to: None,
            is_dir: None,
            note: None,
        },
        "s",
        Origin::Record {
            location: RecordLocation {
                material,
                anchor: anchor as u64,
            },
            output_index: 0,
        },
        ReadingOrder(anchor as i64),
        *blake3::hash(raw).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

async fn admit_events(conn: &mut crate::AdmissionContext, events: Vec<Event>) -> crate::Outcome {
    let mut tx = conn.begin().await.unwrap();
    let out = tx
        .admit(
            &Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "test".into(),
                },
                records: vec![RecordOutcome::Events {
                    location: None,
                    events,
                }],
                archive: vec![],
            },
            &NoDerivations,
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    out
}

/// Plan, apply and commit, then delete the freed files: what the writer
/// does.
async fn excise(conn: &mut PgConnection, store: &Store, target: Target) -> Result<Applied> {
    let plan = plan(conn, target).await?;
    let read_before = crate::ReadMark::now(conn).await?;
    let mut tx = conn.begin().await?;
    let applied = apply(
        &mut tx,
        target,
        &plan,
        OperationId::new(),
        "operator",
        &NoDerivations,
    )
    .await?;
    tx.commit().await?;
    finish_deletions(conn, &store.blobs).await?;
    let read_after = crate::ReadMark::now(conn).await?;
    assert_eq!(
        read_after.excised - read_before.excised,
        i64::try_from(applied.events).unwrap()
    );
    Ok(applied)
}

async fn count(conn: &mut PgConnection, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
}

async fn connect(db: &devkit::pg::TestDatabase) -> crate::AdmissionContext {
    crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap()
}

#[devkit::test]
async fn deletions_a_stop_left_are_finished_and_reused_bytes_kept(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let first = b"secret v1\n".to_vec();
    let (m1, blob1) = material(&mut conn, &store, "s", &first).await;
    let (kept, _) = material(&mut conn, &store, "s", b"keep-1\n").await;
    let old = keyed(m1, &first, 0, "k", 1);
    admit_events(
        &mut conn,
        vec![keyed(kept, b"keep-1\n", 0, "a", 1), old.clone()],
    )
    .await;

    // The excision commits; the process stops before deleting the file.
    let target = Target::Event(old.id);
    let plan = plan(&mut conn, target).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    apply(
        &mut tx,
        target,
        &plan,
        OperationId::new(),
        "operator",
        &NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        store.blobs.read(&blob1).is_ok(),
        "the file outlived the stop"
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM core.blobs WHERE status = 'deleting'"
        )
        .await,
        1
    );

    // The next start finishes it.
    let (deleted, left) = finish_deletions(&mut conn, &store.blobs).await.unwrap();
    assert_eq!((deleted, left.len()), (vec![blob1.clone()], 0));
    assert!(store.blobs.read(&blob1).is_err());
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM core.blobs WHERE status = 'deleting'"
        )
        .await,
        0
    );

    // Bytes registered again while their deletion was waiting are kept.
    let second = b"other\n".to_vec();
    let (_, blob2) = material(&mut conn, &store, "s", &second).await;
    sqlx::query("UPDATE core.blobs SET status = 'deleting' WHERE hash = $1")
        .bind(&blob2)
        .execute(&mut conn)
        .await
        .unwrap();
    material(&mut conn, &store, "s", &second).await;
    let (deleted, _) = finish_deletions(&mut conn, &store.blobs).await.unwrap();
    assert!(deleted.is_empty());
    assert!(store.blobs.read(&blob2).is_ok());
}

#[devkit::test]
async fn every_reading_goes_with_its_materials(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let first = b"secret v1\n".to_vec();
    let second = b"secret v2\n".to_vec();
    let (m1, blob1) = material(&mut conn, &store, "s", &first).await;
    let (m2, blob2) = material(&mut conn, &store, "s", &second).await;
    let (k1, kept1) = material(&mut conn, &store, "s", b"keep-1\n").await;
    let (k2, kept2) = material(&mut conn, &store, "s", b"keep-2\n").await;
    let old = keyed(m1, &first, 0, "k", 1);
    let new = keyed(m2, &second, 0, "k", 2);
    admit_events(
        &mut conn,
        vec![
            keyed(k1, b"keep-1\n", 0, "a", 1),
            old.clone(),
            keyed(k2, b"keep-2\n", 0, "b", 2),
        ],
    )
    .await;
    assert_eq!(admit_events(&mut conn, vec![new.clone()]).await.replaced, 1);

    let applied = excise(&mut conn, &store, Target::Event(old.id))
        .await
        .unwrap();
    assert_eq!(
        applied.events, 2,
        "both readings of the record, live and archived"
    );
    assert_eq!(applied.materials_removed, 2);
    assert_eq!(
        applied.inputs_removed, 0,
        "the input still has other materials"
    );
    assert_eq!(applied.delete_after_commit.len(), 2);
    assert!(store.blobs.read(&blob1).is_err(), "the old file is gone");
    assert!(store.blobs.read(&blob2).is_err());
    assert_eq!(
        count(
            &mut conn,
            &format!(
                "SELECT count(*) FROM raw.materials WHERE id IN ('{}', '{}')",
                m1.0, m2.0
            )
        )
        .await,
        0
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.excised_materials").await,
        2,
        "only the digests stay"
    );
    assert_eq!(store.blobs.read(&kept1).unwrap(), b"keep-1\n");
    assert_eq!(store.blobs.read(&kept2).unwrap(), b"keep-2\n");

    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.event_tombstones").await,
        2
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM core.events e JOIN core.live_equivalence l ON l.event_id = e.id"
        )
        .await,
        2,
        "the other records stay live"
    );
    let archived:Vec<Uuid>=sqlx::query_scalar("SELECT event_id FROM audit.archived ORDER BY event_id").fetch_all(&mut conn).await.unwrap();
    assert_eq!(archived,[old.id.0],"payload-free archive history survives with the excised reading's tombstone");
    assert_eq!(
        count(&mut conn, "SELECT events FROM core.source_counts").await,
        2,
        "the live reading leaves its source's count, the archived one had left it"
    );
}

/// Equal bytes are not the same record: excising one of two equal records
/// leaves the other and the bytes it shares.
#[devkit::test]
async fn equal_records_elsewhere_are_untouched(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let bytes = b"git status\n".to_vec();
    let (m, shared) = material(&mut conn, &store, "s", &bytes).await;
    let (other, other_blob) = material(&mut conn, &store, "s", &bytes).await;
    assert_eq!(shared, other_blob, "equal bytes are stored once");
    let target = keyless(m, &bytes, 0);
    admit_events(&mut conn, vec![target.clone(), keyless(other, &bytes, 0)]).await;
    let applied = excise(&mut conn, &store, Target::Event(target.id))
        .await
        .unwrap();
    assert_eq!(applied.events, 1);
    assert_eq!(
        store.blobs.read(&other_blob).unwrap(),
        b"git status\n",
        "the other material keeps its bytes"
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.live_locations").await,
        1,
        "the equal record stays live"
    );
}

/// An input whose last material goes is removed with its observations and
/// revisions, so no path of it stays; a later part or a cursor that named a
/// removed part names the part before it.
#[devkit::test]
async fn an_input_left_empty_goes_and_part_sequences_stay_whole(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let (alone, _) = material(&mut conn, &store, "lone", b"secret\n").await;
    sqlx::query(
        "INSERT INTO raw.observations (id, input_id, observed_at, last_looked_at)
         SELECT gen_random_uuid(), input_id, now(), now() FROM raw.materials WHERE id = $1",
    )
    .bind(alone.0)
    .execute(&mut conn)
    .await
    .unwrap();
    let event = keyless(alone, b"secret\n", 0);
    admit_events(&mut conn, vec![event.clone()]).await;
    let applied = excise(&mut conn, &store, Target::Event(event.id))
        .await
        .unwrap();
    assert_eq!((applied.materials_removed, applied.inputs_removed), (1, 1));
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM raw.inputs WHERE binding = 'lone:synthetic@h'"
        )
        .await,
        0,
        "no excised path stays"
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM raw.observations").await,
        0
    );

    // Three parts in sequence; the middle one goes.
    let parts: Vec<MaterialId> = {
        let mut parts = Vec::new();
        for bytes in [&b"one\n"[..], b"two\n", b"three\n"] {
            parts.push(material(&mut conn, &store, "seq", bytes).await.0);
        }
        parts
    };
    for (index, pair) in parts.windows(2).enumerate() {
        sqlx::query(
            "UPDATE raw.materials SET previous_part_id = $2, part_index = $3 WHERE id = $1",
        )
        .bind(pair[1].0)
        .bind(pair[0].0)
        .bind(index as i32 + 1)
        .execute(&mut conn)
        .await
        .unwrap();
    }
    // A cursor that names the middle part as the last one stored.
    sqlx::query(
        "INSERT INTO raw.positions (binding, cursor, carry, committed_seq)
         VALUES ('seq:synthetic@h', jsonb_build_object('last_material', $1::text), 'null', 0)",
    )
    .bind(parts[1].0.to_string())
    .execute(&mut conn)
    .await
    .unwrap();
    let middle = keyless(parts[1], b"two\n", 0);
    admit_events(&mut conn, vec![middle.clone()]).await;
    excise(&mut conn, &store, Target::Event(middle.id))
        .await
        .unwrap();
    let previous: Option<Uuid> =
        sqlx::query_scalar("SELECT previous_part_id FROM raw.materials WHERE id = $1")
            .bind(parts[2].0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(previous, Some(parts[0].0));
    let cursor: String = sqlx::query_scalar(
        "SELECT cursor->>'last_material' FROM raw.positions WHERE binding = 'seq:synthetic@h'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        cursor,
        parts[0].0.to_string(),
        "the cursor names a stored part"
    );
}

#[devkit::test]
async fn derived_descendants_go_with_their_parent(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let bytes = b"make\n".to_vec();
    let (m, _) = material(&mut conn, &store, "s", &bytes).await;
    let parent = keyed(m, &bytes, 0, "k", 1);
    admit_events(&mut conn, vec![parent.clone()]).await;
    let fused = Event::build(
        &CommandFused {
            record: "command.executed:k".into(),
            command: "make".into(),
            host: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            cwd: None,
            session: None,
            duration_ns: None,
            exit_code: None,
            observed_by: vec!["s".into()],
        },
        "fusion",
        Origin::Derived {
            parents: vec![parent.id],
        },
        ReadingOrder(1),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    admit_events(&mut conn, vec![fused.clone()]).await;
    let refused = excise(&mut conn, &store, Target::Event(fused.id))
        .await
        .unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("not read from a stored record"),
        "{refused}"
    );
    let applied = excise(&mut conn, &store, Target::Event(parent.id))
        .await
        .unwrap();
    assert_eq!(applied.events, 2);
    let tombstoned: Vec<Uuid> =
        sqlx::query_scalar!("SELECT event_id FROM core.event_tombstones ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let mut expected = vec![parent.id.0, fused.id.0];
    expected.sort();
    assert_eq!(tombstoned, expected);
}

/// Bytes an excised event cites go with it unless another event still cites
/// them.
#[devkit::test]
async fn cited_bytes_go_unless_something_else_cites_them(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let only = store.blobs.put(b"cited by a alone").unwrap();
    let shared = store.blobs.put(b"cited by b and c").unwrap();
    let cite = |mut event: Event, blobs: &[&String]| {
        event.owned_blobs = blobs
            .iter()
            .map(|blob| OwnedBlob {
                blob: (*blob).clone(),
                size: 16,
            })
            .collect();
        event
    };
    let mut records = Vec::new();
    for line in [b"a\n", b"b\n", b"c\n"] {
        let (m, _) = material(&mut conn, &store, "s", line).await;
        records.push(keyless(m, line, 0));
    }
    let [a, b, c] = <[Event; 3]>::try_from(records).unwrap();
    let a = cite(a, &[&only]);
    let b = cite(b, &[&shared]);
    let c = cite(c, &[&shared]);
    admit_events(&mut conn, vec![a.clone(), b.clone(), c]).await;
    let applied = excise(&mut conn, &store, Target::Event(a.id))
        .await
        .unwrap();
    assert_eq!(applied.blobs_deleted, 1);
    assert!(!store.blobs.contains(&only).unwrap());
    excise(&mut conn, &store, Target::Event(b.id))
        .await
        .unwrap();
    assert!(
        store.blobs.contains(&shared).unwrap(),
        "c still cites the shared bytes"
    );
}

#[devkit::test]
async fn excision_of_sealed_material_is_refused_without_changes(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let bytes = b"a\nb\n".to_vec();
    let (m, blob) = material(&mut conn, &store, "s", &bytes).await;
    sqlx::query!(
        "INSERT INTO privacy.data_keys (material_id, key_ref, class, wrapped_key) VALUES ($1, $2, 'sealed', '\\x00')",
        m.0, m.0.to_string()
    ).execute(&mut conn).await.unwrap();
    let a = keyed(m, &bytes, 0, "a", 1);
    admit_events(&mut conn, vec![a.clone(), keyed(m, &bytes, 2, "b", 1)]).await;
    for target in [Target::Event(a.id), Target::Material(m)] {
        let err = excise(&mut conn, &store, target).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("excision of sealed source material is not supported")
        );
    }
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM privacy.data_keys").await,
        1
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.event_tombstones").await,
        0
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.events").await,
        2
    );
    assert_eq!(store.blobs.read(&blob).unwrap(), bytes);
}

/// Two materials with equal bytes share one blob; excising a record of one
/// leaves the other material and the shared blob as they were.
#[devkit::test]
async fn a_blob_shared_by_two_materials_stays_for_the_other(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let bytes = b"same\n".to_vec();
    let (first, blob) = material(&mut conn, &store, "s", &bytes).await;
    let (second, _) = material(&mut conn, &store, "s", &bytes).await;
    let revision = Uuid::now_v7();
    sqlx::query!(
        r#"INSERT INTO raw.revisions (id, input_id, kind)
           SELECT $1, input_id, 'appended' FROM raw.materials WHERE id = $2"#,
        revision,
        first.0
    )
    .execute(&mut conn)
    .await
    .unwrap();
    for id in [first, second] {
        sqlx::query!(
            "UPDATE raw.materials SET revision_id = $2 WHERE id = $1",
            id.0,
            revision
        )
        .execute(&mut conn)
        .await
        .unwrap();
    }
    let event = keyless(first, &bytes, 0);
    admit_events(&mut conn, vec![event.clone(), keyless(second, &bytes, 0)]).await;
    excise(&mut conn, &store, Target::Event(event.id))
        .await
        .unwrap();
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.events").await,
        1
    );
    let second_blob: String =
        sqlx::query_scalar!("SELECT blob FROM raw.materials WHERE id = $1", second.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(second_blob, blob);
    assert_eq!(store.blobs.read(&blob).unwrap(), bytes);
}

#[devkit::test]
async fn a_plan_that_went_stale_is_refused(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let bytes = b"x\n".to_vec();
    let (m, _) = material(&mut conn, &store, "s", &bytes).await;
    let first = keyed(m, &bytes, 0, "k", 1);
    admit_events(&mut conn, vec![first.clone()]).await;
    let stale = plan(&mut conn, Target::Event(first.id)).await.unwrap();
    // A newer reading arrives between planning and applying.
    let (m2, _) = material(&mut conn, &store, "s", b"y\n").await;
    admit_events(&mut conn, vec![keyed(m2, b"y\n", 0, "k", 2)]).await;
    let mut tx = conn.begin().await.unwrap();
    let err = apply(
        &mut tx,
        Target::Event(first.id),
        &stale,
        OperationId::new(),
        "operator",
        &NoDerivations,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("plan the excision again"));
}

#[devkit::test]
async fn excision_removes_search_text(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let store = Store::new();
    let (m, _) = material(&mut conn, &store, "s", b"cargo test\n").await;
    let (other, _) = material(&mut conn, &store, "s", b"ls\n").await;
    let target = keyed(m, b"cargo test\n", 0, "a", 1);
    admit_events(
        &mut conn,
        vec![target.clone(), keyed(other, b"ls\n", 0, "b", 1)],
    )
    .await;
    crate::read_models::tests::catch_up_search_text(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.search_text").await,
        2
    );
    let page = crate::commit_log::after(&mut conn, 0, 10, i64::MAX)
        .await
        .unwrap();
    excise(&mut conn, &store, Target::Event(target.id))
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    crate::read_models::apply_search_text(&mut tx, &page.events)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let left: Vec<Uuid> = sqlx::query_scalar!("SELECT event_id FROM core.search_text")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert!(!left.contains(&target.id.0));
    assert_eq!(left.len(), 1);
}
