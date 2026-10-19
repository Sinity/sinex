//! Excision laws: every reading and its descendants go, a tombstone stays, the
//! bytes are blanked in a new blob with every other anchor unchanged, and
//! neither re-admission nor re-staging the same bytes brings the record back.
use super::*;
use crate::admission::{NoDerivations, admit};
use crate::{Blobs, testdb};
use sinex_model::kinds::{CommandExecuted, CommandFused};
use sinex_model::{
    Batch, BatchKind, Class, Event, Origin, ReadingOrder, RecordLocation, RecordOutcome, RunId,
    Timestamp,
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

/// Register `bytes` as a material, the way admission does for a source batch.
async fn material(conn: &mut PgConnection, store: &Store, bytes: &[u8]) -> (MaterialId, String) {
    let blob = store.blobs.put(bytes).unwrap();
    let id = MaterialId::new();
    sqlx::query!(
        "INSERT INTO core.blobs (hash, size, status) VALUES ($1, $2, 'present') ON CONFLICT DO NOTHING",
        blob,
        bytes.len() as i64
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query!(
        r#"INSERT INTO raw.materials (id, blob, durable_bytes, manifest, parser, parser_version)
           VALUES ($1, $2, $3, '{}', 'lines', 1)"#,
        id.0,
        blob,
        bytes.len() as i64
    )
    .execute(&mut *conn)
    .await
    .unwrap();
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
            ordinal: 0,
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
        &sinex_model::kinds::machine::FileChanged {
            received_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            change: String::from_utf8_lossy(raw).trim().into(),
            path: None,
            path_hex: None,
            to: None,
            is_dir: None,
            note: None,
        },
        "z",
        Origin::Record {
            location: RecordLocation {
                material,
                anchor: anchor as u64,
            },
            ordinal: 0,
        },
        ReadingOrder(anchor as i64),
        *blake3::hash(raw).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

async fn admit_events(conn: &mut PgConnection, events: Vec<Event>) -> crate::Outcome {
    let mut tx = conn.begin().await.unwrap();
    let out = admit(
        &mut tx,
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

/// Plan, write the blanked bytes, apply and commit, then delete the old
/// files: what the writer does.
async fn excise(conn: &mut PgConnection, store: &Store, target: Target) -> Result<Applied> {
    let plan = plan(conn, target).await?;
    let rewrites = rewrites_for(conn, store, &plan).await;
    let mut tx = conn.begin().await?;
    let applied = apply(
        &mut tx,
        target,
        &plan,
        &rewrites,
        OperationId::new(),
        "operator",
        &NoDerivations,
    )
    .await?;
    tx.commit().await?;
    finish_deletions(conn, &store.blobs).await?;
    Ok(applied)
}

/// The blanked bytes for each material a plan rewrites, stored.
async fn rewrites_for(_conn: &mut PgConnection, store: &Store, plan: &Plan) -> Vec<Rewrite> {
    let mut rewrites = Vec::new();
    for m in plan.materials.iter().filter(|m| !m.sealed) {
        let mut bytes = store.blobs.read(&m.blob).unwrap();
        let spans: Vec<(usize, usize)> = if m.whole {
            vec![(0, bytes.len())]
        } else {
            m.anchors
                .iter()
                .map(|a| {
                    let a = *a as usize;
                    (a, a + line_at(&bytes, a).len())
                })
                .collect()
        };
        blank(&mut bytes, &spans);
        rewrites.push(Rewrite {
            material: m.id,
            blob: store.blobs.put(&bytes).unwrap(),
            length: bytes.len() as u64,
        });
    }
    rewrites
}

async fn count(conn: &mut PgConnection, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
}

#[test]
fn blank_keeps_length_framing_and_neighbours() {
    let mut b = b"one\ntwo two\nthree\n".to_vec();
    blank(&mut b, &[(4, 12)]);
    assert_eq!(&b, b"one\n       \nthree\n");
    let mut whole = b"a\nbc\n".to_vec();
    blank(&mut whole, &[(0, 5)]);
    assert_eq!(
        &whole, b"    \n",
        "a whole-material span blanks inner newlines too"
    );
}

#[tokio::test]
async fn deletions_a_stop_left_are_finished_and_reused_bytes_kept() {
    let (_db, mut conn) = testdb::fresh().await;
    let store = Store::new();
    let first = b"keep-1\nsecret v1\n".to_vec();
    let (m1, blob1) = material(&mut conn, &store, &first).await;
    let old = keyed(m1, &first, 7, "k", 1);
    admit_events(&mut conn, vec![keyed(m1, &first, 0, "a", 1), old.clone()]).await;

    // The excision commits; the process stops before deleting the file.
    let target = Target::Event(old.id);
    let plan = plan(&mut conn, target).await.unwrap();
    let rewrites = rewrites_for(&mut conn, &store, &plan).await;
    let mut tx = conn.begin().await.unwrap();
    apply(
        &mut tx,
        target,
        &plan,
        &rewrites,
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

    // Bytes registered again while their deletion was pending are kept.
    let second = b"other\n".to_vec();
    let (_, blob2) = material(&mut conn, &store, &second).await;
    sqlx::query("UPDATE core.blobs SET status = 'deleting' WHERE hash = $1")
        .bind(&blob2)
        .execute(&mut conn)
        .await
        .unwrap();
    material(&mut conn, &store, &second).await;
    let (deleted, _) = finish_deletions(&mut conn, &store.blobs).await.unwrap();
    assert!(deleted.is_empty());
    assert!(store.blobs.read(&blob2).is_ok());
}

#[tokio::test]
async fn every_reading_goes_and_its_bytes_are_blanked_in_place() {
    let (_db, mut conn) = testdb::fresh().await;
    let store = Store::new();
    let first = b"keep-1\nsecret v1\n".to_vec();
    let second = b"secret v2\nkeep-2\n".to_vec();
    let (m1, blob1) = material(&mut conn, &store, &first).await;
    let (m2, _) = material(&mut conn, &store, &second).await;
    let old = keyed(m1, &first, 7, "k", 1);
    let new = keyed(m2, &second, 0, "k", 2);
    admit_events(
        &mut conn,
        vec![
            keyed(m1, &first, 0, "a", 1),
            old.clone(),
            keyed(m2, &second, 10, "b", 2),
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
    assert_eq!(applied.materials_rewritten, 2);
    assert_eq!(applied.delete_after_commit.len(), 2);
    assert!(store.blobs.read(&blob1).is_err(), "the old file is gone");

    let blobs: Vec<String> = sqlx::query_scalar!(
        "SELECT blob FROM raw.materials WHERE id = ANY($1) ORDER BY id",
        &[m1.0, m2.0]
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(store.blobs.read(&blobs[0]).unwrap(), b"keep-1\n         \n");
    assert_eq!(store.blobs.read(&blobs[1]).unwrap(), b"         \nkeep-2\n");

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
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM audit.archived").await,
        0
    );
    // The key is remembered: a later reading of it is suppressed.
    let (m3, _) = material(&mut conn, &store, b"secret v3\n").await;
    let again = admit_events(&mut conn, vec![keyed(m3, b"secret v3\n", 0, "k", 9)]).await;
    assert_eq!(again.hidden, 1);
}

#[tokio::test]
async fn restaging_the_same_bytes_does_not_bring_a_keyless_record_back() {
    let (_db, mut conn) = testdb::fresh().await;
    let store = Store::new();
    let bytes = b"ls\nrm -rf secret\n".to_vec();
    let (m, _) = material(&mut conn, &store, &bytes).await;
    let target = keyless(m, &bytes, 3);
    admit_events(&mut conn, vec![keyless(m, &bytes, 0), target.clone()]).await;
    excise(&mut conn, &store, Target::Event(target.id))
        .await
        .unwrap();
    // The same file staged again: a new material, so new locations.
    let (again, _) = material(&mut conn, &store, &bytes).await;
    let out = admit_events(
        &mut conn,
        vec![keyless(again, &bytes, 0), keyless(again, &bytes, 3)],
    )
    .await;
    assert_eq!((out.admitted, out.hidden), (1, 1));
}

#[tokio::test]
async fn derived_descendants_go_with_their_parent() {
    let (_db, mut conn) = testdb::fresh().await;
    let store = Store::new();
    let bytes = b"make\n".to_vec();
    let (m, _) = material(&mut conn, &store, &bytes).await;
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

#[tokio::test]
async fn a_sealed_material_is_shredded_whole_and_never_in_part() {
    let (_db, mut conn) = testdb::fresh().await;
    let store = Store::new();
    let bytes = b"a\nb\n".to_vec();
    let (m, blob) = material(&mut conn, &store, &bytes).await;
    sqlx::query!(
        "INSERT INTO privacy.material_keys (material_id, key_ref, class, wrapped_key) VALUES ($1, $2, 'sealed', '\\x00')",
        m.0,
        m.0.to_string()
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let a = keyed(m, &bytes, 0, "a", 1);
    admit_events(&mut conn, vec![a.clone(), keyed(m, &bytes, 2, "b", 1)]).await;
    let refused = plan(&mut conn, Target::Event(a.id)).await.unwrap_err();
    assert!(refused.to_string().contains("only the whole material"));

    let applied = excise(&mut conn, &store, Target::Material(m))
        .await
        .unwrap();
    assert_eq!(
        (
            applied.events,
            applied.keys_destroyed,
            applied.materials_rewritten
        ),
        (2, 1, 0)
    );
    assert!(
        store.blobs.read(&blob).is_ok(),
        "the ciphertext stays; its key is gone"
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM privacy.material_keys").await,
        0
    );
}

#[tokio::test]
async fn a_plan_that_went_stale_is_refused() {
    let (_db, mut conn) = testdb::fresh().await;
    let store = Store::new();
    let bytes = b"x\n".to_vec();
    let (m, _) = material(&mut conn, &store, &bytes).await;
    let first = keyed(m, &bytes, 0, "k", 1);
    admit_events(&mut conn, vec![first.clone()]).await;
    let stale = plan(&mut conn, Target::Event(first.id)).await.unwrap();
    // A newer reading arrives between planning and applying.
    let (m2, _) = material(&mut conn, &store, b"y\n").await;
    admit_events(&mut conn, vec![keyed(m2, b"y\n", 0, "k", 2)]).await;
    let mut tx = conn.begin().await.unwrap();
    let err = apply(
        &mut tx,
        Target::Event(first.id),
        &stale,
        &[],
        OperationId::new(),
        "operator",
        &NoDerivations,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("plan the excision again"));
}
