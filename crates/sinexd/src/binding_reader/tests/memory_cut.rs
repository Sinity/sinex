use super::*;
use sinex_model::{MaterialId, MaterialRef};
use sinex_source_core::{StoredMaterial, drivers::DriverError};
use sqlx::Connection;

struct Restoring {
    last: MaterialId,
    older: bool,
    seen: Vec<(MaterialId, u64)>,
    memory: Option<(MaterialId, Option<u64>)>,
    finished: bool,
}

impl Restoring {
    fn new(last: MaterialId, older: bool) -> Self {
        Self {
            last,
            older,
            seen: vec![],
            memory: None,
            finished: false,
        }
    }
}

impl Reader for Restoring {
    fn history_from(&self, _: &Value) -> Option<MaterialId> {
        Some(self.last)
    }

    fn restore(
        &mut self,
        _: &Value,
        part: StoredMaterial,
    ) -> std::result::Result<bool, DriverError> {
        self.seen
            .push((part.reference.id, part.admitted_through()?));
        Ok(true)
    }

    fn restore_memory(
        &mut self,
        memory: &[u8],
        anchor: Option<&MaterialRef>,
        through: Option<u64>,
    ) -> std::result::Result<bool, DriverError> {
        assert_eq!(memory, b"saved");
        self.memory = Some((anchor.unwrap().id, through));
        Ok(self.older)
    }

    fn restored(&mut self) -> std::result::Result<(), DriverError> {
        self.finished = true;
        Ok(())
    }

    fn poll(&mut self, _: &Value) -> std::result::Result<drivers::Acquired, DriverError> {
        unreachable!()
    }
}

async fn history(conn: &mut PgConnection, blob: &str) -> Vec<MaterialId> {
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,20,'present')")
        .bind(blob)
        .execute(&mut *conn)
        .await
        .unwrap();
    let mut ids = Vec::<MaterialId>::new();
    for index in 0..270_i32 {
        let id = MaterialId::new();
        // Directory manifests contain per-file facts, not just a blob name.
        let manifest = json!({"previous_through": 7 + index % 3,
            "members": (0..100).map(|file| json!({"path":format!("synthetic/{index}/{file}"), "length":20})).collect::<Vec<_>>()});
        sqlx::query("INSERT INTO raw.materials(id,blob,length,manifest,parser,parser_version,part_index,previous_part_id) VALUES($1,$2,20,$3,'synthetic',1,$4,$5)")
            .bind(id.0).bind(blob).bind(manifest).bind(index)
            .bind(ids.last().map(|id| id.0)).execute(&mut *conn).await.unwrap();
        ids.push(id);
    }
    sqlx::query("INSERT INTO raw.positions(binding,cursor,carry,committed_seq) VALUES('memory-cut',$1,'{}',1)")
        .bind(json!({"material":ids[269],"through":5})).execute(conn).await.unwrap();
    ids
}

async fn remember(conn: &mut PgConnection, id: Option<MaterialId>) {
    sqlx::query("DELETE FROM raw.reader_memory WHERE binding='memory-cut'")
        .execute(&mut *conn)
        .await
        .unwrap();
    if let Some(id) = id {
        sqlx::query(
            "INSERT INTO raw.reader_memory(binding,material_id,memory) VALUES('memory-cut',$1,$2)",
        )
        .bind(id.0)
        .bind(b"saved".as_slice())
        .execute(conn)
        .await
        .unwrap();
    }
}

#[devkit::test]
async fn saved_memory_does_not_fetch_or_decode_older_directory_manifests(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let blob = blobs.put(b"synthetic body bytes").unwrap();
    let ids = history(&mut conn, &blob).await;
    let unbounded = sinex_store::material_bytes::parts_from(&mut conn, ids[269], 256, None)
        .await
        .unwrap();
    let bounded = sinex_store::material_bytes::parts_from(&mut conn, ids[269], 256, Some(ids[269]))
        .await
        .unwrap();
    assert_eq!((unbounded.len(), bounded.len()), (256, 1));
    let manifest_bytes = |parts: &[(MaterialRef, Option<uuid::Uuid>)]| {
        parts
            .iter()
            .map(|(part, _)| serde_json::to_vec(&part.manifest).unwrap().len())
            .sum::<usize>()
    };
    assert!(manifest_bytes(&unbounded) > 200 * manifest_bytes(&bounded));
    // An unnecessary older manifest must not even be deserialized. The old
    // eager page failed here before restore_memory could stop the reader.
    sqlx::query("UPDATE raw.materials SET manifest=jsonb_set(manifest,'{previous_through}','\"invalid\"') WHERE id=$1")
        .bind(ids[268].0).execute(&mut conn).await.unwrap();
    remember(&mut conn, Some(ids[269])).await;
    let (_stop, stop) = watch::channel(false);
    let mut reader = Restoring::new(ids[269], false);
    restore_reader(&mut conn, &blobs, &mut reader, "memory-cut", None, &stop)
        .await
        .unwrap();
    assert_eq!(reader.memory, Some((ids[269], Some(5))));
    assert!(reader.seen.is_empty());
    assert!(reader.finished);
    // Needed malformed history still refuses rather than hiding corruption.
    let mut reader = Restoring::new(ids[269], true);
    assert!(
        restore_reader(&mut conn, &blobs, &mut reader, "memory-cut", None, &stop)
            .await
            .is_err()
    );
}

#[devkit::test]
async fn memory_anchor_keeps_older_locations_and_admitted_bounds_across_pages(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let blob = blobs.put(b"synthetic body bytes").unwrap();
    let ids = history(&mut conn, &blob).await;
    let (_stop, stop) = watch::channel(false);
    let expected = (0..270)
        .rev()
        .map(|index| {
            (
                ids[index],
                if index == 269 {
                    5
                } else {
                    7 + (index as u64 + 1) % 3
                },
            )
        })
        .collect::<Vec<_>>();
    for (anchor, older) in [(Some(ids[3]), true), (None, true), (Some(ids[3]), false)] {
        remember(&mut conn, anchor).await;
        let mut reader = Restoring::new(ids[269], older);
        restore_reader(&mut conn, &blobs, &mut reader, "memory-cut", None, &stop)
            .await
            .unwrap();
        if older {
            assert_eq!(reader.seen, expected);
        } else {
            assert_eq!(reader.seen, expected[..266]);
        }
        assert_eq!(reader.memory, anchor.map(|id| (id, Some(8))));
        assert!(reader.finished);
    }
    // A saved anchor outside this immutable chain cannot truncate it.
    let unrelated = MaterialId::new();
    sqlx::query("INSERT INTO raw.materials(id,blob,length,manifest,parser,parser_version) SELECT $1,blob,20,'{}','synthetic',1 FROM raw.materials WHERE id=$2")
        .bind(unrelated.0).bind(ids[0].0).execute(&mut conn).await.unwrap();
    remember(&mut conn, Some(unrelated)).await;
    let mut reader = Restoring::new(ids[269], false);
    restore_reader(&mut conn, &blobs, &mut reader, "memory-cut", None, &stop)
        .await
        .unwrap();
    assert_eq!(reader.seen, expected);
    assert!(reader.memory.is_none());
    // The cutoff never supplies missing admission evidence or clamps a cursor.
    for cursor in [
        json!({"material":ids[269]}),
        json!({"material":ids[269],"through":21}),
    ] {
        sqlx::query("UPDATE raw.positions SET cursor=$1 WHERE binding='memory-cut'")
            .bind(cursor)
            .execute(&mut conn)
            .await
            .unwrap();
        let mut reader = Restoring::new(ids[269], true);
        assert!(
            restore_reader(&mut conn, &blobs, &mut reader, "memory-cut", None, &stop)
                .await
                .is_err()
        );
    }
    let mut reader = Restoring::new(MaterialId::new(), true);
    assert!(
        restore_reader(&mut conn, &blobs, &mut reader, "memory-cut", None, &stop)
            .await
            .is_err()
    );
}
