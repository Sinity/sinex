//! Normal material turns share the same native driver, admission and ACK path
//! as explicit EOF reads. The first source can keep producing indefinitely.
use super::*;
use sinex_source_core::drivers::DriverError;
use sinex_source_core::drivers::live_stream::{LiveStream, Rotation, SpoolWriter};
use sinex_sources::sources::journald;
use sqlx::Executor;
use std::sync::atomic::{AtomicUsize, Ordering};

fn line(n: usize) -> Vec<u8> {
    serde_json::to_vec(&json!({"__CURSOR":format!("synthetic-{n}"),"__REALTIME_TIMESTAMP":"1700000000000000","MESSAGE":format!("synthetic message {n}")})).unwrap()
}
fn capture(path: &std::path::Path, count: usize) -> SpoolWriter {
    let mut writer = SpoolWriter::open(
        path,
        Rotation {
            max_bytes: 1,
            max_age: std::time::Duration::from_secs(3600),
        },
    )
    .unwrap();
    for i in 0..count {
        writer
            .append(sinex_model::Timestamp::now(), &line(i))
            .unwrap();
    }
    writer.sync().unwrap();
    writer
}
fn binding(path: &std::path::Path) -> Binding {
    Binding::of(&journald::SPEC, path, Settings::new("synthetic"))
}
async fn round(
    ctx: &mut ReaderContext,
    readers: &mut [BindingReader],
    rounds: &mut Rounds,
    mode: crate::stage::ReadMode,
) -> Vec<(String, Result<Totals>)> {
    let (_stop, stop) = watch::channel(false);
    read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        &Sink(ctx.blobs.clone()),
        readers,
        rounds,
        stop,
        Access::default(),
        mode,
        &mut |_, _| {},
    )
    .await
}
struct Growing {
    inner: LiveStream,
    capture: SpoolWriter,
    polls: Arc<AtomicUsize>,
    allowed: Arc<AtomicUsize>,
    acked: Arc<AtomicUsize>,
}
impl Reader for Growing {
    fn poll(&mut self, cursor: &Value) -> Result<drivers::Acquired, DriverError> {
        let poll = self.polls.fetch_add(1, Ordering::SeqCst) + 1;
        if poll > self.allowed.load(Ordering::SeqCst) {
            return Err(DriverError::Read(
                "polled again before the next binding's turn".into(),
            ));
        }
        let acquired = self.inner.poll(cursor)?;
        assert!(acquired.material.is_some());
        self.capture
            .append(sinex_model::Timestamp::now(), &line(poll))
            .unwrap();
        self.capture.sync().unwrap();
        Ok(acquired)
    }
    fn committed(&mut self, cursor: &Value) -> Result<usize, DriverError> {
        let removed = self.inner.committed(cursor)?;
        self.acked
            .store(self.polls.load(Ordering::SeqCst), Ordering::SeqCst);
        Ok(removed)
    }
}
struct AfterAck {
    inner: LiveStream,
    polls: Arc<AtomicUsize>,
    first_polls: Arc<AtomicUsize>,
    first_acked: Arc<AtomicUsize>,
}
impl Reader for AfterAck {
    fn poll(&mut self, cursor: &Value) -> Result<drivers::Acquired, DriverError> {
        assert_eq!(
            self.first_polls.load(Ordering::SeqCst),
            self.first_acked.load(Ordering::SeqCst),
            "the next binding cannot precede the first poll's committed acknowledgement"
        );
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.inner.poll(cursor)
    }
    fn committed(&mut self, cursor: &Value) -> Result<usize, DriverError> {
        self.inner.committed(cursor)
    }
}
async fn originals(ctx: &mut ReaderContext, name: &str) -> Vec<(uuid::Uuid, Vec<u8>)> {
    let rows:Vec<(uuid::Uuid,String)>=sqlx::query_as("SELECT m.id,m.blob FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.binding=$1 ORDER BY m.part_index")
        .bind(name).fetch_all(&mut ctx.read_conn).await.unwrap();
    rows.into_iter()
        .map(|(id, blob)| (id, ctx.blobs.read(&blob).unwrap()))
        .collect()
}
#[devkit::test]
async fn normal_turn_visits_second_binding_while_first_always_has_more(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first");
    let second = dir.path().join("second");
    let growing = capture(&first, 1);
    let _second = capture(&second, 1);
    let original_first = std::fs::read(first.join("000000000000.ndjson")).unwrap();
    let original_second = std::fs::read(second.join("000000000000.ndjson")).unwrap();
    let first_binding = binding(&first);
    let second_binding = binding(&second);
    let polls = Arc::new(AtomicUsize::new(0));
    let allowed = Arc::new(AtomicUsize::new(1));
    let acked = Arc::new(AtomicUsize::new(0));
    let next_polls = Arc::new(AtomicUsize::new(0));
    let mut readers = vec![
        BindingReader::with_reading(
            &journald::FORMAT,
            first_binding.clone(),
            sinex_source_core::Reading::Polled(Box::new(Growing {
                inner: journald::driver(first.clone(), "synthetic".into(), None),
                capture: growing,
                polls: polls.clone(),
                allowed: allowed.clone(),
                acked: acked.clone(),
            })),
        )
        .unwrap(),
        BindingReader::with_reading(
            &journald::FORMAT,
            second_binding.clone(),
            sinex_source_core::Reading::Polled(Box::new(AfterAck {
                inner: journald::driver(second.clone(), "synthetic".into(), None),
                polls: next_polls.clone(),
                first_polls: polls.clone(),
                first_acked: acked.clone(),
            })),
        )
        .unwrap(),
    ];
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("UTC", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    let mut rounds = Rounds::default();
    for turn in 1..=3 {
        allowed.store(turn, Ordering::SeqCst);
        for (_, result) in round(
            &mut ctx,
            &mut readers,
            &mut rounds,
            crate::stage::ReadMode::Once,
        )
        .await
        {
            result.unwrap();
        }
        assert_eq!(polls.load(Ordering::SeqCst), turn);
        assert_eq!(acked.load(Ordering::SeqCst), turn);
        assert_eq!(next_polls.load(Ordering::SeqCst), turn);
        assert!(
            sinex_store::positions::load(&mut ctx.read_conn, &second_binding.name)
                .await
                .unwrap()
                .is_some()
        );
    }
    let saved = originals(&mut ctx, &first_binding.name).await;
    assert_eq!(saved.len(), 3);
    assert_eq!(saved[0].1, original_first);
    assert_eq!(
        originals(&mut ctx, &second_binding.name).await[0].1,
        original_second
    );
    rounds.end(&mut ctx.read_conn).await.unwrap();
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn normal_turn_retry_and_restart_keep_originals_and_committed_cursor(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let _capture = capture(&spool, 2);
    let original = std::fs::read(spool.join("000000000000.ndjson")).unwrap();
    let b = binding(&spool);
    let store = dir.path().join("store");
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &store,
        sinex_model::time_settings::TimeSettings::new("UTC", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    ctx.read_conn.execute("CREATE FUNCTION core.synthetic_retry() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic transient admission failure' USING ERRCODE='40001'; END $$; CREATE TRIGGER synthetic_retry BEFORE INSERT ON core.events FOR EACH ROW EXECUTE FUNCTION core.synthetic_retry()")
        .await.unwrap();
    let mut readers = vec![BindingReader::new(&journald::FORMAT, b.clone()).unwrap()];
    let mut rounds = Rounds::default();
    let failed = round(
        &mut ctx,
        &mut readers,
        &mut rounds,
        crate::stage::ReadMode::Once,
    )
    .await;
    assert!(failed.iter().any(|(_, r)| r.is_err()));
    assert!(
        sinex_store::positions::load(&mut ctx.read_conn, &b.name)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        std::fs::read(spool.join("000000000000.ndjson")).unwrap(),
        original
    );
    ctx.read_conn
        .execute(
            "DROP TRIGGER synthetic_retry ON core.events; DROP FUNCTION core.synthetic_retry()",
        )
        .await
        .unwrap();
    for (_, r) in round(
        &mut ctx,
        &mut readers,
        &mut rounds,
        crate::stage::ReadMode::Once,
    )
    .await
    {
        r.unwrap();
    }
    let first = originals(&mut ctx, &b.name).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].1, original);
    let before = sinex_store::positions::load(&mut ctx.read_conn, &b.name)
        .await
        .unwrap()
        .unwrap()
        .0;
    rounds.end(&mut ctx.read_conn).await.unwrap();
    ctx.close().await.unwrap();
    drop(readers);
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &store,
        sinex_model::time_settings::TimeSettings::new("UTC", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        sinex_store::positions::load(&mut ctx.read_conn, &b.name)
            .await
            .unwrap()
            .unwrap()
            .0,
        before
    );
    let mut readers = vec![BindingReader::new(&journald::FORMAT, b.clone()).unwrap()];
    let mut rounds = Rounds::default();
    for (_, r) in round(
        &mut ctx,
        &mut readers,
        &mut rounds,
        crate::stage::ReadMode::Once,
    )
    .await
    {
        r.unwrap();
    }
    let after = originals(&mut ctx, &b.name).await;
    assert_eq!(after.len(), 2);
    assert_eq!(after[0], first[0]);
    assert_ne!(
        sinex_store::positions::load(&mut ctx.read_conn, &b.name)
            .await
            .unwrap()
            .unwrap()
            .0,
        before
    );
    rounds.end(&mut ctx.read_conn).await.unwrap();
    ctx.close().await.unwrap();
}

struct Parts {
    driver: sinex_source_core::drivers::append_file::AppendFile,
    state: sinex_source_core::drivers::append_file::AppendState,
    bytes: u64,
    polls: Arc<AtomicUsize>,
}
impl Reader for Parts {
    fn poll(&mut self, cursor: &Value) -> Result<drivers::Acquired, DriverError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.driver
            .poll_in_parts(cursor, &mut self.state, self.bytes)
    }
}
#[devkit::test]
async fn normal_append_turn_settles_only_after_observing_eof(db: devkit::pg::TestDatabase) {
    use sinex_sources::sources::zsh;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    let first = b": 1700000000:0;echo first\n";
    let second = b": 1700000001:0;echo second\n";
    std::fs::write(&path, [first.as_slice(), second.as_slice()].concat()).unwrap();
    let b = Binding::of(&zsh::SPEC, &path, Settings::new("synthetic"));
    let name = b.name.clone();
    let polls = Arc::new(AtomicUsize::new(0));
    let reader = Parts {
        driver: zsh::driver(path, "synthetic".into()),
        state: Default::default(),
        bytes: first.len() as u64,
        polls: polls.clone(),
    };
    let mut readers = vec![
        BindingReader::with_reading(
            &zsh::FORMAT,
            b,
            sinex_source_core::Reading::Polled(Box::new(reader)),
        )
        .unwrap(),
    ];
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("UTC", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    let mut rounds = Rounds::default();
    for wanted in [first.len(), first.len() + second.len()] {
        for (_, r) in round(
            &mut ctx,
            &mut readers,
            &mut rounds,
            crate::stage::ReadMode::Once,
        )
        .await
        {
            r.unwrap();
        }
        let cursor = sinex_store::positions::load(&mut ctx.read_conn, &name)
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(cursor["offset"], json!(wanted));
        assert!(
            readers[0].settled.is_none(),
            "a material turn is not an EOF observation"
        );
    }
    for _ in 0..2 {
        for (_, r) in round(
            &mut ctx,
            &mut readers,
            &mut rounds,
            crate::stage::ReadMode::Once,
        )
        .await
        {
            r.unwrap();
        }
    }
    assert_eq!(
        polls.load(Ordering::SeqCst),
        3,
        "the fourth unchanged round can skip only after the third poll observed EOF"
    );
    rounds.end(&mut ctx.read_conn).await.unwrap();
    ctx.close().await.unwrap();
}
