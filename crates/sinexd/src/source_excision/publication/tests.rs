use super::*;
use sinex_model::{BatchKind, RunId};
use sinex_sources::{drivers, sources::atuin};
use sqlx::Executor;

#[tokio::test]
async fn failed_first_native_admission_keeps_exact_installed_custody_after_provider_edit_and_restart()
 {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider.sqlite");
    let provider = rusqlite::Connection::open(&path).unwrap();
    provider.execute_batch("CREATE TABLE history(id TEXT,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT); INSERT INTO history VALUES('native-1',1700000000000000000,1,0,'original synthetic version','/','s','h')").unwrap();
    let mut ctx = crate::one_shot::OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    sinex_store::bindings::save(
        &mut ctx.read_conn,
        &sinex_store::bindings::Stored {
            binding: "atuin@h".into(),
            source: atuin::SPEC.name.into(),
            input: path.display().to_string(),
            settings: serde_json::json!({"host":"h"}),
        },
    )
    .await
    .unwrap();
    let source = atuin::source();
    let driver = atuin::driver(path.clone(), "h".into());
    let mut state = drivers::sqlite_rows::RowState::default();
    let acquired = loop {
        match driver.poll(&serde_json::Value::Null, &mut state) {
            Err(drivers::DriverError::Pending) => continue,
            result => break result.unwrap(),
        }
    };
    let m = acquired.material.as_ref().unwrap();
    let start_cursor = (m.cursor_at)(0).unwrap();
    let end_cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
    let sink = StagingSink::new(ctx.blobs.clone());
    let guard = ctx.writer.source_guard(atuin::SPEC.name).await.unwrap();
    let permit = ctx
        .writer
        .reserve_native(crate::writer::Priority::Bulk)
        .await
        .unwrap();
    let basis = sinex_store::excision::operations_basis(&mut ctx.read_conn, atuin::SPEC.name)
        .await
        .unwrap();
    let prepared = drivers::prepare(
        &source,
        "atuin@h",
        RunId::new(),
        acquired,
        serde_json::Value::Null,
        &sink,
        sinex_sources::Limits::default(),
    )
    .unwrap()
    .with_excision_basis(basis);
    let batch = prepared.batches().unwrap().next().unwrap().unwrap();
    let BatchKind::Source(progress) = &batch.kind else {
        panic!()
    };
    let material = progress.material.as_ref().unwrap().clone();
    assert!(
        !ctx.blobs.contains(&material.blob).unwrap(),
        "preparation cannot publish readable bytes"
    );
    let pending = native_materials::Pending {
        material: material.id,
        source: atuin::SPEC.name.into(),
        binding: "atuin@h".into(),
        progress: (**progress).clone(),
        image: None,
        base_cursor: serde_json::Value::Null,
        base_carry: serde_json::Value::Null,
        start_cursor,
        end_cursor,
        state: native_materials::State::Reserved,
    };
    ctx.read_conn.execute("CREATE FUNCTION public.reject_native_position() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic refusal'; END $$; CREATE TRIGGER reject_native_position BEFORE INSERT ON raw.positions FOR EACH ROW EXECUTE FUNCTION public.reject_native_position()").await.unwrap();
    let publication = Publication {
        pending,
        stages: sink.take().unwrap(),
        image: None,
        batch,
        permit,
        guard,
    };
    let error = publication.start().await.unwrap().unwrap_err();
    assert!(error.to_string().contains("synthetic refusal"), "{error}");
    let pending = native_materials::load(&mut ctx.read_conn, material.id)
        .await
        .unwrap()
        .unwrap();
    assert!(pending.state == native_materials::State::Installed);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raw.materials")
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap(),
        0
    );
    assert!(
        sinex_store::positions::load(&mut ctx.read_conn, "atuin@h")
            .await
            .unwrap()
            .is_none()
    );
    // Another source's purge may mark a shared name before this pending
    // source's first admission. Its exact installed version still owns it.
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'deleting') ON CONFLICT(hash) DO UPDATE SET status='deleting'")
        .bind(&material.blob).bind(material.length as i64).execute(&mut ctx.read_conn).await.unwrap();
    let (removed, failed) = sinex_store::excision::finish_deletions(&mut ctx.read_conn, &ctx.blobs)
        .await
        .unwrap();
    assert!(
        removed.is_empty(),
        "cleanup removed another source's pending version"
    );
    assert!(failed.is_empty());
    let original = ctx.blobs.read(&material.blob).unwrap();
    assert_eq!(
        ctx.blobs.put(&original).unwrap(),
        material.blob,
        "shared CAS users retain the same representation"
    );
    provider
        .execute("UPDATE history SET command='edited provider version'", [])
        .unwrap();
    drop(prepared);
    drop(driver);
    drop(state);
    ctx.read_conn.execute("DROP TRIGGER reject_native_position ON raw.positions; DROP FUNCTION public.reject_native_position()").await.unwrap();
    ctx.close().await.unwrap();
    let mut ctx = crate::one_shot::OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let pending = native_materials::load(&mut ctx.read_conn, material.id)
        .await
        .unwrap()
        .unwrap();
    let mut file = tempfile::tempfile().unwrap();
    ctx.blobs.copy_to(&material.blob, &mut file).unwrap();
    let retained = drivers::RetainedRecords {
        bodyless: false,
        blob: material.blob.clone(),
        bytes: std::sync::Arc::new(crate::retained::Retained::new(file, material.length)),
    };
    let mut state = drivers::sqlite_rows::RowState::default();
    state
        .restore_pending(material.clone(), retained, &pending.start_cursor)
        .unwrap();
    let driver = atuin::driver(path, "h".into());
    let acquired = driver.poll(&pending.start_cursor, &mut state).unwrap();
    assert_eq!(acquired.material.as_ref().unwrap().id, material.id);
    let sink = crate::one_shot::Sink(ctx.blobs.clone());
    let prepared = drivers::prepare(
        &source,
        "atuin@h",
        RunId::new(),
        acquired,
        serde_json::Value::Null,
        &sink,
        sinex_sources::Limits::default(),
    )
    .unwrap()
    .with_excision_basis(pending.progress.excision_basis.clone().unwrap());
    let mut batch = prepared.batches().unwrap().next().unwrap().unwrap();
    if let BatchKind::Source(p) = &mut batch.kind {
        **p = pending
            .first_progress(p.cursor.clone(), p.carry.clone())
            .unwrap();
    }
    let guard = ctx.writer.source_guard(atuin::SPEC.name).await.unwrap();
    ctx.writer
        .reserve_native(crate::writer::Priority::Bulk)
        .await
        .unwrap()
        .admit(batch, guard)
        .await
        .unwrap();
    assert!(
        native_materials::load(&mut ctx.read_conn, material.id)
            .await
            .unwrap()
            .is_none()
    );
    let payload: serde_json::Value = sqlx::query_scalar("SELECT payload FROM core.events")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(payload["command"], "original synthetic version");
    assert_eq!(ctx.blobs.read(&material.blob).unwrap(), original);
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn queued_native_work_keeps_source_guard_after_waiter_cancellation_and_releases_on_refusal() {
    let (_db, url, mut witness) = crate::testdb::fresh().await;
    let (handle, queues) = crate::writer::channels_for(&url);
    let guard = handle.source_guard("synthetic-source").await.unwrap();
    let permit = handle
        .reserve_native(crate::writer::Priority::Bulk)
        .await
        .unwrap();
    let batch = sinex_model::Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: Vec::new(),
        archive: Vec::new(),
    };
    let waiter = tokio::spawn(permit.admit(batch, guard));
    // Enqueue before cancelling the caller, with no Writer running yet.
    tokio::task::yield_now().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let mut exclusive = witness.begin().await.unwrap();
    exclusive
        .execute("SET LOCAL lock_timeout='40ms'")
        .await
        .unwrap();
    let error = sinex_store::excision::lock_source(&mut exclusive, "synthetic-source", false)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("lock timeout"), "{error}");
    exclusive.rollback().await.unwrap();
    // Other sources remain independent while this source waits in the queue.
    let mut unrelated = witness.begin().await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sinex_store::excision::lock_source(&mut unrelated, "another-source", false),
    )
    .await
    .unwrap()
    .unwrap();
    unrelated.rollback().await.unwrap();
    let writer = tokio::spawn(crate::writer::run(
        url.clone(),
        sqlx::PgConnection::connect(&url).await.unwrap(),
        queues,
        std::sync::Arc::new(sinex_store::NoDerivations),
    ));
    drop(handle);
    writer.await.unwrap();
    let mut exclusive = witness.begin().await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sinex_store::excision::lock_source(&mut exclusive, "synthetic-source", false),
    )
    .await
    .unwrap()
    .unwrap();
    exclusive.rollback().await.unwrap();
}

#[tokio::test]
async fn actual_sealed_append_first_chunk_and_restart_finish_original_bytes_and_duplicate_identity()
{
    use sinex_sources::{Source, binding, sources::zsh};
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    let original = b": 1700000000:0;duplicate\n: 1700000000:1;duplicate\n";
    std::fs::write(&path, original).unwrap();
    let mut ctx = crate::one_shot::OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let source = zsh::source("h".into());
    let name = source.spec().name;
    sinex_store::bindings::save(
        &mut ctx.read_conn,
        &sinex_store::bindings::Stored {
            binding: "history@h".into(),
            source: name.into(),
            input: path.display().to_string(),
            settings: serde_json::json!({"host":"h"}),
        },
    )
    .await
    .unwrap();
    let identity = sinex_store::sealed::Identity::generate();
    let authority =
        crate::sealed_access::Authority::new(identity, std::time::Duration::from_secs(60)).unwrap();
    let sealing = crate::one_shot::SealingSink::new(ctx.blobs.clone(), authority.recipient());
    let mut reader = binding::appended(zsh::driver(path.clone(), "h".into()), source.spec().framer);
    let acquired = reader.poll(&serde_json::Value::Null).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let start = (m.cursor_at)(0).unwrap();
    let final_cursor = (m.cursor_at)(original.len()).unwrap();
    let observation = acquired.observation.clone();
    let input = acquired.input.clone();
    let revision = acquired.revision;
    let basis = sinex_store::excision::operations_basis(&mut ctx.read_conn, name)
        .await
        .unwrap();
    let prepared = drivers::prepare(
        &source,
        "history@h",
        RunId::new(),
        acquired,
        serde_json::Value::Null,
        &sealing,
        sinex_sources::Limits {
            max_events: 1,
            max_bytes: 1024,
        },
    )
    .unwrap()
    .with_excision_basis(basis.clone());
    let mut first = [prepared.batches().unwrap().next().unwrap().unwrap()];
    sealing.seal(&mut first).unwrap();
    let [first] = first;
    let BatchKind::Source(progress) = &first.kind else {
        panic!()
    };
    let material = progress.material.as_ref().unwrap().clone();
    let first_cursor = progress.cursor.clone();
    assert!(first_cursor["through"].as_u64().unwrap() < material.length);
    let publication = Publication {
        pending: native_materials::Pending {
            material: material.id,
            source: name.into(),
            binding: "history@h".into(),
            progress: (**progress).clone(),
            image: None,
            base_cursor: serde_json::Value::Null,
            base_carry: serde_json::Value::Null,
            start_cursor: start,
            end_cursor: final_cursor.clone(),
            state: native_materials::State::Reserved,
        },
        stages: Vec::new(),
        image: None,
        batch: first,
        permit: ctx
            .writer
            .reserve_native(crate::writer::Priority::Bulk)
            .await
            .unwrap(),
        guard: ctx.writer.source_guard(name).await.unwrap(),
    };
    publication.start().await.unwrap().unwrap();
    drop(prepared);
    drop(reader);
    drop(sealing);
    std::fs::write(&path, b": 1700000001:0;edited-provider-value\n").unwrap();
    ctx.close().await.unwrap();
    let mut ctx = crate::one_shot::OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let opened = crate::retained::open(
        &mut ctx.read_conn,
        &ctx.blobs,
        name,
        material.id,
        Some(&authority),
    )
    .await
    .unwrap();
    let records = opened
        .body
        .as_ref()
        .unwrap()
        .records(&opened.reference.manifest, source.spec().framer)
        .unwrap();
    let mut reader = binding::appended(zsh::driver(path.clone(), "h".into()), source.spec().framer);
    reader
        .restore_append(binding::AppendResume {
            reference: opened.reference.clone(),
            stored: opened.records(),
            records,
            erased: None,
            archived_end: None,
            erased_members: Vec::new(),
            base_cursor: serde_json::Value::Null,
            cursor: first_cursor.clone(),
            head: Vec::new(),
            input,
            observation,
            revision,
        })
        .unwrap();
    let acquired = reader.poll(&first_cursor).unwrap();
    assert_eq!(acquired.material.as_ref().unwrap().id, material.id);
    let sealing = crate::one_shot::SealingSink::new(ctx.blobs.clone(), authority.recipient());
    let wrapped = sqlx::query_as::<_, (String, Vec<u8>)>(
        "SELECT key_ref,wrapped_key FROM privacy.material_keys WHERE material_id=$1",
    )
    .bind(material.id.0)
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    sealing
        .restore_reserved(
            material.blob.clone(),
            &sinex_model::WrappedMaterialKey {
                material: material.id,
                key_ref: wrapped.0,
                wrapped: wrapped.1,
            },
            &authority,
        )
        .unwrap();
    let (_, carry) = sinex_store::positions::load(&mut ctx.read_conn, "history@h")
        .await
        .unwrap()
        .unwrap();
    let carry = crate::backfill::open_carry(
        &mut ctx.read_conn,
        &source,
        "history@h",
        &first_cursor,
        &carry,
        &sealing,
        Some(&authority),
    )
    .await
    .unwrap();
    let prepared = drivers::prepare(
        &source,
        "history@h",
        RunId::new(),
        acquired,
        carry,
        &sealing,
        sinex_sources::Limits::default(),
    )
    .unwrap()
    .with_excision_basis(basis);
    for batch in prepared.batches().unwrap() {
        let mut batch = [batch.unwrap()];
        sealing.seal(&mut batch).unwrap();
        let BatchKind::Source(p) = &mut batch[0].kind else {
            panic!()
        };
        p.material_keys.clear();
        let [batch] = batch;
        ctx.writer
            .reserve_native(crate::writer::Priority::Bulk)
            .await
            .unwrap()
            .admit(batch, ctx.writer.source_guard(name).await.unwrap())
            .await
            .unwrap();
    }
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "history@h")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor, final_cursor);
    let key = authority
        .material_key(&mut ctx.read_conn, material.id)
        .await
        .unwrap();
    let encrypted: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT payload FROM core.events ORDER BY anchor")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    let decoded = encrypted
        .iter()
        .map(|v| sinex_store::sealed::unseal_payload(v, &key.key).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0]["n"], 0);
    assert_eq!(decoded[1]["n"], 1);
    assert!(decoded.iter().all(|v| v["command"] == "duplicate"));
    let changed = reader.poll(&cursor).unwrap();
    assert_ne!(changed.material.as_ref().unwrap().id, material.id);
    ctx.close().await.unwrap();
}
