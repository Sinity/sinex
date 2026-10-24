use super::*;
use sinex_model::{
    Class, Event, Origin, ReadingOrder, RecordLocation, Timestamp, event_types::CommandExecuted,
};
use sinex_store::sealed;
use std::time::Duration;
fn event(material: MaterialId, native: &str, hash: [u8; 32]) -> Event {
    Event::build(
        &CommandExecuted {
            native_id: native.into(),
            command: "synthetic unrelated value".into(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1).unwrap(),
            duration_ns: None,
            exit_code: None,
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
                anchor: 0,
            },
            ordinal: 0,
        },
        ReadingOrder(1),
        hash,
        Class::Sealed,
    )
    .unwrap()
}
#[tokio::test]
async fn old_naming_only_exclusions_match_new_epoch_keys_hashes_and_unparsed_bytes_after_restart() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let mut conn = sqlx::Connection::connect(&url).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let identity = sealed::Identity::generate();
    let old = sealed::new_key().unwrap();
    let material = MaterialId::new();
    let raw = b"synthetic erased private record\n";
    let original = event(material, "selected-native-id", [9; 32]);
    let names = MaterialSecrets::from_data_key(&old).naming_only();
    let wrapped = sealed::wrap_secrets(&names, &identity.to_public()).unwrap();
    let body = blobs.put_sealed(raw, &old).unwrap();
    let scope = metadata::Scope::Replay {
        source: "s".into(),
        material,
        parser: "p".into(),
        parser_version: 1,
        length: raw.len() as u64,
        whole: true,
        append_end: None,
    };
    let mut ciphertext = Vec::new();
    metadata::seal_replay(
        &mut ciphertext,
        &identity.to_public(),
        &scope,
        [metadata::ErasedRecord {
            start: 0,
            end: raw.len() as u64,
            raw_digest: names.name_raw_record(raw),
            transition: None,
        }],
        || Ok(()),
    )
    .unwrap();
    let replay = blobs.put(&ciphertext).unwrap();
    for (blob, length) in [(&body, raw.len()), (&replay, ciphertext.len())] {
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
            .bind(blob)
            .bind(length as i64)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO raw.materials(id,blob,durable_bytes,manifest,parser,parser_version,erased_replay_blob) VALUES($1,$2,$3,'{}','p',1,$4)").bind(material.0).bind(&body).bind(raw.len() as i64).bind(&replay).execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO privacy.material_keys(material_id,key_ref,class,wrapped_key,role) VALUES($1,'names-only','sealed',$2,'record-naming-only')").bind(material.0).bind(&wrapped).execute(&mut conn).await.unwrap();
    let operation = Uuid::now_v7();
    let scope = serde_json::json!({"material":material,"materials":[material],"sources":["s"],"record_domains_version":1,"record_domains":[{"material":material,"source":"s","naming":"sealed"}]});
    sqlx::query(
        "INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'excise','synthetic',$2)",
    )
    .bind(operation)
    .bind(scope)
    .execute(&mut conn)
    .await
    .unwrap();
    sqlx::query("UPDATE raw.materials SET manifest=jsonb_build_object('excised_whole',$2::text) WHERE id=$1").bind(material.0).bind(operation.to_string()).execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO core.hidden_occurrences(source,event_type,equivalence_key,kind,operation_id) VALUES('s',$1,$2,'excise',$3)").bind(original.event_type).bind(names.name_equivalence(&original.equivalence_key.as_ref().unwrap().0).0).bind(operation).execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO core.excised_digests(source,event_type,record_hash,operation_id) VALUES('s',$1,$2,$3)").bind(original.event_type).bind(names.name_record_hash(&original.record_hash).as_slice()).bind(operation).execute(&mut conn).await.unwrap();
    assert!(sealed::unwrap(&wrapped, &identity).is_err());
    drop(old);
    let authority = Authority::new(identity, Duration::from_secs(60)).unwrap();
    assert!(
        AcquisitionPolicy::load(&mut conn, &blobs, "s", None)
            .await
            .is_err()
    );
    let policy = AcquisitionPolicy::load(&mut conn, &blobs, "s", Some(&authority))
        .await
        .unwrap();
    let fresh = MaterialId::new();
    assert!(
        policy.excludes(fresh, 0, raw, [9; 32], &[]).unwrap(),
        "failed/no-event exact bytes remain excluded"
    );
    assert!(
        policy
            .excludes(
                fresh,
                0,
                b"changed bytes",
                [99; 32],
                &[event(fresh, "selected-native-id", [99; 32])]
            )
            .unwrap(),
        "native keys cross fresh data-key epochs"
    );
    assert!(
        policy
            .excludes(
                fresh,
                0,
                b"changed bytes",
                [9; 32],
                &[event(fresh, "different-native-id", [9; 32])]
            )
            .unwrap()
    );
    assert!(
        !policy
            .excludes(
                fresh,
                0,
                b"unrelated bytes",
                [88; 32],
                &[event(fresh, "different-native-id", [88; 32])]
            )
            .unwrap()
    );
    let opened = crate::retained::open(&mut conn, &blobs, "s", material, Some(&authority))
        .await
        .unwrap();
    assert!(opened.body.is_none());
    let backing = opened.records();
    assert!(backing.bodyless);
    assert!(backing.bytes.read_exact_at(0, &mut [0; 1]).is_err());
    assert!(opened.transitions(0).unwrap().is_some());
    let restarted = AcquisitionPolicy::load(&mut conn, &blobs, "s", Some(&authority))
        .await
        .unwrap();
    assert!(restarted.excludes(fresh, 0, raw, [9; 32], &[]).unwrap());
    sqlx::query("INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'excise','synthetic','{\"sources\":[\"s\"]}')").bind(Uuid::now_v7()).execute(&mut conn).await.unwrap();
    assert!(policy.revalidate(&mut conn).await.is_err());
    assert!(
        AcquisitionPolicy::load(&mut conn, &blobs, "s", Some(&authority))
            .await
            .is_err(),
        "missing legacy domain evidence refuses"
    );
    authority.revoke();
    assert!(restarted.excludes(fresh, 0, raw, [9; 32], &[]).is_err());
}

#[tokio::test]
async fn plain_native_exclusion_filters_reacquisition_and_replay_without_unlock() {
    use sinex_sources::{Source, drivers};
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider.sqlite");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE history(id TEXT,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT); INSERT INTO history VALUES('native-1',1700000000000000000,1,0,'synthetic removed value','/','s','h'),('native-2',1700000001000000000,1,0,'synthetic retained value','/','s','h')").unwrap();
    let mut ctx = crate::one_shot::OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let source = sinex_sources::sources::atuin::source();
    let binding = sinex_sources::Binding {
        name: "atuin@h".into(),
        source: source.spec().name.into(),
        parser: source.spec().parser.into(),
        input: path.clone(),
        settings: sinex_sources::Settings::new("h"),
    };
    crate::backfill::record(&mut ctx.read_conn, &binding)
        .await
        .unwrap();
    let driver = sinex_sources::sources::atuin::driver(path, "h".into());
    let mut state = drivers::sqlite_rows::RowState::default();
    let acquired = loop {
        match driver.poll(&serde_json::Value::Null, &mut state) {
            Err(drivers::DriverError::Pending) => continue,
            r => break r.unwrap(),
        }
    };
    let sink = crate::one_shot::Sink(ctx.blobs.clone());
    for batch in drivers::batches(
        &source,
        &binding.name,
        sinex_model::RunId::new(),
        acquired,
        serde_json::Value::Null,
        &sink,
        sinex_sources::Limits::default(),
    )
    .unwrap()
    {
        ctx.writer
            .submit(crate::writer::Priority::Bulk, batch)
            .await
            .unwrap();
    }
    let selected: Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE payload->>'native_id'='native-1'")
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    crate::excision::excise_with(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        excision::Target::Event(sinex_model::EventId(selected)),
        "synthetic",
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE history SET cwd='/updated-private-stage-control' WHERE id='native-2'",
        [],
    )
    .unwrap();
    let policy = AcquisitionPolicy::load(&mut ctx.read_conn, &ctx.blobs, source.spec().name, None)
        .await
        .unwrap();
    let mut restarted = drivers::sqlite_rows::RowState::default();
    let acquired = loop {
        match driver.poll(&serde_json::Value::Null, &mut restarted) {
            Err(drivers::DriverError::Pending) => continue,
            r => break r.unwrap(),
        }
    };
    let filtered = policy
        .reconcile(
            &mut ctx.read_conn,
            &ctx.blobs,
            &source,
            acquired,
            &serde_json::Value::Null,
            None,
        )
        .await
        .unwrap();
    let filtered = policy
        .filter(
            &ctx.blobs,
            &source,
            filtered,
            &serde_json::Value::Null,
            source.spec().class,
            None,
            &publication::StagingSink::new(ctx.blobs.clone()),
        )
        .unwrap();
    assert!(
        filtered.publication.is_none() && filtered.erased.is_none(),
        "stateless plain rows need no sealed transition envelope"
    );
    let material = filtered.acquired.material.as_ref().unwrap();
    let id = material.id;
    let Some(drivers::StoredBytes::FramedFile(stored)) = &material.stored else {
        panic!("filtered body must stay in private custody");
    };
    let mut retained = Vec::new();
    std::io::Read::read_to_end(&mut &**stored, &mut retained).unwrap();
    let name = format!("blake3:{}", blake3::hash(&retained));
    assert!(
        !ctx.blobs.contains(&name).unwrap(),
        "filtering published a plain CAS name before reservation"
    );
    assert!(!String::from_utf8_lossy(&retained).contains("synthetic removed value"));
    assert!(String::from_utf8_lossy(&retained).contains("synthetic retained value"));
    let prepared = drivers::prepare(
        &source,
        &binding.name,
        sinex_model::RunId::new(),
        filtered.acquired,
        serde_json::Value::Null,
        &sink,
        sinex_sources::Limits::default(),
    )
    .unwrap()
    .with_excision_basis(policy.basis.clone());
    for batch in prepared.batches().unwrap() {
        ctx.writer
            .submit(crate::writer::Priority::Bulk, batch.unwrap())
            .await
            .unwrap();
    }
    let body: String = sqlx::query_scalar("SELECT blob FROM raw.materials WHERE id=$1")
        .bind(id.0)
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(ctx.blobs.read(&body).unwrap(), retained);
    crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                binding: Some(binding.name),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            authority: None,
        },
    )
    .await
    .unwrap();
    let commands:Vec<String>=sqlx::query_scalar("SELECT e.payload->>'command' FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.source=$1 ORDER BY e.payload->>'native_id'").bind(source.spec().name).fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(commands, ["synthetic retained value"]);
    let tombstone: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.event_tombstones WHERE event_id=$1)")
            .bind(selected)
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    assert!(tombstone);
    ctx.close().await.unwrap();
}
