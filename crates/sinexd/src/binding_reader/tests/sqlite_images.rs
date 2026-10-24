use super::*;
use sinex_sources::sources::atuin;

#[tokio::test]
async fn whole_database_image_class_checks_other_bindings_before_opening_provider_data() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let provider = rusqlite::Connection::open(&path).unwrap();
    provider.execute_batch("CREATE TABLE history(id TEXT,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT);").unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        sinex_sources::sources::activitywatch::SPEC.name,
        Class::Sealed,
        "synthetic",
    )
    .await
    .unwrap();
    let mut settings = Settings::new("synthetic");
    settings.options =
        json!({"sqlite_images":{"scope":"whole_database","class":"open","interval_secs":3600}});
    let binding = Binding::of(&atuin::SPEC, &path, settings);
    let mut b = Bound::new(&atuin::KIND, binding).unwrap();
    let alias = dir.path().join("alias.db");
    std::fs::hard_link(&path, &alias).unwrap();
    let known = [sinex_store::bindings::Stored {
        binding: "other-selected-table".into(),
        source: sinex_sources::sources::activitywatch::SPEC.name.into(),
        input: alias.display().to_string(),
        settings: serde_json::to_value(&b.binding.settings).unwrap(),
    }];
    let err = check_image_class(&mut ctx.read_conn, &mut b, &known)
        .await
        .unwrap_err();
    assert!(err.contains("source/field class"));
    // The check itself did not create a pending read view or material.
    b.binding.settings.options["sqlite_images"]["class"] = json!("sealed");
    b.reader.configure(&b.binding.settings).unwrap();
    check_image_class(&mut ctx.read_conn, &mut b, &known)
        .await
        .unwrap();
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn native_pipeline_recovers_frozen_plain_and_sealed_image_publication_after_first_admission_refusal()
 {
    use sqlx::Executor;
    for class in [Class::Open, Class::Sealed] {
        let (_db, url, _) = crate::testdb::fresh().await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.db");
        let provider = rusqlite::Connection::open(&path).unwrap();
        provider.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE history(id TEXT,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT); INSERT INTO history VALUES('a',1700000000000000000,1,0,'older unrelated baseline','/','s','h')").unwrap();
        let authority = crate::sealed_access::Authority::new(
            sinex_store::sealed::Identity::generate(),
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let recipient = authority.recipient();
        let access = Access {
            recipient: Some(&recipient),
            authority: Some(&authority),
        };
        let mut settings = Settings::new("synthetic");
        settings.options = json!({"sqlite_images":{"scope":"whole_database","class":class,"interval_secs":3600,"turn_pages":1}});
        let binding = Binding::of(&atuin::SPEC, &path, settings);
        let name = binding.name.clone();
        let mut b = Bound::new(&atuin::KIND, binding.clone()).unwrap();
        b.history_floor = class;
        let mut ctx = OneShot::open(&url, &dir.path().join("store"))
            .await
            .unwrap();
        if class == Class::Sealed {
            sinex_store::privacy::set_source_class(
                &mut ctx.read_conn,
                atuin::SPEC.name,
                class,
                "synthetic",
            )
            .await
            .unwrap();
        }
        let mut older = Binding::of(&atuin::SPEC, &path, Settings::new("synthetic"));
        older.name = "older-row-binding".into();
        let sink = Sink(ctx.blobs.clone());
        let (_stop, stop) = watch::channel(false);
        read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut [Bound::new(&atuin::KIND, older).unwrap()],
            stop.clone(),
            access,
        )
        .await
        .into_iter()
        .next()
        .unwrap()
        .1
        .unwrap();
        let older_material: uuid::Uuid =
            sqlx::query_scalar("SELECT material_id FROM core.events LIMIT 1")
                .fetch_one(&mut ctx.read_conn)
                .await
                .unwrap();
        provider
            .execute("UPDATE history SET command='original retained version'", [])
            .unwrap();
        ctx.read_conn.execute("CREATE FUNCTION public.reject_native_position() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic first admission refusal'; END $$; CREATE TRIGGER reject_native_position BEFORE INSERT ON raw.positions FOR EACH ROW EXECUTE FUNCTION public.reject_native_position()").await.unwrap();
        let sink = Sink(ctx.blobs.clone());
        let (_stop, stop) = watch::channel(false);
        let result = read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut [b],
            stop.clone(),
            access,
        )
        .await;
        assert!(
            result[0]
                .1
                .as_ref()
                .err()
                .unwrap()
                .contains("synthetic first admission refusal"),
            "{:?}",
            result[0].1.as_ref().map(|t| t.to_json())
        );
        let ids = sinex_store::native_materials::binding_pending(&mut ctx.read_conn, &name)
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);
        let frozen = sinex_store::native_materials::load(&mut ctx.read_conn, ids[0])
            .await
            .unwrap()
            .unwrap();
        let original = frozen.progress.material.as_ref().unwrap().clone();
        let image = frozen
            .progress
            .image
            .as_ref()
            .expect("installed image costs were frozen before refused admission")
            .clone();
        assert!(ctx.blobs.contains(&original.blob).unwrap());
        let refusal = sinex_store::excision::plan(
            &mut ctx.read_conn,
            sinex_store::excision::Target::Material(sinex_model::MaterialId(older_material)),
        )
        .await
        .unwrap_err();
        assert!(
            refusal.to_string().contains("pending SQLite image"),
            "{refusal}"
        );

        assert!(
            sinex_store::positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .is_none()
        );
        provider
            .execute(
                "UPDATE history SET command='provider edited after failed admission'",
                [],
            )
            .unwrap();
        ctx.read_conn.execute("DROP TRIGGER reject_native_position ON raw.positions; DROP FUNCTION public.reject_native_position()").await.unwrap();
        ctx.close().await.unwrap();
        let mut ctx = OneShot::open(&url, &dir.path().join("store"))
            .await
            .unwrap();
        let sink = Sink(ctx.blobs.clone());
        let result = read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut [Bound::new(&atuin::KIND, binding).unwrap()],
            stop,
            access,
        )
        .await;
        result.into_iter().next().unwrap().1.unwrap();
        assert!(
            sinex_store::native_materials::binding_pending(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .is_empty()
        );
        let ready = sinex_store::material_bytes::reference(&mut ctx.read_conn, original.id)
            .await
            .unwrap()
            .unwrap();
        let mut expected = original.clone();
        expected.input = ready.input;
        assert!(expected.input.is_some());
        assert_eq!(
            ready, expected,
            "provider edits replaced the frozen first version"
        );
        let loaded = sinex_store::sqlite_images::load(&mut ctx.read_conn, image.material.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.0, image.metadata);
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM core.events WHERE material_id=$1")
                .bind(original.id.0)
                .fetch_one(&mut ctx.read_conn)
                .await
                .unwrap();
        assert_eq!(count, 1, "original retained row was never admitted");
        let opened = crate::retained::open(
            &mut ctx.read_conn,
            &ctx.blobs,
            atuin::SPEC.name,
            original.id,
            Some(&authority),
        )
        .await
        .unwrap();
        let records = opened.records();
        let mut bytes = vec![0; original.length as usize];
        records.bytes.read_exact_at(0, &mut bytes).unwrap();
        assert!(
            String::from_utf8(bytes)
                .unwrap()
                .contains("original retained version")
        );
        ctx.close().await.unwrap();
    }
}
