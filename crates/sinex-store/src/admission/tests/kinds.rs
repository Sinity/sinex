use super::*;
use sinex_model::{EventPayload, KindDeclaration, KindRef};
use sqlx::Executor;

fn declared(name: &str, version: u64) -> KindDeclaration {
    let mut d = KindDeclaration::schemaless(
        KindRef {
            name: name.into(),
            version,
        },
        "example".into(),
    );
    d.payload_schema = Some(
        json!({"type":"object","properties":{"id":{"type":"string"},"at":{"type":"string","format":"date-time"},"value":{"type":"integer"}},"required":["id","at","value"]}),
    );
    d.key_rule = Some(sinex_model::kinds::KeyRule::Fields {
        fields: vec!["id".into()],
    });
    d.time_rule = Some(sinex_model::kinds::TimeRule::FirstPresent {
        fields: vec!["at".into()],
        quality: sinex_model::TimeQuality::Intrinsic,
    });
    d
}

#[devkit::test]
async fn immutable_registration_checks_the_whole_frame_and_exact_versions(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let d = declared("example.reading", 9);
    let mut tx = conn.begin().await.unwrap();
    let first = sinex_schema::kinds::register(&mut tx, "example", std::slice::from_ref(&d))
        .await
        .unwrap();
    assert_eq!(
        first,
        sinex_schema::kinds::register(&mut tx, "example", std::slice::from_ref(&d))
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    let mut changed = d.clone();
    changed.search = sinex_model::kinds::Search::None;
    let tenth = declared("example.reading", 10);
    let mut tx = conn.begin().await.unwrap();
    assert!(
        sinex_schema::kinds::register(&mut tx, "example", &[tenth.clone(), changed])
            .await
            .unwrap_err()
            .to_string()
            .contains("definition_conflict")
    );
    tx.rollback().await.unwrap();
    assert!(
        sinex_schema::kinds::load(&mut *conn, std::slice::from_ref(&tenth.kind))
            .await
            .unwrap()
            .is_empty()
    );
    let mut tx = conn.begin().await.unwrap();
    sinex_schema::kinds::register(&mut tx, "example", std::slice::from_ref(&tenth))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        sinex_schema::kinds::load(&mut *conn, &[d.kind.clone(), tenth.kind.clone()])
            .await
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        [d.kind, tenth.kind]
    );
    let mut bad = declared("notification.shown", 3);
    bad.declaring_producer = "example".into();
    let mut tx = conn.begin().await.unwrap();
    assert!(
        sinex_schema::kinds::register(&mut tx, "example", &[bad])
            .await
            .unwrap_err()
            .to_string()
            .contains("namespace_refused")
    );
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn external_validation_and_rules_apply_on_every_writer_route(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let d = declared("example.reading", 10);
    let mut tx = conn.begin().await.unwrap();
    sinex_schema::kinds::register(&mut tx, "example", std::slice::from_ref(&d))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for kind in [
        BatchKind::Api {
            actor: "test".into(),
        },
        BatchKind::Replay {
            operation: OperationId::new(),
        },
        BatchKind::Derived {
            derivation: "test".into(),
            scope: json!({}),
        },
    ] {
        let event = Event::submitted(
            d.kind.clone(),
            json!({"id":"one","at":"2026-02-30T00:00:00Z","value":"secret-value"}),
            "example".into(),
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            [0; 32],
            Class::Open,
        );
        let mut batch = batch(vec![event]);
        batch.kind = kind;
        let mut tx = conn.begin().await.unwrap();
        let error = tx
            .admit(&batch, &NoDerivations)
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret-value"));
        tx.rollback().await.unwrap();
    }
    let event = Event::submitted(
        d.kind,
        json!({"id":"one","at":"2026-10-27T12:00:00+02:00","value":1}),
        "example".into(),
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [0; 32],
        Class::Open,
    );
    let out = admit_one(&mut conn, &batch(vec![event])).await;
    assert_eq!(out.admitted, 1);
    let metadata: (i64, String) = sqlx::query_as("SELECT kind_version,validation FROM core.events")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(metadata, (10, "validated".into()));
}

#[devkit::test]
async fn compressed_upgrade_keeps_immutable_rows_and_historical_metadata(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let schema_before = crate::test_schema::catalog(&mut *conn).await;
    admit_one(&mut conn, &batch(vec![keyed("example", 1, 1, 1)])).await;
    conn.execute("INSERT INTO telemetry.events (id,event_type,source,run_id,material_id,anchor,output_index,parents,ts_orig,ts_quality,ts_persisted,commit_seq,payload,equivalence_key,reading_order,record_hash,record_state,class,kind_version,validation) SELECT id,event_type,source,run_id,material_id,anchor,output_index,parents,ts_orig,ts_quality,ts_persisted,commit_seq,payload,equivalence_key,reading_order,record_hash,record_state,class,kind_version,validation FROM core.events")
        .await
        .unwrap();
    let before:Vec<(uuid::Uuid,Value,Vec<u8>)>=sqlx::query_as("SELECT id,payload,record_hash FROM core.events UNION ALL SELECT id,payload,record_hash FROM telemetry.events ORDER BY id").fetch_all(&mut *conn).await.unwrap();
    crate::test_schema::downgrade(&mut *conn).await;
    conn.execute("INSERT INTO registry.event_types (name,payload_schema,key_fields,replace_rule) VALUES ('historic..reading','{}',ARRAY[]::text[],'suppress_repeat')").await.unwrap();
    conn.execute("UPDATE sinex_schema.applied SET version=3")
        .await
        .unwrap();
    conn.execute("SELECT compress_chunk(format('%I.%I',chunk_schema,chunk_name)::regclass) FROM timescaledb_information.chunks WHERE hypertable_schema IN ('core','telemetry') AND hypertable_name='events'").await.unwrap();
    let compressed:i64=sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='events' AND is_compressed").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(compressed, 2);
    assert_eq!(
        sinex_schema::apply(&mut *conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from: 3,
            to: sinex_schema::VERSION
        }
    );
    assert_eq!(crate::test_schema::catalog(&mut *conn).await, schema_before);
    let still_compressed:i64=sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='events' AND is_compressed").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(still_compressed, compressed);
    let after:Vec<(uuid::Uuid,Value,Vec<u8>)>=sqlx::query_as("SELECT id,payload,record_hash FROM core.events UNION ALL SELECT id,payload,record_hash FROM telemetry.events ORDER BY id").fetch_all(&mut *conn).await.unwrap();
    assert_eq!(after, before);
    let metadata:Vec<(i64,String)>=sqlx::query_as("SELECT kind_version,validation FROM core.events UNION ALL SELECT kind_version,validation FROM telemetry.events").fetch_all(&mut *conn).await.unwrap();
    assert_eq!(metadata, vec![(1, "unvalidated".into()); 2]);
    let historical = sinex_schema::kinds::load(
        &mut *conn,
        &[KindRef {
            name: "command.executed".into(),
            version: 1,
        }],
    )
    .await
    .unwrap();
    assert!(
        historical
            .values()
            .all(|d| d.payload_schema.is_none() && d.key_rule.is_none())
    );
    let historical_name = KindRef {
        name: "historic..reading".into(),
        version: 1,
    };
    assert!(
        sinex_schema::kinds::load(&mut *conn, std::slice::from_ref(&historical_name))
            .await
            .unwrap()
            .contains_key(&historical_name)
    );
    let once = sinex_schema::catalog(&mut *conn).await.unwrap();
    sinex_schema::apply(&mut *conn).await.unwrap();
    assert_eq!(once, sinex_schema::catalog(&mut *conn).await.unwrap());
    assert!(
        sqlx::query("UPDATE core.events SET payload='{}'")
            .execute(&mut *conn)
            .await
            .is_err()
    );
}

#[devkit::test]
async fn concurrent_registration_returns_one_immutable_definition(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let d = declared("example.concurrent", 9);
    let register = |url: String, d: KindDeclaration| async move {
        let mut conn = PgConnection::connect(&url).await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        let result = sinex_schema::kinds::register(&mut tx, "example", &[d])
            .await
            .unwrap();
        tx.commit().await.unwrap();
        result
    };
    let (a, b) = tokio::join!(register(url.clone(), d.clone()), register(url.clone(), d));
    assert_eq!(a, b);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM registry.event_types WHERE name='example.concurrent'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[devkit::test]
async fn current_apply_recreates_missing_views_and_preserves_outside_declarations(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let d = declared("example.current", 9);
    let mut tx = conn.begin().await.unwrap();
    sinex_schema::kinds::register(&mut tx, "example", std::slice::from_ref(&d))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    conn.execute("DROP VIEW event_types.command_executed")
        .await
        .unwrap();
    assert_eq!(
        sinex_schema::apply(&mut *conn).await.unwrap(),
        sinex_schema::Applied::Current {
            version: sinex_schema::VERSION
        }
    );
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('event_types.command_executed') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(exists);
    assert_eq!(
        sinex_schema::kinds::load(&mut *conn, std::slice::from_ref(&d.kind))
            .await
            .unwrap()[&d.kind],
        d
    );
    let once = sinex_schema::catalog(&mut *conn).await.unwrap();
    sinex_schema::apply(&mut *conn).await.unwrap();
    assert_eq!(once, sinex_schema::catalog(&mut *conn).await.unwrap());
    let owners = sinex_schema::kinds::Owners {
        prefixes: BTreeMap::from([("polylogue".into(), "different".into())]),
    };
    assert!(
        sinex_schema::apply_with_owners(&mut *conn, &owners)
            .await
            .unwrap_err()
            .to_string()
            .contains("definition_conflict")
    );
    assert_eq!(once, sinex_schema::catalog(&mut *conn).await.unwrap());
}

#[devkit::test]
async fn search_modes_apply_to_live_indexing_and_its_rebuild(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut excluded = declared("example.raw", 9);
    excluded.search = sinex_model::kinds::Search::None;
    let searchable = declared("example.typed", 9);
    let mut words = declared("example.words", 9);
    words.search = sinex_model::kinds::Search::Words;
    let mut tx = conn.begin().await.unwrap();
    sinex_schema::kinds::register(
        &mut tx,
        "example",
        &[excluded.clone(), searchable.clone(), words.clone()],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let events = [&excluded, &searchable, &words]
        .into_iter()
        .map(|d| {
            Event::submitted(
                d.kind.clone(),
                json!({"id":"one","at":"2026-10-27T12:00:00Z","value":1}),
                "example".into(),
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                [0; 32],
                Class::Open,
            )
        })
        .collect();
    admit_one(&mut conn, &batch(events)).await;
    let page = crate::commit_log::after(&mut *conn, 0, 10, crate::PAGE_BYTES)
        .await
        .unwrap();
    crate::read_models::apply_search_text(&mut *conn, &page.events)
        .await
        .unwrap();
    let indexed:Vec<String>=sqlx::query_scalar("SELECT e.event_type FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY 1").fetch_all(&mut *conn).await.unwrap();
    assert_eq!(indexed, ["example.typed", "example.words"]);
    conn.execute("TRUNCATE core.search_text").await.unwrap();
    crate::read_models::apply_search_text(&mut *conn, &page.events)
        .await
        .unwrap();
    let indexed:Vec<String>=sqlx::query_scalar("SELECT e.event_type FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY 1").fetch_all(&mut *conn).await.unwrap();
    assert_eq!(indexed, ["example.typed", "example.words"]);
}

#[devkit::test]
async fn a_removal_of_a_historical_reading_keeps_its_payload_class_and_exact_reference(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut original = keyed("example", 1, 1, 1);
    original.class = Class::Guarded;
    let location = original.location().unwrap().0;
    admit_one(&mut conn, &batch(vec![original.clone()])).await;
    crate::test_schema::downgrade(&mut *conn).await;
    conn.execute("UPDATE sinex_schema.applied SET version=3")
        .await
        .unwrap();
    sinex_schema::apply(&mut *conn).await.unwrap();
    let mut removal = batch(vec![]);
    removal.archive = vec![ArchiveRequest::RemovedAtSource {
        locations: vec![location],
    }];
    assert_eq!(admit_one(&mut conn, &removal).await.admitted, 1);
    let reading:(i64,String,String,Value,String)=sqlx::query_as("SELECT e.kind_version,e.validation,e.class,e.payload,e.record_state FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.source='example'").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(
        reading,
        (
            1,
            "unvalidated".into(),
            "guarded".into(),
            original.payload,
            "removed".into()
        )
    );
}

#[devkit::test]
async fn exact_versions_never_select_another_versions_schema(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ninth = declared("example.versions", 9);
    let mut tenth = declared("example.versions", 10);
    tenth.payload_schema.as_mut().unwrap()["properties"]["value"] = json!({"type":"string"});
    let mut tx = conn.begin().await.unwrap();
    sinex_schema::kinds::register(&mut tx, "example", &[ninth.clone(), tenth.clone()])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for (d, value) in [(&ninth, json!(9)), (&tenth, json!("ten"))] {
        let make = |value: Value| {
            Event::submitted(
                d.kind.clone(),
                json!({"id":format!("v{}",d.kind.version),"at":"2026-10-27T12:00:00Z","value":value}),
                "example".into(),
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                [0; 32],
                Class::Open,
            )
        };
        let invalid = if value.is_string() {
            json!(10)
        } else {
            json!("nine")
        };
        let mut tx = conn.begin().await.unwrap();
        assert!(
            tx.admit(&batch(vec![make(invalid)]), &NoDerivations)
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            admit_one(&mut conn, &batch(vec![make(value)]))
                .await
                .admitted,
            1
        );
    }
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT kind_version FROM core.events ORDER BY kind_version")
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    assert_eq!(versions, [9, 10]);
}

#[devkit::test]
async fn a_saved_definition_hash_cannot_disagree_with_its_fields(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    conn.execute("INSERT INTO registry.event_types (name,version,declaring_producer,payload_schema,normalization,key_rule,replace_rule,time_rule,field_hints,removed_rule,keep_known,owned_blobs,search,definition_hash) VALUES ('example.broken',9,'example',NULL,'json','{\"rule\":\"location\"}','suppress_repeat','{\"rule\":\"unknown\"}','[]','{\"rule\":\"never\"}',ARRAY[]::text[],'[]','words_and_trigram','different-hash')").await.unwrap();
    let error = sinex_schema::kinds::load(
        &mut *conn,
        &[KindRef {
            name: "example.broken".into(),
            version: 9,
        }],
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("definition_conflict"));
}

#[devkit::test]
async fn a_saved_rust_declaration_needs_the_exact_executable_validator(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut declaration = sinex_model::event_types::CommandExecuted::declaration();
    declaration.kind.version = 9;
    sqlx::query("INSERT INTO registry.event_types (name,version,declaring_producer,payload_schema,normalization,key_rule,replace_rule,time_rule,field_hints,removed_rule,keep_known,owned_blobs,search,definition_hash) SELECT name,$1,declaring_producer,payload_schema,normalization,key_rule,replace_rule,time_rule,field_hints,removed_rule,keep_known,owned_blobs,search,$2 FROM registry.event_types WHERE name=$3 AND version=2")
        .bind(9i64).bind(declaration.definition_hash()).bind(&declaration.kind.name).execute(&mut *conn).await.unwrap();
    let mut reading = keyed("test", 1, 1, 1);
    reading.kind_version = 9;
    let mut tx = conn.begin().await.unwrap();
    let error = tx
        .admit(&batch(vec![reading]), &NoDerivations)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("validator_unavailable"));
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.events")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        0
    );
}

#[devkit::test]
async fn applying_a_changed_bundled_definition_refuses_startup(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut declaration = sinex_model::event_types::CommandExecuted::declaration();
    declaration.search = sinex_model::kinds::Search::None;
    conn.execute("ALTER TABLE registry.event_types DISABLE TRIGGER keep_definition")
        .await
        .unwrap();
    sqlx::query("UPDATE registry.event_types SET search='none',definition_hash=$1 WHERE name=$2 AND version=2")
        .bind(declaration.definition_hash()).bind(&declaration.kind.name).execute(&mut *conn).await.unwrap();
    conn.execute("ALTER TABLE registry.event_types ENABLE TRIGGER keep_definition")
        .await
        .unwrap();
    assert!(
        sinex_schema::apply(&mut *conn)
            .await
            .unwrap_err()
            .to_string()
            .contains("definition_conflict")
    );
    assert_eq!(
        sinex_schema::kinds::load(&mut *conn, std::slice::from_ref(&declaration.kind))
            .await
            .unwrap()[&declaration.kind],
        declaration
    );
}

#[devkit::test]
async fn field_hints_are_saved_without_setting_a_kind_wide_class(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut declaration = declared("example.field_classes", 2);
    declaration.field_hints = vec![sinex_model::kinds::FieldHint {
        field: "/value".into(),
        class: Class::Guarded,
    }];
    let mut tx = conn.begin().await.unwrap();
    sinex_schema::kinds::register(&mut tx, "example", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        sinex_schema::kinds::load(&mut *conn, std::slice::from_ref(&declaration.kind))
            .await
            .unwrap()[&declaration.kind],
        declaration
    );
    for (index, supplied_class) in [Class::Open, Class::Guarded].into_iter().enumerate() {
        let event = Event::submitted(
            declaration.kind.clone(),
            json!({"id":index.to_string(),"at":"2026-10-27T12:00:00Z","value":index}),
            "example".into(),
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            [index as u8; 32],
            supplied_class,
        );
        let id = event.id;
        assert_eq!(admit_one(&mut conn, &batch(vec![event])).await.admitted, 1);
        let class: String = sqlx::query_scalar("SELECT class FROM core.events WHERE id=$1")
            .bind(id.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(class, crate::privacy::class_name(supplied_class));
    }
}
