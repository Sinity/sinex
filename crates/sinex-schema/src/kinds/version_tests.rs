use super::*;
use sinex_model::{Class, EventId, MaterialId, RunId};
use sqlx::{Connection, Executor};

/// Current declarations separated from unversioned historical version one.
pub(crate) const CURRENT_VERSION_TWO: &[&str] = &[
    "activity.window",
    "activity.session",
    "activity.span",
    "input.coverage",
    "tag.applied",
    "conversation.day",
    "rest.fused",
    "work.act",
    "work.episode",
    "work.suspension",
    "work.resumption",
    "context.pack",
    "activity.day",
    "activity.project_span",
    "source.coverage",
    "analysis.request",
    "analysis.panel",
    "recipe.answer",
    "analysis.finding",
    "input.bout",
    "input.workload",
    "rhythm.deviation",
    "reference.about",
];

#[test]
fn current_kind_map_separates_known_version_two_from_unknown_version_one() {
    assert_eq!(CURRENT_VERSION_TWO.len(), 23);
    let names: std::collections::BTreeSet<_> = CURRENT_VERSION_TWO.iter().collect();
    assert_eq!(names.len(), CURRENT_VERSION_TWO.len());
    for name in CURRENT_VERSION_TWO {
        let current = (sinex_model::event_types::find(name).unwrap().declaration)();
        assert_eq!(
            current.kind,
            KindRef {
                name: (*name).into(),
                version: 2
            }
        );
        assert!(current.payload_schema.is_some());
        assert!(current.key_rule.is_some());
        assert!(current.time_rule.is_some());
        let historical = KindDeclaration::historical((*name).into());
        assert_eq!(historical.kind.version, 1);
        assert!(historical.payload_schema.is_none());
        assert!(historical.key_rule.is_none());
        assert!(historical.time_rule.is_none());
    }
}

pub(crate) async fn assert_historical_and_current(conn: &mut PgConnection) {
    let references: Vec<_> = CURRENT_VERSION_TWO
        .iter()
        .flat_map(|name| {
            [1, 2].map(|version| KindRef {
                name: (*name).into(),
                version,
            })
        })
        .collect();
    let saved = load(conn, &references).await.unwrap();
    assert_eq!(saved.len(), references.len());
    for name in CURRENT_VERSION_TWO {
        let historical = KindDeclaration::historical((*name).into());
        assert_eq!(
            saved[&historical.kind], historical,
            "{name} historical metadata stays unknown"
        );
        let current = (sinex_model::event_types::find(name).unwrap().declaration)();
        assert_eq!(
            saved[&current.kind], current,
            "{name} retains its exact known declaration"
        );
    }
}

#[devkit::test]
async fn already_versioned_compressed_readings_keep_known_schemas_and_references(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let current = (sinex_model::event_types::find("tag.applied")
        .unwrap()
        .declaration)();
    assert_eq!(current.kind.version, 2);
    let mut known_one = current.clone();
    known_one.kind.version = 1;
    register_checked(&mut conn, "sinex", std::slice::from_ref(&known_one))
        .await
        .unwrap();
    let payload = json!({"tag":"synthetic","record":"fixture","event_type":"command.imported","rules":[],"at":null});
    jsonschema::validator_for(known_one.payload_schema.as_ref().unwrap())
        .unwrap()
        .validate(&payload)
        .unwrap();
    let evaluated = known_one.evaluate(&payload, Class::Open).unwrap();
    let id = EventId::new();
    let material = MaterialId::new();
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,output_index,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,kind_version,validation) VALUES($1,'tag.applied','fixture',$2,$3,123,32767,'unknown',1,$4,$5,1,decode(repeat('01',32),'hex'),1,'validated')")
        .bind(id.0).bind(RunId::new().0).bind(material.0).bind(&payload).bind(evaluated.equivalence_key.map(|key|key.0)).execute(&mut conn).await.unwrap();
    conn.execute("INSERT INTO core.live_equivalence SELECT source,event_type,core.encode_equivalence_key(equivalence_key),id,reading_order,record_hash FROM core.events; INSERT INTO core.commit_log SELECT commit_seq,id,event_type FROM core.events; UPDATE core.counters SET value=1 WHERE name='commit_seq'").await.unwrap();
    let reference_sql = "SELECT to_jsonb(e)||jsonb_build_object('citation','sinex:record/'||material_id||'/'||anchor||'/'||event_type||'/'||output_index) FROM core.events e WHERE id=$1";
    let before: Value = sqlx::query_scalar(reference_sql)
        .bind(id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(before["kind_version"], 1);
    let compressed: i64 =
        sqlx::query_scalar("SELECT count(compress_chunk(c)) FROM show_chunks('core.events') c")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(compressed, 1);
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        crate::Applied::Current {
            version: crate::VERSION
        }
    );
    let after: Value = sqlx::query_scalar(reference_sql)
        .bind(id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(after, before);
    let references = [known_one.kind.clone(), current.kind.clone()];
    let saved = load(&mut conn, &references).await.unwrap();
    assert_eq!(saved[&known_one.kind], known_one);
    assert_eq!(saved[&current.kind], current);
    let still_compressed:i64=sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema='core' AND hypertable_name='events' AND is_compressed").fetch_one(&mut conn).await.unwrap();
    assert_eq!(still_compressed, compressed);
    let mut conflict = known_one.clone();
    conflict.payload_schema.as_mut().unwrap()["description"] =
        json!("a conflicting known definition");
    let mut newer = current.clone();
    newer.kind.version = 3;
    let error = register_checked(&mut conn, "sinex", &[newer.clone(), conflict])
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("definition_conflict: tag.applied version 1"),
        "{error}"
    );
    let after_conflict = load(
        &mut conn,
        &[known_one.kind.clone(), current.kind.clone(), newer.kind],
    )
    .await
    .unwrap();
    assert_eq!(after_conflict, saved);
    assert_eq!(
        sqlx::query_scalar::<_, Value>(reference_sql)
            .bind(id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        before
    );
}

#[devkit::test]
async fn outside_known_version_one_remains_registered_after_schema_apply(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut declaration = KindDeclaration::schemaless(
        KindRef {
            name: "fixture.known".into(),
            version: 1,
        },
        "fixture".into(),
    );
    declaration.payload_schema = Some(
        json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}),
    );
    register(&mut conn, "fixture", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        crate::Applied::Current {
            version: crate::VERSION
        }
    );
    let saved = load(&mut conn, std::slice::from_ref(&declaration.kind))
        .await
        .unwrap();
    assert_eq!(saved[&declaration.kind], declaration);
    let mut conflict = declaration.clone();
    conflict.payload_schema.as_mut().unwrap()["properties"]["value"]["type"] = json!("number");
    assert!(
        register(&mut conn, "fixture", &[conflict])
            .await
            .unwrap_err()
            .to_string()
            .contains("definition_conflict")
    );
    assert_eq!(
        load(&mut conn, &[declaration.kind.clone()]).await.unwrap(),
        saved
    );
}
