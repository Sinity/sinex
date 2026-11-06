use super::*;
use crate::NoDerivations;
use serde_json::json;
use sinex_model::{
    Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp,
};

async fn original_saved(
    conn: &mut crate::AdmissionContext,
    name: &str,
    parser: Option<&str>,
    parent: Option<EventId>,
) -> (EventId, Json, tempfile::TempDir) {
    let reference = KindRef {
        name: "query.saved".into(),
        version: 2,
    };
    let declaration = sinex_model::query::historical::original_declaration(&reference).unwrap();
    sinex_schema::kinds::register(conn, "sinex", &[declaration])
        .await
        .unwrap();
    let written_at = Timestamp::from_unix_secs(1_789_372_800).unwrap();
    let original = json!({"name":name,"query":"events count","parameters":[],"by":"synthetic","written_at":written_at});
    let bytes = serde_json::to_vec(&original).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let blobs = crate::Blobs::open(directory.path()).unwrap();
    let blob = blobs.put(&bytes).unwrap();
    let material = MaterialId::new();
    if let Some(parser) = parser {
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
            .bind(&blob)
            .bind(bytes.len() as i64)
            .execute(&mut **conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO raw.materials(id,blob,length,manifest,parser,parser_version) VALUES($1,$2,$3,$4,$5,1)").bind(material.0).bind(&blob).bind(bytes.len() as i64)
            .bind(json!({"path":"query","media_type":"application/x-sinex-query-request","actor":"synthetic","written_at_ns":written_at.unix_ns().to_string()})).bind(parser).execute(&mut **conn).await.unwrap();
    }
    let origin = match parser {
        Some(_) => Origin::Record {
            location: RecordLocation {
                material,
                anchor: 0,
            },
            output_index: 0,
        },
        None => Origin::Derived {
            parents: vec![parent.unwrap()],
        },
    };
    let mut event = Event::build(
        &Saved {
            name: name.into(),
            query: "events | agg count()".into(),
            template: sinex_model::query::native::parse(
                "events",
                &Context::utc(jiff::Timestamp::UNIX_EPOCH),
            )
            .unwrap()
            .into(),
            parameters: vec![],
            by: "synthetic".into(),
            written_at,
        },
        "query.request",
        origin,
        ReadingOrder(1),
        *blake3::hash(&bytes).as_bytes(),
        Class::Guarded,
    )
    .unwrap();
    // This constructs an original admitted-format fixture before admission;
    // the stored event is never modified to the current representation.
    event.kind_version = 2;
    event.payload = original.clone();
    let id = event.id;
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut transaction = conn.begin().await.unwrap();
    transaction.admit(&batch, &NoDerivations).await.unwrap();
    transaction.commit().await.unwrap();
    (id, original, directory)
}

#[devkit::test]
async fn original_saved_read_projection_preserves_captured_events_and_requires_its_actual_source(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    set_grants(&mut conn, "guarded").await.unwrap();
    let (event, original, _custody) =
        original_saved(&mut conn, "retained", Some("query-request"), None).await;
    let before: Json = sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e WHERE id=$1")
        .bind(event.0)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let projected = saved(&mut conn, event).await.unwrap();
    assert_eq!(projected.query, original["query"].as_str().unwrap());
    assert!(matches!(
        projected.template,
        sinex_model::query::saved::Definition::FormatOne { .. }
    ));
    let after: Json = sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e WHERE id=$1")
        .bind(event.0)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(after["kind_version"], 2);
    assert_eq!(after["payload"], original);
    let (foreign, _, _foreign_custody) =
        original_saved(&mut conn, "foreign", Some("foreign-request"), None).await;
    assert!(saved(&mut conn, foreign).await.is_err());
    let (missing, _, _empty_custody) =
        original_saved(&mut conn, "missing", None, Some(event)).await;
    assert!(saved(&mut conn, missing).await.is_err());
}

async fn admit_original(conn: &mut crate::AdmissionContext, event: Event) {
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut transaction = conn.begin().await.unwrap();
    transaction.admit(&batch, &NoDerivations).await.unwrap();
    transaction.commit().await.unwrap();
}

async fn original_request(
    conn: &mut crate::AdmissionContext,
) -> (EventId, Json, tempfile::TempDir) {
    let reference = KindRef {
        name: "analysis.request".into(),
        version: 2,
    };
    sinex_schema::kinds::register(
        conn,
        "sinex",
        &[sinex_model::query::historical::original_declaration(&reference).unwrap()],
    )
    .await
    .unwrap();
    let at = Timestamp::from_unix_secs(1_789_372_800).unwrap();
    let selection = json!({"query":"recall yesterday","now":at,"read_as":null,"zone":"UTC","day_start":"00:00:00","grants":[],"selected":{"kind":"window","from":"2026-09-13T00:00:00Z","to":"2026-09-14T00:00:00Z","_fingerprint":"request-inputs","_query":"original native question"},"declarations":[]});
    let original = json!({"selection":selection,"request":"retained-request","operation":"recall","recipe":null,"from":"2026-09-13","to":"2026-09-13","metrics":[],"min_observed_fraction":null,"zone":"UTC","day_start":"00:00:00","grants":[],"started_at":at,"ended_at":at,"by":"synthetic","requested_at":at});
    let bytes = serde_json::to_vec(&original).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let blobs = crate::Blobs::open(directory.path()).unwrap();
    let blob = blobs.put(&bytes).unwrap();
    let material = MaterialId::new();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
        .bind(&blob)
        .bind(bytes.len() as i64)
        .execute(&mut **conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.materials(id,blob,length,manifest,parser,parser_version) VALUES($1,$2,$3,$4,'analysis-request',1)").bind(material.0).bind(blob).bind(bytes.len() as i64).bind(json!({"path":"analysis","media_type":"application/x-sinex-analysis-request","actor":"synthetic"})).execute(&mut **conn).await.unwrap();
    let request = AnalysisRequest {
        selection: None,
        request: "retained-request".into(),
        operation: "recall".into(),
        recipe: None,
        from: "2026-09-13".into(),
        to: "2026-09-13".into(),
        metrics: vec![],
        min_observed_fraction: None,
        zone: "UTC".into(),
        day_start: "00:00:00".into(),
        grants: vec![],
        started_at: at,
        ended_at: at,
        by: "synthetic".into(),
        requested_at: at,
    };
    let mut event = Event::build(
        &request,
        "analysis.request",
        Origin::Record {
            location: RecordLocation {
                material,
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(&bytes).as_bytes(),
        Class::Open,
    )
    .unwrap();
    event.kind_version = 2;
    event.payload = original.clone();
    let id = event.id;
    admit_original(conn, event).await;
    (id, selection, directory)
}

async fn original_pack(
    conn: &mut crate::AdmissionContext,
    parent: EventId,
    selection: Json,
    source: &str,
) -> (EventId, Json) {
    let at = Timestamp::from_unix_secs(1_789_372_800).unwrap();
    let pack = ContextPack {
        evaluated_at: None,
        day_setting: None,
        producer_versions: None,
        read_as: Some(1),
        analysis: Some(
            json!({"kind":"recall","answer":{"rows":[{"text":"retained row"}]},"selected":selection}),
        ),
        pack: "query:retained-request".into(),
        episode: String::new(),
        at,
        stated_goal: None,
        trail: vec![],
        last_edit: None,
        last_edit_path: None,
        open_failures: vec![],
        coverage: vec![],
        profile: "synthetic".into(),
    };
    let original = serde_json::to_value(&pack).unwrap();
    let event = Event::build(
        &pack,
        source,
        Origin::Derived {
            parents: vec![parent],
        },
        ReadingOrder(1),
        *blake3::hash(&serde_json::to_vec(&original).unwrap()).as_bytes(),
        Class::Guarded,
    )
    .unwrap();
    let id = event.id;
    admit_original(conn, event).await;
    (id, original)
}

#[devkit::test]
async fn old_pack_projection_uses_its_immutable_request_parent_without_rewriting_rows(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    set_grants(&mut conn, "guarded").await.unwrap();
    let (request, mut selection, _custody) = original_request(&mut conn).await;
    selection["selected"]["_fingerprint"] = json!("materialized-inputs");
    let (pack, original) =
        original_pack(&mut conn, request, selection.clone(), "recall.products").await;
    let projected = context_pack(&mut conn, pack).await.unwrap();
    assert_eq!(
        projected["analysis"]["answer"],
        original["analysis"]["answer"]
    );
    assert_eq!(
        projected["analysis"]["selected"]["selected"],
        selection["selected"]
    );
    assert!(projected["analysis"]["selected"].get("template").is_some());
    let retained: Json = sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
        .bind(pack.0)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(retained, original);
    let (unrelated, _, _other_custody) =
        original_saved(&mut conn, "unrelated", Some("query-request"), None).await;
    let (wrong_parent, _) = original_pack(
        &mut conn,
        unrelated,
        selection.clone(),
        "recall.products.wrong-parent",
    )
    .await;
    assert!(context_pack(&mut conn, wrong_parent).await.is_err());
    selection["grants"] = json!(["guarded"]);
    let (changed, _) = original_pack(
        &mut conn,
        request,
        selection,
        "recall.products.changed-selection",
    )
    .await;
    assert!(context_pack(&mut conn, changed).await.is_err());
}

#[devkit::test]
async fn retained_contract_checks_include_exact_schema_names_and_source_fingerprints(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let reference = KindRef {
        name: "query.saved".into(),
        version: 2,
    };
    let original = sinex_model::query::historical::original_declaration(&reference).unwrap();
    assert_eq!(original.payload_schema.as_ref().unwrap()["title"], "Saved");
    let analysis = sinex_model::query::historical::original_declaration(&KindRef {
        name: "analysis.request".into(),
        version: 2,
    })
    .unwrap();
    assert_eq!(
        analysis.payload_schema.as_ref().unwrap()["title"],
        "AnalysisRequest"
    );
    assert!(
        analysis.payload_schema.as_ref().unwrap()["$defs"]
            .get("QuerySelection")
            .is_some()
    );
    sinex_schema::kinds::register(&mut conn, "sinex", std::slice::from_ref(&original))
        .await
        .unwrap();
    check_kind(&mut conn, &reference, &original).await.unwrap();
    let mut renamed = original.clone();
    renamed.payload_schema.as_mut().unwrap()["title"] = json!("DifferentSaved");
    assert!(check_kind(&mut conn, &reference, &renamed).await.is_err());
    let source = sinex_model::SourceDeclaration {
        source: SourceRef {
            name: "query.request".into(),
            version: 1,
        },
        family: "sinex".into(),
        kinds: vec![original.clone()],
    };
    crate::source_declarations::register(&mut conn, std::slice::from_ref(&source))
        .await
        .unwrap();
    let mut captured = Captured {
        event_type: "query.saved".into(),
        kind_version: 2,
        source: source.source.name.clone(),
        source_version: Some(1),
        source_fingerprint: Some("different-captured-fingerprint".into()),
        producer_family: Some("sinex".into()),
        payload: None,
        material_id: None,
    };
    assert!(check_source(&mut conn, &captured, &original).await.is_err());
    captured.source_fingerprint = Some(source.fingerprint());
    check_source(&mut conn, &captured, &original).await.unwrap();
    captured.producer_family = Some("different-family".into());
    assert!(check_source(&mut conn, &captured, &original).await.is_err());
}
