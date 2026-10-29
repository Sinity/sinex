use super::*;
use sinex_source_core::{DrainInput, Limits, Parser, RecordContext, RecordError, drain};

/// One source-shaped record containing an array of outputs, all at its original offset.
struct ArrayRecord;
impl Parser for ArrayRecord {
    type Record<'a> = serde_json::Map<String, Value>;
    type Carry = ();
    type Payload = CommandImported;
    fn parse(
        &self,
        record: &Self::Record<'_>,
        _: &(),
        _: &RecordContext<'_>,
    ) -> std::result::Result<(Vec<CommandImported>, ()), RecordError> {
        let commands = record["commands"]
            .as_array()
            .ok_or_else(|| RecordError::new("commands array"))?;
        let payloads = commands
            .iter()
            .map(|value| {
                Ok(CommandImported {
                    command: value
                        .as_str()
                        .ok_or_else(|| RecordError::new("command text"))?
                        .into(),
                    hostname: "synthetic".into(),
                    started_at: None,
                    header_elapsed: None,
                    paths: vec![],
                    n: 0,
                })
            })
            .collect::<std::result::Result<Vec<_>, RecordError>>()?;
        Ok((payloads, ()))
    }
}

#[devkit::test]
async fn one_record_keeps_more_than_65535_outputs_and_their_citations(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let count = 65_538_u64;
    let bytes = serde_json::to_vec(
        &json!({"commands":(0..count).map(|i|format!("command-{i}")).collect::<Vec<_>>()}),
    )
    .unwrap();
    let material = MaterialId::new();
    let framed = [(0, bytes.len())];
    let chunks = drain(
        &ArrayRecord,
        DrainInput {
            source: "synthetic-array",
            class: Class::Open,
            material,
            bytes: bytes.as_slice().into(),
            records: &framed,
            carry: Value::Null,
            reading_order: &|_| ReadingOrder(1),
            limits: Limits::default(),
            manifest: RecordContext::none().manifest,
        },
    )
    .unwrap();
    assert_eq!(
        chunks.len(),
        1,
        "a record larger than a transfer batch remains one record"
    );
    assert_eq!(chunks[0].failures, 0);
    assert_eq!(chunks[0].events, usize::try_from(count).unwrap());
    let mut outputs = chunks.into_iter().next().unwrap().outcomes;
    let events = match &outputs[0] {
        RecordOutcome::Events { events, .. } => events,
        _ => panic!("valid array record"),
    };
    for (ordinal, event) in events.iter().enumerate() {
        assert_eq!(
            event.location(),
            Some((
                RecordLocation {
                    material,
                    anchor: 0
                },
                u64::try_from(ordinal).unwrap()
            ))
        );
    }
    let ids: Vec<_> = events.iter().map(|e| e.id.0).collect();
    let mut b = batch(vec![]);
    b.records = std::mem::take(&mut outputs);
    assert_eq!(
        admit_one(&mut conn, &b).await.admitted,
        usize::try_from(count).unwrap()
    );
    let stored:(i64,i64,i64)=sqlx::query_as("SELECT count(*),min(output_index),max(output_index) FROM core.live_locations WHERE material_id=$1")
        .bind(material.0).fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        stored,
        (
            i64::try_from(count).unwrap(),
            0,
            i64::try_from(count - 1).unwrap()
        )
    );
    for ordinal in [0, 32_767, 32_768, 65_535, 65_536, count - 1] {
        let text = format!("sinex:record/{}/0/command.imported/{ordinal}", material.0);
        let reference = sinex_model::query::parse_reference(&text).unwrap();
        let id = crate::query::resolve(&mut conn, &reference).await.unwrap();
        assert_eq!(id, ids[usize::try_from(ordinal).unwrap()]);
        let shown = crate::query::show(&mut conn, &reference).await.unwrap();
        assert_eq!(shown["citation"], text);
        assert_eq!(shown["output"], ordinal);
        assert_eq!(shown["payload"]["command"], format!("command-{ordinal}"));
    }
    assert_eq!(
        admit_one(&mut conn, &b).await.admitted,
        0,
        "same coordinate slots are repeats"
    );
}

#[devkit::test]
async fn output_beyond_sql_range_is_refused_before_any_event_is_written(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut event = keyed("range", 1, 1, 1);
    if let Origin::Record { output_index, .. } = &mut event.origin {
        *output_index = u64::MAX;
    }
    let b = batch(vec![event]);
    let mut tx = conn.begin().await.unwrap();
    let error = tx.admit(&b, &NoDerivations).await.unwrap_err();
    assert!(
        error.to_string().contains("output index beyond i64"),
        "{error}"
    );
    tx.rollback().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let invalid = sinex_model::query::Reference::Record {
        material: uuid::Uuid::nil(),
        offset: 0,
        event_type: None,
        output: Some(u64::MAX),
    };
    assert!(
        crate::query::resolve(&mut conn, &invalid)
            .await
            .unwrap_err()
            .to_string()
            .contains("output index beyond i64")
    );
}

#[devkit::test]
async fn negative_sql_ordinals_are_refused_by_each_coordinate_table(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let event = Event::build(
        &CommandImported {
            command: "coordinate check seed".into(),
            hostname: "synthetic".into(),
            started_at: None,
            header_elapsed: None,
            paths: vec![],
            n: 0,
        },
        "negative",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    let id = event.id.0;
    assert_eq!(admit_one(&mut conn, &batch(vec![event])).await.admitted, 1);
    let live: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.live_locations WHERE event_id=$1")
            .bind(id)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        live, 1,
        "the location-keyed seed must exercise coordinate rows"
    );
    let statements = [
        "INSERT INTO core.live_locations SELECT material_id,anchor,event_type,-1,event_id,reading_order,record_hash FROM core.live_locations LIMIT 1",
        "INSERT INTO core.hidden_records (source,event_type,material_id,anchor,output_index,operation_id) SELECT 'negative',event_type,material_id,anchor,-1,id FROM core.events LIMIT 1",
        "INSERT INTO core.admission_stage (ord,id,event_type,source,material_id,anchor,output_index,ts_quality,payload,replace_rule,reading_order,record_hash,record_state,class) SELECT 0,id,event_type,source,material_id,anchor,-1,ts_quality,payload,'suppress_repeat',reading_order,record_hash,record_state,class FROM core.events LIMIT 1",
        "INSERT INTO core.admission_stage (ord,id,event_type,source,material_id,anchor,output_index,ts_quality,payload,replace_rule,reading_order,record_hash,record_state,class) SELECT -1,id,event_type,source,material_id,anchor,0,ts_quality,payload,'suppress_repeat',reading_order,record_hash,record_state,class FROM core.events LIMIT 1",
    ];
    let constraints = [
        "output_index_in_range",
        "output_index_in_range",
        "admission_stage_output_index_check",
        "admission_stage_ord_check",
    ];
    for (sql, constraint) in statements.into_iter().zip(constraints) {
        let error = sqlx::query(sql).execute(&mut conn).await.unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("23514"),
            "{error}"
        );
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some(constraint),
            "{error}"
        );
    }
}

#[devkit::test]
async fn largest_stored_output_round_trips_through_live_lookup_and_show(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let location = RecordLocation {
        material: MaterialId::new(),
        anchor: 1,
    };
    let output = u64::try_from(i64::MAX).unwrap();
    let event = Event::build(
        &CommandImported {
            command: "largest coordinate".into(),
            hostname: "synthetic".into(),
            started_at: None,
            header_elapsed: None,
            paths: vec![],
            n: 0,
        },
        "wide",
        Origin::Record {
            location,
            output_index: output,
        },
        ReadingOrder(1),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    let id = event.id.0;
    assert_eq!(
        admit_one(&mut conn, &batch(vec![event.clone()]))
            .await
            .admitted,
        1
    );
    assert_eq!(
        crate::live::at(&mut conn, location, output).await.unwrap(),
        Some(id)
    );
    let reference = sinex_model::query::Reference::Record {
        material: location.material.0,
        offset: 1,
        event_type: Some("command.imported".into()),
        output: Some(output),
    };
    assert_eq!(
        crate::query::show(&mut conn, &reference).await.unwrap()["output"],
        output
    );
    crate::stage::copy(&mut conn, &[(i64::MAX, &event)])
        .await
        .unwrap();
    let staged: (i64, i64) =
        sqlx::query_as("SELECT ord,output_index FROM core.admission_stage WHERE ord=$1")
            .bind(i64::MAX)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(staged, (i64::MAX, i64::MAX));
    let hide = Batch {
        archive: vec![ArchiveRequest::Hide {
            target: EventId(id),
            operation: OperationId::new(),
        }],
        ..batch(vec![])
    };
    admit_one(&mut conn, &hide).await;
    let hidden: i64 =
        sqlx::query_scalar("SELECT output_index FROM core.hidden_records WHERE material_id=$1")
            .bind(location.material.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(hidden, i64::MAX);
    assert_eq!(
        crate::live::at(&mut conn, location, output).await.unwrap(),
        None
    );
    let unhide = Batch {
        archive: vec![ArchiveRequest::Unhide {
            target: EventId(id),
            operation: OperationId::new(),
        }],
        ..batch(vec![])
    };
    admit_one(&mut conn, &unhide).await;
    assert_eq!(
        crate::live::at(&mut conn, location, output).await.unwrap(),
        Some(id)
    );

    assert!(
        crate::live::at(&mut conn, location, output + 1)
            .await
            .is_err()
    );
}
