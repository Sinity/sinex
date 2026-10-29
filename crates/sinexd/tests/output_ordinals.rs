//! One parser record retains every output through admission and citation reads.
use serde_json::{Value, json};
use sinex_model::{
    Batch, BatchKind, Class, MaterialId, ReadingOrder, RecordLocation, RecordOutcome, RunId,
    event_types::CommandImported,
};
use sinex_source_core::{DrainInput, Limits, Parser, RecordContext, RecordError, drain};
use sinex_store::{AdmissionContext, NoDerivations, Outcome};

async fn admit_one(conn: &mut AdmissionContext, batch: &Batch) -> Outcome {
    let mut tx = conn.begin().await.unwrap();
    let outcome = tx.admit(batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    outcome
}

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
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
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
    let b = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "fixture".into(),
        },
        records: std::mem::take(&mut outputs),
        archive: vec![],
    };
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
        let id = sinex_store::query::resolve(&mut conn, &reference)
            .await
            .unwrap();
        assert_eq!(id, ids[usize::try_from(ordinal).unwrap()]);
        let shown = sinex_store::query::show(&mut conn, &reference)
            .await
            .unwrap();
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
