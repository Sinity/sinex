use super::*;
use sqlx::Connection;

#[devkit::test]
async fn snapshot_reads_selected_classes_and_checks_registered_definitions(
    db: devkit::pg::TestDatabase,
) {
    use crate::{AdmissionContext, NoDerivations};
    use sinex_model::{
        Batch, BatchKind, Class, Event, KindDeclaration, MaterialId, Origin, ReadingOrder,
        RecordLocation, RecordOutcome, RunId,
    };
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let declaration = KindDeclaration::schemaless(
        KindRef {
            name: "synthetic.snapshot".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    sinex_schema::kinds::register(&mut conn, "synthetic", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    let events = (0..8192)
        .map(|n| {
            Event::submitted(
                declaration.kind.clone(),
                serde_json::json!({"n":n}),
                "synthetic".into(),
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                *blake3::hash(n.to_string().as_bytes()).as_bytes(),
                if n == 0 { Class::Guarded } else { Class::Open },
            )
        })
        .collect();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(
        tx.admit(&batch, &NoDerivations).await.unwrap().admitted,
        8192
    );
    tx.commit().await.unwrap();
    let position = crate::query::current_seq(&mut conn).await.unwrap();
    let kinds = vec![(declaration.kind.clone(), declaration.definition_hash())];

    let mut probe = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut tx = probe.begin().await.unwrap();
    assert!(kinds_registered(&mut tx, &kinds).await.unwrap());
    let mut changed = kinds.clone();
    changed[0].1 = "0".repeat(64);
    assert!(!kinds_registered(&mut tx, &changed).await.unwrap());
    changed[0].0.version = 2;
    assert!(!kinds_registered(&mut tx, &changed).await.unwrap());

    let names = vec![declaration.kind.name.clone()];
    let guarded = vec!["guarded".to_owned()];
    let selected = readings(
        &mut tx,
        &Selection {
            position,
            kinds: &names,
            classes: &guarded,
        },
        None,
        2,
    )
    .await
    .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].class, "guarded");
    assert!(!selected[0].archived);
    let both = vec!["open".to_owned(), "guarded".to_owned()];
    let all = Selection {
        position,
        kinds: &names,
        classes: &both,
    };
    let first = readings(&mut tx, &all, None, 3).await.unwrap();
    assert_eq!(first.len(), 3);
    let last = first.last().unwrap();
    let rest = readings(&mut tx, &all, Some((last.commit_seq, last.id)), 10_000)
        .await
        .unwrap();
    assert_eq!(first.len() + rest.len(), 8192);
    assert!(!tombstoned_since(&mut tx, position, &names).await.unwrap());
    tx.rollback().await.unwrap();
}
