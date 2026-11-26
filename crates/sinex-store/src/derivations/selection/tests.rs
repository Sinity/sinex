//! What a switch requires of its comparison. All fixtures are synthetic.
use super::*;
use crate::{AdmissionContext, NoDerivations};
use serde_json::json;
use sinex_model::{
    Batch, BatchKind, Class, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp,
};

const BUILD: &str = "synthetic-build";
const SETTINGS: &str = "synthetic-day";

fn kinds() -> Vec<String> {
    vec!["command.imported".into()]
}

fn event(n: u32) -> Event {
    let at = Timestamp::from_unix_secs(1_600_000_000 + i64::from(n)).unwrap();
    (sinex_model::event_types::find("command.imported")
        .unwrap()
        .build)(
        &json!({"command":format!("synthetic-{n}"),"hostname":"synthetic","started_at":at,"header_elapsed":0,"paths":[],"n":n}),
        "synthetic",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: 0,
            },
            output_index: 0,
        },
        ReadingOrder(i64::from(n)),
        [n as u8; 32],
        Class::Open,
    )
    .unwrap()
}

async fn admit(writer: &mut AdmissionContext, event: Event) -> EventId {
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
    let mut tx = writer.begin().await.unwrap();
    tx.admit(&batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    id
}

/// A stored comparison of version 3 by `BUILD`, over the whole history or
/// one hour of it.
async fn compared(conn: &mut PgConnection, full: bool) -> i64 {
    let hour = (
        Some(Timestamp::from_unix_secs(1_600_000_000).unwrap()),
        Some(Timestamp::from_unix_secs(1_600_003_600).unwrap()),
    );
    crate::derivations::store_comparison(
        conn,
        &crate::derivations::StoredComparison {
            id: 0,
            derivation: "synthetic".into(),
            version: 3,
            range: (!full).then_some(hour),
            made_at: Timestamp::now(),
            keys: 1,
            unchanged: 0,
            added: 1,
            changed: 0,
            removed: 0,
            context: json!({"build":BUILD,"input_types":kinds(),"output_types":["document.text"],"kind":"per_event","history":sinex_model::DerivationHistory::Recompute,"settings":SETTINGS}),
            examples: json!([]),
        },
    )
    .await
    .unwrap()
}

fn selection(comparison: i64) -> Selection {
    Selection {
        comparison,
        operation: uuid::Uuid::now_v7(),
        actor: "synthetic".into(),
        history: sinex_model::DerivationHistory::Recompute,
        version: 3,
        build: BUILD.into(),
        settings: SETTINGS.into(),
        inputs: kinds(),
        outputs: vec!["document.text".into()],
        kind: "per_event".into(),
    }
}

/// Inputs committed, archived and restored after a comparison leave it
/// selectable: the switch computes from what is live when it runs.
#[devkit::test]
async fn a_comparison_stays_selectable_while_its_inputs_change(db: devkit::pg::TestDatabase) {
    use sinex_model::lifecycle::Act;
    let mut writer = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let first = admit(&mut writer, event(1)).await;
    let comparison = compared(&mut writer, true).await;
    let selected = selection(comparison);
    validate(&mut writer, "synthetic", &selected).await.unwrap();
    admit(&mut writer, event(2)).await;
    for act in [Act::Archive, Act::Restore] {
        let moved = crate::lifecycle::move_events(&mut writer, act, vec![first], &NoDerivations);
        assert!(
            matches!(moved.await, crate::lifecycle::Ended::Ok { .. }),
            "{act:?}"
        );
    }
    validate(&mut writer, "synthetic", &selected).await.unwrap();
}

/// A switch is refused for a comparison another build made, of another
/// version, declaration or day setting, or of only a time range.
#[devkit::test]
async fn a_switch_needs_a_full_comparison_of_its_own_build_and_declaration(
    db: devkit::pg::TestDatabase,
) {
    let mut writer = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let comparison = compared(&mut writer, true).await;
    let cases: [(fn(&mut Selection), &str); 5] = [
        (|s| s.build = "another-build".into(), "version and build"),
        (|s| s.version = 4, "version and build"),
        (
            |s| s.inputs.push("command.executed".into()),
            "does not describe this producer declaration",
        ),
        (
            |s| s.history = sinex_model::DerivationHistory::KeepHistory,
            "does not describe this producer declaration",
        ),
        (
            |s| s.settings = "another-day".into(),
            "logical-day settings changed",
        ),
    ];
    for (change, reason) in cases {
        let mut changed = selection(comparison);
        change(&mut changed);
        let refused = validate(&mut writer, "synthetic", &changed)
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains(reason), "{refused}");
    }
    let ranged = compared(&mut writer, false).await;
    let refused = validate(&mut writer, "synthetic", &selection(ranged))
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("full comparison"), "{refused}");
    assert!(matches!(
        validate(&mut writer, "synthetic", &selection(ranged + 1)).await,
        Err(Error::NotFound(_))
    ));
}
