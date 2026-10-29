use super::*;
use crate::{AdmissionContext, derivations::StoredComparison};
use serde_json::json;
use sinex_model::{
    Class, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp,
    event_types::operator::LogEntry,
};
fn event() -> Event {
    Event::build(
        &LogEntry {
            written_at: None,
            label: None,
            text: "privateword".into(),
        },
        "fixture.preview",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [1; 32],
        Class::Guarded,
    )
    .unwrap()
}
#[devkit::test]
async fn saved_comparison_examples_require_same_active_policy_and_grants(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    crate::query::set_grants(&mut conn, "guarded")
        .await
        .unwrap();
    assert_eq!(
        public_key(&mut conn, &json!("ordinary key"), &[], false)
            .await
            .unwrap(),
        json!("ordinary key")
    );
    let read_scope = scope(&mut conn).await.unwrap();
    let saved = StoredComparison {
        id: 0,
        derivation: "fixture.preview".into(),
        version: 1,
        range: None,
        made_at: Timestamp::from_unix_secs(1).unwrap(),
        keys: 1,
        unchanged: 0,
        added: 1,
        changed: 0,
        removed: 0,
        examples: json!([{"new":{"text":"privateword"}}]),
        context: json!({"read_scope":read_scope}),
    };
    crate::derivations::store_comparison(&mut conn, &saved)
        .await
        .unwrap();
    assert_eq!(
        crate::derivations::comparisons(&mut conn, None, 20)
            .await
            .unwrap()[0]
            .examples,
        saved.examples
    );
    crate::query::set_grants(&mut conn, "").await.unwrap();
    assert!(
        crate::derivations::comparisons(&mut conn, None, 20)
            .await
            .unwrap_err()
            .to_string()
            .contains("comparison_privacy_scope_changed")
    );
    crate::query::set_grants(&mut conn, "guarded")
        .await
        .unwrap();
    super::super::add_rule(
        &mut conn,
        &super::super::NewRule {
            name: "configured preview".into(),
            source: "fixture.preview".into(),
            event_type: None,
            field: Some("/text".into()),
            matcher: "pattern".into(),
            matcher_arg: Some("privateword".into()),
            class: Class::Open,
            action: super::super::Action::Mask,
            author: "fixture".into(),
        },
    )
    .await
    .unwrap();
    assert!(
        crate::derivations::comparisons(&mut conn, None, 20)
            .await
            .is_err()
    );
    crate::privacy::projection::ensure_current(&mut conn)
        .await
        .unwrap();
    assert!(
        crate::derivations::comparisons(&mut conn, None, 20)
            .await
            .unwrap_err()
            .to_string()
            .contains("comparison_privacy_scope_changed")
    );
    assert!(
        crate::derivations::store_comparison(&mut conn, &saved)
            .await
            .unwrap_err()
            .to_string()
            .contains("comparison_privacy_scope_changed"),
        "a final-check/store race cannot stamp a newer scope on old examples"
    );
    let original: Value = sqlx::query_scalar("SELECT examples FROM derivation.comparisons")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        original, saved.examples,
        "private audit originals stay exact"
    );
    let mut candidate = vec![event()];
    let changed = project_new(&mut conn, &mut candidate).await.unwrap();
    assert!(changed);
    let key = public_key(&mut conn, &json!("privateword"), &[], changed)
        .await
        .unwrap();
    assert!(key.as_str().unwrap().starts_with("hash:"));
    assert!(!key.to_string().contains("privateword"));
    assert_eq!(candidate[0].payload["text"], "[masked]");
    crate::query::set_grants(&mut conn, "").await.unwrap();
    assert!(
        project_new(&mut conn, &mut [event()])
            .await
            .unwrap_err()
            .to_string()
            .contains("comparison_grants_unavailable")
    );
}
