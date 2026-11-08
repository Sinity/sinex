use super::*;
use serde_json::json;
use sinex_model::Timestamp;
use sinex_model::event_types::library::ValueOrigin;

fn comparison(rev: &str, author: &str, scale: &str, order: Value) -> Curation {
    Curation {
        root: rev.into(),
        rev: rev.into(),
        revises: vec![],
        withdraws: vec![],
        kind: CurationKind::Comparison,
        target: None,
        key: Some(scale.into()),
        value: Some(json!({ "order": order, "blind": false })),
        origin: ValueOrigin::Authored,
        cites: None,
        author: author.into(),
        via: "test".into(),
        at: Timestamp::from_unix_secs(1).unwrap(),
        request: rev.into(),
        request_digest: String::new(),
        slots: vec![],
    }
}

fn t(item: &str) -> Value {
    json!({ "item": item, "revision": "r", "rendition": null, "part": null })
}

#[devkit::test]
fn a_comparison_is_one_judgment_with_its_tie_groups() {
    let records = [
        comparison(
            "c1",
            "operator",
            "liking",
            json!([[t("a")], [t("b"), t("c")], [t("d")]]),
        ),
        comparison("c2", "operator", "other", json!([[t("a")], [t("b")]])),
    ];
    let found = judgments(&records, "liking");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].judge, "operator");
    assert_eq!(
        found[0].order,
        vec![
            vec!["a".to_owned()],
            vec!["b".to_owned(), "c".to_owned()],
            vec!["d".to_owned()]
        ]
    );
}

#[devkit::test]
fn the_default_judge_set_is_the_operator_and_agents_change_nothing_in_it() {
    let operator = comparison("c1", "operator", "liking", json!([[t("a")], [t("b")]]));
    let agent = comparison("c2", "agent", "liking", json!([[t("b")], [t("a")]]));
    let items = ["a".to_owned(), "b".to_owned()];
    let options = ranking::FitOptions {
        judges: judge_set(None),
        ..ranking::FitOptions::default()
    };
    let alone = ranking::fit(&items, &judgments(&[operator.clone()], "liking"), &options).unwrap();
    let with_agent =
        ranking::fit(&items, &judgments(&[operator, agent], "liking"), &options).unwrap();
    assert_eq!(alone, with_agent);
    assert_eq!(alone.items[0].item, "a");
    assert_eq!(judge_set(Some("all")), JudgeSet::All);
    assert_eq!(
        judge_set(Some("agent, operator")),
        JudgeSet::named(["agent", "operator"])
    );
}
