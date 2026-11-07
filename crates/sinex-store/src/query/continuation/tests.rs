use super::*;
use serde_json::json;

#[devkit::test]
fn original_walk_cursors_authenticate_before_matching_the_complete_operation() {
    let ctx = Context::utc("2026-09-14T12:00:00Z".parse().unwrap());
    let text = "trace sinex:ev/00000000-0000-4000-8000-000000000001";
    let operation = sinex_model::query::native::parse(text, &ctx).unwrap();
    let (_, original) = sinex_model::query::historical::decode_with_identity(text, &ctx).unwrap();
    assert_eq!(original_walk_question(&operation).unwrap(), original);
    let seal = CursorSeal::new([7; 32]);
    let after = OrderPosition {
        values: vec![json!(4)],
        row_identity: "synthetic".into(),
    };
    let raw = encode(
        &seal,
        identity("query", &original, "open").unwrap(),
        &ctx,
        BTreeMap::new(),
        BTreeMap::new(),
        after.clone(),
    )
    .unwrap();
    let authenticated = authenticate(&seal, &raw).unwrap();
    let (form, question) = authenticated_operation_question(
        &authenticated,
        operation.identity().unwrap(),
        Some(&operation),
        "open",
    )
    .unwrap();
    assert_eq!(form, original);
    assert_eq!(
        bind_authenticated(authenticated, question, &ctx)
            .unwrap()
            .after,
        after
    );
    for (text, grants) in [
        (
            "parents sinex:ev/00000000-0000-4000-8000-000000000001",
            "open",
        ),
        (
            "trace sinex:ev/00000000-0000-4000-8000-000000000002",
            "open",
        ),
        (text, "guarded"),
    ] {
        let changed = sinex_model::query::native::parse(text, &ctx).unwrap();
        let authenticated = authenticate(&seal, &raw).unwrap();
        let (_, question) = authenticated_operation_question(
            &authenticated,
            changed.identity().unwrap(),
            Some(&changed),
            grants,
        )
        .unwrap();
        assert!(bind_authenticated(authenticated, question, &ctx).is_err());
    }
    let mut tampered = raw.into_bytes();
    let last = tampered.last_mut().unwrap();
    *last = if *last == b'0' { b'1' } else { b'0' };
    assert!(authenticate(&seal, std::str::from_utf8(&tampered).unwrap()).is_err());
    let mut changed = operation.clone();
    changed
        .relation
        .transforms
        .push(query_language::ast::Transform {
            span: Default::default(),
            kind: query_language::ast::TransformKind::Limit { count: 1 },
        });
    assert!(original_walk_question(&changed).is_none());
}
