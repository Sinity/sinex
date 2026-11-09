use super::*;
use serde_json::json;

#[devkit::test]
fn cursors_bind_only_to_their_own_question_and_refuse_tampering() {
    let ctx = Context::utc("2026-09-14T12:00:00Z".parse().unwrap());
    let text = "trace sinex:ev/00000000-0000-4000-8000-000000000001";
    let question = sinex_model::query::native::parse(text, &ctx)
        .unwrap()
        .identity()
        .unwrap();
    let seal = CursorSeal::new([7; 32]);
    let after = OrderPosition {
        values: vec![json!(4)],
        row_identity: "synthetic".into(),
    };
    let raw = encode(
        &seal,
        identity("query", &question, "open").unwrap(),
        &ctx,
        BTreeMap::new(),
        BTreeMap::new(),
        after.clone(),
    )
    .unwrap();
    let own = identity("query", &question, "open").unwrap();
    assert_eq!(
        bind_authenticated(authenticate(&seal, &raw).unwrap(), own, &ctx)
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
        let other = sinex_model::query::native::parse(text, &ctx)
            .unwrap()
            .identity()
            .unwrap();
        let other = identity("query", &other, grants).unwrap();
        assert!(bind_authenticated(authenticate(&seal, &raw).unwrap(), other, &ctx).is_err());
    }
    let mut tampered = raw.into_bytes();
    let last = tampered.last_mut().unwrap();
    *last = if *last == b'0' { b'1' } else { b'0' };
    assert!(authenticate(&seal, std::str::from_utf8(&tampered).unwrap()).is_err());
}
