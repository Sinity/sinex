use super::*;

struct Titles;
impl DirtyKeys for Titles {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(String, Value)> {
        event
            .payload
            .get("title")
            .filter(|v| !v.is_null())
            .map(|title| vec![("test.titles".into(), title.clone())])
            .unwrap_or_default()
    }
}

#[devkit::test]
async fn known_fields_merge_in_one_batch_and_supply_dirty_keys(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut first = watched(Some("kept title"), 1);
    let mut second = watched(Some("other title"), 1);
    second.equivalence_key = Some(sinex_model::EquivalenceKey("second".into()));
    first.payload["channel_url"] = json!("https://example.test/channel");
    admit_one(&mut conn, &batch(vec![first.clone(), second.clone()])).await;
    let mut newer = watched(None, 2);
    newer.payload.as_object_mut().unwrap().remove("title");
    newer.payload["channel"] = json!("new channel");
    let mut other = watched(None, 2);
    other.payload["title"] = Value::Null;
    other.equivalence_key = second.equivalence_key.clone();
    // A repeat and an older reading must not enter the merge relation.
    let older = watched(None, 0);
    let mut tx = conn.begin().await.unwrap();
    let out = tx
        .admit(
            &batch(vec![newer.clone(), other.clone(), second, older]),
            &Titles,
        )
        .await
        .unwrap();
    assert_eq!((out.admitted, out.replaced, out.superseded), (2, 2, 2));
    tx.commit().await.unwrap();
    let rows: Vec<(Value, Vec<u8>)> = sqlx::query_as(
        "SELECT payload,record_hash FROM core.events WHERE id=ANY($1) ORDER BY payload->>'title'",
    )
    .bind(vec![newer.id.0, other.id.0])
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(rows[0].0["title"], "kept title");
    assert_eq!(rows[0].0["channel"], "new channel");
    assert_eq!(rows[0].0["channel_url"], "https://example.test/channel");
    assert_eq!(rows[0].1, newer.record_hash);
    assert_eq!(rows[1].0["title"], "other title");
    let dirty: Vec<Value> = sqlx::query_scalar(
        "SELECT key FROM derivation.dirty WHERE derivation='test.titles' ORDER BY key",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(dirty, vec![json!("kept title"), json!("other title")]);
}

#[devkit::test]
async fn inherited_fields_keep_their_class(db: devkit::pg::TestDatabase) {
    use crate::privacy::{self, Action, NewRule};

    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    privacy::add_rule(
        &mut conn,
        &NewRule {
            name: "guarded titles".into(),
            source: "youtube".into(),
            event_type: None,
            field: Some("/title".into()),
            matcher: "pattern".into(),
            matcher_arg: Some("private title".into()),
            class: Class::Guarded,
            action: Action::Classify,
            author: "test".into(),
        },
    )
    .await
    .unwrap();
    admit_one(&mut conn, &batch(vec![watched(Some("private title"), 1)])).await;
    let mut inherited = watched(None, 2);
    inherited.payload.as_object_mut().unwrap().remove("title");
    let inherited_id = inherited.id;
    admit_one(&mut conn, &batch(vec![inherited])).await;
    privacy::projection::ensure_current(&mut conn).await.unwrap();
    let (payload, class): (Value, String) =
        sqlx::query_as("SELECT payload,class FROM privacy.event_outputs WHERE event_id=$1")
            .bind(inherited_id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(payload["title"], "private title");
    assert_eq!(class, "guarded");
    let visible: Value = sqlx::query_scalar("SELECT payload FROM core.visible_events WHERE id=$1")
        .bind(inherited_id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(visible, json!({"withheld": "guarded"}));

    // Supplying every known field does not inherit the old payload or class.
    let replacement = watched(Some("public title"), 3);
    let replacement_id = replacement.id;
    admit_one(&mut conn, &batch(vec![replacement])).await;
    privacy::projection::ensure_current(&mut conn).await.unwrap();
    let class: String = sqlx::query_scalar("SELECT class FROM privacy.event_outputs WHERE event_id=$1")
        .bind(replacement_id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(class, "open");

    // A copied value cannot lower the incoming event's higher class.
    let mut guarded = watched(None, 4);
    guarded.class = Class::Guarded;
    let guarded_id = guarded.id;
    admit_one(&mut conn, &batch(vec![guarded])).await;
    privacy::projection::ensure_current(&mut conn).await.unwrap();
    let class: String = sqlx::query_scalar("SELECT class FROM privacy.event_outputs WHERE event_id=$1")
        .bind(guarded_id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(class, "guarded");
}
