use super::*;

struct Titles;
impl DirtyKeys for Titles {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(&'static str, Value)> {
        event
            .payload
            .get("title")
            .filter(|v| !v.is_null())
            .map(|title| vec![("test.titles", title.clone())])
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
