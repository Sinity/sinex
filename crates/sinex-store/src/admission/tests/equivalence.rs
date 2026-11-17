use super::*;

fn keys() -> Vec<String> {
    let digest = "a7".repeat(32);
    vec![
        digest.clone(),
        format!("hash:{digest}"),
        format!("sealed:{digest}"),
        digest.to_uppercase(),
        format!("{digest}\n"),
        format!("hash:{digest}\r\n"),
        format!("hash:{}", digest.to_uppercase()),
        String::new(),
        "zażółć 🦀".into(),
        "03a7".into(),
    ]
}

#[devkit::test]
async fn every_key_form_keeps_its_identity_and_public_spelling(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let keys = keys();
    let events: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| {
            let mut event = keyed("s", i as u8, 1, 1);
            event.equivalence_key = Some(sinex_model::EquivalenceKey(key.clone()));
            event
        })
        .collect();
    assert_eq!(
        admit_one(&mut conn, &batch(events.clone())).await.admitted,
        keys.len()
    );
    let encoded: Vec<(String,Vec<u8>)> = sqlx::query_as("SELECT core.decode_equivalence_key(equivalence_key),equivalence_key FROM core.live_equivalence ORDER BY equivalence_key")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(encoded.len(), keys.len());
    for (key, bytes) in &encoded {
        assert!(keys.contains(key));
        if bytes[0] < 3 {
            assert_eq!(bytes.len(), 33);
        }
    }
    for event in &events {
        let reference = sinex_model::query::Reference::Key {
            source: "s".into(),
            event_type: event.event_type.clone(),
            key: event.equivalence_key.as_ref().unwrap().0.clone(),
        };
        // Resolve through the public query path as well as classification.
        let shown = crate::query::show(&mut conn, &reference).await.unwrap();
        assert!(shown.to_string().contains(&event.id.to_string()));
    }
    assert_eq!(
        admit_one(&mut conn, &batch(events)).await.repeats,
        keys.len()
    );
}

#[devkit::test]
async fn keyed_and_location_readings_stay_separate_when_the_text_matches(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut located = keyed("s", 1, 1, 1);
    located.equivalence_key = None;
    let Origin::Record {
        location,
        output_index,
    } = located.origin
    else {
        unreachable!()
    };
    let mut keyed = keyed("s", 2, 2, 2);
    keyed.equivalence_key = Some(sinex_model::EquivalenceKey(format!(
        "{}:{}:{}",
        location.material.0, location.record_offset, output_index
    )));
    let b = batch(vec![located.clone(), keyed.clone()]);
    let outcome = admit_one(&mut conn, &b).await;
    assert_eq!((outcome.admitted, outcome.superseded), (2, 0));
    let before = crate::query::current_seq(&mut conn).await.unwrap();
    let outcome = admit_one(&mut conn, &b).await;
    assert_eq!(
        (outcome.admitted, outcome.repeats, outcome.commit_seq),
        (0, 2, None)
    );
    assert_eq!(crate::query::current_seq(&mut conn).await.unwrap(), before);
    let stage: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_temp.admission_stage")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(stage, 0);
    crate::lifecycle::move_events(
        &mut conn,
        sinex_model::lifecycle::Act::Archive,
        vec![located.id],
        &NoDerivations,
    )
    .await;
    // Archiving the location's reading leaves the keyed reading live, and a
    // new reading of the archived record is admitted live.
    let mut read_again = located.clone();
    read_again.id = EventId::new();
    let outcome = admit_one(&mut conn, &batch(vec![read_again.clone(), keyed.clone()])).await;
    assert_eq!((outcome.admitted, outcome.repeats), (1, 1));
    let live: Vec<Uuid> = sqlx::query_scalar("SELECT event_id FROM core.live_locations")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(live, [read_again.id.0]);
}
