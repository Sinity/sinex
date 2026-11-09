use super::*;

/// Every untyped outcome shape stored before becomes its typed result with
/// the same facts: older admission counts, a replay, a refusal and a kind
/// registration, with and without a content hash.
#[devkit::test]
async fn untyped_frame_outcomes_become_typed_results(db: devkit::pg::TestDatabase) {
    let mut conn = <PgConnection as Connection>::connect(&db.url(None))
        .await
        .unwrap();
    conn.execute(
        r#"INSERT INTO core.ingress_frames (producer, frame, outcome, refusal) VALUES
        ('p', 'older', '{"admitted":2,"hidden":0,"older":1,"repeats":3,"replaced":0,"superseded":0}', NULL),
        ('p', 'current', '{"frame_content_hash":"blake3:aa","registered_kinds":[],"commit_range":[4,5],"superseded_input":0,"replay_archived":0,"removed_at_source":1,"dirty_keys":2,"admitted":2,"repeats":0,"replaced":0,"older":0,"superseded":0,"failures":0,"refusal_kind":null,"source":"s","event_types":["e"]}', NULL),
        ('p', 'replay', '{"admitted":3,"replay":"01890000-0000-7000-8000-000000000001","frame_content_hash":"blake3:bb"}', NULL),
        ('p', 'refused', '{"frame_content_hash":"blake3:cc","registered_kinds":[],"commit_range":null,"admitted":0,"failures":2,"refusal_kind":"content","source":null,"event_types":["e","f"]}', 'bad reading'),
        ('p', 'kinds', '{"frame_content_hash":"blake3:dd","registered_kinds":[[{"name":"x.y","version":1},"hash"]],"commit_range":null,"admitted":0,"refusal_kind":null,"source":null,"event_types":[]}', NULL)"#,
    )
    .await
    .unwrap();
    let mut tx = conn.begin().await.unwrap();
    for statement in crate::frame_outcomes::statements() {
        tx.execute(statement.as_str()).await.unwrap();
    }
    tx.commit().await.unwrap();
    let rows: Vec<(String, serde_json::Value)> =
        sqlx::query_as("SELECT frame, outcome FROM core.ingress_frames ORDER BY frame")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let counts = |admitted: u64, repeats: u64, older: u64, removed: u64, dirty: u64| serde_json::json!({"admitted":admitted,"repeats":repeats,"replaced":0,"older":older,"superseded":0,"superseded_input":0,"failures":0,"dirty_keys":dirty,"replay_archived":0,"removed_at_source":removed});
    assert_eq!(
        rows,
        vec![
            (
                "current".into(),
                serde_json::json!({"content_hash":"blake3:aa","result":"admitted","counts":counts(2,0,0,1,2),"commit_range":[4,5]})
            ),
            (
                "kinds".into(),
                serde_json::json!({"content_hash":"blake3:dd","result":"kinds_registered","kinds":[[{"name":"x.y","version":1},"hash"]]})
            ),
            (
                "older".into(),
                serde_json::json!({"content_hash":null,"result":"admitted","counts":counts(2,3,1,0,0),"commit_range":null})
            ),
            (
                "refused".into(),
                serde_json::json!({"content_hash":"blake3:cc","result":"refused","kind":"content","failures":2,"source":null,"event_types":["e","f"]})
            ),
            (
                "replay".into(),
                serde_json::json!({"content_hash":"blake3:bb","result":"replayed","operation":"01890000-0000-7000-8000-000000000001"})
            ),
        ]
    );
}

/// A nonzero count of suppressed hidden readings has nowhere to go, so the
/// upgrade stops instead of dropping it.
#[devkit::test]
async fn hidden_reading_counts_stop_the_frame_outcome_upgrade(db: devkit::pg::TestDatabase) {
    let mut conn = <PgConnection as Connection>::connect(&db.url(None))
        .await
        .unwrap();
    conn.execute(
        r#"INSERT INTO core.ingress_frames (producer, frame, outcome) VALUES ('p', 'f', '{"admitted":0,"hidden":1}')"#,
    )
    .await
    .unwrap();
    let mut tx = conn.begin().await.unwrap();
    let mut failed = false;
    for statement in crate::frame_outcomes::statements() {
        if tx.execute(statement.as_str()).await.is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed);
}
