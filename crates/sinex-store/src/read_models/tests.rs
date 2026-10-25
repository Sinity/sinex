use super::*;

#[devkit::test]
async fn indexes_committed_payload_text_once_and_resumes(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let ids = crate::query::tests::admit_commands(
        &mut conn,
        &[("Cargo test --workspace", "/src/tide"), ("ls", "/")],
    )
    .await;
    let up_to = crate::query::current_seq(&mut conn).await.unwrap();
    assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), up_to);
    let text = sqlx::query_scalar!(
        "SELECT text FROM core.search_text WHERE event_id = $1",
        ids[0].0
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert!(text.contains("cargo test workspace"), "{text}");
    // Running again changes nothing.
    assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), up_to);
    let n = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.search_text"#)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    // Two commands, each an observation and a fused command.
    assert_eq!(n, 4);
}
