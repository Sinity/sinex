use super::*;

/// Bring the search read model up to the newest commit without the daemon's
/// reader, page by page with its position.
pub(crate) async fn catch_up_search_text(conn: &mut PgConnection) -> Result<i64> {
    let (mut position, _) = crate::commit_log::consumer_position(conn, SEARCH_TEXT).await?;
    loop {
        let page = crate::commit_log::after(conn, position, 2_000, i64::MAX).await?;
        let Some(up_to) = page.up_to else {
            return Ok(position);
        };
        let mut tx = sqlx::Connection::begin(&mut *conn).await?;
        apply_search_text(&mut tx, &page.events).await?;
        crate::commit_log::set_consumer_position(&mut tx, SEARCH_TEXT, up_to, 0).await?;
        tx.commit().await?;
        position = up_to;
    }
}

#[devkit::test]
async fn indexes_committed_payload_text_once_and_resumes(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
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
