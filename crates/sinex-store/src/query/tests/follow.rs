use super::*;

#[devkit::test]
async fn follow_watermark_waits_for_durable_search_projection(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut conn = crate::AdmissionContext::connect(&url).await.unwrap();
    admit_commands(&mut conn, &[("cargo build", "/src/tide")]).await;
    let committed = current_seq(&mut conn).await.unwrap();
    assert!(committed > 0);
    assert_eq!(follow_position(&mut conn).await.unwrap(), 0);

    let mut indexer = PgConnection::connect(&url).await.unwrap();
    let mut tx = indexer.begin().await.unwrap();
    let page = crate::commit_log::after(&mut tx, 0, 100, i64::MAX)
        .await
        .unwrap();
    crate::read_models::apply_search_text(&mut tx, &page.events)
        .await
        .unwrap();
    crate::commit_log::set_consumer_position(
        &mut tx,
        crate::read_models::SEARCH_TEXT,
        page.up_to.unwrap(),
        0,
    )
    .await
    .unwrap();
    assert_eq!(follow_position(&mut conn).await.unwrap(), 0);
    tx.commit().await.unwrap();
    assert_eq!(follow_position(&mut conn).await.unwrap(), committed);
}

#[devkit::test]
async fn follow_range_excludes_later_commits_from_every_page(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_commands(&mut conn, &[("cargo before", "/src/tide")]).await;
    let after = current_seq(&mut conn).await.unwrap();
    let within = admit_commands(
        &mut conn,
        &[
            ("cargo first", "/src/tide"),
            ("cargo second", "/src/tide"),
            ("cargo third", "/src/tide"),
        ],
    )
    .await;
    catch_up_search_text(&mut conn).await.unwrap();
    let through = follow_position(&mut conn).await.unwrap();
    let later = admit_commands(&mut conn, &[("cargo later", "/src/tide")]).await;
    catch_up_search_text(&mut conn).await.unwrap();
    let context = ctx();
    let text = "events where cargo";
    let bound = bind(text, &context).unwrap();
    let query = follow_query(&bound, after, through);
    let mut page = Page {
        size: Some(2),
        cursor: None,
    };
    let mut rows = Vec::new();
    loop {
        let answer = run(&mut conn, text, Some(&query), &context, &page)
            .await
            .unwrap();
        rows.extend(column(&answer, "id"));
        page.cursor = answer.page.cursor;
        if page.cursor.is_none() {
            break;
        }
    }
    let expected: std::collections::BTreeSet<_> = within
        .iter()
        .flat_map(|(fused, observation)| [json!(fused).to_string(), json!(observation).to_string()])
        .collect();
    assert_eq!(rows.len(), expected.len(), "range ({after}, {through}], rows={rows:?}, expected={expected:?}");
    assert_eq!(
        rows.into_iter()
            .map(|id| id.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        expected
    );

    let next = follow_query(&bound, through, follow_position(&mut conn).await.unwrap());
    let answer = run(&mut conn, text, Some(&next), &context, &Page::default())
        .await
        .unwrap();
    let expected: std::collections::BTreeSet<_> = later
        .iter()
        .flat_map(|(fused, observation)| [json!(fused).to_string(), json!(observation).to_string()])
        .collect();
    let rows = column(&answer, "id");
    assert_eq!(rows.len(), expected.len());
    assert_eq!(
        rows.into_iter()
            .map(|id| id.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        expected
    );
}
