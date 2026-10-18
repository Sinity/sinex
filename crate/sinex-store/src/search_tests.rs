use super::*;

#[test]
fn snippet_marks_phrase_and_prefix_matches_in_the_first_matching_field() {
    let mut terms = Vec::new();
    bind(r#"commands where "git push" or deplo*"#)
        .unwrap()
        .filter
        .unwrap()
        .terms(&mut terms);
    let fields = serde_json::to_string(&["ls", "Git push; deploy now"]).unwrap();
    let found = snippet(&fields, &terms).unwrap();
    assert_eq!(found["text"], "Git push; deploy now");
    assert_eq!(found["spans"], json!([[0, 3], [4, 8], [10, 16]]));
    let none = snippet(&fields, &[]).unwrap();
    assert_eq!(none["text"], "ls");
    assert_eq!(none["spans"], json!([]));
}

/// Checks the SQL for unknown times directly; the Atuin parser does not emit them.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL credentials"]
async fn unknown_time_stays_unknown_under_negation() -> Result<()> {
    let url = std::env::var("SINEX_TEST_READER_URL").expect("reader URL required");
    let mut connection = sqlx::PgConnection::connect(&url).await?;
    for (predicate, known) in [
        ("when = 2026-09-14T00:00:00Z", true),
        ("when != 2026-09-14T00:00:00Z", false),
        ("not when != 2026-09-14T00:00:00Z", true),
        ("when < 2026-09-14T00:00:00Z", false),
        ("not when > 2026-09-14T00:00:00Z", true),
    ] {
        let bound = bind(&format!("commands where {predicate}")).unwrap();
        let mut query = QueryBuilder::<Postgres>::new(
            "WITH s(n,ts_orig,ts_orig_tiebreak) AS (VALUES(0,NULL::timestamptz,789),(1,'2026-09-14T00:00:00Z'::timestamptz,0)) SELECT array_agg(matched ORDER BY n) FROM (SELECT n,",
        );
        expression(&mut query, bound.filter.as_ref().unwrap());
        query.push(" AS matched FROM s) truth");
        let matched: Vec<Option<bool>> = query
            .build_query_scalar()
            .fetch_one(&mut connection)
            .await?;
        assert_eq!(matched, vec![None, Some(known)], "{predicate}");
    }
    connection.close().await?;
    Ok(())
}
