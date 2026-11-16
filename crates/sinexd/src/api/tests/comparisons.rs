use super::*;

#[devkit::test]
async fn comparisons_continue_from_the_last_row(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let mut expected = Vec::new();
    for _ in 0..3 {
        let (_, body) = call_unbounded(
            &world,
            Some(OPERATOR),
            json!({"method":"recompute","derivation":"shell.sessions","compare":true}),
        )
        .await;
        assert_eq!(body["outcome"], "ok", "{body}");
        expected.push(body["result"]["comparison"].as_i64().unwrap());
    }
    expected.reverse();
    let mut cursor = None::<String>;
    let mut seen = Vec::new();
    loop {
        let (_, body) = call_unbounded(&world, Some(READ), json!({"method":"comparisons","derivation":"shell.sessions","page-size":1,"cursor":cursor})).await;
        assert_eq!(body["outcome"], "ok", "{body}");
        let rows = body["result"]["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1, "{body}");
        seen.push(rows[0]["id"].as_i64().unwrap());
        cursor = body["result"]["page"]["cursor"].as_str().map(str::to_owned);
        if let Some(cursor) = &cursor {
            assert_eq!(cursor, &seen.last().unwrap().to_string());
        } else {
            break;
        }
    }
    assert_eq!(seen, expected);
}
