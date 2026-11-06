//! Projection values and names remain separate from relation identity and order.
use super::*;

async fn fixture(db: &devkit::pg::TestDatabase) -> crate::AdmissionContext {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_commands(
        &mut conn,
        &[("a", "/one"), ("bbbb", "/two"), ("cc", "/one")],
    )
    .await;
    conn
}

#[devkit::test]
async fn projected_private_name_keeps_public_value_and_cursor_identity(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = fixture(&db).await;
    let query = "commands | fields len(command) as _id, command | sort _id";
    let mut cursor = None;
    let mut rows = Vec::new();
    loop {
        let answer = ask_page(
            &mut conn,
            query,
            Page {
                size: Some(1),
                cursor,
            },
        )
        .await;
        assert_eq!(answer.total, 3);
        assert_eq!(answer.columns, ["_id", "command"]);
        rows.extend(answer.rows);
        cursor = answer.page.cursor;
        if cursor.is_none() {
            break;
        }
        assert!(rows.len() < 3, "cursor repeated a projection row");
    }
    assert_eq!(
        rows,
        [
            json!({"_id": 1, "command": "a"}),
            json!({"_id": 2, "command": "cc"}),
            json!({"_id": 4, "command": "bbbb"})
        ]
    );
}

#[devkit::test]
async fn projected_expressions_and_aliases_feed_filters_sorts_and_groups(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = fixture(&db).await;
    let answer = ask(&mut conn, "commands | fields len(command) + 1 as score, cwd as place | where score >= 3 | sort score desc | fields place, score | group by place | agg sum(score) as total | sort place").await;
    assert_eq!(
        answer.rows,
        [
            json!({"place": "/one", "total": 3}),
            json!({"place": "/two", "total": 5})
        ]
    );
}

#[devkit::test]
async fn projection_preserves_order_when_replacing_the_sort_column(db: devkit::pg::TestDatabase) {
    let mut conn = fixture(&db).await;
    let answer = ask(
        &mut conn,
        "commands | sort command | fields len(command) as command",
    )
    .await;
    assert_eq!(column(&answer, "command"), [json!(1), json!(4), json!(2)]);
}

#[devkit::test]
async fn sorting_after_a_limit_sorts_the_selected_rows(db: devkit::pg::TestDatabase) {
    let mut conn = fixture(&db).await;
    let limited_first = ask(
        &mut conn,
        "commands | limit 2 | sort command desc | fields command",
    )
    .await;
    let sorted_first = ask(
        &mut conn,
        "commands | sort command desc | limit 2 | fields command",
    )
    .await;
    assert_eq!(
        column(&limited_first, "command"),
        [json!("bbbb"), json!("a")]
    );
    assert_eq!(
        column(&sorted_first, "command"),
        [json!("cc"), json!("bbbb")]
    );
}

async fn private_parents(conn: &mut PgConnection, text: &str) -> Vec<Vec<Uuid>> {
    let context = ctx();
    let query = bind(text, &context).unwrap();
    let compiled = compile::Compiler::new(&context, current_seq(conn).await.unwrap())
        .compile(&query)
        .unwrap();
    let mut statement = sql::SqlFragment::text("SELECT q.\"_parents\" FROM (");
    statement.append(compiled.answer).push(") q");
    let (text, values) = statement.render();
    sql::bind_scalar(sqlx::query_scalar::<_, Vec<Uuid>>(&text), values)
        .fetch_all(&mut *conn)
        .await
        .unwrap()
}

fn parent_set(parents: &[Uuid]) -> std::collections::BTreeSet<Uuid> {
    parents.iter().copied().collect()
}

#[devkit::test]
async fn grouping_keeps_event_contributors_after_public_identity_is_hidden(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = admit_commands(
        &mut conn,
        &[("a", "/one"), ("bbbb", "/two"), ("cc", "/one")],
    )
    .await;
    let parents = private_parents(&mut conn, "commands | fields command as _parents | count").await;
    assert_eq!(parents.len(), 1);
    assert_eq!(
        parent_set(&parents[0]),
        ids.iter().map(|(fused, _)| *fused).collect()
    );
}

#[devkit::test]
async fn pair_relations_keep_all_matches_child_order_and_exact_event_contributors(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = admit_commands(
        &mut conn,
        &[("a", "/one"), ("bbbb", "/two"), ("cc", "/one")],
    )
    .await;
    let text = "commands where command = a | fields command | join near (commands where cwd = /one | sort when desc | fields command) within 5m | fields command, match.command";
    let answer = ask(&mut conn, text).await;
    assert_eq!(column(&answer, "match.command"), [json!("cc"), json!("a")]);
    let parents = private_parents(&mut conn, text).await;
    assert_eq!(parents.len(), 2);
    assert_eq!(parent_set(&parents[0]), parent_set(&[ids[0].0, ids[2].0]));
    assert_eq!(parent_set(&parents[1]), parent_set(&[ids[0].0]));
}

#[devkit::test]
async fn positive_order_predicates_keep_all_witnesses_and_skip_false_or_negated_branches(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = admit_commands(
        &mut conn,
        &[("a", "/one"), ("bbbb", "/two"), ("cc", "/one")],
    )
    .await;
    let positive = private_parents(&mut conn, "commands where command = a AND before(commands where command != a) within 5m | fields command as id").await;
    assert_eq!(positive.len(), 1);
    assert_eq!(
        parent_set(&positive[0]),
        ids.iter().map(|(fused, _)| *fused).collect()
    );
    let false_branch = private_parents(&mut conn, "commands where (before(commands where command = bbbb) within 5m AND command = missing) OR command = a").await;
    assert_eq!(false_branch.len(), 1);
    assert_eq!(parent_set(&false_branch[0]), parent_set(&[ids[0].0]));
    let negated = private_parents(
        &mut conn,
        "commands where command = a AND NOT before(commands where command = missing) within 5m",
    )
    .await;
    assert_eq!(negated.len(), 1);
    assert_eq!(parent_set(&negated[0]), parent_set(&[ids[0].0]));
}

#[devkit::test]
async fn an_order_predicate_publishes_its_nearest_match_to_later_stages(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = fixture(&db).await;
    let answer = ask(&mut conn, "commands where command = a AND before(commands where command != a | sort when desc) within 5m | where match.command = bbbb | fields match.command, match.distance").await;
    assert_eq!(column(&answer, "match.command"), [json!("bbbb")]);
    assert_eq!(answer.rows[0]["match.distance"].as_f64(), Some(60.0));
}
