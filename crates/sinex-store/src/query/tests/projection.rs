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
    catch_up_search_text(&mut conn).await.unwrap();
    let searched = ask(&mut conn, &format!("{text} | where \"a\"")).await;
    assert_eq!(
        column(&searched, "match.command"),
        [json!("cc"), json!("a")]
    );
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

#[devkit::test]
async fn glob_preserves_case_and_literal_sql_metacharacters_with_dynamic_patterns(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_commands(
        &mut conn,
        &[("A%_\\中", "A*中"), ("a%_\\中", "A*中"), ("Axyz中", "A*中")],
    )
    .await;
    let answer = ask(
        &mut conn,
        "commands where glob(command, cwd) = true | fields command | sort command",
    )
    .await;
    assert_eq!(
        column(&answer, "command"),
        [json!("A%_\\中"), json!("Axyz中")]
    );
    let literal = ask(
        &mut conn,
        r#"commands where glob(command, "*%_\\中") = true | fields command | sort command"#,
    )
    .await;
    assert_eq!(
        column(&literal, "command"),
        [json!("A%_\\中"), json!("a%_\\中")]
    );
    let missing = ask(
        &mut conn,
        "commands | fields glob(command, unknown) as matched",
    )
    .await;
    assert!(missing.rows.iter().all(|row| row["matched"].is_null()));
    assert_eq!(
        ask(&mut conn, r#"events where source = "terminal.*""#)
            .await
            .total,
        0
    );
    assert_eq!(
        ask(
            &mut conn,
            r#"events where glob(source, "terminal.*") = true"#
        )
        .await
        .total,
        3
    );
}

#[devkit::test]
async fn first_in_each_group_keeps_original_rows_order_and_page_identity(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = fixture(&db).await;
    let text = "commands | sort command desc | group by cwd | first 1 | fields id, command, cwd";
    let first = ask_page(
        &mut conn,
        text,
        Page {
            size: Some(1),
            cursor: None,
        },
    )
    .await;
    assert_eq!(first.total, 2);
    assert_eq!(first.rows[0]["command"], json!("cc"));
    let second = ask_page(
        &mut conn,
        text,
        Page {
            size: Some(1),
            cursor: first.page.cursor,
        },
    )
    .await;
    assert_eq!(second.rows[0]["command"], json!("bbbb"));
    assert_ne!(first.rows[0]["id"], second.rows[0]["id"]);
    assert!(second.page.cursor.is_none());
    let after_limit = ask(
        &mut conn,
        "commands | limit 2 | sort command desc | group by cwd | first 1 | fields command",
    )
    .await;
    assert_eq!(column(&after_limit, "command"), [json!("bbbb"), json!("a")]);
}

#[devkit::test]
async fn generated_snippets_and_actual_span_bounds_remain_projectable(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let answer = ask(&mut conn, "events where text:workspace | fields snippet").await;
    assert!(!answer.rows.is_empty());
    assert!(answer.rows.iter().all(|row| {
        row["snippet"]
            .as_str()
            .is_some_and(|text| text.contains("workspace"))
    }));
    let no_search = ask(&mut conn, "commands | fields snippet").await;
    assert!(no_search.rows.iter().all(|row| row["snippet"].is_null()));
    let spans = ask(
        &mut conn,
        "events where event_type = command.fused | fields start, end",
    )
    .await;
    assert_eq!(spans.total, 5);
    assert!(
        spans
            .rows
            .iter()
            .all(|row| row["start"].is_string() && row["end"].is_string())
    );
}

#[devkit::test]
async fn absolute_numeric_values_keep_unknown_rows_and_integer_magnitude(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = fixture(&db).await;
    let answer = ask(
        &mut conn,
        "commands | fields command, abs(exit_code - 1) as magnitude | sort command",
    )
    .await;
    assert_eq!(
        column(&answer, "magnitude"),
        [json!(1), json!(0), serde_json::Value::Null]
    );
}

#[devkit::test]
async fn explicit_witness_order_preserves_outer_order_identity_and_near_ties(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = fixture(&db).await;
    let chosen=ask(&mut conn,"commands where near (commands) within 5m pick first by when desc | fields command, match.command").await;
    assert_eq!(
        column(&chosen, "command"),
        [json!("a"), json!("bbbb"), json!("cc")]
    );
    assert_eq!(
        column(&chosen, "match.command"),
        [json!("cc"), json!("cc"), json!("cc")]
    );
    let tied=ask(&mut conn,"commands where command = bbbb AND near (commands where command != bbbb) within 1m pick first by abs(match.distance), when, match.id | fields match.command").await;
    assert_eq!(column(&tied, "match.command"), [json!("a")]);
}

#[devkit::test]
async fn during_can_select_the_earliest_actual_span_without_changing_outer_grain(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_commands_with_duration(
        &mut conn,
        &[("early", "/one"), ("later", "/one")],
        120_000_000_000,
    )
    .await;
    let chosen=ask(&mut conn,"commands where during (commands) at match.start pick first by start, match.id | fields command, match.command").await;
    assert_eq!(chosen.total, 2);
    assert_eq!(
        column(&chosen, "match.command"),
        [json!("early"), json!("early")]
    );
}

#[devkit::test]
async fn source_identity_projection_is_the_existing_binding_key(db: devkit::pg::TestDatabase) {
    let mut conn = fixture(&db).await;
    let answer = ask(
        &mut conn,
        "sources where events > 0 | fields id, binding | sort binding",
    )
    .await;
    assert!(!answer.rows.is_empty());
    assert!(answer.rows.iter().all(|row| row["id"] == row["binding"]));
}

#[devkit::test]
async fn absolute_unknown_values_stay_unknown(db: devkit::pg::TestDatabase) {
    let mut conn = fixture(&db).await;
    let answer = ask(&mut conn, "commands | fields abs(unknown) as magnitude").await;
    assert_eq!(answer.total, 3);
    assert!(answer.rows.iter().all(|row| row["magnitude"].is_null()));
}

#[devkit::test]
async fn filter_operands_read_the_input_witness_before_exposing_a_new_one(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = fixture(&db).await;
    let prefix = "commands where near (commands where command = a) within 5m";
    let condition = "near (commands where command = cc) within 5m AND match.command = a";
    let reversed = "match.command = a AND near (commands where command = cc) within 5m";
    let first_query = format!("{prefix} | where {condition} | fields command, match.command");
    let second_query = format!("{prefix} | where {reversed} | fields command, match.command");
    let first = ask_page(
        &mut conn,
        &first_query,
        Page {
            size: Some(1),
            cursor: None,
        },
    )
    .await;
    assert_eq!(first.total, 3);
    assert_eq!(first.rows[0]["match.command"], json!("cc"));
    let next = ask_page(
        &mut conn,
        &second_query,
        Page {
            size: Some(2),
            cursor: first.page.cursor,
        },
    )
    .await;
    assert_eq!(column(&next, "command"), [json!("bbbb"), json!("cc")]);
    assert!(
        next.rows
            .iter()
            .all(|row| row["match.command"] == json!("cc"))
    );
    for condition in [
        "near (commands where command = bbbb) within 5m AND near (commands where command = cc) within 5m",
        "near (commands where command = bbbb) within 5m OR near (commands where command = cc) within 5m",
    ] {
        let query = format!("{prefix} | where {condition} | fields match.command");
        let answer = ask(&mut conn, &query).await;
        assert_eq!(answer.total, 3);
        assert!(
            answer
                .rows
                .iter()
                .all(|row| row["match.command"] == json!("a")),
            "{query}"
        );
    }
    let negative=ask(&mut conn,&format!("{prefix} | where NOT near (commands where command = cc) within 0s | fields command, match.command")).await;
    assert_eq!(column(&negative, "command"), [json!("a"), json!("bbbb")]);
    assert!(
        negative
            .rows
            .iter()
            .all(|row| row["match.command"] == json!("a"))
    );
}
