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
