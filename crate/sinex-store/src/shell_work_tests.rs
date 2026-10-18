use super::WORK_TABLES;
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

fn plans(value: &Value) -> Vec<&Value> {
    let mut out = vec![value];
    if let Some(children) = value["Plans"].as_array() {
        for child in children {
            out.extend(plans(child));
        }
    }
    out
}

#[tokio::test]
async fn work_table_indexes_serve_pending_and_output_lookups()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sinex_testkit::database().await;
    let mut connection = PgConnection::connect(&db.writer).await?;
    let role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&mut connection)
        .await?;
    assert_eq!(role, "sinex_writer");
    let additions = [
        "sinex_shell_nodes_pending",
        "sinex_shell_groups_pending",
        "sinex_shell_groups_output",
    ];
    let mut results = Vec::new();
    let mut evidence = Vec::new();
    for indexed in [false, true] {
        // The baseline omits only these three indexes; data and queries are
        // the same, so results must match.
        for statement in WORK_TABLES {
            if indexed || !additions.iter().any(|name| statement.contains(name)) {
                sqlx::query(statement).execute(&mut connection).await?;
            }
        }
        for statement in [
            "INSERT INTO sinex_shell_nodes(id,source,host,command,command_hash,at_ns,digest,anchor,ekey) SELECT lpad(to_hex(n),32,'0')::uuid,0,'synthetic','command',md5(n::text),n,'digest',n,'key' FROM generate_series(1,20000)n",
            "INSERT INTO sinex_shell_groups(token,parents,strategy) SELECT id,ARRAY[id],0 FROM sinex_shell_nodes",
            "UPDATE sinex_shell_nodes SET paired=true WHERE id<=lpad(to_hex(19936),32,'0')::uuid",
            "UPDATE sinex_shell_groups g SET done=(n<=19936),output_id=CASE WHEN n%3=0 THEN NULL ELSE lpad(to_hex(n+20000),32,'0')::uuid END FROM generate_series(1,20000)n WHERE g.token=lpad(to_hex(n),32,'0')::uuid",
            "UPDATE sinex_shell_groups SET output_id=lpad(to_hex(40000),32,'0')::uuid WHERE token=lpad(to_hex(19999),32,'0')::uuid",
        ] {
            sqlx::query(statement).execute(&mut connection).await?;
        }
        let indexes: Vec<String> = sqlx::query_scalar("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid IN ('pg_temp.sinex_shell_nodes'::regclass,'pg_temp.sinex_shell_groups'::regclass) ORDER BY indexrelid")
            .fetch_all(&mut connection).await?;
        for name in additions {
            assert_eq!(
                indexes.iter().any(|definition| definition.contains(name)),
                indexed
            );
        }
        let mut actual = Vec::new();
        let mut captured = Vec::new();
        for changed in [false, true] {
            if changed {
                // An early row becomes pending again while a late one finishes;
                // both lookups must see both updates.
                for statement in [
                    "UPDATE sinex_shell_nodes SET paired=false WHERE id=lpad(to_hex(2),32,'0')::uuid",
                    "UPDATE sinex_shell_nodes SET paired=true WHERE id=lpad(to_hex(20000),32,'0')::uuid",
                    "UPDATE sinex_shell_groups SET done=false,output_id=lpad(to_hex(424242),32,'0')::uuid WHERE token=lpad(to_hex(2),32,'0')::uuid",
                    "UPDATE sinex_shell_groups SET done=true WHERE token=lpad(to_hex(20000),32,'0')::uuid",
                ] {
                    sqlx::query(statement).execute(&mut connection).await?;
                }
            }
            let expected: Vec<Uuid> = if changed {
                std::iter::once(2)
                    .chain(19937..20000)
                    .map(Uuid::from_u128)
                    .collect()
            } else {
                (19937..=20000).map(Uuid::from_u128).collect()
            };
            let mut sets = Vec::new();
            for (name, text) in [
                (
                    "sinex_shell_nodes_pending",
                    "SELECT id FROM sinex_shell_nodes WHERE NOT paired ORDER BY id LIMIT 64",
                ),
                (
                    "sinex_shell_groups_pending",
                    "SELECT token FROM sinex_shell_groups WHERE NOT done ORDER BY token LIMIT 64",
                ),
            ] {
                let rows: Vec<Uuid> = sqlx::query_scalar(text).fetch_all(&mut connection).await?;
                assert_eq!(rows, expected);
                sets.push(rows);
                let plan: Value =
                    sqlx::query_scalar(&format!("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {text}"))
                        .fetch_one(&mut connection)
                        .await?;
                if indexed {
                    assert!(
                        plans(&plan[0]["Plan"])
                            .iter()
                            .any(|node| node["Index Name"] == name),
                        "{plan}"
                    );
                    assert!(
                        plans(&plan[0]["Plan"])
                            .iter()
                            .all(
                                |node| node["Rows Removed by Filter"].as_f64().unwrap_or(0.0)
                                    == 0.0
                            ),
                        "{plan}"
                    );
                } else {
                    assert!(
                        plans(&plan[0]["Plan"])
                            .iter()
                            .any(
                                |node| node["Rows Removed by Filter"].as_f64().unwrap_or(0.0)
                                    >= 19000.0
                            ),
                        "{plan}"
                    );
                }
                captured.push(json!({"query":text,"changed":changed,"plan":plan}));
            }
            let mut membership = Vec::new();
            for (id, expected) in [
                (None, false),
                (Some(0), false),
                (Some(20003), false),
                (Some(40000), true),
                (Some(20002), !changed),
                (Some(424242), changed),
            ] {
                let id = id.map(Uuid::from_u128);
                let found: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM sinex_shell_groups WHERE output_id=$1)",
                )
                .bind(id)
                .fetch_one(&mut connection)
                .await?;
                assert_eq!(found, expected);
                membership.push(found);
                let plan: Value = sqlx::query_scalar("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT EXISTS(SELECT 1 FROM sinex_shell_groups WHERE output_id=$1)")
                    .bind(id).fetch_one(&mut connection).await?;
                if indexed && id.is_some() {
                    assert!(
                        plans(&plan[0]["Plan"])
                            .iter()
                            .any(|node| node["Index Name"] == "sinex_shell_groups_output"),
                        "{plan}"
                    );
                }
                if !indexed && id.is_some_and(|id| id.is_nil()) {
                    assert!(
                        plans(&plan[0]["Plan"])
                            .iter()
                            .any(
                                |node| node["Rows Removed by Filter"].as_f64().unwrap_or(0.0)
                                    == 20000.0
                            ),
                        "{plan}"
                    );
                }
                captured.push(json!({"output_id":id,"changed":changed,"plan":plan}));
            }
            actual.push(json!({"sets":sets,"membership":membership}));
        }
        results.push(actual);
        evidence.push(json!({"indexed":indexed,"indexes":indexes,"plans":captured}));
    }
    assert_eq!(results[0], results[1]);
    println!(
        "SHELL_WORK_PROOF {}",
        json!({"population":20000,"pending":64,"baseline_indexed_equal":true,"evidence":evidence})
    );
    connection.close().await?;
    Ok(())
}
