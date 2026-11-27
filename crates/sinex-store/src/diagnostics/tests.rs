use super::*;
use serde_json::{Value, json};
use sqlx::{Connection, Executor};

async fn insert(conn: &mut PgConnection, kind: &str, payload: Value, class: &str, archived: bool) {
    // Stored readings may predate the current payload declaration, including
    // an absent or unknown pair state. Read diagnostics must keep that meaning.
    let id = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class) VALUES($1,$2,'fixture.fusion',uuidv7(),uuidv7(),0,'unknown',1,$3,1,decode(repeat('01',32),'hex'),$4)")
        .bind(id).bind(kind).bind(payload).bind(class).execute(&mut *conn).await.unwrap();
    if archived {
        sqlx::query(
            "INSERT INTO audit.archived(event_id,archived_seq,reason) VALUES($1,2,'archive')",
        )
        .bind(id)
        .execute(&mut *conn)
        .await
        .unwrap();
    }
}

fn counts(family: &str, undecided: i64, decided: i64, ambiguous_fused: i64) -> FusionAmbiguity {
    FusionAmbiguity {
        family: family.into(),
        undecided,
        decided,
        ambiguous_fused,
    }
}

async fn project_fusion_readings(conn: &mut PgConnection) {
    conn.execute(
        "INSERT INTO core.fusion_readings (event_id, name, figure, class, live)
         SELECT e.id, f.name, f.figure, e.class,
                NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
         FROM core.events e CROSS JOIN LATERAL core.fusion_figure(e.event_type, e.payload) f;
         INSERT INTO core.fusion_counts (name, figure, class, count)
         SELECT name, figure, class, count(*) FROM core.fusion_readings WHERE live
         GROUP BY name, figure, class",
    )
    .await
    .unwrap();
}

#[devkit::test]
async fn fusion_counts_preserve_duplicates_zeros_unknown_states_and_visibility(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    for state in [
        Some("undecided"),
        Some("undecided"),
        Some("same"),
        Some("distinct"),
        Some("future"),
        None,
    ] {
        insert(
            &mut conn,
            "fusion.considered_pair",
            json!({"family":"command","state":state}),
            "open",
            false,
        )
        .await;
    }
    insert(
        &mut conn,
        "fusion.considered_pair",
        json!({"family":"command"}),
        "open",
        false,
    )
    .await;
    insert(
        &mut conn,
        "fusion.considered_pair",
        json!({"family":"command","state":"undecided"}),
        "guarded",
        false,
    )
    .await;
    insert(
        &mut conn,
        "fusion.considered_pair",
        json!({"family":"command","state":"undecided"}),
        "open",
        true,
    )
    .await;
    for state in ["undecided", "same"] {
        insert(
            &mut conn,
            "fusion.considered_pair",
            json!({"family":"mail","state":state}),
            "open",
            false,
        )
        .await;
    }
    for (kind, ambiguous, class, archived) in [
        ("command.fused", Some(2), "open", false),
        ("command.fused", Some(3), "open", false),
        ("command.fused", Some(1), "open", false),
        ("command.fused", None, "open", false),
        ("command.fused", Some(2), "guarded", false),
        ("command.fused", Some(2), "open", true),
        ("mail.fused", Some(2), "open", false),
        ("unrequested.fused", Some(2), "open", false),
    ] {
        insert(
            &mut conn,
            kind,
            json!({"ambiguous":ambiguous}),
            class,
            archived,
        )
        .await;
    }
    let families: Vec<_> = [
        ("mail", "mail.fused"),
        ("command", "command.fused"),
        ("command", "command.fused"),
        ("command", "mail.fused"),
        ("empty", "empty.fused"),
    ]
    .into_iter()
    .map(|(name, kind)| (name.into(), kind.into()))
    .collect();
    project_fusion_readings(&mut conn).await;
    crate::query::set_grants(&mut conn, "").await.unwrap();
    assert_eq!(
        fusion_ambiguity(&mut conn, &families).await.unwrap(),
        [
            counts("command", 2, 3, 2),
            counts("command", 2, 3, 2),
            counts("command", 2, 3, 1),
            counts("empty", 0, 0, 0),
            counts("mail", 1, 1, 1)
        ]
    );
    assert!(fusion_ambiguity(&mut conn, &[]).await.unwrap().is_empty());
    crate::query::set_grants(&mut conn, "guarded")
        .await
        .unwrap();
    assert_eq!(
        fusion_ambiguity(&mut conn, &families).await.unwrap(),
        [
            counts("command", 3, 3, 3),
            counts("command", 3, 3, 3),
            counts("command", 3, 3, 1),
            counts("empty", 0, 0, 0),
            counts("mail", 1, 1, 1)
        ]
    );
}

// Former family-correlated counts provide a work discriminator against the
// same population and visibility rules, rather than a wall-clock threshold.
const REPEATED_COUNTS: &str = r#"
SELECT f.family,
 (SELECT count(*) FROM core.visible_events e WHERE e.event_type=$3 AND e.payload->>'family'=f.family AND e.payload->>'state'='undecided' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)),
 (SELECT count(*) FROM core.visible_events e WHERE e.event_type=$3 AND e.payload->>'family'=f.family AND e.payload->>'state'<>'undecided' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)),
 (SELECT count(*) FROM core.visible_events e WHERE e.event_type=f.fused_type AND (e.payload->>'ambiguous')::bigint>1 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id))
FROM unnest($1::text[],$2::text[]) f(family,fused_type) ORDER BY f.family"#;

fn visited_event_rows(node: &Value) -> f64 {
    let scanned = matches!(
        node["Node Type"].as_str(),
        Some("Seq Scan" | "Index Scan" | "Index Only Scan" | "Bitmap Heap Scan")
    ) && node["Relation Name"]
        .as_str()
        .is_some_and(|name| name.starts_with("_hyper_") || name == "events");
    let own = if scanned {
        (node["Actual Rows"].as_f64().unwrap()
            + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
            * node["Actual Loops"].as_f64().unwrap()
    } else {
        0.0
    };
    own + node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .map(visited_event_rows)
        .sum::<f64>()
}

#[devkit::test]
async fn fusion_status_reads_counts_without_visiting_event_history(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT uuidv7(),CASE WHEN n<=8192 THEN 'fusion.considered_pair' WHEN n<=10240 THEN 'command.fused' ELSE 'fixture.unrelated' END,
       'fixture.fusion',uuidv7(),uuidv7(),0,'unknown',n,
       jsonb_build_object('family',CASE WHEN n%3=0 THEN 'alpha' WHEN n%3=1 THEN 'beta' ELSE 'gamma' END,'state',CASE WHEN n%2=0 THEN 'undecided' ELSE 'same' END,'ambiguous',2),
       1,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,14336)n;
       ANALYZE core.events").await.unwrap();
    let names: Vec<String> = ["gamma", "alpha", "beta", "alpha"]
        .repeat(3)
        .into_iter()
        .map(String::from)
        .collect();
    project_fusion_readings(&mut conn).await;
    let kinds = vec!["command.fused".to_owned(); names.len()];
    let families = names
        .iter()
        .cloned()
        .zip(kinds.iter().cloned())
        .collect::<Vec<_>>();
    let actual = fusion_ambiguity(&mut conn, &families).await.unwrap();
    assert_eq!(
        actual
            .iter()
            .map(|row| row.ambiguous_fused)
            .collect::<Vec<_>>(),
        vec![2048; names.len()]
    );
    let mut plans = Vec::new();
    for (query, baseline) in [
        (REPEATED_COUNTS, true),
        (include_str!("fusion_counts.sql"), false),
    ] {
        let explain = format!("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON,TIMING OFF) {query}");
        let mut query = sqlx::query_scalar(&explain).bind(&names).bind(&kinds);
        if baseline {
            query = query.bind("fusion.considered_pair");
        }
        let plan: Value = query.fetch_one(&mut conn).await.unwrap();
        plans.push(plan);
    }
    let repeated = visited_event_rows(&plans[0][0]["Plan"]);
    let current = visited_event_rows(&plans[1][0]["Plan"]);
    assert!(
        repeated >= 10240.0,
        "oracle must observe selected rows: {repeated}"
    );
    assert_eq!(current, 0.0);
    let blocks = |plan: &Value| {
        plan[0]["Plan"]["Shared Hit Blocks"].as_f64().unwrap()
            + plan[0]["Plan"]["Shared Read Blocks"].as_f64().unwrap()
    };
    assert!(
        blocks(&plans[1]) < blocks(&plans[0]),
        "current={},baseline={}",
        blocks(&plans[1]),
        blocks(&plans[0])
    );
}
