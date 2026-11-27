use super::*;
use sinex_model::EventPayload;
use sqlx::Executor;

// The previous source-correlated relation is the semantic and work control.
const REPEATED_SOURCES: &str = r#"(
    SELECT s.*,
      CASE
        WHEN s.exclusion IS NOT NULL THEN 'excluded'
        WHEN c.missing OR f.silent OR f.overdue THEN 'stalled'
        WHEN f.absent THEN 'unavailable'
        WHEN f.recent THEN 'capturing'
        ELSE 'quiet'
      END AS state,
      CASE
        WHEN s.exclusion IS NOT NULL THEN s.exclusion
        WHEN c.missing THEN c.last_error
        WHEN f.silent THEN 'no reading within ' || s.silent_after::text
        WHEN f.overdue THEN 'no coverage read within ' || s.every::text
        WHEN f.absent THEN 'not_present'
      END AS reason,
      c.last_error
    FROM (
      SELECT r.name, r.shapes, r.version, r.event_types, r.purpose, r.expectation,
        r.silent_after, r.every, r.expires_after, r.exclusion FROM registry.sources r
      UNION ALL
      SELECT sc.source, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
      FROM core.source_counts sc
      WHERE NOT EXISTS (SELECT 1 FROM registry.sources r WHERE r.name = sc.source)
    ) s
    LEFT JOIN core.source_counts sc ON sc.source = s.name
    CROSS JOIN LATERAL (
      SELECT count(*) AS readings,
        bool_or(e.payload->>'state' = 'missing') AS missing,
        (array_agg((e.payload->>'ended_at')::timestamptz ORDER BY e.commit_seq DESC))[1] AS ended_at,
        max(e.ts_coided) AS last_seen,
        (array_agg(e.payload->>'reason' ORDER BY e.commit_seq DESC)
          FILTER (WHERE e.payload->>'state' = 'missing'))[1] AS last_error
      FROM core.visible_events e
      WHERE e.event_type = '{coverage_kind}'
        AND e.kind_version = (SELECT max(version) FROM registry.event_types WHERE name = '{coverage_kind}')
        AND e.payload->>'observed_source' = s.name
        AND e.commit_seq <= {seq} AND NOT audit.is_archived_at(e.id, {seq})
    ) c
    CROSS JOIN LATERAL (
      SELECT
        s.expectation = 'continuous' AND sc.last_seen < now() - s.silent_after AS silent,
        s.expectation = 'cadence' AND c.ended_at < now() - s.every AS overdue,
        coalesce(sc.events, 0) = 0 AND c.readings = 0 AS absent,
        greatest(sc.last_seen, c.last_seen) >= now() - interval '1 hour' AS recent
    ) f
) {t}"#;

fn relation(template: &str) -> String {
    format!(
        "SELECT to_jsonb(s) FROM {} ORDER BY s.name",
        template
            .replace("{coverage_kind}", SourceCoverage::EVENT_TYPE)
            .replace("{seq}", "$1")
            .replace("{t}", "s")
    )
}

async fn rows(conn: &mut PgConnection, template: &str, seq: i64) -> Vec<Json> {
    sqlx::query_scalar(&relation(template))
        .bind(seq)
        .fetch_all(conn)
        .await
        .unwrap()
}

fn old_id() -> Uuid {
    // ts_coided is generated from the reading ID, as in ordinary admission.
    Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::NoContext,
        1_600_000_000,
        0,
    ))
}

async fn project_coverage(conn: &mut PgConnection) {
    conn.execute(
        "INSERT INTO core.coverage_readings (event_id, source, kind_version, class, commit_seq, seen_at, ended_at, missing, reason, live)
         SELECT e.id, f.source, e.kind_version, e.class, e.commit_seq, e.ts_coided, f.ended_at, f.missing, f.reason,
                NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
         FROM core.events e CROSS JOIN LATERAL core.coverage_fields(e.payload) f WHERE e.event_type = 'source.coverage';
         ANALYZE core.coverage_readings",
    )
    .await
    .unwrap();
}

async fn insert_coverage(
    conn: &mut PgConnection,
    source: &str,
    seq: i64,
    state: &str,
    reason: Option<&str>,
    ended_at: Option<&str>,
) -> Uuid {
    // Direct rows include legacy NULL payload fields that a current typed
    // producer cannot emit. Their query semantics must remain unchanged.
    sqlx::query_scalar(
        "INSERT INTO core.events(id,event_type,kind_version,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class)
         VALUES($1,'source.coverage',$2,'fixture.coverage',uuidv7(),uuidv7(),0,'unknown',$3,$4,1,decode(repeat('01',32),'hex'),'open') RETURNING id",
    )
    .bind(old_id())
    .bind(SourceCoverage::VERSION as i64)
    .bind(seq)
    .bind(json!({"observed_source":source,"state":state,"reason":reason,"ended_at":ended_at}))
    .fetch_one(conn)
    .await
    .unwrap()
}

#[devkit::test]
async fn current_and_historical_coverage_keep_the_same_visibility_and_latest_nulls(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let names = [
        "test.cadence",
        "test.missing",
        "test.older-missing",
        "test.versioned",
        "test.archived",
        "test.future",
        "test.guarded",
        "test.excluded",
        "test.empty",
    ];
    let catalog: Vec<_> = names
        .iter()
        .map(|name| {
            source(
                name,
                if *name == "test.cadence" {
                    Expectation::Cadence {
                        every: Duration::from_secs(300),
                    }
                } else {
                    Expectation::OnDemand
                },
            )
        })
        .collect();
    write_sources(&mut conn, &catalog).await.unwrap();
    write_exclusions(
        &mut conn,
        &[("test.excluded".into(), "deliberate exclusion".into())].into(),
    )
    .await
    .unwrap();
    admit(
        &mut conn,
        vec![reading(
            "test.unregistered",
            Timestamp::from_unix_secs(1_600_000_000).unwrap(),
        )],
    )
    .await;
    for (name, state, reason) in [
        ("test.cadence", "observed", None),
        ("test.missing", "missing", Some("old error")),
        (
            "test.older-missing",
            "missing",
            Some("older binding failed"),
        ),
        ("test.archived", "missing", Some("archived error")),
        ("test.excluded", "missing", Some("excluded error")),
    ] {
        insert_coverage(
            &mut conn,
            name,
            10,
            state,
            reason,
            Some("2020-01-01T00:00:00Z"),
        )
        .await;
    }
    insert_coverage(&mut conn, "test.cadence", 20, "observed", None, None).await;
    insert_coverage(&mut conn, "test.missing", 20, "missing", None, None).await;
    insert_coverage(&mut conn, "test.older-missing", 20, "observed", None, None).await;
    insert_coverage(&mut conn, "test.versioned", 10, "observed", None, None).await;
    insert_coverage(
        &mut conn,
        "test.future",
        40,
        "missing",
        Some("future error"),
        None,
    )
    .await;
    // Earlier kind versions and ungranted coverage cannot affect current health.
    sqlx::query("INSERT INTO core.events(id,event_type,kind_version,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      VALUES($1,'source.coverage',$3::bigint-1,'fixture.coverage',uuidv7(),uuidv7(),0,'unknown',10,'{\"observed_source\":\"test.versioned\",\"state\":\"missing\",\"reason\":\"old kind\"}',1,decode(repeat('01',32),'hex'),'open'),
      ($2,'source.coverage',$3,'fixture.coverage',uuidv7(),uuidv7(),0,'unknown',10,'{\"observed_source\":\"test.guarded\",\"state\":\"missing\",\"reason\":\"private error\"}',1,decode(repeat('01',32),'hex'),'guarded')")
      .bind(old_id()).bind(old_id()).bind(SourceCoverage::VERSION as i64).execute(&mut *conn).await.unwrap();
    let archived: Uuid = sqlx::query_scalar(
        "SELECT id FROM core.events WHERE payload->>'observed_source'='test.archived'",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    sqlx::query("INSERT INTO audit.archive_changes(change_seq,event_id,commit_seq,archived,reason) VALUES(1,$1,20,true,'archive'),(2,$1,30,false,'restore')").bind(archived).execute(&mut *conn).await.unwrap();
    conn.execute("UPDATE core.counters SET value=40 WHERE name='commit_seq'")
        .await
        .unwrap();
    project_coverage(&mut conn).await;
    // Status uses its configured database connection and caller grants. The
    // restricted sinex_reader role belongs only to the raw-SQL surface.
    for grants in ["", "guarded"] {
        set_grants(&mut conn, grants).await.unwrap();
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .unwrap();
        for seq in [10, 19, 20, 29, 30, 39, 40] {
            let actual = rows(&mut conn, &super::super::super::source_views::SOURCES, seq).await;
            assert_eq!(
                actual,
                rows(&mut conn, REPEATED_SOURCES, seq).await,
                "seq={seq}, grants={grants}"
            );
            let row = |name: &str| actual.iter().find(|row| row["name"] == name).unwrap();
            assert_eq!(row("test.empty")["state"], "unavailable");
            assert_eq!(row("test.unregistered")["state"], "quiet");
            assert_eq!(row("test.versioned")["state"], "quiet");
            assert_eq!(row("test.excluded")["reason"], "deliberate exclusion");
            assert_eq!(
                row("test.guarded")["state"],
                if grants.is_empty() {
                    "unavailable"
                } else {
                    "stalled"
                }
            );
            assert_eq!(
                row("test.archived")["state"],
                if (20..30).contains(&seq) {
                    "unavailable"
                } else {
                    "stalled"
                }
            );
            assert_eq!(
                row("test.future")["state"],
                if seq < 40 { "unavailable" } else { "stalled" }
            );
            if seq >= 20 {
                assert_eq!(
                    row("test.cadence")["state"],
                    "quiet",
                    "newest NULL does not revive an older ended_at"
                );
                assert!(
                    row("test.missing")["last_error"].is_null(),
                    "newest missing reason may be NULL"
                );
                assert_eq!(
                    row("test.older-missing")["last_error"],
                    "older binding failed",
                    "a newer successful binding does not erase another missing binding"
                );
            } else {
                assert_eq!(row("test.cadence")["state"], "stalled");
                assert_eq!(row("test.missing")["last_error"], "old error");
            }
        }
        conn.execute("COMMIT").await.unwrap();
    }
}

fn rows_read(node: &Json, relation: impl Fn(&str) -> bool + Copy) -> f64 {
    let scan = matches!(
        node["Node Type"].as_str(),
        Some("Seq Scan" | "Index Scan" | "Index Only Scan" | "Bitmap Heap Scan")
    ) && node["Relation Name"].as_str().is_some_and(relation);
    let own = if scan {
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
        .map(|node| rows_read(node, relation))
        .sum::<f64>()
}

#[devkit::test]
async fn coverage_plan_does_not_scan_every_source_event(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let catalog: Vec<_> = (0..16)
        .map(|n| source(&format!("test.work-{n:02}"), Expectation::OnDemand))
        .collect();
    write_sources(&mut conn, &catalog).await.unwrap();
    conn.execute("INSERT INTO core.events(id,event_type,kind_version,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT uuidv7(),CASE WHEN n<=5120 THEN 'source.coverage' ELSE 'fixture.unrelated' END,CASE WHEN n<=4096 THEN 3 ELSE 2 END,
       'fixture.coverage',uuidv7(),uuidv7(),0,'unknown',n,
       jsonb_build_object('observed_source','test.work-'||lpad((n%16)::text,2,'0'),'state','observed','ended_at','2020-01-01T00:00:00Z'),
       1,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,6144)n;
       ANALYZE core.events;
       UPDATE core.counters SET value=6144 WHERE name='commit_seq'").await.unwrap();
    project_coverage(&mut conn).await;
    // Guarded coverage is visible to the caller but absent in this fixture.
    // A seek must skip the open entries while checking that class.
    set_grants(&mut conn, "guarded").await.unwrap();
    conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .unwrap();
    assert_eq!(
        rows(&mut conn, &super::super::super::source_views::SOURCES, 6144).await,
        rows(&mut conn, REPEATED_SOURCES, 6144).await
    );
    let mut plans = Vec::new();
    for template in [
        REPEATED_SOURCES,
        &super::super::super::source_views::SOURCES,
    ] {
        let plan: Json = sqlx::query_scalar(&format!(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON,TIMING OFF) {}",
            relation(template)
        ))
        .bind(6144_i64)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        plans.push(plan);
    }
    let event = |relation: &str| relation.starts_with("_hyper_") || relation == "events";
    let baseline = rows_read(&plans[0][0]["Plan"], event);
    let current = &plans[1][0]["Plan"];
    assert!(baseline >= 4096.0, "positive work control: {baseline}");
    assert_eq!(rows_read(current, event), 0.0);
    let coverage = |relation: &str| relation == "coverage_readings";
    assert!(
        rows_read(current, coverage) <= (16 * 3 * 3) as f64,
        "{current}"
    );
    let blocks = |p: &Json| {
        p[0]["Plan"]["Shared Hit Blocks"].as_f64().unwrap()
            + p[0]["Plan"]["Shared Read Blocks"].as_f64().unwrap()
    };
    assert!(
        blocks(&plans[1]) < blocks(&plans[0]),
        "indexed={}, baseline={}",
        blocks(&plans[1]),
        blocks(&plans[0])
    );

    // Restricting the public query to one source reduces the same seeks.
    let context = ctx();
    let query = bind(
        "sources where source = test.work-07 | fields source,state",
        &context,
    )
    .unwrap();
    let compiled = compile::Compiler::new(&context, 6144)
        .compile(&query)
        .unwrap();
    let mut statement = SqlFragment::text("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON,TIMING OFF) ");
    statement.append(compiled.answer);
    let (text, values) = statement.render();
    let plan: Json = sql::bind_scalar(sqlx::query_scalar(&text), values)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        plan[0]["Plan"]["Actual Rows"].as_f64(),
        Some(1.0),
        "one selected source"
    );
    let reads = rows_read(&plan[0]["Plan"], coverage);
    assert!(reads > 0.0 && reads <= 9.0, "one source's seeks: {plan}");
    conn.execute("COMMIT").await.unwrap();
}
