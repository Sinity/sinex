use super::*;
use serde_json::json;
use sqlx::Executor;

pub(super) async fn before_status_readings(conn: &mut PgConnection) {
    conn.execute(
        "DROP VIEW core.visible_coverage_readings, core.visible_fusion_counts;
         DROP FUNCTION core.coverage_fields(jsonb), core.fusion_figure(text, jsonb);
         DROP TABLE core.coverage_readings, core.fusion_readings, core.fusion_counts",
    )
    .await
    .unwrap();
}

#[devkit::test]
async fn status_upgrade_projects_compressed_coverage_and_counts_only_live_fusion(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    before_status_readings(&mut conn).await;
    sqlx::query("DELETE FROM sinex_schema.applied WHERE version = $1")
        .bind(VERSION)
        .execute(&mut conn)
        .await
        .unwrap();
    // The template records only its fresh version.
    sqlx::query("INSERT INTO sinex_schema.applied (version) VALUES ($1)")
        .bind(VERSION - 1)
        .execute(&mut conn)
        .await
        .unwrap();
    for (index, (kind, payload, archived)) in [
        ("source.coverage", json!({"observed_source":"fixture.logs","state":"missing","reason":"absent"}), true),
        ("source.coverage", json!({"observed_source":"fixture.logs","state":"observed","ended_at":"2020-01-01T00:00:00Z"}), false),
        ("fusion.considered_pair", json!({"family":"command","state":"undecided"}), false),
        ("fusion.considered_pair", json!({"family":"command","state":"undecided"}), true),
        ("command.fused", json!({"ambiguous":3}), false),
        ("fixture.unrelated", json!({"ambiguous":2}), false),
    ].into_iter().enumerate() {
        let id = sinex_model::EventId::new().0;
        sqlx::query(
            "INSERT INTO core.events (id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash)
             VALUES($1,$2,'fixture',$1,$1,0,'unknown',$3,$4,1,decode(repeat('01',32),'hex'))",
        )
        .bind(id)
        .bind(kind)
        .bind(index as i64 + 1)
        .bind(payload)
        .execute(&mut conn)
        .await
        .unwrap();
        if archived {
            sqlx::query("INSERT INTO audit.archived(event_id,archived_seq,reason) VALUES($1,7,'archive')")
                .bind(id)
                .execute(&mut conn)
                .await
                .unwrap();
        }
    }
    conn.execute("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunk")
        .await
        .unwrap();
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: VERSION - 1,
            to: VERSION
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let coverage: Vec<(bool, bool, Option<String>, bool)> = sqlx::query_as(
        "SELECT live, missing, reason, ended_at IS NOT NULL
         FROM core.coverage_readings WHERE source = 'fixture.logs' ORDER BY commit_seq",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        coverage,
        [
            (false, true, Some("absent".into()), false),
            (true, false, None, true)
        ]
    );
    let counts: Vec<(String, String, i64)> =
        sqlx::query_as("SELECT name, figure, count FROM core.fusion_counts ORDER BY name")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        counts,
        [
            ("command".into(), "undecided".into(), 1),
            ("command.fused".into(), "ambiguous".into(), 1)
        ]
    );
}
