//! Coverage and fusion figures change in the transaction that changes the
//! underlying readings. Status can read them without scanning event payloads.
use crate::Result;
use sinex_model::EventPayload;
use sinex_model::event_types::{
    coverage::SourceCoverage,
    fused::{CommandFused, ConsideredPair, MailFused},
};
use sqlx::PgConnection;
use uuid::Uuid;

pub(crate) const FUSION_KINDS: [&str; 3] = [
    ConsideredPair::EVENT_TYPE,
    CommandFused::EVENT_TYPE,
    MailFused::EVENT_TYPE,
];

pub(crate) async fn admitted(
    conn: &mut PgConnection,
    first: i64,
    covered: bool,
    fused: bool,
) -> Result<()> {
    if covered {
        sqlx::query!(
            "WITH positioned AS (
                SELECT id, event_type, kind_version, class, payload,
                       $1::bigint + row_number() OVER (ORDER BY ord) - 1 AS seq
                FROM admission_stage
             )
             INSERT INTO core.coverage_readings
                 (event_id, source, kind_version, class, commit_seq, seen_at, ended_at, missing, reason, live)
             SELECT p.id, f.source, p.kind_version, p.class, p.seq, uuid_extract_timestamp(p.id),
                    f.ended_at, f.missing, f.reason, true
             FROM positioned p CROSS JOIN LATERAL core.coverage_fields(p.payload) f
             WHERE p.event_type = $2",
            first,
            SourceCoverage::EVENT_TYPE,
        )
        .execute(&mut *conn)
        .await?;
    }
    if fused {
        sqlx::query!(
            "WITH added AS (
                 INSERT INTO core.fusion_readings (event_id, name, figure, class, live)
                 SELECT p.id, f.name, f.figure, p.class, true
                 FROM admission_stage p CROSS JOIN LATERAL core.fusion_figure(p.event_type, p.payload) f
                 WHERE p.event_type = ANY($1::text[])
                 RETURNING name, figure, class
             )
             SELECT core.change_fusion_count(name, figure, class, count(*))
             FROM added GROUP BY name, figure, class",
            &FUSION_KINDS.map(str::to_owned),
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

pub(crate) async fn set_live(conn: &mut PgConnection, events: &[Uuid], live: bool) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    sqlx::query!(
        "UPDATE core.coverage_readings SET live = $2
         WHERE event_id = ANY($1) AND live <> $2",
        events,
        live,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "WITH moved AS (
             UPDATE core.fusion_readings SET live = $2
             WHERE event_id = ANY($1) AND live <> $2
             RETURNING name, figure, class
         )
         SELECT core.change_fusion_count(name, figure, class, count(*) * CASE WHEN $2 THEN 1 ELSE -1 END)
         FROM moved GROUP BY name, figure, class",
        events,
        live,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Tombstoning removes only archived rows, whose live counts have already fallen.
pub(crate) async fn tombstoned(conn: &mut PgConnection, events: &[Uuid]) -> Result<()> {
    sqlx::query!(
        "DELETE FROM core.coverage_readings WHERE event_id = ANY($1)",
        events
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "DELETE FROM core.fusion_readings WHERE event_id = ANY($1)",
        events
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}
