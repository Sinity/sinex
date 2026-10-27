//! What the daemon's telemetry samples read: how far each reader of the
//! commit log is behind, each derivation's waiting work, and each source's
//! counts. One read-only transaction per sample.
use crate::Result;
use sqlx::{Connection, PgConnection};
use std::collections::BTreeMap;

/// One part's numbers, as a sample records them.
#[derive(Clone, Debug, PartialEq)]
pub struct Reading {
    /// `writer`, `publisher`, `search index`, `derivation` or `source`.
    pub component: &'static str,
    /// The derivation or source; empty for the daemon's own parts.
    pub name: String,
    pub measures: BTreeMap<String, f64>,
}

fn reading(component: &'static str, name: &str, measures: &[(&str, i64)]) -> Reading {
    Reading {
        component,
        name: name.to_owned(),
        // Counts are far below 2^53, where a JSON number stops being exact.
        measures: measures
            .iter()
            .map(|(k, v)| ((*k).to_owned(), *v as f64))
            .collect(),
    }
}

/// Every part's numbers now.
pub async fn read(conn: &mut PgConnection) -> Result<Vec<Reading>> {
    let mut tx = conn.begin().await?;
    sqlx::query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let head = sqlx::query!(
        r#"SELECT
             (SELECT value FROM core.counters WHERE name = 'commit_seq') AS "commit_seq!",
             (SELECT coalesce((SELECT commit_seq FROM core.consumer_positions WHERE consumer = 'publisher'), 0)) AS "published!",
             (SELECT count(*) FROM raw.failures WHERE failed_at > now() - interval '1 hour') AS "failures_1h!""#,
    )
    .fetch_one(&mut *tx)
    .await?;
    let search = crate::diagnostics::positions(&mut tx)
        .await?
        .search_position;
    let mut out = vec![
        reading(
            "writer",
            "",
            &[
                ("commit_seq", head.commit_seq),
                ("failed_records_1h", head.failures_1h),
            ],
        ),
        reading(
            "publisher",
            "",
            &[("behind", head.commit_seq - head.published)],
        ),
        reading(
            "search index",
            "",
            &[("behind", head.commit_seq - search.unwrap_or(0))],
        ),
    ];
    let keyed = sqlx::query!(
        r#"SELECT d.name AS "name!",
             (SELECT count(*) FROM derivation.dirty k WHERE k.derivation = d.name) AS "dirty!",
             (SELECT count(*) FROM derivation.waiting w WHERE w.derivation = d.name) AS "waiting!",
             (SELECT p.commit_seq FROM derivation.positions p WHERE p.derivation = d.name) AS position
           FROM registry.derivations d ORDER BY d.name"#,
    )
    .fetch_all(&mut *tx)
    .await?;
    for d in keyed {
        let mut measures = vec![("dirty_keys", d.dirty), ("waiting_keys", d.waiting)];
        if let Some(position) = d.position {
            measures.push(("behind", head.commit_seq - position));
        }
        out.push(reading("derivation", &d.name, &measures));
    }
    let sources = sqlx::query!(
        r#"SELECT source, events, extract(epoch FROM now() - last_seen)::bigint AS "seconds_since_seen!"
           FROM core.source_counts ORDER BY source"#,
    )
    .fetch_all(&mut *tx)
    .await?;
    for s in sources {
        out.push(reading(
            "source",
            &s.source,
            &[
                ("events", s.events),
                ("seconds_since_seen", s.seconds_since_seen),
            ],
        ));
    }
    tx.rollback().await?;
    Ok(out)
}

/// The newest stored sample of each part from the last hour, as payloads.
pub async fn latest(conn: &mut PgConnection) -> Result<Vec<serde_json::Value>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT DISTINCT ON (payload->>'component', payload->>'name') payload AS "payload!"
           FROM telemetry.visible_events
           WHERE ts_orig > now() - interval '1 hour'
           ORDER BY payload->>'component', payload->>'name', ts_orig DESC"#,
    )
    .fetch_all(&mut *conn)
    .await?)
}

#[cfg(test)]
mod tests;
