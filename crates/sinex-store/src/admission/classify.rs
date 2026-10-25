//! Staging a batch's events and classifying each against the live readings
//! and the remembered removals, in one set-based query.
use super::Outcome;
use crate::{Error, Result, stage};
use sinex_model::Event;
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    New,
    Repeat,
    Newer,
    Older,
    Hidden,
    Superseded,
}
/// What classification decided for a batch's events, by their index (`ord`).
pub(super) struct Classified {
    /// Events to admit.
    pub admit: Vec<i32>,
    /// Admitted events that replace a live reading, with that reading.
    pub replaced: Vec<(i32, Uuid)>,
    /// The live reading each event ends as.
    pub resolved: Vec<Option<Uuid>>,
}

/// Stage `events` and classify them; counts the suppressed ones in `outcome`.
pub(super) async fn stage_and_classify(
    conn: &mut PgConnection,
    events: &[Event],
    outcome: &mut Outcome,
) -> Result<Classified> {
    sqlx::query!("TRUNCATE core.admission_stage")
        .execute(&mut *conn)
        .await?;
    let numbered: Vec<(i32, &Event)> = events
        .iter()
        .enumerate()
        .map(|(i, e)| (i32::try_from(i).expect("batch fits i32"), e))
        .collect();
    stage::copy(conn, &numbered).await?;

    let rows = sqlx::query!(
        r#"WITH s AS (
             SELECT st.*,
               row_number() OVER (
                 PARTITION BY st.source, st.event_type,
                   coalesce(st.equivalence_key, st.material_id::text || ':' || st.anchor || ':' || st.ordinal)
                 -- Equal reading orders with different content: the
                 -- greater content hash wins, here and against the live
                 -- reading below, so arrival never decides.
                 ORDER BY st.reading_order DESC, st.record_hash DESC, st.ord DESC) AS rank
             FROM core.admission_stage st)
           SELECT s.ord AS "ord!",
             CASE
               WHEN s.rank > 1 THEN 'superseded'
               WHEN hk.id IS NOT NULL OR hl.id IS NOT NULL THEN 'hidden'
               WHEN coalesce(le.event_id, lc.event_id) IS NULL THEN 'new'
               WHEN coalesce(le.record_hash, lc.record_hash) = s.record_hash THEN 'repeat'
               -- The source dropping a record is a fact, not another
               -- reading of the same content: it replaces even where
               -- repeats are suppressed.
               WHEN s.record_state = 'removed'
                 AND (s.reading_order, s.record_hash)
                   > (coalesce(le.reading_order, lc.reading_order), coalesce(le.record_hash, lc.record_hash)) THEN 'newer'
               WHEN s.replace_rule = 'suppress_repeat' THEN 'repeat'
               WHEN (s.reading_order, s.record_hash)
                   > (coalesce(le.reading_order, lc.reading_order), coalesce(le.record_hash, lc.record_hash)) THEN 'newer'
               ELSE 'older'
             END AS "state!",
             coalesce(le.event_id, lc.event_id) AS live_id
           FROM s
           LEFT JOIN core.hidden_occurrences hk
             ON s.equivalence_key IS NOT NULL AND hk.source = s.source
             AND hk.event_type = s.event_type AND hk.equivalence_key = s.equivalence_key
           LEFT JOIN core.hidden_occurrences hl
             ON s.equivalence_key IS NULL AND hl.material_id = s.material_id
             AND hl.anchor = s.anchor AND hl.event_type = s.event_type AND hl.ordinal = s.ordinal
           LEFT JOIN core.live_equivalence le
             ON s.equivalence_key IS NOT NULL AND le.source = s.source
             AND le.event_type = s.event_type AND le.equivalence_key = s.equivalence_key
           LEFT JOIN core.live_locations lc
             ON s.equivalence_key IS NULL AND s.material_id IS NOT NULL
             AND lc.material_id = s.material_id AND lc.anchor = s.anchor
             AND lc.event_type = s.event_type AND lc.ordinal = s.ordinal
           ORDER BY s.ord"#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut resolved = vec![None; events.len()];
    let mut admit_ords: Vec<i32> = Vec::new();
    let mut replaced: Vec<(i32, Uuid)> = Vec::new();
    for row in rows {
        let state = match row.state.as_str() {
            "new" => State::New,
            "repeat" => State::Repeat,
            "newer" => State::Newer,
            "older" => State::Older,
            "hidden" => State::Hidden,
            "superseded" => State::Superseded,
            other => return Err(Error::Invalid(format!("unknown admission state {other}"))),
        };
        let at = usize::try_from(row.ord).expect("ord is an index");
        match state {
            State::New => {
                admit_ords.push(row.ord);
                resolved[at] = Some(events[at].id.0);
            }
            State::Newer => {
                admit_ords.push(row.ord);
                resolved[at] = Some(events[at].id.0);
                replaced.push((row.ord, row.live_id.expect("newer has a live reading")));
            }
            State::Repeat => {
                outcome.repeats += 1;
                resolved[at] = row.live_id;
            }
            State::Older => {
                outcome.older += 1;
                resolved[at] = row.live_id;
            }
            State::Hidden => outcome.hidden += 1,
            State::Superseded => outcome.superseded += 1,
        }
    }
    Ok(Classified {
        admit: admit_ords,
        replaced,
        resolved,
    })
}

/// A newer reading keeps the replaced reading's value for the fields its type
/// declares `keep_known` when it leaves them unknown.
pub(super) async fn keep_known(
    conn: &mut PgConnection,
    events: &[Event],
    replaced: &[(i32, Uuid)],
) -> Result<()> {
    for (ord, live) in replaced {
        let event_type = &events[usize::try_from(*ord).expect("ord is an index")].event_type;
        let keep = sinex_model::event_types::find(event_type).map_or(&[][..], |k| k.keep_known);
        if keep.is_empty() {
            continue;
        }
        let keep: Vec<String> = keep.iter().map(|f| (*f).to_owned()).collect();
        sqlx::query!(
            r#"UPDATE core.admission_stage st
               SET payload = st.payload || coalesce((
                 SELECT jsonb_object_agg(f, e.payload->f)
                 FROM unnest($3::text[]) f
                 WHERE coalesce(st.payload->f, 'null'::jsonb) = 'null'::jsonb
                   AND coalesce(e.payload->f, 'null'::jsonb) <> 'null'::jsonb), '{}'::jsonb)
               FROM core.events e
               WHERE st.ord = $1 AND e.id = $2"#,
            ord,
            live,
            &keep,
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}
