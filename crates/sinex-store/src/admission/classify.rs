//! Staging a batch's events and classifying each against the live readings
//! and the remembered removals, in one set-based query.
use super::Outcome;
use crate::{Error, Result, stage};
use sinex_model::Event;
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Classification {
    New,
    Repeat,
    Newer,
    Older,
    Superseded,
}
/// What classification decided for a batch's events, by their index (`ord`).
pub(super) struct Classified {
    /// Events to admit.
    pub admit: Vec<i64>,
    /// Admitted events that replace a live reading, with that reading.
    pub replaced: Vec<(i64, Uuid)>,
    /// The live reading each event ends as.
    pub resolved: Vec<Option<Uuid>>,
}

/// Stage `events` and classify them; counts the suppressed ones in `outcome`.
pub(super) async fn stage_and_classify(
    conn: &mut PgConnection,
    events: &[Event],
    outcome: &mut Outcome,
) -> Result<Classified> {
    let started = std::time::Instant::now();
    sqlx::query!("TRUNCATE core.admission_stage")
        .execute(&mut *conn)
        .await?;
    outcome.timings.insert("reset", started.elapsed());
    let numbered: Vec<(i64, &Event)> = events
        .iter()
        .enumerate()
        .map(|(i, e)| {
            i64::try_from(i)
                .map(|ord| (ord, e))
                .map_err(|_| Error::Invalid("batch ordinal beyond i64".into()))
        })
        .collect::<Result<_>>()?;
    let started = std::time::Instant::now();
    stage::copy(conn, &numbered).await?;
    outcome.timings.insert("copy", started.elapsed());
    let started = std::time::Instant::now();

    let rows = sqlx::query_file!("src/admission/classification.sql")
        .fetch_all(&mut *conn)
        .await?;

    outcome.timings.insert("classification", started.elapsed());
    let mut resolved = vec![None; events.len()];
    let mut admit_ords: Vec<i64> = Vec::new();
    let mut replaced: Vec<(i64, Uuid)> = Vec::new();
    for row in rows {
        let state = match row.state.as_str() {
            "new" => Classification::New,
            "repeat" => Classification::Repeat,
            "newer" => Classification::Newer,
            "older" => Classification::Older,
            "superseded" => Classification::Superseded,
            other => return Err(Error::Invalid(format!("unknown admission state {other}"))),
        };
        let at = usize::try_from(row.ord).expect("ord is an index");
        match state {
            Classification::New => {
                admit_ords.push(row.ord);
                resolved[at] = Some(events[at].id.0);
            }
            Classification::Newer => {
                admit_ords.push(row.ord);
                resolved[at] = Some(events[at].id.0);
                replaced.push((row.ord, row.live_id.expect("newer has a live reading")));
            }
            Classification::Repeat => {
                outcome.repeats += 1;
                resolved[at] = row.live_id;
            }
            Classification::Older => {
                outcome.older += 1;
                resolved[at] = row.live_id;
            }
            Classification::Superseded => outcome.superseded += 1,
        }
    }
    Ok(Classified {
        admit: admit_ords,
        replaced,
        resolved,
    })
}

/// A newer reading keeps the replaced reading's value for the fields its type
/// declares `keep_known` when it leaves them unknown. Private original values
/// and their baseline class are carried before current output rules run.
pub(super) async fn keep_known(
    conn: &mut PgConnection,
    events: &mut [Event],
    replaced: &[(i64, Uuid)],
    declarations: &std::collections::BTreeMap<sinex_model::KindRef, sinex_model::KindDeclaration>,
) -> Result<()> {
    let inputs: Vec<_> = replaced
        .iter()
        .filter_map(|(ord, live)| {
            let event = &events[usize::try_from(*ord).expect("ord is an index")];
            let fields = declarations
                .get(&event.kind_ref())
                .map_or(&[][..], |kind| kind.keep_known.as_slice());
            (!fields.is_empty())
                .then(|| serde_json::json!({"ord": ord, "live_id": live, "fields": fields}))
        })
        .collect();
    if inputs.is_empty() {
        return Ok(());
    }
    let rows = sqlx::query!(
        r#"WITH kept AS (
           SELECT st.ord, e.class, (
             SELECT jsonb_object_agg(f, e.payload->f)
             FROM jsonb_array_elements_text(r.fields) f
             WHERE coalesce(st.payload->f, 'null'::jsonb) = 'null'::jsonb
               AND coalesce(e.payload->f, 'null'::jsonb) <> 'null'::jsonb) AS payload
           FROM jsonb_to_recordset($1::jsonb) AS r(ord bigint, live_id uuid, fields jsonb)
           JOIN core.admission_stage st ON st.ord = r.ord
           JOIN core.events e ON e.id = r.live_id)
           UPDATE core.admission_stage st
           SET payload = st.payload || kept.payload,
               class = CASE
                 WHEN st.class = 'sealed' OR kept.class = 'sealed' THEN 'sealed'
                 WHEN st.class = 'guarded' OR kept.class = 'guarded' THEN 'guarded'
                 ELSE st.class END
           FROM kept
           WHERE st.ord = kept.ord AND kept.payload IS NOT NULL
           RETURNING st.ord, st.payload, st.class"#,
        serde_json::Value::Array(inputs),
    )
    .fetch_all(&mut *conn)
    .await?;
    for row in rows {
        let event = &mut events[usize::try_from(row.ord).expect("ord is an index")];
        event.payload = row.payload;
        event.class = crate::privacy::parse_class(&row.class)?;
    }
    Ok(())
}
