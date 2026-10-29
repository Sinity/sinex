//! Structural state-run bounds: retain refreshes until the first changed state.
use super::*;
pub(super) async fn bounds(
    conn: &mut PgConnection,
    kinds: &[Kind],
    start: OffsetDateTime,
    end: OffsetDateTime,
    scope_field: &str,
    subject_fields: &[&str],
    now: OffsetDateTime,
) -> Result<(OffsetDateTime, OffsetDateTime)> {
    let (types, contains) = kind_arrays(kinds);
    let subjects: Vec<String> = subject_fields.iter().map(|field| (*field).into()).collect();
    let row=sqlx::query!(
        r#"WITH eligible AS NOT MATERIALIZED (
             SELECT e.id,e.ts_orig,e.commit_seq,(e.payload->>$5 IS NOT NULL) AS known_scope,
               jsonb_build_array(e.source,coalesce(e.payload->>$5,e.id::text)) AS scope,
               jsonb_build_array(e.event_type,coalesce(
                 (SELECT e.payload->field FROM unnest($6::text[]) WITH ORDINALITY AS f(field,n)
                   WHERE e.payload->field IS NOT NULL AND e.payload->field <> 'null'::jsonb
                   ORDER BY n LIMIT 1),to_jsonb(e.id::text))) AS subject
             FROM core.derivation_events e
             WHERE e.event_type=ANY($1) AND e.ts_orig IS NOT NULL
               AND EXISTS (SELECT 1 FROM unnest($1::text[],$2::jsonb[]) AS k(event_type,contains)
                           WHERE k.event_type=e.event_type AND e.payload @> k.contains)
               AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
           ), day_scopes AS MATERIALIZED (
             SELECT DISTINCT scope FROM eligible WHERE ts_orig >= $3 AND ts_orig < $4
           ), previous AS (
             SELECT prior.* FROM day_scopes scope CROSS JOIN LATERAL (
               SELECT e.ts_orig FROM eligible e WHERE e.scope=scope.scope AND e.ts_orig < $3
               ORDER BY e.ts_orig DESC,e.commit_seq DESC LIMIT 1
             ) prior
           ), latest AS (
             SELECT latest.* FROM day_scopes scope CROSS JOIN LATERAL (
               SELECT e.scope,e.ts_orig,e.commit_seq,e.subject,e.known_scope FROM eligible e
               WHERE e.scope=scope.scope AND e.ts_orig < $4
               ORDER BY e.ts_orig DESC,e.commit_seq DESC LIMIT 1
             ) latest
           ), closers AS (
             SELECT CASE WHEN NOT last.known_scope THEN $4 ELSE coalesce((SELECT next.ts_orig FROM eligible next
                  WHERE next.scope=last.scope AND next.ts_orig >= $4 AND next.subject IS DISTINCT FROM last.subject
                  ORDER BY next.ts_orig,next.commit_seq LIMIT 1),$7) END AS finish
             FROM latest last
           ) SELECT least($3,(SELECT min(ts_orig) FROM previous)) AS start,
               greatest($4,(SELECT max(finish) FROM closers)) AS finish"#,
        &types,&contains,start,end,scope_field,&subjects,now,
    ).fetch_one(&mut *conn).await?;
    let from = row.start.unwrap_or(start);
    let until = row.finish.unwrap_or(end);
    // Include the authoritative closing transition in an exclusive-end read.
    let until = Timestamp::from_unix_ns(Timestamp(until).unix_ns() + 1_000)
        .map_err(|error| crate::Error::Invalid(error.to_string()))?
        .0;
    Ok((from, until))
}
