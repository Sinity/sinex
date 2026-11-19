//! Terminal failures live with derivation progress. They are not source record
//! failures: their coordinates are native keys or true admitted input IDs.
use super::*;

pub(super) async fn record(
    conn: &mut PgConnection,
    derivation: &str,
    refused: &[RefusedWork],
    run: sinex_model::RunId,
    read: ReadMark,
) -> Result<()> {
    let now = ReadMark::now(conn).await?;
    let unchanged = now.changes <= read.changes && now.tombstoned <= read.tombstoned;
    let inputs: Vec<Value> = refused
        .iter()
        .map(|r| serde_json::to_value(&r.input).expect("work identities serialize"))
        .collect();
    let positions: Vec<i64> = refused.iter().map(|r| r.input_seq).collect();
    let reasons: Vec<String> = refused.iter().map(|r| r.reason.clone()).collect();
    sqlx::query!(
        r#"INSERT INTO derivation.refusals (derivation,input,input_seq,reason,run_id)
           SELECT $1,r.input,r.seq,r.reason,$5 FROM unnest($2::jsonb[],$3::bigint[],$4::text[]) r(input,seq,reason)
           WHERE CASE WHEN r.input ? 'key' THEN NOT EXISTS (
             SELECT 1 FROM derivation.dirty d WHERE d.derivation=$1 AND d.key=r.input->'key' AND d.last_dirty_seq>r.seq
           ) ELSE $6 OR EXISTS (SELECT 1 FROM core.live_equivalence e WHERE e.event_id=(r.input->>'input')::uuid)
                    OR EXISTS (SELECT 1 FROM core.live_locations e WHERE e.event_id=(r.input->>'input')::uuid) END
           ON CONFLICT (derivation,input) DO UPDATE SET input_seq=EXCLUDED.input_seq,reason=EXCLUDED.reason,run_id=EXCLUDED.run_id,refused_at=now()
           WHERE derivation.refusals.input_seq<=EXCLUDED.input_seq"#,
        derivation, &inputs, &positions, &reasons, run.0, unchanged,
    ).execute(&mut *conn).await?;
    Ok(())
}

pub(super) async fn position(
    conn: &mut PgConnection,
    derivation: &str,
    position: i64,
    order: Option<i64>,
) -> Result<()> {
    sqlx::query!(
        r#"INSERT INTO derivation.positions (derivation,commit_seq,reading_order) VALUES ($1,$2,coalesce($3::bigint,0))
           ON CONFLICT (derivation) DO UPDATE SET
           commit_seq=greatest(derivation.positions.commit_seq,EXCLUDED.commit_seq),
           reading_order=greatest(derivation.positions.reading_order,$3)"#,
        derivation, position, order,
    ).execute(conn).await?;
    Ok(())
}

async fn clear(conn: &mut PgConnection, derivation: &str, inputs: Vec<RefusedInput>) -> Result<()> {
    if inputs.is_empty() {
        return Ok(());
    }
    let inputs: Vec<Value> = inputs
        .into_iter()
        .map(|input| serde_json::to_value(input).expect("work identities serialize"))
        .collect();
    sqlx::query!(
        "DELETE FROM derivation.refusals WHERE derivation=$1 AND input=ANY($2)",
        derivation,
        &inputs
    )
    .execute(conn)
    .await?;
    Ok(())
}

pub(super) async fn clear_keys(
    conn: &mut PgConnection,
    derivation: &str,
    keys: &[Value],
) -> Result<()> {
    clear(
        conn,
        derivation,
        keys.iter().cloned().map(RefusedInput::Key).collect(),
    )
    .await
}

pub(super) async fn clear_inputs(
    conn: &mut PgConnection,
    derivation: &str,
    inputs: &[Uuid],
) -> Result<()> {
    clear(
        conn,
        derivation,
        inputs.iter().copied().map(RefusedInput::Input).collect(),
    )
    .await
}

pub(super) async fn clear_all(conn: &mut PgConnection, derivation: &str) -> Result<()> {
    sqlx::query!(
        "DELETE FROM derivation.refusals WHERE derivation=$1",
        derivation
    )
    .execute(conn)
    .await?;
    Ok(())
}
