//! Excision, through the one writer: what it would remove, and removing it.
//! An operator request is planned under its source's lock, the blanked bytes
//! are written, and the writer applies the plan in one transaction. The same
//! request run again finds only what is left. Hide and unhide are plain
//! writer batches.
use crate::writer::{Excision, WriterHandle};
use sinex_model::OperationId;
use sinex_sources::sources;
use sinex_store::Blobs;
use sinex_store::excision::{self, Plan, Rewrite, Target};
use sqlx::PgConnection;
pub(crate) mod exclusions;
mod guard;
pub(crate) use guard::{AcquisitionGuard, ExcisionGuard};

pub fn describe(plan: &Plan) -> String {
    let records: usize = plan.materials.iter().map(|m| m.anchors.len()).sum();
    let whole = plan.materials.iter().filter(|m| m.whole).count();
    format!(
        "events {} | records blanked {records} | whole materials blanked {whole}",
        plan.events.len(),
    )
}

/// Excise `target` with a planning connection of its own, as tests do.
#[cfg(test)]
pub async fn excise_with(
    read_conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    target: Target,
    actor: &str,
) -> Result<String, String> {
    let plan = excision::plan(read_conn, target)
        .await
        .map_err(|e| e.to_string())?;
    let Some(source) = source_of(read_conn, &plan).await? else {
        return Ok(summary_of_nothing());
    };
    let guard = writer.excision_guard(source).await?;
    guarded(blobs, writer, target, actor, guard).await
}

/// Excise `target`. The planning connection becomes the source lock's holder.
pub(crate) async fn excise_pooled(
    mut connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    blobs: &Blobs,
    writer: &WriterHandle,
    target: Target,
    actor: &str,
) -> Result<String, String> {
    let plan = excision::plan(&mut connection, target)
        .await
        .map_err(|e| e.to_string())?;
    let Some(source) = source_of(&mut connection, &plan).await? else {
        return Ok(summary_of_nothing());
    };
    let guard = ExcisionGuard::acquire(connection, source).await?;
    guarded(blobs, writer, target, actor, guard).await
}

fn summary_of_nothing() -> String {
    "excised events 0 | materials rewritten 0 | blob files deleted 0".into()
}

/// The one source a plan's materials and events belong to.
async fn source_of(conn: &mut PgConnection, plan: &Plan) -> Result<Option<String>, String> {
    if plan.events.is_empty() && plan.materials.is_empty() {
        return Ok(None);
    }
    let materials = plan.materials.iter().map(|m| m.id.0).collect::<Vec<_>>();
    let mut sources: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT b.source FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id JOIN raw.bindings b ON b.binding=i.binding WHERE m.id=ANY($1) ORDER BY 1",
    )
    .bind(&materials)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| e.to_string())?;
    if sources.is_empty() {
        // No binding: the planned events no other planned event derives from.
        let events = plan.events.iter().copied().collect::<Vec<_>>();
        sources = sqlx::query_scalar(
            "SELECT DISTINCT source FROM core.events WHERE id=ANY($1) AND (parents IS NULL OR NOT parents && $1) ORDER BY 1",
        )
        .bind(&events)
        .fetch_all(conn)
        .await
        .map_err(|e| e.to_string())?;
    }
    if sources.len() != 1 {
        return Err("an excision covers exactly one source".into());
    }
    Ok(sources.pop())
}

async fn guarded(
    blobs: &Blobs,
    writer: &WriterHandle,
    target: Target,
    actor: &str,
    mut guard: ExcisionGuard,
) -> Result<String, String> {
    let prepared = async {
        let plan = excision::plan(guard.connection(), target)
            .await
            .map_err(|e| e.to_string())?;
        if source_of(guard.connection(), &plan).await?.as_deref() != Some(guard.source()) {
            return Err("the excision's source changed while waiting; run it again".into());
        }
        let rewrites = rewrites(blobs, &plan)?;
        Ok((plan, rewrites))
    }
    .await;
    let (plan, rewrites) = match prepared {
        Ok(value) => value,
        Err(error) => {
            guard.release().await?;
            return Err(error);
        }
    };
    let done = writer
        .excise(Excision {
            guard: Some(guard),
            target,
            plan,
            rewrites,
            operation: OperationId::new(),
            actor: actor.into(),
            blobs: blobs.clone(),
        })
        .await
        .map_err(|e| e.to_string())?;
    let a = &done.applied;
    let mut out = format!(
        "excised events {} | materials rewritten {} | blob files deleted {}",
        a.events,
        a.materials_rewritten,
        a.delete_after_commit.len() - done.files_left.len()
    );
    if !done.files_left.is_empty() {
        out.push_str(&format!(
            "\nblob files no row refers to but that could not be deleted: {}",
            done.files_left.join(", ")
        ));
    }
    Ok(out)
}

/// The blanked bytes of every planned material, written to the store before
/// the plan is applied (bytes before rows).
fn rewrites(blobs: &Blobs, plan: &Plan) -> Result<Vec<Rewrite>, String> {
    let mut rewrites = Vec::new();
    for m in &plan.materials {
        let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
        let length = blobs
            .copy_to(&m.blob, &mut file)
            .map_err(|e| e.to_string())?;
        let stored = crate::stored::StoredReader::new(file, length);
        let length =
            usize::try_from(stored.length()).map_err(|_| "material exceeds address space")?;
        let spans: Vec<(usize, usize)> = if m.whole {
            vec![(0, length)]
        } else {
            // Framing belongs to the source's declaration, whatever the
            // binding's settings.
            let spec = sources::specs()
                .into_iter()
                .find(|s| s.parser == m.parser)
                .ok_or_else(|| format!("material {}: no source has parser {}", m.id, m.parser))?;
            let framed = stored.records(&m.manifest, spec.framer)?;
            if framed
                .iter()
                .any(|&(start, end)| start > end || end > length)
            {
                return Err(format!("material {}: invalid stored record ranges", m.id));
            }
            m.anchors
                .iter()
                .map(|a| {
                    framed
                        .iter()
                        .find(|(s, _)| i64::try_from(*s).is_ok_and(|s| s == *a))
                        .copied()
                        .ok_or_else(|| format!("material {}: no record starts at {a}", m.id))
                })
                .collect::<Result<_, _>>()?
        };
        let reader = crate::stored::Blanked::new(stored.into_reader()?, spans);
        rewrites.push(Rewrite {
            material: m.id,
            blob: blobs.put_reader(reader).map_err(|e| e.to_string())?,
            length: length as u64,
        });
    }
    Ok(rewrites)
}
