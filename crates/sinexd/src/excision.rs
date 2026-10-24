//! Excision (permanent; architecture §3.4), through the one writer: what it
//! would remove, and removing it. Hide and unhide are plain writer batches.
use crate::writer::{Excision, WriterHandle};
use sinex_model::OperationId;
use sinex_sources::sources;
use sinex_store::Blobs;
use sinex_store::excision::{self, Plan, Rewrite, Target};
use sqlx::PgConnection;
mod guard;
mod sealed;
pub(crate) use guard::ExcisionGuard;
pub(crate) use sealed::plan as sealed_plan;

pub fn describe(plan: &Plan) -> String {
    let records: usize = plan.materials.iter().map(|m| m.anchors.len()).sum();
    let whole = plan
        .materials
        .iter()
        .filter(|m| m.whole && !m.sealed)
        .count();
    let sealed = plan.materials.iter().filter(|m| m.sealed).count();
    format!(
        "events {} | records blanked {records} | whole materials blanked {whole} | sealed materials whose key is destroyed {sealed}",
        plan.events.len(),
    )
}

/// Excise `target`: blank its records' bytes in new blobs, then remove every
/// reading in one transaction; the old blob files go after commit. Takes a
/// read connection, the content store and the writer.
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
    let source = source_scope(read_conn, &plan).await?;
    let Some(source) = source else {
        return Ok(
            "excised events 0 | materials rewritten 0 | keys destroyed 0 | blob files deleted 0"
                .into(),
        );
    };
    let guard = writer.excision_guard(source).await?;
    guarded(blobs, writer, target, actor, guard, None).await
}

/// API requests move their existing planning connection into the guard.
pub(crate) async fn excise_pooled(
    mut connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    blobs: &Blobs,
    writer: &WriterHandle,
    target: Target,
    actor: &str,
    authority: Option<crate::sealed_access::Authority>,
) -> Result<String, String> {
    let plan = excision::plan(&mut connection, target)
        .await
        .map_err(|e| e.to_string())?;
    let Some(source) = source_scope(&mut connection, &plan).await? else {
        return Ok(
            "excised events 0 | materials rewritten 0 | keys destroyed 0 | blob files deleted 0"
                .into(),
        );
    };
    let guard = ExcisionGuard::acquire(connection, source).await?;
    guarded(blobs, writer, target, actor, guard, authority).await
}

async fn source_scope(conn: &mut PgConnection, plan: &Plan) -> Result<Option<String>, String> {
    if plan.events.is_empty() && plan.materials.is_empty() {
        return Ok(None);
    }
    let ids = plan.materials.iter().map(|m| m.id.0).collect::<Vec<_>>();
    let mut sources:Vec<String>=sqlx::query_scalar("SELECT DISTINCT b.source FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id JOIN raw.bindings b ON b.binding=i.binding WHERE m.id=ANY($1) ORDER BY b.source")
        .bind(&ids).fetch_all(&mut *conn).await.map_err(|e|e.to_string())?;
    if sources.is_empty() {
        sources = sqlx::query_scalar(
            "SELECT DISTINCT source FROM core.events WHERE id=ANY($1) ORDER BY source",
        )
        .bind(&plan.seeds)
        .fetch_all(conn)
        .await
        .map_err(|e| e.to_string())?;
    }
    if sources.len() != 1 {
        return Err("excision needs one unambiguous native source namespace".into());
    }
    Ok(sources.pop())
}
struct Cancel(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}
async fn guarded(
    blobs: &Blobs,
    writer: &WriterHandle,
    target: Target,
    actor: &str,
    mut guard: ExcisionGuard,
    authority: Option<crate::sealed_access::Authority>,
) -> Result<String, String> {
    let result = async {
        let plan = excision::plan(guard.connection(), target)
            .await
            .map_err(|e| e.to_string())?;
        let source = source_scope(guard.connection(), &plan).await?;
        if source.as_deref() != Some(guard.source()) {
            return Err("excision source namespace changed while waiting; retry planning".into());
        }
        if let Some(authority) = &authority {
            let prepared = sealed::prepare(guard.connection(), blobs, target, authority).await?;
            Ok((prepared.plan, prepared.rewrites, Some(prepared.sealed)))
        } else {
            let (plan, rewrites) = prepare_rewrites(guard.connection(), blobs, target).await?;
            Ok((plan, rewrites, None))
        }
    }
    .await;
    let (plan, rewrites, prepared) = match result {
        Ok(value) => value,
        Err(error) => {
            guard.release().await?;
            return Err(error);
        }
    };
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _cancel = Cancel(cancelled.clone());
    let done = writer
        .excise(Excision {
            sealed: prepared,
            authority,
            guard: Some(guard),
            cancelled,
            target,
            plan,
            rewrites,
            operation: OperationId::new(),
            actor: actor.into(),
            blobs: blobs.clone(),
        })
        .await
        .map_err(|e| e.to_string())?;
    summary(done)
}
async fn prepare_rewrites(
    conn: &mut PgConnection,
    blobs: &Blobs,
    target: Target,
) -> Result<(Plan, Vec<Rewrite>), String> {
    let plan = excision::plan(conn, target)
        .await
        .map_err(|e| e.to_string())?;
    if plan.materials.iter().any(|m| m.sealed) {
        return Err("sealed excision needs scoped prepared key and body replacements".into());
    }
    let mut rewrites = Vec::new();
    for m in plan.materials.iter().filter(|m| !m.sealed) {
        let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
        let length = blobs
            .copy_to(&m.blob, &mut file)
            .map_err(|e| e.to_string())?;
        let retained = crate::retained::Retained::new(file, length);
        let length =
            usize::try_from(retained.length()).map_err(|_| "material exceeds address space")?;
        let spans: Vec<(usize, usize)> = if m.whole {
            vec![(0, length)]
        } else {
            // Framing belongs to the source's declaration, whatever the
            // binding's settings.
            let spec = sources::specs()
                .into_iter()
                .find(|s| s.parser == m.parser)
                .ok_or_else(|| format!("material {}: no source has parser {}", m.id, m.parser))?;
            let framed = retained.records(&m.manifest, spec.framer)?;
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
        let reader = crate::retained::Blanked::new(retained.into_reader()?, spans);
        rewrites.push(Rewrite {
            manifest: None,
            material: m.id,
            blob: blobs.put_reader(reader).map_err(|e| e.to_string())?,
            length: length as u64,
        });
    }
    Ok((plan, rewrites))
}
fn summary(done: crate::writer::Excised) -> Result<String, String> {
    let a = &done.applied;
    let mut out = format!(
        "excised events {} | materials rewritten {} | keys destroyed {} | blob files deleted {}",
        a.events,
        a.materials_rewritten,
        a.keys_destroyed,
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
