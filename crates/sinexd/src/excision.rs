//! Excision (permanent; architecture §3.4), through the one writer: what it
//! would remove, and removing it. Hide and unhide are plain writer batches.
use crate::writer::{Excision, WriterHandle};
use sinex_model::OperationId;
use sinex_sources::sources;
use sinex_store::Blobs;
use sinex_store::excision::{self, Plan, Rewrite, Target};
use sqlx::PgConnection;
mod guard;
pub(crate) use guard::ExcisionGuard;

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
    let done = writer
        .excise(Excision {
            sealed: None,
            authority: None,
            guard: None,
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            target,
            plan,
            rewrites,
            operation: OperationId::new(),
            actor: actor.to_owned(),
            blobs: blobs.clone(),
        })
        .await
        .map_err(|e| e.to_string())?;
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
