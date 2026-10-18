//! The operator's actions on stored events, through the one writer: hide and
//! unhide (reversible), and excision (permanent; architecture §3.4).
use crate::context::Context;
use crate::writer::{Excision, Priority};
use sinex_model::{ArchiveRequest, Batch, BatchKind, EventId, OperationId, RunId};
use sinex_sources::sources;
use sinex_store::excision::{self, Plan, Rewrite, Target};

/// Hide (or with `undo`, unhide) the record `target` was read from.
pub async fn hide(ctx: &Context, target: EventId, undo: bool, actor: &str) -> Result<(), String> {
    let operation = OperationId::new();
    let request = if undo {
        ArchiveRequest::Unhide { target, operation }
    } else {
        ArchiveRequest::Hide { target, operation }
    };
    ctx.writer
        .submit(
            Priority::Live,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: actor.to_owned(),
                },
                records: Vec::new(),
                archive: vec![request],
            },
        )
        .await
        .map(drop)
        .map_err(|e| e.to_string())
}

/// What an excision removes, without removing it.
pub async fn plan(ctx: &mut Context, target: Target) -> Result<Plan, String> {
    excision::plan(&mut ctx.aux, target)
        .await
        .map_err(|e| e.to_string())
}

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
/// reading in one transaction; the old blob files go after commit.
pub async fn excise(ctx: &mut Context, target: Target, actor: &str) -> Result<String, String> {
    let plan = plan(ctx, target).await?;
    let mut rewrites = Vec::new();
    for m in plan.materials.iter().filter(|m| !m.sealed) {
        let mut bytes = ctx.blobs.read(&m.blob).map_err(|e| e.to_string())?;
        let spans: Vec<(usize, usize)> = if m.whole {
            vec![(0, bytes.len())]
        } else {
            let source = sources::by_parser(&m.parser, &m.host)
                .ok_or_else(|| format!("material {}: no source has parser {}", m.id, m.parser))?;
            let framed = (source.spec().framer)(&bytes).0;
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
        excision::blank(&mut bytes, &spans);
        rewrites.push(Rewrite {
            material: m.id,
            blob: ctx.blobs.put(&bytes).map_err(|e| e.to_string())?,
            length: bytes.len() as u64,
        });
    }
    let done = ctx
        .writer
        .excise(Excision {
            target,
            plan,
            rewrites,
            operation: OperationId::new(),
            actor: actor.to_owned(),
            blobs: ctx.blobs.clone(),
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
