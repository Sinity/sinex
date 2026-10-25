//! Excision, through the one writer: what it would remove, and removing it.
//! A request is planned and checked here, the replacement bytes are written,
//! and the writer applies the plan in one transaction after checking that a
//! fresh plan is the same. The same request run again finds only what is left.
//!
//! Excision removes whole materials only. The operator can excise a record
//! when every material it was read from holds nothing else, and only from a
//! source declared excisable: one whose every stored copy of a record is a
//! reading of it. An outside producer can excise whole materials it
//! registered itself. Every other request is refused before anything changes;
//! a shared material is never rewritten.
use crate::writer::{Excision, WriterHandle};
use sinex_model::OperationId;
use sinex_sources::sources;
use sinex_store::Blobs;
use sinex_store::excision::{self, Material, Plan, Rewrite, Target};
use sqlx::PgConnection;
use std::io::Read;

pub(crate) fn describe(plan: &Plan) -> String {
    format!(
        "events {} | materials blanked {}",
        plan.events.len(),
        plan.materials.len()
    )
}

/// What the operator's excision of `target` would remove, or why it is
/// refused.
pub(crate) async fn plan(
    conn: &mut PgConnection,
    blobs: &Blobs,
    target: Target,
) -> Result<Plan, String> {
    let plan = excision::plan(conn, target)
        .await
        .map_err(|e| e.to_string())?;
    for m in &plan.materials {
        let Some(source) = &m.source else {
            return Err(format!(
                "material {} was registered by an outside producer; that producer excises its \
                 own materials",
                m.id
            ));
        };
        let spec = excisable(source, &m.parser)?;
        if !m.whole {
            holds_only_the_record(blobs, spec, m)?;
        }
    }
    Ok(plan)
}

/// The declaration of the source that read a material, when its records can
/// be excised.
fn excisable(source: &str, parser: &str) -> Result<&'static sinex_sources::SourceSpec, String> {
    let spec = sources::specs()
        .into_iter()
        .find(|s| s.name == source && s.parser == parser)
        .ok_or_else(|| format!("{source} read with {parser}: no registered source"))?;
    if !spec.excisable {
        return Err(format!(
            "records of {source} cannot be excised: the source can read a record again or keep \
             another copy of it (a rewrite, a restart, a re-fetch, a repeat, a new export), and \
             excision would leave those copies"
        ));
    }
    Ok(spec)
}

/// Refuse a material that holds records besides the planned ones: excision
/// never rewrites a material other records share.
fn holds_only_the_record(
    blobs: &Blobs,
    spec: &sinex_sources::SourceSpec,
    m: &Material,
) -> Result<(), String> {
    let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
    let length = blobs
        .copy_to(&m.blob, &mut file)
        .map_err(|e| e.to_string())?;
    let records =
        crate::stored::StoredReader::new(file, length).records(&m.manifest, spec.framer)?;
    if records
        .iter()
        .any(|(start, _)| i64::try_from(*start).map_or(true, |s| !m.anchors.contains(&s)))
    {
        return Err(format!(
            "material {} holds other records too; excision removes whole materials and does not \
             rewrite one that other records share",
            m.id
        ));
    }
    Ok(())
}

/// Excise `target` at the operator's request.
pub(crate) async fn excise(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    target: Target,
    actor: &str,
) -> Result<String, String> {
    let plan = plan(conn, blobs, target).await?;
    let done = apply(blobs, writer, target, plan, actor).await?;
    let a = &done.applied;
    let mut out = format!(
        "excised events {} | materials blanked {} | blob files deleted {}",
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

/// Excise whole `materials` at the request of `producer`, which must have
/// registered every one of them. All are planned and checked before any is
/// excised; each is then excised in its own transaction, so a request sent
/// again finishes an interrupted one.
pub(crate) async fn excise_for_producer(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    producer: &str,
    materials: &[sinex_model::MaterialId],
) -> Result<sinex_protocol::ExciseReply, String> {
    if materials.is_empty() {
        return Err("an excise request names at least one material".into());
    }
    let own = format!("ingress:{producer}");
    let mut plans = Vec::with_capacity(materials.len());
    for material in materials {
        let target = Target::Material(*material);
        let plan = excision::plan(conn, target)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(other) = plan
            .materials
            .iter()
            .find(|m| m.binding.as_deref() != Some(own.as_str()))
        {
            return Err(format!(
                "material {} was not registered by {producer}; a producer excises only its own \
                 materials",
                other.id
            ));
        }
        plans.push((target, plan));
    }
    let mut reply = sinex_protocol::ExciseReply::default();
    for (target, plan) in plans {
        let done = apply(blobs, writer, target, plan, producer).await?;
        reply.materials += done.applied.materials_rewritten as u64;
        reply.events += done.applied.events as u64;
    }
    Ok(reply)
}

/// Write the replacement bytes of `plan`'s materials and have the writer
/// apply it.
async fn apply(
    blobs: &Blobs,
    writer: &WriterHandle,
    target: Target,
    plan: Plan,
    actor: &str,
) -> Result<crate::writer::Excised, String> {
    let rewrites = plan
        .materials
        .iter()
        .map(|m| blanked(blobs, m))
        .collect::<Result<Vec<_>, _>>()?;
    writer
        .excise(Excision {
            target,
            plan,
            rewrites,
            operation: OperationId::new(),
            actor: actor.into(),
            blobs: blobs.clone(),
        })
        .await
        .map_err(|e| e.to_string())
}

/// The material's replacement: spaces of the same length, ending in the
/// material's closing newline if it has one, so the stored part chain keeps
/// its offsets and holds no record. Written to the store before the plan is
/// applied (bytes before rows).
fn blanked(blobs: &Blobs, m: &Material) -> Result<Rewrite, String> {
    let newline = m.length > 0
        && blobs
            .read_range(&m.blob, m.length - 1, 1)
            .map_err(|e| e.to_string())?
            == b"\n";
    let spaces = std::io::repeat(b' ')
        .take(m.length - u64::from(newline))
        .chain(if newline { &b"\n"[..] } else { &b""[..] });
    Ok(Rewrite {
        material: m.id,
        blob: blobs.put_reader(spaces).map_err(|e| e.to_string())?,
        length: m.length,
    })
}

#[cfg(test)]
pub(crate) mod tests;
