//! Restore the exact storage-owned native version before consulting its provider.
use super::*;
use sinex_model::MaterialId;
use sinex_store::native_materials;

pub(super) async fn restore(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    b: &mut Bound,
    access: Access<'_>,
) -> Result<bool, String> {
    if native_materials::binding_pending(conn, &b.binding.name)
        .await
        .map_err(|e| e.to_string())?
        .is_empty()
    {
        return Ok(false);
    }
    let mut guard = writer
        .source_guard(b.source.spec().name)
        .await
        .map_err(|e| e.to_string())?;
    let result = restore_locked(guard.connection(), blobs, b, access).await;
    guard.release().await.map_err(|e| e.to_string())?;
    result
}
async fn restore_locked(
    conn: &mut PgConnection,
    blobs: &Blobs,
    b: &mut Bound,
    access: Access<'_>,
) -> Result<bool, String> {
    let ids = native_materials::binding_pending(conn, &b.binding.name)
        .await
        .map_err(|e| e.to_string())?;
    if ids.is_empty() {
        return Ok(false);
    }
    if ids.len() != 1 {
        return Err("binding has multiple pending native owners".into());
    }
    let p = native_materials::load(conn, ids[0])
        .await
        .map_err(|e| e.to_string())?
        .ok_or("pending native owner disappeared")?;
    if p.source != b.source.spec().name || p.progress.binding != b.binding.name {
        return Err("pending native source differs from its configured binding".into());
    }
    let current = positions::load(conn, &b.binding.name)
        .await
        .map_err(|e| e.to_string())?
        .unwrap_or((Value::Null, Value::Null));
    if current != (p.base_cursor.clone(), p.base_carry.clone()) {
        return Err("pending native base position changed".into());
    }
    if p.progress.material.is_none() {
        return Ok(false);
    }
    let opened = crate::retained::open_pending(blobs, &p, access.authority)?;
    if opened.reference.parser != b.source.spec().parser
        || opened.reference.parser_version != b.source.spec().parser_version
    {
        return Err("pending native parser version is unavailable".into());
    }
    match b.source.spec().driver {
        sinex_sources::Driver::SqliteRows => b
            .reader
            .restore_rows(opened.reference.clone(), opened.records(), &p.base_cursor)
            .map_err(|e| e.to_string())?,
        sinex_sources::Driver::AppendFile => {
            let records = opened
                .body
                .as_ref()
                .ok_or("pending append body is unavailable")?
                .records(&opened.reference.manifest, b.source.spec().framer)?;
            let head = predecessor_head(conn, blobs, b.source.spec().name, &p, access).await?;
            let stored = opened.records();
            let reference = opened.reference.clone();
            let archived_end = opened.append_end.clone();
            let erased = opened.transitions(0)?;
            b.reader
                .restore_append(sinex_sources::binding::AppendResume {
                    reference,
                    stored,
                    records,
                    erased,
                    archived_end,
                    erased_members: Vec::new(),
                    base_cursor: p.base_cursor.clone(),
                    cursor: p.base_cursor.clone(),
                    head,
                    input: p.progress.input.clone(),
                    observation: p.progress.observation.clone(),
                    revision: p.progress.revision,
                })
                .map_err(|e| e.to_string())?;
        }
        _ => return Err("pending native driver shape is unavailable".into()),
    }
    Ok(true)
}

async fn predecessor_head(
    conn: &mut PgConnection,
    blobs: &Blobs,
    source: &str,
    p: &native_materials::Pending,
    access: Access<'_>,
) -> Result<Vec<u8>, String> {
    let reference = p
        .progress
        .material
        .as_ref()
        .ok_or("pending append material is unavailable")?;
    let start = &reference.manifest["append_resume"]["start"];
    let offset = start["offset"]
        .as_u64()
        .ok_or("pending append start offset is unavailable")?;
    let wanted =
        usize::try_from(offset.min(4096)).map_err(|_| "append head exceeds address space")?;
    if wanted == 0 {
        return Ok(Vec::new());
    }
    if p.base_cursor["offset"].as_u64() != Some(offset) {
        return Err("append predecessor cut differs from frozen start".into());
    }
    let last: MaterialId = serde_json::from_value(p.base_cursor["material"].clone())
        .map_err(|_| "append predecessor identity is unavailable")?;
    let cut = p.base_cursor["through"]
        .as_u64()
        .ok_or("append predecessor admitted cut is unavailable")?;
    let chain = sinex_store::replay::current_revision_chain_ending(conn, last.0)
        .await
        .map_err(|e| e.to_string())?;
    let mut pieces = Vec::new();
    let mut remaining = wanted;
    for (id, _) in chain.into_iter().rev() {
        let opened =
            crate::retained::open(conn, blobs, source, MaterialId(id), access.authority).await?;
        let end = if id == last.0 {
            cut
        } else {
            opened.reference.length
        };
        if end > opened.reference.length {
            return Err("append predecessor cut exceeds retained length".into());
        }
        let take = remaining.min(
            usize::try_from(end).map_err(|_| "append predecessor length exceeds address space")?,
        );
        let mut bytes = vec![0; take];
        let body = opened.records();
        if body.bodyless {
            return Err(
                "erased append predecessor cannot provide the original cursor window".into(),
            );
        }
        body.bytes
            .read_exact_at(end - take as u64, &mut bytes)
            .map_err(|e| e.to_string())?;
        pieces.push(bytes);
        remaining -= take;
        if remaining == 0 {
            break;
        }
    }
    if remaining != 0 {
        return Err("append predecessor window is incomplete".into());
    }
    let head = pieces.into_iter().rev().flatten().collect::<Vec<_>>();
    if start["window"].as_str() != Some(blake3::hash(&head).to_hex().as_str()) {
        return Err("append predecessor bytes differ from frozen cursor window".into());
    }
    Ok(head)
}
