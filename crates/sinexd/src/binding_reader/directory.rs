//! Directory restart restores membership from accounted record locations and
//! explicit cursor bounds. Raw registration alone is not parsing progress.
use super::*;
use sinex_model::{MaterialId, MaterialRef};

pub(super) async fn restore(
    conn: &mut PgConnection,
    blobs: &Blobs,
    b: &mut Bound,
    cursor: &Value,
    access: Access<'_>,
    stop: watch::Receiver<bool>,
) -> Result<(), String> {
    if *stop.borrow() {
        return Err("directory restore cancelled".into());
    }
    let Some(last) = cursor
        .get("material")
        .and_then(Value::as_str)
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
    else {
        return Ok(());
    };
    let configured_class = b.source.spec().class.max(
        sinex_store::privacy::load(conn, std::iter::once(b.source.spec().name))
            .await
            .map_err(|e| e.to_string())?
            .floors
            .get(b.source.spec().name)
            .copied()
            .unwrap_or_default(),
    );
    let chain = sinex_store::replay::chain_ending(conn, last)
        .await
        .map_err(|e| e.to_string())?;
    let mut reference: Option<MaterialRef> = None;
    for (id, _) in chain {
        if *stop.borrow() {
            return Err("directory restore cancelled".into());
        }
        let next = sinex_store::material_bytes::reference(conn, MaterialId(id))
            .await
            .map_err(|e| e.to_string())?
            .ok_or("directory history material is missing")?;
        if let Some(previous) = reference.take() {
            // New captures retain the exact preceding cursor, not an inferred
            // claim that an earlier registered part was fully processed.
            let prior = &next.manifest["directory_resume"]["previous"];
            let frontier = if next.manifest.get("directory_resume").is_some() {
                let id = prior["material"]
                    .as_str()
                    .and_then(|s| uuid::Uuid::parse_str(s).ok())
                    .map(MaterialId);
                if next.previous_part != Some(previous.id) || id != Some(previous.id) {
                    return Err("directory previous cursor differs from material chain".into());
                }
                Some(
                    prior["through"]
                        .as_u64()
                        .ok_or("directory previous cursor has no processed end")?,
                )
            } else {
                None
            };
            restore_one(
                conn,
                blobs,
                b,
                previous,
                frontier,
                None,
                configured_class,
                access,
                stop.clone(),
            )
            .await?;
        }
        reference = Some(next);
    }
    let Some(reference) = reference else {
        return Err("directory cursor has no retained material chain".into());
    };
    if reference.id != MaterialId(last) {
        return Err("directory history does not reach cursor material".into());
    }
    let through = cursor.get("through").and_then(Value::as_u64);
    if through.is_some_and(|n| n > reference.length) {
        return Err("directory cursor exceeds its material".into());
    }
    let pending =
        match through {
            Some(n) if n < reference.length => Some(cursor),
            None => return Err(
                "legacy directory cursor has no processed-end proof; retained replay and an explicit cursor repair are required"
                    .into(),
            ),
            _ => None,
        };
    restore_one(
        conn,
        blobs,
        b,
        reference,
        through,
        pending,
        configured_class,
        access,
        stop,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn restore_one(
    conn: &mut PgConnection,
    blobs: &Blobs,
    b: &mut Bound,
    reference: MaterialRef,
    through: Option<u64>,
    pending: Option<&Value>,
    configured_class: Class,
    access: Access<'_>,
    stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let sealed = blobs
        .is_sealed(&reference.blob)
        .map_err(|e| e.to_string())?;
    if sealed {
        if configured_class < Class::Sealed {
            access.authority.ok_or("directory history is sealed; lowering its class requires explicit scoped authority")?.check()?;
        }
        b.history_floor = b.history_floor.max(Class::Sealed);
    }
    let mut anchors = sinex_store::positions::accounted_anchors(conn, reference.id)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|n| usize::try_from(n).map_err(|_| "directory outcome anchor is invalid".to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(through) = through {
        let records: Vec<(usize, usize)> =
            serde_json::from_value(reference.manifest["records"].clone())
                .map_err(|e| e.to_string())?;
        if through != 0 && !records.iter().any(|(_, end)| *end as u64 == through) {
            return Err("directory processed end is not a record boundary".into());
        }
        anchors.extend(
            records
                .into_iter()
                .filter_map(|(start, end)| (end as u64 <= through).then_some(start)),
        );
    }
    let retained = if pending.is_some() {
        Some(open(conn, blobs, &reference, sealed, access, stop).await?)
    } else {
        None
    };
    b.reader
        .restore_directory(reference, &anchors, retained, pending)
        .map_err(|e| e.to_string())
}

async fn open(
    conn: &mut PgConnection,
    blobs: &Blobs,
    reference: &MaterialRef,
    sealed: bool,
    access: Access<'_>,
    stop: watch::Receiver<bool>,
) -> Result<drivers::RetainedRecords, String> {
    let retained = if sealed {
        use std::io::Seek;
        let authority = access
            .authority
            .ok_or("sealed directory retry requires scoped authority")?
            .clone();
        let key = authority.material_key(conn, reference.id).await?;
        let check = authority.clone();
        let halted = stop.clone();
        let mut reader = blobs
            .read_sealed(
                &reference.blob,
                Some(reference.length),
                &key,
                crate::sealed_access::read_limits(reference.length)?,
                move || {
                    check.check().map_err(std::io::Error::other)?;
                    if *halted.borrow() {
                        return Err(std::io::Error::other("directory restore cancelled").into());
                    }
                    Ok(())
                },
            )
            .map_err(|e| e.to_string())?;
        std::io::copy(&mut reader, &mut std::io::sink()).map_err(|e| e.to_string())?;
        reader.rewind().map_err(|e| e.to_string())?;
        crate::retained::Retained::checked(reader, reference.length, move || {
            authority.check().map_err(std::io::Error::other)?;
            if *stop.borrow() {
                return Err(std::io::Error::other("directory restore cancelled"));
            }
            Ok(())
        })
    } else {
        let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
        let halted = stop.clone();
        let length = blobs
            .copy_to_checked(&reference.blob, &mut file, move || {
                if *halted.borrow() {
                    Err(std::io::Error::other("directory restore cancelled").into())
                } else {
                    Ok(())
                }
            })
            .map_err(|e| e.to_string())?;
        if length != reference.length {
            return Err("retained directory length differs from material".into());
        }
        crate::retained::Retained::checked(file, length, move || {
            if *stop.borrow() {
                Err(std::io::Error::other("directory restore cancelled"))
            } else {
                Ok(())
            }
        })
    };
    Ok(drivers::RetainedRecords {
        blob: reference.blob.clone(),
        bytes: Arc::new(retained),
    })
}
