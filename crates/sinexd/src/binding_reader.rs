//! A binding owns its parser and cursor; bindings of the same stored file share one pass over it.
use crate::backfill::{self, Totals};
use crate::material_sink::Sink;
use crate::writer::WriterHandle;
use serde_json::Value;
use sinex_model::{Class, RunId};
use sinex_sources::shared_pass::Resume;
use sinex_sources::{Binding, InputFormat, Reader, Reading, Source, drivers};
use sinex_store::{Blobs, positions};
use sqlx::PgConnection;
use std::sync::Arc;
use tokio::sync::watch;

pub(crate) struct Bound {
    pub binding: Binding,
    pub source: Box<dyn Source>,
    reading: Reading,
    remembered: bool,
    history_floor: Class,
}
impl Bound {
    pub(crate) fn new(kind: &InputFormat, binding: Binding) -> Result<Self, String> {
        let reader = kind
            .reader
            .ok_or_else(|| format!("{} is not read by a driver", kind.name))?;
        Ok(Self {
            source: (kind.source)(&binding)?,
            reading: reader(&binding)?,
            binding,
            remembered: false,
            history_floor: Class::Open,
        })
    }
}

pub(crate) use crate::sealed_access::Access;

mod shared;
use shared::{Pass, read_shared, same_file};

/// Read independent bindings, reporting each error without preventing other bindings.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    run: RunId,
    sink: &Sink,
    bound: &mut [Bound],
    stop: watch::Receiver<bool>,
    access: Access<'_>,
) -> Vec<(String, Result<Totals, String>)> {
    let mut results = Vec::new();
    let mut passes: Vec<Pass> = Vec::new();
    for (index, b) in bound.iter_mut().enumerate() {
        let init = async {
            if !b.remembered {
                backfill::record(conn, &b.binding).await?;
                restore(conn, blobs, b, access, &stop).await?;
                b.remembered = true;
            }
            Ok::<_, String>(())
        }
        .await;
        if let Err(e) = init {
            results.push((b.binding.source.clone(), Err(e)));
            continue;
        }
        if let Reading::Shared(pass) = &b.reading {
            let choice = async {
                let facts = drivers::file_facts(pass.path()).map_err(|e| e.to_string())?;
                let cursor = positions::load(conn, &b.binding.name)
                    .await
                    .map_err(|e| e.to_string())?
                    .map(|(cursor, _)| cursor)
                    .unwrap_or(Value::Null);
                let floor = sinex_store::privacy::load(conn, std::iter::once(b.source.spec().name))
                    .await
                    .map_err(|e| e.to_string())?
                    .floors
                    .get(b.source.spec().name)
                    .copied()
                    .unwrap_or_default();
                let class = b.source.spec().class.max(floor);
                let resume = pass
                    .resume(&cursor, &facts, class)
                    .map_err(|e| e.to_string())?;
                Ok::<_, String>((resume, facts, class))
            }
            .await;
            match choice {
                Ok((Resume::Read(stored), facts, class)) => {
                    // Host and effective class are the acquisition/privacy context. Physical
                    // file facts permit aliases without conflating authored logical identities.
                    let compatible = passes.iter_mut().find(|g| {
                        g.class == class
                            && class != Class::Sealed
                            && same_file(&g.facts, &facts)
                            && g.host == b.binding.settings.host
                    });
                    if let Some(group) = compatible {
                        group.indices.push(index);
                        group.stored = group.stored.take().or(stored);
                    } else {
                        passes.push(Pass {
                            indices: vec![index],
                            stored,
                            facts,
                            class,
                            host: b.binding.settings.host.clone(),
                        });
                    }
                }
                Ok((Resume::Finished, _, _)) => {
                    results.push((b.binding.source.clone(), Ok(Totals::default())))
                }
                Err(e) => results.push((
                    b.binding.source.clone(),
                    Err(format!("{}: {e}", b.binding.name)),
                )),
            }
        } else if let Reading::Polled(reader) = &mut b.reading {
            let result = backfill::drive(
                conn,
                writer,
                run,
                b.source.as_ref(),
                &b.binding.name,
                sink,
                access,
                b.history_floor,
                reader.as_mut(),
                stop.clone(),
            )
            .await;
            let result = match result {
                Ok(totals) => match positions::load(conn, &b.binding.name).await {
                    Ok(Some((cursor, _))) => reader
                        .remove_passed(&cursor)
                        .map(|_| totals)
                        .map_err(|e| e.to_string()),
                    Ok(None) => Ok(totals),
                    Err(e) => Err(e.to_string()),
                },
                Err(e) => Err(e),
            };
            results.push((b.binding.source.clone(), result));
        }
    }
    for pass in passes {
        let outcome = read_shared(
            conn,
            blobs,
            writer,
            run,
            sink,
            bound,
            &pass,
            stop.clone(),
            access,
        )
        .await;
        match outcome {
            Ok(per_route) => results.extend(
                pass.indices
                    .iter()
                    .zip(per_route)
                    .map(|(&i, r)| (bound[i].binding.source.clone(), r)),
            ),
            Err(e) => results.extend(pass.indices.iter().map(|&i| {
                (
                    bound[i].binding.source.clone(),
                    Err(format!("{}: {e}", bound[i].binding.name)),
                )
            })),
        }
    }
    results
}
/// Offer a polled reader the stored history it rebuilds its memory from. A
/// sealed part raises the binding's class to sealed. A binding read in a
/// shared pass resumes from its cursor alone.
async fn restore(
    conn: &mut PgConnection,
    blobs: &Blobs,
    b: &mut Bound,
    access: Access<'_>,
    stop: &watch::Receiver<bool>,
) -> Result<(), String> {
    let Reading::Polled(reader) = &mut b.reading else {
        return Ok(());
    };
    let sealed = restore_reader(
        conn,
        blobs,
        reader.as_mut(),
        &b.binding.name,
        access.unlock,
        stop,
    )
    .await?;
    if sealed {
        b.history_floor = b.history_floor.max(Class::Sealed);
    }
    Ok(())
}

/// How many parts one query of a restart's history reads.
const PARTS_PER_QUERY: i32 = 256;

/// Offer `reader` the committed cursor's material and the parts before it,
/// newest first, until it has what it needs or the walk reaches the part its
/// saved memory was written at; then the saved memory. Parts are read a page
/// at a time, so a reader that stops early reads no more of the chain. True
/// when a part was sealed.
pub(crate) async fn restore_reader(
    conn: &mut PgConnection,
    blobs: &Blobs,
    reader: &mut dyn Reader,
    binding: &str,
    unlock: Option<&crate::sealed_access::Unlock>,
    stop: &watch::Receiver<bool>,
) -> Result<bool, String> {
    let cursor = positions::load(conn, binding)
        .await
        .map_err(|e| e.to_string())?
        .map(|(cursor, _)| cursor)
        .unwrap_or(Value::Null);
    let Some(last) = reader.history_from(&cursor) else {
        return Ok(false);
    };
    let saved = positions::reader_memory(conn, binding)
        .await
        .map_err(|e| e.to_string())?;
    let mut reached_saved = false;
    let mut sealed = false;
    let mut newest_revision = None;
    let mut next = Some(last);
    'parts: while let Some(newest) = next {
        let page = sinex_store::material_bytes::parts_from(conn, newest, PARTS_PER_QUERY)
            .await
            .map_err(|e| e.to_string())?;
        if page.is_empty() {
            return Err(format!("stored part {newest} is missing"));
        }
        if newest == last {
            newest_revision = page[0].1;
        }
        next = page
            .last()
            .and_then(|(reference, _)| reference.previous_part);
        for (reference, revision) in page {
            if *stop.borrow() {
                return Err("restoring the reader's memory was cancelled".into());
            }
            if saved.as_ref().is_some_and(|m| m.material == reference.id) {
                reached_saved = true;
                break 'parts;
            }
            let bytes =
                crate::stored::MaterialOnDemand::new(conn, blobs, reference.clone(), unlock)
                    .await?;
            sealed |= bytes.sealed();
            let more = reader
                .restore(
                    &cursor,
                    sinex_sources::StoredMaterial {
                        reference,
                        bytes: Arc::new(bytes),
                        same_revision: revision == newest_revision,
                    },
                )
                .map_err(|e| e.to_string())?;
            if !more {
                break 'parts;
            }
        }
    }
    if let Some(saved) = saved.filter(|_| reached_saved) {
        reader
            .restore_memory(&saved.bytes)
            .map_err(|e| e.to_string())?;
        // The parts the saved memory stands for were not opened.
        sealed |= positions::has_sealed_material(conn, binding)
            .await
            .map_err(|e| e.to_string())?;
    }
    reader.restored().map_err(|e| e.to_string())?;
    Ok(sealed)
}

#[cfg(test)]
mod tests;
