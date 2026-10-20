//! A binding owns its parser and cursor; compatible archives share physical traversal.
use crate::backfill::{self, Totals};
use crate::one_shot::{Sink, stored_chain};
use crate::writer::WriterHandle;
use serde_json::Value;
use sinex_model::{Class, ObservationFacts, RunId};
use sinex_sources::drivers::export_archive::group::{self, Cursor, Pending, Route};
use sinex_sources::{Binding, InputKind, Reader, Source, drivers};
use sinex_store::{Blobs, positions};
use sqlx::PgConnection;
use std::io::Read;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{mpsc, watch};

pub(crate) struct Bound {
    pub binding: Binding,
    pub source: Box<dyn Source>,
    reader: Box<dyn Reader>,
    remembered: bool,
}
impl Bound {
    pub fn new(kind: &InputKind, binding: Binding) -> Result<Self, String> {
        let reader = kind
            .reader
            .ok_or_else(|| format!("{} is not read by a driver", kind.name))?;
        Ok(Self {
            source: (kind.source)(&binding)?,
            reader: reader(&binding)?,
            binding,
            remembered: false,
        })
    }
}

enum Choice {
    Complete,
    Sealed,
    Retained(ObservationFacts, Class),
}

struct Archive {
    indices: Vec<usize>,
    facts: ObservationFacts,
    class: Class,
    host: String,
}

/// Read independent bindings, reporting each error without preventing other bindings.
pub(crate) async fn read(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    run: RunId,
    sink: &Sink,
    bound: &mut [Bound],
    stop: watch::Receiver<bool>,
) -> Vec<(String, Result<Totals, String>)> {
    let mut results = Vec::new();
    let mut archives: Vec<Archive> = Vec::new();
    for (index, b) in bound.iter_mut().enumerate() {
        let init = async {
            if !b.remembered {
                backfill::record(conn, &b.binding).await?;
                // Export drivers have no memory derived from old material bodies.
                if b.reader.export_archive().is_none() {
                    let cursor = positions::load(conn, &b.binding.name)
                        .await
                        .map_err(|e| e.to_string())?
                        .map(|(cursor, _)| cursor)
                        .unwrap_or(Value::Null);
                    b.reader
                        .remember(&stored_chain(conn, blobs, &cursor).await?);
                }
                b.remembered = true;
            }
            Ok::<_, String>(())
        }
        .await;
        if let Err(e) = init {
            results.push((b.binding.source.clone(), Err(e)));
            continue;
        }
        if let Some(driver) = b.reader.export_archive() {
            let candidate = async {
                let facts = drivers::file_facts(&driver.path).map_err(|e| e.to_string())?;
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
                if class == Class::Sealed {
                    if cursor.get("version").is_some() {
                        return Err("a retained plain archive needs an explicit keyed migration before continuing as sealed".into());
                    }
                    return Ok(Choice::Sealed);
                }
                // A legacy byte cursor cannot be translated into exact retained record
                // identities by guessing. Keep it unchanged and require migration.
                let previous = Cursor::parse(&cursor).map_err(|e| e.to_string())?;
                if previous.as_ref().is_some_and(|c| c.done && c.same_file(&facts)) {
                    return Ok(Choice::Complete);
                }
                Ok::<_, String>(Choice::Retained(facts, class))
            }
            .await;
            match candidate {
                Ok(Choice::Sealed) => {
                    let result = backfill::drive(
                        conn,
                        writer,
                        run,
                        b.source.as_ref(),
                        &b.binding.name,
                        sink,
                        |cursor| b.reader.poll(cursor),
                    )
                    .await;
                    results.push((b.binding.source.clone(), result));
                }
                Ok(Choice::Retained(facts, class)) => {
                    // Host and effective class are the acquisition/privacy context. Physical
                    // file facts permit aliases without conflating authored logical identities.
                    let compatible = archives.iter_mut().find(|g| {
                        g.class == class
                            && same_file(&g.facts, &facts)
                            && g.host == b.binding.settings.host
                    });
                    if let Some(group) = compatible {
                        group.indices.push(index);
                    } else {
                        archives.push(Archive {
                            indices: vec![index],
                            facts,
                            class,
                            host: b.binding.settings.host.clone(),
                        });
                    }
                }
                Ok(Choice::Complete) => {
                    results.push((b.binding.source.clone(), Ok(Totals::default())))
                }
                Err(e) => results.push((
                    b.binding.source.clone(),
                    Err(format!("{}: {e}", b.binding.name)),
                )),
            }
        } else {
            let result = backfill::drive(
                conn,
                writer,
                run,
                b.source.as_ref(),
                &b.binding.name,
                sink,
                |cursor| b.reader.poll(cursor),
            )
            .await;
            let result = match result {
                Ok(totals) => match positions::load(conn, &b.binding.name).await {
                    Ok(Some((cursor, _))) => b
                        .reader
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
    for archive in archives {
        let outcome = read_archive(
            conn,
            blobs,
            writer,
            run,
            sink,
            bound,
            &archive,
            stop.clone(),
        )
        .await;
        match outcome {
            Ok(per_route) => results.extend(
                archive
                    .indices
                    .iter()
                    .zip(per_route)
                    .map(|(&i, r)| (bound[i].binding.source.clone(), r)),
            ),
            Err(e) => results.extend(archive.indices.iter().map(|&i| {
                (
                    bound[i].binding.source.clone(),
                    Err(format!("{}: {e}", bound[i].binding.name)),
                )
            })),
        }
    }
    results
}
fn same_file(a: &ObservationFacts, b: &ObservationFacts) -> bool {
    a.size == b.size
        && a.mtime_ns == b.mtime_ns
        && a.ctime_ns == b.ctime_ns
        && a.inode == b.inode
        && a.device == b.device
}

struct Delivery {
    index: usize,
    acquired: Result<drivers::Acquired, drivers::DriverError>,
    ack: std::sync::mpsc::SyncSender<bool>,
}
struct Cancel(Arc<AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
struct Checked<R> {
    input: R,
    stop: Arc<AtomicBool>,
}
impl<R: Read> Read for Checked<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if self.stop.load(Ordering::Relaxed)
            || sinex_protocol::private_mode::PrivateMode::default().on()
        {
            return Err(std::io::Error::other("archive capture cancelled"));
        }
        self.input.read(bytes)
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_archive(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    run: RunId,
    sink: &Sink,
    bound: &mut [Bound],
    archive: &Archive,
    mut stop: watch::Receiver<bool>,
) -> Result<Vec<Result<Totals, String>>, String> {
    let started = std::time::Instant::now();
    let cancel = Cancel(Arc::new(AtomicBool::new(false)));
    let mut routes = Vec::new();
    let mut retained = None;
    let mut slots = Vec::new();
    let mut totals: Vec<Result<Totals, String>> = archive
        .indices
        .iter()
        .map(|_| Ok(Totals::default()))
        .collect();
    for (slot, &index) in archive.indices.iter().enumerate() {
        let route = async {
            let b = &mut bound[index];
            let cursor = positions::load(conn, &b.binding.name)
                .await
                .map_err(|e| e.to_string())?
                .map(|(cursor, _)| cursor)
                .unwrap_or(Value::Null);
            let saved = Cursor::parse(&cursor).map_err(|e| e.to_string())?;
            if let Some(c) = saved.as_ref().filter(|c| c.same_file(&archive.facts)) {
                retained = Some((c.capture.blob.clone(), c.capture.length));
            }
            let pending = if let Some(id) = saved.as_ref().and_then(Cursor::pending_material) {
                let material = sinex_store::material_bytes::reference(conn, id)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("missing pending archive material {id}"))?;
                let bytes = blobs.read(&material.blob).map_err(|e| e.to_string())?;
                Some(Pending::new(material, bytes, b.source.spec()).map_err(|e| e.to_string())?)
            } else {
                None
            };
            Ok::<_, String>(Route {
                driver: b.reader.export_archive().expect("archive group").clone(),
                cursor,
                pending,
            })
        }
        .await;
        match route {
            Ok(route) => {
                routes.push(route);
                slots.push(slot);
            }
            Err(error) => totals[slot] = Err(error),
        }
    }
    if routes.is_empty() {
        return Ok(totals);
    }
    let path = routes[0].driver.path.clone();
    let facts = archive.facts.clone();
    let store = blobs.clone();
    let material_sink = Sink(blobs.clone());
    let cancelled = Arc::clone(&cancel.0);
    let (send, mut receive) = mpsc::channel::<Delivery>(1);
    let worker = tokio::task::spawn_blocking(move || {
        let captured = retained.is_none();
        let (blob, length) = match retained {
            Some(captured) => captured,
            None => {
                let input = std::fs::File::open(&path).map_err(|e| e.to_string())?;
                let blob = store
                    .put_reader(Checked {
                        input,
                        stop: Arc::clone(&cancelled),
                    })
                    .map_err(|e| e.to_string())?;
                let after = drivers::file_facts(&path).map_err(|e| e.to_string())?;
                if !same_file(&facts, &after) {
                    return Err(
                        "archive changed during capture; retry from an unchanged revision".into(),
                    );
                }
                (blob, facts.size.ok_or("archive size unavailable")?)
            }
        };
        let verify_stop = Arc::clone(&cancelled);
        let reader = store
            .read_plain(&blob, Some(length), move || {
                if verify_stop.load(Ordering::Relaxed)
                    || sinex_protocol::private_mode::PrivateMode::default().on()
                {
                    Err(std::io::Error::other("archive verification cancelled").into())
                } else {
                    Ok(())
                }
            })
            .map_err(|e| e.to_string())?;
        group::read(
            Box::new(Checked {
                input: reader,
                stop: Arc::clone(&cancelled),
            }),
            blob,
            length,
            facts,
            cancelled,
            routes,
            &material_sink,
            &mut |index, acquired| {
                let (ack, answer) = std::sync::mpsc::sync_channel(0);
                send.blocking_send(Delivery {
                    index,
                    acquired,
                    ack,
                })
                .map_err(|_| drivers::DriverError::Cancelled)?;
                answer.recv().map_err(|_| drivers::DriverError::Cancelled)
            },
        )
        .map(|work| (work, captured))
        .map_err(|e| e.to_string())
    });
    loop {
        let next = tokio::select! {
            delivery = receive.recv() => delivery,
            _ = stop.wait_for(|s| *s) => { cancel.0.store(true, Ordering::Relaxed); receive.close(); None }
        };
        let Some(delivery) = next else {
            break;
        };
        let slot = slots[delivery.index];
        let index = archive.indices[slot];
        let b = &mut bound[index];
        let result = async {
            let acquired = delivery.acquired.map_err(|e| e.to_string())?;
            if sinex_protocol::private_mode::PrivateMode::default().on() {
                return Err("private mode is on; archive admission stopped".into());
            }
            let carry = positions::load(conn, &b.binding.name)
                .await
                .map_err(|e| e.to_string())?
                .map(|(_, carry)| carry)
                .unwrap_or(Value::Null);
            let total = totals[slot].as_mut().map_err(|e| e.clone())?;
            backfill::submit(
                conn,
                writer,
                run,
                b.source.as_ref(),
                &b.binding.name,
                sink,
                None,
                acquired,
                carry,
                total,
            )
            .await
        }
        .await;
        let keep = result.is_ok();
        if let Err(e) = result {
            totals[slot] = Err(e);
        }
        let _ = delivery.ack.send(keep);
    }
    drop(receive);
    let physical = worker.await.map_err(|e| e.to_string())?;
    if let Ok((work, captured)) = &physical {
        if let Some(total) = totals.iter_mut().find_map(|t| t.as_mut().ok()) {
            total.archive_work(work, *captured);
        }
    }
    if let Err(e) = physical {
        // A stopped route has its own error; a traversal-wide error affects every
        // route still running, without claiming completed source progress.
        for total in &mut totals {
            if total.is_ok() {
                *total = Err(e.clone());
            }
        }
    }
    for total in totals.iter_mut().filter_map(|t| t.as_mut().ok()) {
        total.finish(started);
    }
    Ok(totals)
}

#[cfg(test)]
mod tests;
