//! A binding owns its parser and cursor; bindings of the same stored file share one pass over it.
use crate::backfill::{self, Totals};
use crate::material_sink::Sink;
use crate::writer::WriterHandle;
use serde_json::Value;
use sinex_model::{Class, ObservationFacts, RunId};
use sinex_sources::shared_pass::{self, Resume, StoredCopy};
use sinex_sources::{Binding, InputKind, Reader, Reading, Source, drivers};
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
    reading: Reading,
    remembered: bool,
    history_floor: Class,
}
impl Bound {
    pub fn new(kind: &InputKind, binding: Binding) -> Result<Self, String> {
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

/// Bindings read in one shared pass over the same file.
struct Pass {
    indices: Vec<usize>,
    /// The stored copy of the file a binding's cursor names, if any.
    stored: Option<StoredCopy>,
    facts: ObservationFacts,
    class: Class,
    host: String,
}

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
            let bytes = crate::stored::Lazy::new(conn, blobs, reference.clone(), unlock).await?;
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
            || sinex_transport::private_mode::PrivateMode::default().on()
        {
            return Err(std::io::Error::other("capture cancelled"));
        }
        self.input.read(bytes)
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_shared(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    run: RunId,
    sink: &Sink,
    bound: &mut [Bound],
    pass: &Pass,
    mut stop: watch::Receiver<bool>,
    access: Access<'_>,
) -> Result<Vec<Result<Totals, String>>, String> {
    let started = std::time::Instant::now();
    let cancel = Cancel(Arc::new(AtomicBool::new(false)));
    let sealing = if pass.class == Class::Sealed {
        let configured = if access.recipient.is_none() {
            crate::material_sink::class_key()?
        } else {
            None
        };
        let recipient = access
            .recipient
            .cloned()
            .or(configured)
            .or_else(|| access.unlock.map(|a| a.recipient()))
            .ok_or("sealed capture requires the class key's public recipient")?;
        Some(Arc::new(crate::material_sink::SealingSink::new(
            blobs.clone(),
            recipient,
        )))
    } else {
        None
    };
    // Reading a stored sealed copy needs the key of the material that holds
    // it; a sealed group has one binding.
    if let (Some(stored), Some(sealing)) = (&pass.stored, &sealing) {
        let unlock = access
            .unlock
            .ok_or("continuing a sealed file needs an operator unlock")?;
        let material = stored
            .material
            .ok_or("a sealed stored copy names no material")?;
        let key = unlock.material_key(conn, material).await?;
        sealing.restore_key(stored.blob.clone(), key)?;
    }
    let stored_copy = pass
        .stored
        .as_ref()
        .map(|stored| (stored.blob.clone(), stored.length));
    let mut routes = Vec::new();
    let mut slots = Vec::new();
    let mut totals: Vec<Result<Totals, String>> =
        pass.indices.iter().map(|_| Ok(Totals::default())).collect();
    for (slot, &index) in pass.indices.iter().enumerate() {
        let route = async {
            let b = &mut bound[index];
            let cursor = positions::load(conn, &b.binding.name)
                .await
                .map_err(|e| e.to_string())?
                .map(|(cursor, _)| cursor)
                .unwrap_or(Value::Null);
            let Reading::Shared(shared) = &b.reading else {
                unreachable!("only bindings read in a shared pass are grouped")
            };
            Ok::<_, String>(shared.route(cursor, pass.class))
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
    let Reading::Shared(shared) = &bound[pass.indices[slots[0]]].reading else {
        unreachable!("only bindings read in a shared pass are grouped")
    };
    let path = shared.path().to_owned();
    let facts = pass.facts.clone();
    let store = blobs.clone();
    let material_sink = Sink(blobs.clone());
    let worker_sealing = sealing.clone();
    let unlock = access.unlock.cloned();
    let cancelled = Arc::clone(&cancel.0);
    let (send, mut receive) = mpsc::channel::<Delivery>(1);
    let worker = tokio::task::spawn_blocking(move || {
        let captured = stored_copy.is_none();
        let (blob, length) = match stored_copy {
            Some(captured) => captured,
            None => {
                let input = std::fs::File::open(&path).map_err(|e| e.to_string())?;
                let input = Checked {
                    input,
                    stop: Arc::clone(&cancelled),
                };
                let blob = match &worker_sealing {
                    Some(sealing) => sealing.put_reader(input)?,
                    None => store.put_reader(input).map_err(|e| e.to_string())?,
                };
                let after = drivers::file_facts(&path).map_err(|e| e.to_string())?;
                if !same_file(&facts, &after) {
                    return Err(
                        "file changed during capture; retry from an unchanged revision".into(),
                    );
                }
                (blob, facts.size.ok_or("file size unavailable")?)
            }
        };
        let verify_stop = Arc::clone(&cancelled);
        let check = move || {
            if verify_stop.load(Ordering::Relaxed)
                || sinex_transport::private_mode::PrivateMode::default().on()
            {
                return Err(std::io::Error::other("verification cancelled").into());
            }
            if let Some(unlock) = &unlock {
                unlock.check().map_err(std::io::Error::other)?;
            }
            Ok(())
        };
        let input = match &worker_sealing {
            Some(sealing) => {
                let key = sealing.key(&blob)?;
                shared_pass::Input::Seekable(Box::new(Checked {
                    input: store
                        .read_sealed(
                            &blob,
                            Some(length),
                            &key,
                            crate::sealed_access::read_limits(length)?,
                            check,
                        )
                        .map_err(|e| e.to_string())?,
                    stop: Arc::clone(&cancelled),
                }))
            }
            None => shared_pass::Input::Sequential(Box::new(Checked {
                input: store
                    .read_plain(&blob, Some(length), check)
                    .map_err(|e| e.to_string())?,
                stop: Arc::clone(&cancelled),
            })),
        };
        let physical_sink: &dyn drivers::MaterialSink = match &worker_sealing {
            Some(sealing) => sealing.as_ref(),
            None => &material_sink,
        };
        shared_pass::read(
            input,
            blob,
            length,
            facts,
            cancelled,
            routes,
            physical_sink,
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
        let index = pass.indices[slot];
        let b = &mut bound[index];
        let result = async {
            let mut acquired = delivery.acquired.map_err(|e| e.to_string())?;
            if sinex_transport::private_mode::PrivateMode::default().on() {
                return Err("private mode is on; admission stopped".into());
            }
            if let Some(unlock) = access.unlock {
                unlock.check()?;
            }
            let (cursor, carry) = positions::load(conn, &b.binding.name)
                .await
                .map_err(|e| e.to_string())?
                .unwrap_or((Value::Null, Value::Null));
            drivers::note_previous_through(&cursor, &mut acquired);
            let carry = if acquired.starts_over().map_err(|e| e.to_string())? {
                Value::Null
            } else if acquired.material.is_some() {
                match &sealing {
                    Some(sealing) => {
                        backfill::open_carry(conn, &b.binding.name, &carry, sealing, access.unlock)
                            .await?
                    }
                    None => carry,
                }
            } else {
                carry
            };
            let physical_sink: &dyn drivers::MaterialSink = match &sealing {
                Some(sealing) => sealing.as_ref(),
                None => sink,
            };
            let total = totals[slot].as_mut().map_err(|e| e.clone())?;
            backfill::submit(
                writer,
                run,
                b.source.as_ref(),
                &b.binding.name,
                physical_sink,
                sealing.as_deref(),
                access.unlock,
                pass.class,
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
    if let Ok((work, captured)) = &physical
        && let Some(total) = totals.iter_mut().find_map(|t| t.as_mut().ok())
    {
        total.pass_work(work, *captured);
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

impl<R: Read + std::io::Seek> std::io::Seek for Checked<R> {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("capture cancelled"));
        }
        self.input.seek(position)
    }
}

#[cfg(test)]
mod tests;
