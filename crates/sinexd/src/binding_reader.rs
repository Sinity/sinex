//! A binding owns its parser and cursor; compatible archives share physical traversal.
use crate::backfill::{self, Totals};
use crate::one_shot::Sink;
use crate::writer::WriterHandle;
use serde_json::Value;
use sinex_model::{Class, ObservationFacts, RunId};
use sinex_sources::drivers::export_archive::group::{self, Cursor, Route};
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
    history_floor: Class,
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
            history_floor: Class::Open,
        })
    }
}

enum Choice {
    Complete,
    Read(ObservationFacts, Class),
}

pub(crate) use crate::sealed_access::Access;

struct Archive {
    indices: Vec<usize>,
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
    let mut archives: Vec<Archive> = Vec::new();
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
        if let Some(driver) = b.reader.export_archive() {
            let choice = async {
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
                let previous = Cursor::parse(&cursor).map_err(|e| e.to_string())?;
                if previous
                    .as_ref()
                    .is_some_and(|c| c.done && c.same_file(&facts) && c.capture.class == class)
                {
                    return Ok(Choice::Complete);
                }
                Ok::<_, String>(Choice::Read(facts, class))
            }
            .await;
            match choice {
                Ok(Choice::Read(facts, class)) => {
                    // Host and effective class are the acquisition/privacy context. Physical
                    // file facts permit aliases without conflating authored logical identities.
                    let compatible = archives.iter_mut().find(|g| {
                        g.class == class
                            && class != Class::Sealed
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
                access,
                b.history_floor,
                b.reader.as_mut(),
                stop.clone(),
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
            access,
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
/// Offer the reader the stored history it rebuilds its memory from. A sealed
/// part raises the binding's class to sealed.
async fn restore(
    conn: &mut PgConnection,
    blobs: &Blobs,
    b: &mut Bound,
    access: Access<'_>,
    stop: &watch::Receiver<bool>,
) -> Result<(), String> {
    let sealed = restore_reader(
        conn,
        blobs,
        b.reader.as_mut(),
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
/// newest first, until it has what it needs. Parts are read a page at a time,
/// so a reader that stops early reads no more of the chain. True when a part
/// was sealed.
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
        next = page.last().and_then(|(reference, _)| reference.previous_part);
        for (reference, revision) in page {
            if *stop.borrow() {
                return Err("restoring the reader's memory was cancelled".into());
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
    access: Access<'_>,
) -> Result<Vec<Result<Totals, String>>, String> {
    let started = std::time::Instant::now();
    let cancel = Cancel(Arc::new(AtomicBool::new(false)));
    let sealing = if archive.class == Class::Sealed {
        let configured = if access.recipient.is_none() {
            crate::one_shot::class_key()?
        } else {
            None
        };
        let recipient = access
            .recipient
            .cloned()
            .or(configured)
            .or_else(|| access.unlock.map(|a| a.recipient()))
            .ok_or("sealed archive capture requires the class key's public recipient")?;
        Some(Arc::new(crate::one_shot::SealingSink::new(
            blobs.clone(),
            recipient,
        )))
    } else {
        None
    };
    let mut routes = Vec::new();
    let mut stored_archive = None;
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
            if let Some(c) = saved
                .as_ref()
                .filter(|c| c.same_file(&archive.facts) && c.capture.class == archive.class)
            {
                stored_archive = Some((c.capture.blob.clone(), c.capture.length));
            }
            let continuing = saved
                .as_ref()
                .is_some_and(|c| c.same_file(&archive.facts) && c.capture.class == archive.class);
            // Reading a stored sealed archive needs the key of the material
            // that holds it.
            if continuing && let Some(sealing) = &sealing {
                let unlock = access
                    .unlock
                    .ok_or("continuing a sealed archive needs an operator unlock")?;
                let cursor = saved.as_ref().expect("continuing");
                let original = cursor
                    .original_material
                    .ok_or("sealed archive cursor has no original material")?;
                let key = unlock.material_key(conn, original).await?;
                sealing.restore_key(cursor.capture.blob.clone(), key)?;
            }
            Ok::<_, String>(Route {
                class: archive.class,
                driver: b.reader.export_archive().expect("archive group").clone(),
                cursor,
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
    let worker_sealing = sealing.clone();
    let unlock = access.unlock.cloned();
    let cancelled = Arc::clone(&cancel.0);
    let (send, mut receive) = mpsc::channel::<Delivery>(1);
    let worker = tokio::task::spawn_blocking(move || {
        let captured = stored_archive.is_none();
        let (blob, length) = match stored_archive {
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
                        "archive changed during capture; retry from an unchanged revision".into(),
                    );
                }
                (blob, facts.size.ok_or("archive size unavailable")?)
            }
        };
        let verify_stop = Arc::clone(&cancelled);
        let check = move || {
            if verify_stop.load(Ordering::Relaxed)
                || sinex_transport::private_mode::PrivateMode::default().on()
            {
                return Err(std::io::Error::other("archive verification cancelled").into());
            }
            if let Some(unlock) = &unlock {
                unlock.check().map_err(std::io::Error::other)?;
            }
            Ok(())
        };
        let input = match &worker_sealing {
            Some(sealing) => {
                let key = sealing.key(&blob)?;
                group::Input::Seekable(Box::new(Checked {
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
            None => group::Input::Sequential(Box::new(Checked {
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
        group::read_input(
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
        let index = archive.indices[slot];
        let b = &mut bound[index];
        let result = async {
            let mut acquired = delivery.acquired.map_err(|e| e.to_string())?;
            if sinex_transport::private_mode::PrivateMode::default().on() {
                return Err("private mode is on; archive admission stopped".into());
            }
            if let Some(unlock) = access.unlock {
                unlock.check()?;
            }
            let (cursor, carry) = positions::load(conn, &b.binding.name)
                .await
                .map_err(|e| e.to_string())?
                .unwrap_or((Value::Null, Value::Null));
            drivers::note_previous_through(&cursor, &mut acquired);
            let carry = if acquired
                .material
                .as_ref()
                .is_some_and(|m| m.manifest["role"] == "original-archive")
            {
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
                archive.class,
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

impl<R: Read + std::io::Seek> std::io::Seek for Checked<R> {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("archive capture cancelled"));
        }
        self.input.seek(position)
    }
}

#[cfg(test)]
mod tests;
