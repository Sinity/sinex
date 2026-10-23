//! A binding owns its parser and cursor; compatible archives share physical traversal.
mod directory;
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
    history_floor: Class,
}
impl Bound {
    pub fn new(kind: &InputKind, binding: Binding) -> Result<Self, String> {
        let reader = kind
            .reader
            .ok_or_else(|| format!("{} is not read by a driver", kind.name))?;
        let mut reader = reader(&binding)?;
        reader.configure(&binding.settings)?;
        Ok(Self {
            source: (kind.source)(&binding)?,
            reader,
            binding,
            remembered: false,
            history_floor: Class::Open,
        })
    }
}

enum Choice {
    Complete,
    Retained(ObservationFacts, Class),
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
    let mut known = match sinex_store::bindings::list(conn).await {
        Ok(known) => known,
        Err(e) => {
            return bound
                .iter()
                .map(|b| (b.binding.source.clone(), Err(e.to_string())))
                .collect();
        }
    };
    known.extend(bound.iter().map(|b| sinex_store::bindings::Stored {
        binding: b.binding.name.clone(),
        source: b.binding.source.clone(),
        input: b.binding.input.display().to_string(),
        settings: serde_json::to_value(&b.binding.settings).expect("settings serialize"),
    }));
    for (index, b) in bound.iter_mut().enumerate() {
        let init = async {
            check_image_class(conn, b, &known).await?;
            if !b.remembered {
                backfill::record(conn, &b.binding).await?;
                // Export drivers have no memory derived from old material bodies.
                if b.reader.export_archive().is_none() {
                    let cursor = positions::load(conn, &b.binding.name)
                        .await
                        .map_err(|e| e.to_string())?
                        .map(|(cursor, _)| cursor)
                        .unwrap_or(Value::Null);
                    if b.source.spec().driver == sinex_sources::Driver::DirSnapshot {
                        directory::restore(conn, blobs, b, &cursor, access, stop.clone()).await?;
                    } else {
                        b.reader
                            .remember(&stored_chain(conn, blobs, &cursor).await?);
                        if b.reader.image_policy().is_some() {
                            restore_sqlite(conn, blobs, b, &cursor).await?;
                        }
                    }
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
                // A legacy byte cursor cannot be translated into exact retained record
                // identities by guessing. Keep it unchanged and require migration.
                let previous = Cursor::parse(&cursor).map_err(|e| e.to_string())?;
                if previous
                    .as_ref()
                    .is_some_and(|c| c.done && c.same_file(&facts) && c.capture.class == class)
                {
                    return Ok(Choice::Complete);
                }
                Ok::<_, String>(Choice::Retained(facts, class))
            }
            .await;
            match candidate {
                Ok(Choice::Retained(facts, class)) => {
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
                |cursor| {
                    if *stop.borrow() {
                        b.reader.cancel_pending();
                        return Err(drivers::DriverError::Cancelled);
                    }
                    let acquired = b.reader.poll(cursor)?;
                    if let Some(image) = &acquired.image {
                        let stop = stop.clone();
                        image
                            .set_check(Arc::new(move || {
                                if *stop.borrow()
                                    || sinex_protocol::private_mode::PrivateMode::default().on()
                                {
                                    return Err("SQLite image capture cancelled".into());
                                }
                                Ok(())
                            }))
                            .map_err(drivers::DriverError::Io)?;
                    }
                    Ok(acquired)
                },
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
async fn check_image_class(
    conn: &mut PgConnection,
    b: &mut Bound,
    known: &[sinex_store::bindings::Stored],
) -> Result<(), String> {
    let Some(policy) = b.reader.image_policy() else {
        return Ok(());
    };
    let mut classes = std::collections::HashMap::new();
    for input in known {
        let host = input.settings["host"].as_str().unwrap_or_default();
        if host != b.binding.settings.host
            || !same_database(&b.binding.input, std::path::Path::new(&input.input))
        {
            continue;
        }
        let class = sinex_sources::sources::spec(&input.source)
            .map(|s| s.class)
            .ok_or("configured database binding has no known source class")?;
        classes
            .entry(input.source.clone())
            .and_modify(|c: &mut Class| *c = (*c).max(class))
            .or_insert(class);
    }
    let rules = sinex_store::privacy::load(conn, classes.keys().map(String::as_str))
        .await
        .map_err(|e| e.to_string())?;
    let class = classes
        .into_iter()
        .fold(b.source.spec().class, |c, (name, declared)| {
            c.max(declared)
                .max(rules.floors.get(&name).copied().unwrap_or_default())
        });
    let class = rules.rules.iter().fold(class, |c, r| c.max(r.class));
    if policy.class < class {
        return Err("whole-database image class is below a configured source/field class".into());
    }
    Ok(())
}
fn same_database(a: &std::path::Path, b: &std::path::Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    if a == b {
        return true;
    }
    if let (Ok(a), Ok(b)) = (a.metadata(), b.metadata()) {
        return a.dev() == b.dev() && a.ino() == b.ino();
    }
    false
}

async fn restore_sqlite(
    conn: &mut PgConnection,
    blobs: &Blobs,
    b: &mut Bound,
    cursor: &Value,
) -> Result<(), String> {
    let database = b
        .binding
        .input
        .canonicalize()
        .map_err(|e| e.to_string())?
        .display()
        .to_string();
    if let Some(id) = sinex_store::sqlite_images::latest(conn, &database, &b.binding.settings.host)
        .await
        .map_err(|e| e.to_string())?
    {
        let (metadata, extents, material) = sinex_store::sqlite_images::load(conn, id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("retained SQLite image is missing")?;
        let map = if metadata.class == Class::Sealed {
            None
        } else {
            let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
            let map = [sinex_model::ImageExtent {
                blob: material.blob,
                offset: metadata.map_offset,
                length: metadata.map_bytes,
            }];
            let copied = std::io::copy(
                &mut blobs.assemble(&map).map_err(|e| e.to_string())?,
                &mut file,
            )
            .map_err(|e| e.to_string())?;
            if copied != metadata.map_bytes {
                return Err("retained SQLite page map is short".into());
            }
            Some(Arc::new(file))
        };
        b.reader.restore_image(drivers::sqlite_image::Previous {
            metadata,
            extents,
            map,
        });
    }
    let Some(id) = cursor
        .get("material")
        .and_then(Value::as_str)
        .and_then(|s| uuid::Uuid::parse_str(s).ok().map(sinex_model::MaterialId))
    else {
        return Ok(());
    };
    let reference = sinex_store::material_bytes::reference(conn, id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("SQLite cursor material is missing")?;
    if cursor["through"].as_u64().unwrap_or(0) >= reference.length {
        return Ok(());
    }
    if blobs
        .is_sealed(&reference.blob)
        .map_err(|e| e.to_string())?
    {
        return Err("retained sealed SQLite rows are pending explicit operator unlock".into());
    }
    let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
    let copied = blobs
        .copy_to(&reference.blob, &mut file)
        .map_err(|e| e.to_string())?;
    if copied != reference.length {
        return Err("retained SQLite row length differs".into());
    }
    let retained = drivers::RetainedRecords {
        blob: reference.blob.clone(),
        bytes: Arc::new(crate::retained::Retained::new(file, reference.length)),
    };
    b.reader
        .restore_rows(reference, retained, cursor)
        .map_err(|e| e.to_string())
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
            .or_else(|| access.authority.map(|a| a.recipient()))
            .ok_or("sealed archive capture requires the class key's public recipient")?;
        Some(Arc::new(crate::one_shot::SealingSink::new(
            blobs.clone(),
            recipient,
        )))
    } else {
        None
    };
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
            if let Some(c) = saved
                .as_ref()
                .filter(|c| c.same_file(&archive.facts) && c.capture.class == archive.class)
            {
                retained = Some((c.capture.blob.clone(), c.capture.length));
            }
            let continuing = saved
                .as_ref()
                .is_some_and(|c| c.same_file(&archive.facts) && c.capture.class == archive.class);
            if continuing && let Some(sealing) = &sealing {
                let authority = access
                    .authority
                    .ok_or("retained sealed archive is pending explicit operator unlock")?;
                let cursor = saved.as_ref().expect("continuing");
                let original = cursor.original_material.ok_or(
                    "sealed cursor has no original material identity; explicit migration required",
                )?;
                let (key_ref, secrets) = authority.material_secrets(conn, original).await?;
                sealing.restore_secrets(cursor.capture.blob.clone(), key_ref, secrets)?;
                if let Some(material) = cursor.material {
                    let reference = sinex_store::material_bytes::reference(conn, material)
                        .await
                        .map_err(|e| e.to_string())?
                        .ok_or("sealed cursor material is missing")?;
                    let (key_ref, secrets) = authority.material_secrets(conn, material).await?;
                    sealing.restore_current(material, reference.blob, key_ref, secrets)?;
                }
            }
            let pending = if let Some(id) = saved
                .as_ref()
                .filter(|_| continuing)
                .and_then(Cursor::pending_material)
            {
                let material = sinex_store::material_bytes::reference(conn, id)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("missing pending archive material {id}"))?;
                let bytes = if let Some(sealing) = &sealing {
                    let key = sealing.key(&material.blob)?;
                    let authority = access.authority.expect("scoped key loading").clone();
                    let mut reader = blobs
                        .read_sealed(
                            &material.blob,
                            Some(material.length),
                            &key,
                            crate::sealed_access::read_limits(material.length)?,
                            move || {
                                authority
                                    .check()
                                    .map_err(|e| std::io::Error::other(e).into())
                            },
                        )
                        .map_err(|e| e.to_string())?;
                    let mut bytes = Vec::new();
                    reader.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
                    bytes
                } else {
                    blobs.read(&material.blob).map_err(|e| e.to_string())?
                };
                Some(Pending::new(material, bytes, b.source.spec()).map_err(|e| e.to_string())?)
            } else {
                None
            };
            Ok::<_, String>(Route {
                class: archive.class,
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
    let worker_sealing = sealing.clone();
    let authority = access.authority.cloned();
    let cancelled = Arc::clone(&cancel.0);
    let (send, mut receive) = mpsc::channel::<Delivery>(1);
    let worker = tokio::task::spawn_blocking(move || {
        let captured = retained.is_none();
        let (blob, length) = match retained {
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
                || sinex_protocol::private_mode::PrivateMode::default().on()
            {
                return Err(std::io::Error::other("archive verification cancelled").into());
            }
            if let Some(authority) = &authority {
                authority.check().map_err(std::io::Error::other)?;
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
            let acquired = delivery.acquired.map_err(|e| e.to_string())?;
            if sinex_protocol::private_mode::PrivateMode::default().on() {
                return Err("private mode is on; archive admission stopped".into());
            }
            if let Some(authority) = access.authority {
                authority.check()?;
            }
            let carry = positions::load(conn, &b.binding.name)
                .await
                .map_err(|e| e.to_string())?
                .map(|(_, carry)| carry)
                .unwrap_or(Value::Null);
            let carry = if acquired
                .material
                .as_ref()
                .is_some_and(|m| m.manifest["role"] == "original-archive")
            {
                Value::Null
            } else if acquired.material.is_some() {
                match &sealing {
                    Some(sealing) => sealing.open_carry(&carry)?,
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
                access.authority,
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
