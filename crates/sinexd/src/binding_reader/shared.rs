//! One pass over a stored file shared by every compatible binding of it:
//! the file is stored once (sealed when the bindings' class is), read forward
//! once, and each binding's acquisitions are admitted as the pass reaches them.
use super::{Access, Bound};
use crate::backfill::{self, Totals};
use crate::material_sink::Sink;
use crate::writer::WriterHandle;
use serde_json::Value;
use sinex_model::{Class, ObservationFacts, RunId};
use sinex_sources::Reading;
use sinex_sources::drivers;
use sinex_sources::shared_pass::{self, StoredCopy};
use sinex_store::{Blobs, positions};
use sqlx::PgConnection;
use std::io::Read;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{mpsc, watch};

/// Bindings read in one shared pass over the same file.
pub(super) struct Pass {
    pub(super) indices: Vec<usize>,
    /// The stored copy of the file a binding's cursor names, if any.
    pub(super) stored: Option<StoredCopy>,
    pub(super) facts: ObservationFacts,
    pub(super) class: Class,
    pub(super) host: String,
}

pub(super) fn same_file(a: &ObservationFacts, b: &ObservationFacts) -> bool {
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
pub(super) async fn read_shared(
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
        let key = unlock.data_key(conn, material).await?;
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
        let archive_sink: &dyn drivers::MaterialSink = match &worker_sealing {
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
            archive_sink,
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
            let archive_sink: &dyn drivers::MaterialSink = match &sealing {
                Some(sealing) => sealing.as_ref(),
                None => sink,
            };
            let total = totals[slot].as_mut().map_err(|e| e.clone())?;
            backfill::submit(
                writer,
                run,
                b.source.as_ref(),
                &b.binding.name,
                archive_sink,
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
