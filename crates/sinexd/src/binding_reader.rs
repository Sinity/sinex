//! A binding owns its parser and cursor; bindings of the same stored file share one pass over it.
use crate::backfill::{self, Totals};
use crate::sealing::Sink;
use crate::writer::WriterHandle;
use serde_json::Value;
use sinex_model::{Class, RunId};
use sinex_source_core::shared_pass::SharedReadStart;
use sinex_source_core::{Binding, InputFormat, Reader, Reading, Source, drivers};
use sinex_store::{Blobs, positions};
use sqlx::PgConnection;
use std::sync::Arc;
use tokio::sync::watch;

pub(crate) struct BindingReader {
    pub binding: Binding,
    pub source: Box<dyn Source>,
    reading: Reading,
    remembered: bool,
    history_floor: Class,
    /// An append-only file's facts when a poll last read it to its end: while
    /// they hold, nothing was appended and the poll is skipped.
    settled: Option<sinex_model::ObservationFacts>,
}
impl BindingReader {
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
            settled: None,
        })
    }
}

pub(crate) use crate::sealing::Access;

mod shared;
use shared::{SharedFileRead, read_shared, same_file};

/// Each binding's outcome as it is read: its source and result, kept for the
/// caller, and handed at once to `report` with the binding's name.
struct Results<'a> {
    list: Vec<(String, Result<Totals, String>)>,
    report: &'a mut (dyn FnMut(&str, &Result<Totals, String>) + Send),
}

impl Results<'_> {
    fn add(&mut self, binding: &Binding, result: Result<Totals, String>) {
        (self.report)(&binding.name, &result);
        self.list.push((binding.source.clone(), result));
    }
}

/// Read independent bindings, reporting each error without preventing other
/// bindings. `report` sees each binding's outcome as soon as it is known, so a
/// round that takes hours does not hide its failures until it ends.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    run: RunId,
    sink: &Sink,
    readers: &mut [BindingReader],
    stop: watch::Receiver<bool>,
    access: Access<'_>,
    report: &mut (dyn FnMut(&str, &Result<Totals, String>) + Send),
) -> Vec<(String, Result<Totals, String>)> {
    let mut results = Results {
        list: Vec::new(),
        report,
    };
    let mut passes: Vec<SharedFileRead> = Vec::new();
    for (index, b) in readers.iter_mut().enumerate() {
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
            results.add(&b.binding, Err(e));
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
                Ok((SharedReadStart::Read(stored), facts, class)) => {
                    // Host and effective class are the acquisition/privacy context. Physical
                    // file facts permit aliases without conflating configured input identities.
                    let compatible = passes.iter_mut().find(|g| {
                        g.class == class
                            && crate::sealing::can_share(class)
                            && same_file(&g.facts, &facts)
                            && g.host == b.binding.settings.host
                    });
                    if let Some(group) = compatible {
                        group.indices.push(index);
                        group.stored = group.stored.take().or(stored);
                    } else {
                        passes.push(SharedFileRead {
                            indices: vec![index],
                            stored,
                            facts,
                            class,
                            host: b.binding.settings.host.clone(),
                        });
                    }
                }
                Ok((SharedReadStart::Finished, _, _)) => {
                    results.add(&b.binding, Ok(Totals::default()))
                }
                Err(e) => results.add(&b.binding, Err(format!("{}: {e}", b.binding.name))),
            }
        } else if let Reading::Polled(reader) = &mut b.reading {
            // Taken before the poll, so bytes appended during it change them.
            let facts = (b.source.spec().driver == sinex_source_core::Driver::AppendFile)
                .then(|| drivers::file_facts(&b.binding.input).ok())
                .flatten();
            if let (Some(now), Some(then)) = (&facts, &b.settled)
                && same_file(now, then)
            {
                results.add(&b.binding, Ok(Totals::default()));
                continue;
            }
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
            b.settled = if result.is_ok() { facts } else { None };
            results.add(&b.binding, result);
        }
    }
    for pass in passes {
        let outcome = read_shared(
            conn,
            blobs,
            writer,
            run,
            readers,
            &pass,
            stop.clone(),
            access,
        )
        .await;
        match outcome {
            Ok(per_route) => {
                for (&i, r) in pass.indices.iter().zip(per_route) {
                    results.add(&readers[i].binding, r);
                }
            }
            Err(e) => {
                for &i in &pass.indices {
                    let failed = Err(format!("{}: {e}", readers[i].binding.name));
                    results.add(&readers[i].binding, failed);
                }
            }
        }
    }
    results.list
}
/// Offer a polled reader the stored history it rebuilds its memory from. A
/// sealed part raises the binding's class to sealed. A binding read in a
/// shared pass resumes from its cursor alone.
async fn restore(
    conn: &mut PgConnection,
    blobs: &Blobs,
    b: &mut BindingReader,
    access: Access<'_>,
    stop: &watch::Receiver<bool>,
) -> Result<(), String> {
    let Reading::Polled(reader) = &mut b.reading else {
        return Ok(());
    };
    let floor = restore_reader(
        conn,
        blobs,
        reader.as_mut(),
        &b.binding.name,
        access.unlock,
        stop,
    )
    .await?;
    b.history_floor = b.history_floor.max(floor);
    Ok(())
}

/// How many parts one query of a restart's history reads.
const PARTS_PER_QUERY: i32 = 256;

/// Offer `reader` the committed cursor's material and the parts before it,
/// newest first, until it has what it needs or the walk reaches the part its
/// saved memory was written at; then the saved memory. Parts are read a page
/// at a time, so a reader that stops early reads no more of the chain. Returns
/// the privacy floor of the restored history.
pub(crate) async fn restore_reader(
    conn: &mut PgConnection,
    blobs: &Blobs,
    reader: &mut dyn Reader,
    binding: &str,
    unlock: Option<&crate::sealing::Unlock>,
    stop: &watch::Receiver<bool>,
) -> Result<Class, String> {
    let cursor = positions::load(conn, binding)
        .await
        .map_err(|e| e.to_string())?
        .map(|(cursor, _)| cursor)
        .unwrap_or(Value::Null);
    let Some(last) = reader.history_from(&cursor) else {
        return Ok(Class::default());
    };
    let saved = positions::reader_memory(conn, binding)
        .await
        .map_err(|e| e.to_string())?;
    let mut reached_saved = false;
    let mut floor = Class::default();
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
                crate::sealing::MaterialOnDemand::new(conn, blobs, reference.clone(), unlock)
                    .await?;
            floor = floor.max(bytes.floor());
            let more = reader
                .restore(
                    &cursor,
                    sinex_source_core::StoredMaterial {
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
        floor = floor.max(crate::sealing::history_floor(conn, binding).await?);
    }
    reader.restored().map_err(|e| e.to_string())?;
    Ok(floor)
}

#[cfg(test)]
mod tests;
