//! Replay: read stored materials again with the current parsers and rules. The
//! scope is materials (named, a binding's, or those with readings of some types
//! in a time window), narrowed by type and time.
//!
//! Each part sequence (an input's parts, linked first to last) is replayed
//! from its first part, because a parser's carried state flows from part to
//! part; only in-scope materials' readings are archived and admitted. A sequence
//! is one transaction: its live in-scope readings (and their live derived
//! descendants) are archived and the new readings admitted together, so a
//! failure changes nothing and a crash leaves the sequence as it was. Finished
//! sequences are journaled under the operation, and `--resume` skips them.
//!
//! `--compare` runs exactly the same transactions and rolls each back after
//! reading what it would change.
use crate::writer::{ReplaySequence, WriterHandle};
use serde_json::{Value, json};
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, MaterialId, OperationId, RecordOutcome, ReplayScope, RunId,
    Timestamp,
};
use sinex_sources::{DrainInput, Limits, Source, drivers, sources};
use sinex_store::Blobs;
use sinex_store::bindings;
use sinex_store::replay::{self, Change, Diff, Stored};
use sqlx::PgConnection;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

#[derive(Clone, Debug, Default)]
pub(crate) struct Scope {
    pub materials: Vec<Uuid>,
    pub binding: Option<String>,
    pub event_types: Option<Vec<String>>,
    pub from: Option<Timestamp>,
    pub until: Option<Timestamp>,
}

impl Scope {
    fn to_json(&self) -> Value {
        json!({
            "materials": self.materials,
            "binding": self.binding,
            "event_types": self.event_types,
            "from": self.from,
            "until": self.until,
        })
    }

    fn from_json(v: &Value) -> Result<Self, String> {
        let bad = |what: &str| format!("stored replay scope: bad {what}");
        Ok(Self {
            materials: serde_json::from_value(v["materials"].clone())
                .map_err(|_| bad("materials"))?,
            binding: serde_json::from_value(v["binding"].clone()).map_err(|_| bad("binding"))?,
            event_types: serde_json::from_value(v["event_types"].clone())
                .map_err(|_| bad("event_types"))?,
            from: serde_json::from_value(v["from"].clone()).map_err(|_| bad("from"))?,
            until: serde_json::from_value(v["until"].clone()).map_err(|_| bad("until"))?,
        })
    }
}

/// What a replay did, or in compare mode would do.
#[derive(Debug, Default)]
pub(crate) struct Report {
    pub operation: OperationId,
    pub sequences: usize,
    pub sequences_skipped: usize,
    pub materials: usize,
    pub events_read: usize,
    pub failures: usize,
    pub archived: usize,
    pub admitted: usize,
    pub diff: Diff,
}

impl std::fmt::Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "operation {} | sequences {} (skipped as done: {}) | materials re-read {} | readings {} | failures {} | archived {} | admitted {}",
            self.operation,
            self.sequences,
            self.sequences_skipped,
            self.materials,
            self.events_read,
            self.failures,
            self.archived,
            self.admitted
        )?;
        write!(
            f,
            "live records: {} unchanged, {} changed, {} added, {} removed",
            self.diff.unchanged,
            self.diff.changed.len(),
            self.diff.added.len(),
            self.diff.removed.len()
        )
    }
}

impl Report {
    /// The report as a reply: the counts, the summary line, and the changes
    /// (all of them with `all`, otherwise the first few of each kind).
    pub(crate) fn to_json(&self, compare: bool, all: bool) -> Value {
        json!({
            "operation": self.operation,
            "compare_only": compare,
            "sequences": self.sequences,
            "sequences_skipped": self.sequences_skipped,
            "materials": self.materials,
            "readings": self.events_read,
            "failures": self.failures,
            "archived": self.archived,
            "admitted": self.admitted,
            "unchanged": self.diff.unchanged,
            "changed": self.diff.changed.len(),
            "added": self.diff.added.len(),
            "removed": self.diff.removed.len(),
            "summary": self.to_string(),
            "changes": describe(&self.diff, all),
        })
    }
}

pub(crate) struct Options {
    pub scope: Scope,
    pub compare: bool,
    pub resume: Option<OperationId>,
    pub actor: String,
    pub unlock: Option<crate::sealed_access::Unlock>,
}

/// Replay with its parts: a read connection, the content store and the writer
/// (the daemon's API and a local `sinexd admin` run hold these).
pub(crate) async fn run(
    read_conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    o: Options,
) -> Result<Report, String> {
    let (operation, scope, done) = match o.resume {
        Some(op) => {
            let stored = replay::scope_of(read_conn, op)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("no replay operation {op}"))?;
            let done: BTreeSet<Uuid> = replay::sequences_done(read_conn, op)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .collect();
            (op, Scope::from_json(&stored)?, done)
        }
        None => (OperationId::new(), o.scope, BTreeSet::new()),
    };
    let in_scope: BTreeSet<Uuid> = replay::select(
        read_conn,
        &scope.materials,
        scope.binding.as_deref(),
        scope.event_types.as_deref(),
        scope.from.map(|t| t.0),
        scope.until.map(|t| t.0),
    )
    .await
    .map_err(|e| e.to_string())?
    .into_iter()
    .collect();
    let in_scope_list: Vec<Uuid> = in_scope.iter().copied().collect();
    let inputs = replay::inputs_of(read_conn, &in_scope_list)
        .await
        .map_err(|e| e.to_string())?;
    let stored = replay::materials_of(read_conn, &inputs, &in_scope_list)
        .await
        .map_err(|e| e.to_string())?;

    let mut report = Report {
        operation,
        ..Report::default()
    };
    for sequence in sequences(&stored) {
        let last = match sequence.iter().rposition(|m| in_scope.contains(&m.id)) {
            Some(i) => i,
            None => continue,
        };
        let root = sequence[0].id;
        if done.contains(&root) {
            report.sequences_skipped += 1;
            continue;
        }
        let sequence: Vec<Stored> = sequence[..=last].iter().map(|m| (*m).clone()).collect();
        let parsers = sources_of(read_conn, &sequence).await?;
        let mut keys = HashMap::new();
        for material in &sequence {
            if material.manifest.role == sinex_model::MaterialRole::OriginalArchive {
                continue;
            }
            if blobs.is_sealed(&material.blob).map_err(|e| e.to_string())? {
                let unlock = o.unlock.as_ref().ok_or_else(|| {
                    format!(
                        "material {}: sealed replay needs an operator unlock and --sealed",
                        material.id
                    )
                })?;
                let key = unlock.data_key(read_conn, MaterialId(material.id)).await?;
                keys.insert(material.id, key);
            }
        }
        let unlock = o.unlock.clone();
        // The sequence is read on a blocking thread while the writer admits its
        // batches as they come, a few at a time.
        let (sender, batches) = tokio::sync::mpsc::channel(READ_AHEAD);
        let reading = {
            let (blobs, in_scope, scope) = (blobs.clone(), in_scope.clone(), scope.clone());
            tokio::task::spawn_blocking(move || {
                read_sequence(
                    &blobs,
                    &sequence,
                    &parsers,
                    &in_scope,
                    &scope,
                    operation,
                    &sender,
                    &keys,
                    unlock.as_ref(),
                )
            })
        };
        let replayed = writer
            .replay(ReplaySequence {
                operation,
                actor: o.actor.clone(),
                scope: {
                    let mut value = scope.to_json();
                    value["sealed"] = Value::Bool(o.unlock.is_some());
                    value
                },
                root,
                compare: o.compare,
                batches,
            })
            .await;
        let read = reading.await.map_err(|e| e.to_string())?;
        let (outcomes, diff) =
            replayed.map_err(|e| format!("sequence from material {root}: {e}"))?;
        report.materials += read.materials;
        report.events_read += read.events_read;
        report.failures += read.failures;
        report.sequences += 1;
        for out in &outcomes {
            report.archived += out.replay_archived;
            report.admitted += out.admitted;
        }
        merge(&mut report.diff, diff);
    }
    Ok(report)
}

/// Group stored materials into sequences, each first part to last.
/// Each material's parser, built with the settings its binding was read with
/// (stored by backfill and the binding runner). A material with no stored
/// binding (sent by an outside producer) is read by the source whose parser
/// it names.
async fn sources_of(
    read_conn: &mut sqlx::PgConnection,
    sequence: &[Stored],
) -> Result<Vec<Box<dyn Source>>, String> {
    let mut out = Vec::with_capacity(sequence.len());
    let mut bound: HashMap<String, sinex_sources::Binding> = HashMap::new();
    for m in sequence {
        let stored_binding = match &m.binding {
            Some(name) if !bound.contains_key(name) => {
                let found = bindings::load(read_conn, name)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(s) = found {
                    let b = sinex_sources::Binding {
                        name: s.binding,
                        source: s.source,
                        parser: m.parser.clone(),
                        input: s.input.into(),
                        settings: serde_json::from_value(s.settings)
                            .map_err(|e| format!("binding {name}: settings: {e}"))?,
                    };
                    bound.insert(name.clone(), b);
                }
                bound.get(name).cloned()
            }
            Some(name) => bound.get(name).cloned(),
            None => None,
        };
        // The material names the parser it was read with.
        let binding = match stored_binding {
            Some(b) => sinex_sources::Binding {
                parser: m.parser.clone(),
                ..b
            },
            None => sources::binding_for_parser(&m.parser, m.host.as_deref().unwrap_or_default())
                .ok_or_else(|| {
                format!("material {}: no source has parser {}", m.id, m.parser)
            })?,
        };
        out.push(sources::source_for(&binding).map_err(|e| format!("material {}: {e}", m.id))?);
    }
    Ok(out)
}

fn sequences(stored: &[Stored]) -> Vec<Vec<&Stored>> {
    let by_id: HashMap<Uuid, &Stored> = stored.iter().map(|m| (m.id, m)).collect();
    let mut next: HashMap<Uuid, Vec<&Stored>> = HashMap::new();
    let mut roots = Vec::new();
    for m in stored {
        match m.previous.filter(|p| by_id.contains_key(p)) {
            Some(p) => next.entry(p).or_default().push(m),
            None => roots.push(m),
        }
    }
    roots
        .into_iter()
        .map(|root| {
            let mut sequence = vec![root];
            let mut at = root.id;
            while let Some(after) = next.get(&at) {
                // Parts form a line; were there ever two successors, the
                // earlier one continues the sequence and the other starts its own.
                let mut after: Vec<&Stored> = after.clone();
                after.sort_by_key(|m| (m.part_index, m.id));
                sequence.push(after[0]);
                at = after[0].id;
            }
            sequence
        })
        .collect()
}

/// Batches of one sequence read ahead of the writer.
const READ_AHEAD: usize = 4;

/// What reading one sequence counted.
#[derive(Debug, Default)]
struct Read {
    materials: usize,
    events_read: usize,
    failures: usize,
}

/// Drain every material of a sequence in order, carrying parser state, and send
/// the batches that replace the in-scope materials' readings to the writer as
/// they are built. The first batch carries the request to archive what is
/// re-read. A material that cannot be read is sent as an error, which rolls
/// the sequence back.
#[allow(clippy::too_many_arguments)]
fn read_sequence(
    blobs: &sinex_store::Blobs,
    sequence: &[Stored],
    parsers: &[Box<dyn Source>],
    in_scope: &BTreeSet<Uuid>,
    scope: &Scope,
    operation: OperationId,
    out: &tokio::sync::mpsc::Sender<Result<Batch, String>>,
    keys: &HashMap<Uuid, sinex_store::sealed::DataKey>,
    unlock: Option<&crate::sealed_access::Unlock>,
) -> Read {
    let mut read = Read::default();
    if let Err(error) = read_sequence_into(
        blobs, sequence, parsers, in_scope, scope, operation, out, &mut read, keys, unlock,
    ) {
        // The writer may already have stopped reading; nothing else to tell.
        let _ = out.blocking_send(Err(error));
    }
    read
}

#[allow(clippy::too_many_arguments)]
fn read_sequence_into(
    blobs: &sinex_store::Blobs,
    sequence: &[Stored],
    parsers: &[Box<dyn Source>],
    in_scope: &BTreeSet<Uuid>,
    scope: &Scope,
    operation: OperationId,
    out: &tokio::sync::mpsc::Sender<Result<Batch, String>>,
    report: &mut Read,
    keys: &HashMap<Uuid, sinex_store::sealed::DataKey>,
    unlock: Option<&crate::sealed_access::Unlock>,
) -> Result<(), String> {
    let send = |batch: Batch| {
        if let Some(unlock) = unlock {
            unlock.check()?;
        }
        out.blocking_send(Ok(batch))
            .map_err(|_| "the writer stopped reading the sequence".to_owned())
    };
    let whole = scope.event_types.is_none() && scope.from.is_none() && scope.until.is_none();
    let narrowing = ReplayScope {
        materials: Vec::new(),
        event_types: scope.event_types.clone(),
        from: scope.from,
        until: scope.until,
    };
    send(Batch {
        run: RunId::new(),
        kind: BatchKind::Replay { operation },
        records: Vec::new(),
        archive: vec![ArchiveRequest::Replay {
            scope: ReplayScope {
                materials: sequence
                    .iter()
                    .filter(|m| in_scope.contains(&m.id))
                    .map(|m| MaterialId(m.id))
                    .collect(),
                ..narrowing.clone()
            },
            operation,
        }],
    })?;
    let mut carry = Value::Null;
    for (position, (m, source)) in sequence.iter().zip(parsers).enumerate() {
        // A later part read again what the reader had not admitted of this one.
        let admitted = sequence
            .get(position + 1)
            .filter(|next| next.previous == Some(m.id))
            .and_then(|next| next.manifest.previous_through);
        if m.manifest.role == sinex_model::MaterialRole::OriginalArchive {
            continue;
        }
        let key = keys.get(&m.id);
        let reference = sinex_model::MaterialRef {
            id: MaterialId(m.id),
            input: m.input.map(sinex_model::InputId),
            blob: m.blob.clone(),
            length: u64::try_from(m.length)
                .map_err(|_| format!("material {}: negative length", m.id))?,
            part_index: u32::try_from(m.part_index)
                .map_err(|_| format!("material {}: negative part", m.id))?,
            previous_part: m.previous.map(MaterialId),
            parser: m.parser.clone(),
            parser_version: u32::try_from(m.parser_version)
                .map_err(|_| format!("material {}: negative parser version", m.id))?,
            manifest: m.manifest.clone(),
        };
        let sender = out.clone();
        let stored = crate::stored::open_checked(
            blobs,
            reference,
            key.cloned(),
            unlock,
            std::sync::Arc::new(move || {
                if sender.is_closed() {
                    Err("the writer stopped reading the sequence".to_owned())
                } else {
                    Ok(())
                }
            }),
        )?
        .body;
        let mut records = stored.records(&m.manifest, source.spec().framer)?;
        if let Some(admitted) = admitted {
            records.retain(|&(_, end)| end as u64 <= admitted);
        }
        let order = drivers::reading_order(&m.manifest);
        // Chunks are drained one at a time, so a material's events are never
        // all in memory at once. A material out of scope is still read for
        // the carry it passes on.
        let chunks = source
            .drainer(DrainInput {
                source: source.spec().name,
                class: source.spec().class,
                material: MaterialId(m.id),
                bytes: sinex_sources::drain::RecordBytes::Reader(&*stored),
                records: &records,
                carry: carry.clone(),
                reading_order: &*order,
                limits: Limits::default(),
                manifest: &m.manifest,
            })
            .map_err(|e| format!("material {}: {e}", m.id))?;
        let scoped = in_scope.contains(&m.id);
        if scoped {
            report.materials += 1;
        }
        for chunk in chunks {
            let chunk = chunk.map_err(|e| format!("material {}: {e}", m.id))?;
            carry = chunk.carry.clone();
            if !scoped {
                continue;
            }
            let mut records: Vec<RecordOutcome> = chunk
                .outcomes
                .into_iter()
                .filter_map(|outcome| match outcome {
                    RecordOutcome::Events { location, events } => {
                        let events: Vec<_> = events
                            .into_iter()
                            .filter(|e| narrowing.includes(e))
                            .collect();
                        report.events_read += events.len();
                        (!events.is_empty()).then_some(RecordOutcome::Events { location, events })
                    }
                    RecordOutcome::Failure { .. } if whole => {
                        report.failures += 1;
                        Some(outcome)
                    }
                    RecordOutcome::Failure { .. } => None,
                })
                .collect();
            if records.is_empty() {
                continue;
            }
            if let Some(key) = key {
                for record in &mut records {
                    match record {
                        RecordOutcome::Events { events, .. } => {
                            for event in events {
                                sinex_store::sealed::seal_event(event, key)
                                    .map_err(|e| e.to_string())?;
                            }
                        }
                        RecordOutcome::Failure { reason, .. } => {
                            *reason =
                                sinex_store::sealed::seal_value(&Value::String(reason.clone()), key)
                                    .map_err(|e| e.to_string())?
                                    .to_string()
                        }
                    }
                }
            }
            send(Batch {
                run: RunId::new(),
                kind: BatchKind::Replay { operation },
                records,
                archive: Vec::new(),
            })?;
        }
    }
    Ok(())
}

fn merge(into: &mut Diff, from: Diff) {
    into.unchanged += from.unchanged;
    into.changed.extend(from.changed);
    into.added.extend(from.added);
    into.removed.extend(from.removed);
}

/// The changes, for printing: every one when `all`, otherwise the first few
/// of each kind and how many more there are.
pub(crate) fn describe(diff: &Diff, all: bool) -> String {
    let mut out = String::new();
    let shown = if all { usize::MAX } else { 10 };
    for (label, changes) in [
        ("changed", &diff.changed),
        ("added", &diff.added),
        ("removed", &diff.removed),
    ] {
        for c in changes.iter().take(shown) {
            out.push_str(&line(label, c));
        }
        if changes.len() > shown {
            out.push_str(&format!(
                "{label}: {} more (--all shows every one)\n",
                changes.len() - shown
            ));
        }
    }
    out
}

fn line(label: &str, c: &Change) -> String {
    let show = |v: &Option<Value>| v.as_ref().map_or("-".into(), Value::to_string);
    format!(
        "{label} {} {}: {} -> {}\n",
        c.event_type,
        c.record,
        show(&c.before),
        show(&c.after)
    )
}

#[cfg(test)]
mod tests;
