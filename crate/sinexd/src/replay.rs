//! Replay (architecture §5.5): read stored materials again with the current
//! parsers and rules. The scope is materials (named, a binding's, or those
//! with readings of some types in a time window), narrowed by type and time.
//!
//! Each material chain (an input's parts, linked first to last) is replayed
//! from its first part, because a parser's carried state flows from part to
//! part; only in-scope materials' readings are archived and admitted. A chain
//! is one transaction: its live in-scope readings (and their live derived
//! descendants) are archived and the new readings admitted together, so a
//! failure changes nothing and a crash leaves the chain as it was. Finished
//! chains are journaled under the operation, and `--resume` skips them.
//!
//! `--compare` runs exactly the same transactions and rolls each back after
//! reading what it would change.
use crate::writer::{ReplayChain, WriterHandle};
use serde_json::{Value, json};
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, MaterialId, OperationId, RecordOutcome, ReplayScope, RunId,
    Timestamp,
};
use sinex_sources::{DrainInput, Limits, drivers, sources};
use sinex_store::Blobs;
use sinex_store::replay::{self, Change, Diff, Stored};
use sqlx::PgConnection;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

#[derive(Clone, Debug, Default)]
pub struct Scope {
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
pub struct Report {
    pub operation: OperationId,
    pub chains: usize,
    pub chains_skipped: usize,
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
            "operation {} | chains {} (skipped as done: {}) | materials re-read {} | readings {} | failures {} | archived {} | admitted {}",
            self.operation,
            self.chains,
            self.chains_skipped,
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
    pub fn to_json(&self, compare: bool, all: bool) -> Value {
        json!({
            "operation": self.operation,
            "compare_only": compare,
            "chains": self.chains,
            "chains_skipped": self.chains_skipped,
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

pub struct Options {
    pub scope: Scope,
    pub compare: bool,
    pub resume: Option<OperationId>,
    pub actor: String,
}

/// Replay with its parts: a read connection, the content store and the writer
/// (the daemon's API and a local `sinexd admin` run hold these).
pub async fn run(
    aux: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    o: Options,
) -> Result<Report, String> {
    let (operation, scope, done) = match o.resume {
        Some(op) => {
            let stored = replay::scope_of(aux, op)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("no replay operation {op}"))?;
            let done: BTreeSet<Uuid> = replay::chains_done(aux, op)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .collect();
            (op, Scope::from_json(&stored)?, done)
        }
        None => (OperationId::new(), o.scope, BTreeSet::new()),
    };
    let in_scope: BTreeSet<Uuid> = replay::select(
        aux,
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
    let inputs = replay::inputs_of(aux, &in_scope_list)
        .await
        .map_err(|e| e.to_string())?;
    let stored = replay::chains(aux, &inputs, &in_scope_list)
        .await
        .map_err(|e| e.to_string())?;

    let mut report = Report {
        operation,
        ..Report::default()
    };
    for chain in chains(&stored) {
        let last = match chain.iter().rposition(|m| in_scope.contains(&m.id)) {
            Some(i) => i,
            None => continue,
        };
        let root = chain[0].id;
        if done.contains(&root) {
            report.chains_skipped += 1;
            continue;
        }
        let chain: Vec<Stored> = chain[..=last].iter().map(|m| (*m).clone()).collect();
        // The chain is read on a blocking thread while the writer admits its
        // batches as they come, a few at a time.
        let (sender, batches) = tokio::sync::mpsc::channel(READ_AHEAD);
        let reading = {
            let (blobs, in_scope, scope) = (blobs.clone(), in_scope.clone(), scope.clone());
            tokio::task::spawn_blocking(move || {
                read_chain(&blobs, &chain, &in_scope, &scope, operation, &sender)
            })
        };
        let replayed = writer
            .replay(ReplayChain {
                operation,
                actor: o.actor.clone(),
                scope: scope.to_json(),
                root,
                compare: o.compare,
                batches,
            })
            .await;
        let read = reading.await.map_err(|e| e.to_string())?;
        let (outcomes, diff) = replayed.map_err(|e| format!("chain from material {root}: {e}"))?;
        report.materials += read.materials;
        report.events_read += read.events_read;
        report.failures += read.failures;
        report.chains += 1;
        for out in &outcomes {
            report.archived += out.replay_archived;
            report.admitted += out.admitted;
        }
        merge(&mut report.diff, diff);
    }
    Ok(report)
}

/// Group stored materials into chains, each first part to last.
fn chains(stored: &[Stored]) -> Vec<Vec<&Stored>> {
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
            let mut chain = vec![root];
            let mut at = root.id;
            while let Some(after) = next.get(&at) {
                // Parts form a line; were there ever two successors, the
                // earlier one continues the chain and the other starts its own.
                let mut after: Vec<&Stored> = after.clone();
                after.sort_by_key(|m| (m.part_index, m.id));
                chain.push(after[0]);
                at = after[0].id;
            }
            chain
        })
        .collect()
}

/// Batches of one chain read ahead of the writer.
const READ_AHEAD: usize = 4;

/// What reading one chain counted.
#[derive(Debug, Default)]
struct Read {
    materials: usize,
    events_read: usize,
    failures: usize,
}

/// Drain every material of a chain in order, carrying parser state, and send
/// the batches that replace the in-scope materials' readings to the writer as
/// they are built. The first batch carries the request to archive what is
/// re-read. A material that cannot be read is sent as an error, which rolls
/// the chain back.
fn read_chain(
    blobs: &sinex_store::Blobs,
    chain: &[Stored],
    in_scope: &BTreeSet<Uuid>,
    scope: &Scope,
    operation: OperationId,
    out: &tokio::sync::mpsc::Sender<Result<Batch, String>>,
) -> Read {
    let mut read = Read::default();
    if let Err(error) = read_chain_into(blobs, chain, in_scope, scope, operation, out, &mut read) {
        // The writer may already have stopped reading; nothing else to tell.
        let _ = out.blocking_send(Err(error));
    }
    read
}

fn read_chain_into(
    blobs: &sinex_store::Blobs,
    chain: &[Stored],
    in_scope: &BTreeSet<Uuid>,
    scope: &Scope,
    operation: OperationId,
    out: &tokio::sync::mpsc::Sender<Result<Batch, String>>,
    report: &mut Read,
) -> Result<(), String> {
    let send = |batch: Batch| {
        out.blocking_send(Ok(batch))
            .map_err(|_| "the writer stopped reading the chain".to_owned())
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
                materials: chain
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
    for m in chain {
        let source = sources::by_parser(&m.parser, m.host.as_deref().unwrap_or_default())
            .ok_or_else(|| format!("material {}: no source has parser {}", m.id, m.parser))?;
        // A compressed export's records are in its decoded member stream
        // (`ExportArchive`), which replay does not decode yet.
        if let Some(container) = m.manifest.get("container").and_then(Value::as_str)
            && container != "plain"
        {
            return Err(format!(
                "material {}: replaying a {container} export needs its member stream decoded, which replay does not do yet",
                m.id
            ));
        }
        // A sealed material needs the class unlocked to be read; replay does
        // not unlock, so it is deferred and named rather than read blind.
        if blobs.is_sealed(&m.blob).map_err(|e| e.to_string())? {
            return Err(format!(
                "material {}: sealed; replaying it needs the sealed class unlocked, which replay does not do yet (deferred)",
                m.id
            ));
        }
        let bytes = blobs.read(&m.blob).map_err(|e| e.to_string())?;
        let records = drivers::stored_records(&m.manifest, &bytes, source.spec().framer);
        let order = drivers::reading_order(&m.manifest);
        let chunks = source
            .drain(DrainInput {
                source: source.spec().name,
                class: source.spec().class,
                material: MaterialId(m.id),
                bytes: &bytes,
                records: &records,
                carry,
                reading_order: &*order,
                limits: Limits::default(),
                manifest: &m.manifest,
            })
            .map_err(|e| format!("material {}: {e}", m.id))?;
        carry = chunks.last().map_or(Value::Null, |c| c.carry.clone());
        if !in_scope.contains(&m.id) {
            continue;
        }
        report.materials += 1;
        for chunk in chunks {
            let records: Vec<RecordOutcome> = chunk
                .outcomes
                .into_iter()
                .filter_map(|outcome| match outcome {
                    RecordOutcome::Events { location, events } => {
                        let events: Vec<_> =
                            events.into_iter().filter(|e| narrowing.admits(e)).collect();
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
pub fn describe(diff: &Diff, all: bool) -> String {
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
