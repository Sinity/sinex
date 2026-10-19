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
use crate::context::Context;
use crate::writer::ReplayChain;
use serde_json::{Value, json};
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, MaterialId, OperationId, RecordOutcome, ReplayScope, RunId,
    Timestamp,
};
use sinex_sources::{DrainInput, Limits, drivers, sources};
use sinex_store::replay::{self, Change, Diff, Stored};
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

pub struct Options {
    pub scope: Scope,
    pub compare: bool,
    pub resume: Option<OperationId>,
    pub actor: String,
}

pub async fn run(ctx: &mut Context, o: Options) -> Result<Report, String> {
    let (operation, scope, done) = match o.resume {
        Some(op) => {
            let stored = replay::scope_of(&mut ctx.aux, op)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("no replay operation {op}"))?;
            let done: BTreeSet<Uuid> = replay::chains_done(&mut ctx.aux, op)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .collect();
            (op, Scope::from_json(&stored)?, done)
        }
        None => (OperationId::new(), o.scope, BTreeSet::new()),
    };
    let in_scope: BTreeSet<Uuid> = replay::select(
        &mut ctx.aux,
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
    let inputs = replay::inputs_of(&mut ctx.aux, &in_scope_list)
        .await
        .map_err(|e| e.to_string())?;
    let stored = replay::chains(&mut ctx.aux, &inputs, &in_scope_list)
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
        let chain = &chain[..=last];
        let batches = read_chain(ctx, chain, &in_scope, &scope, operation, &mut report)?;
        let (outcomes, diff) = ctx
            .writer
            .replay(ReplayChain {
                operation,
                actor: o.actor.clone(),
                scope: scope.to_json(),
                root,
                compare: o.compare,
                batches,
            })
            .await
            .map_err(|e| format!("chain from material {root}: {e}"))?;
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

/// Drain every material of a chain in order, carrying parser state, and
/// build the batches that replace the in-scope materials' readings. The
/// first batch carries the request to archive what is re-read.
fn read_chain(
    ctx: &Context,
    chain: &[&Stored],
    in_scope: &BTreeSet<Uuid>,
    scope: &Scope,
    operation: OperationId,
    report: &mut Report,
) -> Result<Vec<Batch>, String> {
    let whole = scope.event_types.is_none() && scope.from.is_none() && scope.until.is_none();
    let narrowing = ReplayScope {
        materials: Vec::new(),
        event_types: scope.event_types.clone(),
        from: scope.from,
        until: scope.until,
    };
    let mut batches = vec![Batch {
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
    }];
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
        if ctx.blobs.is_sealed(&m.blob).map_err(|e| e.to_string())? {
            return Err(format!(
                "material {}: sealed; replaying it needs the sealed class unlocked, which replay does not do yet (deferred)",
                m.id
            ));
        }
        let bytes = ctx.blobs.read(&m.blob).map_err(|e| e.to_string())?;
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
            batches.push(Batch {
                run: RunId::new(),
                kind: BatchKind::Replay { operation },
                records,
                archive: Vec::new(),
            });
        }
    }
    Ok(batches)
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
