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
//! sequences are journaled under the operation, and `resume` skips them.
//! The new readings carry the operation's ID as their run, so an undo finds
//! what the replay admitted.
//!
//! `--compare` runs exactly the same transactions and rolls each back after
//! reading what it would change.
use crate::writer::{ReplaySequence, WriterHandle};
use crate::{Error, Result};
use serde_json::{Value, json};
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, MaterialId, OperationId, RecordOutcome, ReplayScope, RunId,
    Timestamp,
};
use sinex_source_core::{DrainInput, Limits, Source, drivers};
use sinex_sources::sources;
use sinex_store::Blobs;
use sinex_store::bindings;
use sinex_store::replay::{self, Change, Diff, Stored};
use sqlx::PgConnection;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;
pub(crate) mod prepared;

pub(crate) fn build_identity() -> Result<Value> {
    static BUILD: std::sync::OnceLock<Result<String, command_surface::Failure>> =
        std::sync::OnceLock::new();
    let build = BUILD
        .get_or_init(|| {
            let read = || -> Result<String> {
                let mut executable = std::fs::File::open("/proc/self/exe")?;
                let mut hash = blake3::Hasher::new();
                std::io::copy(&mut executable, &mut hash)?;
                Ok(hash.finalize().to_hex().to_string())
            };
            read().map_err(Error::failure)
        })
        .clone()?;
    let mut declarations: Vec<_> = sinex_model::event_types::declarations()
        .iter()
        .map(|d| (d.kind.clone(), d.definition_hash()))
        .collect();
    declarations.sort();
    Ok(json!({"build": build, "declarations": declarations}))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Scope {
    pub materials: Vec<Uuid>,
    pub binding: Option<String>,
    pub event_types: Option<Vec<String>>,
    pub from: Option<Timestamp>,
    pub until: Option<Timestamp>,
    /// Readings per admission batch; the drain's default when unset.
    pub batch_events: Option<u64>,
}

impl Scope {
    fn to_json(&self) -> Value {
        json!({
            "materials": self.materials,
            "binding": self.binding,
            "event_types": self.event_types,
            "from": self.from,
            "until": self.until,
            "batch_events": self.batch_events,
        })
    }

    fn from_json(v: &Value) -> Result<Self> {
        let bad = |what: &str| Error::internal(format!("stored replay scope: bad {what}"));
        Ok(Self {
            materials: serde_json::from_value(v["materials"].clone())
                .map_err(|_| bad("materials"))?,
            binding: serde_json::from_value(v["binding"].clone()).map_err(|_| bad("binding"))?,
            event_types: serde_json::from_value(v["event_types"].clone())
                .map_err(|_| bad("event_types"))?,
            from: serde_json::from_value(v["from"].clone()).map_err(|_| bad("from"))?,
            until: serde_json::from_value(v["until"].clone()).map_err(|_| bad("until"))?,
            batch_events: serde_json::from_value(v["batch_events"].clone())
                .map_err(|_| bad("batch_events"))?,
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
    pub refused: Vec<Value>,
}

impl std::fmt::Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "operation {} | sequences {} (skipped as done: {}) | materials re-read {} | readings {} | failures {} | archived {} | admitted {} | refused materials {}",
            self.operation,
            self.sequences,
            self.sequences_skipped,
            self.materials,
            self.events_read,
            self.failures,
            self.archived,
            self.admitted,
            self.refused.len()
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
            "refused": self.refused,
            "partial": !self.refused.is_empty(),
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
    pub unlock: Option<crate::material_access::sealing::Unlock>,
}

/// Replay with its parts: a read connection, the content store and the writer
/// (the daemon's API and a local `sinexd admin` run hold these).
pub(crate) async fn run(
    read_conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    o: Options,
) -> Result<Report> {
    let (operation, scope, done, prior_scope) = match o.resume {
        Some(op) => {
            let stored = replay::scope_of(read_conn, op)
                .await?
                .ok_or_else(|| Error::bad_request(format!("no replay operation {op}")))?;
            let done: BTreeSet<Uuid> = replay::sequences_done(read_conn, op)
                .await?
                .into_iter()
                .collect();
            (op, Scope::from_json(&stored)?, done, Some(stored))
        }
        None => (OperationId::new(), o.scope, BTreeSet::new(), None),
    };
    let in_scope: BTreeSet<Uuid> = match &prior_scope {
        Some(saved) => serde_json::from_value(saved["selected_materials"].clone())
            .map_err(|e| Error::internal(format!("stored replay material selection: {e}")))?,
        None => replay::select(
            read_conn,
            &scope.materials,
            scope.binding.as_deref(),
            scope.event_types.as_deref(),
            scope.from.map(|t| t.0),
            scope.until.map(|t| t.0),
        )
        .await?
        .into_iter()
        .collect(),
    };
    let in_scope_list: Vec<Uuid> = in_scope.iter().copied().collect();
    let inputs = replay::inputs_of(read_conn, &in_scope_list).await?;
    let stored = replay::materials_of(read_conn, &inputs, &in_scope_list).await?;
    // An archived material is not replayed until it is restored, and a
    // tombstoned one never is.
    if let Some(m) = stored
        .iter()
        .find(|m| in_scope.contains(&m.id) && m.status != "live")
    {
        return Err(Error::bad_request(format!(
            "material {} is {}; {}",
            m.id,
            m.status,
            if m.status == "archived" {
                "restore it before replaying it"
            } else {
                "it is never read again"
            }
        )));
    }
    // A material stored with no parser has nothing to read it with.
    if let Some(m) = stored
        .iter()
        .find(|m| in_scope.contains(&m.id) && m.parser.is_none())
    {
        return Err(Error::refused(m.parsed_with().unwrap_err()));
    }
    // A tombstoned part's bytes may be gone; the parts after it read on
    // from a fresh carry.
    let stored: Vec<Stored> = stored
        .into_iter()
        .filter(|m| m.status != "tombstoned")
        .collect();

    let saved_scope = match &prior_scope {
        Some(saved) => saved.clone(),
        None => {
            let mut saved = scope.to_json();
            saved["identity"] = build_identity()?;
            saved["sealed"] = Value::Bool(o.unlock.is_some());
            saved["selected_materials"] = json!(in_scope_list);
            saved["parsers"] = json!(
                stored
                    .iter()
                    .map(|m| (&m.parser, m.parser_version))
                    .collect::<BTreeSet<_>>()
            );
            saved
        }
    };
    if !o.compare && prior_scope.is_none() {
        writer
            .record_replay(operation, o.actor.clone(), saved_scope.clone(), vec![])
            .await?;
    }
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
        if saved_scope["identity"] != build_identity()? {
            return Err(Error::conflict(
                "replay_build_changed: running build or declarations differ",
            ));
        }
        let sequence: Vec<Stored> = sequence[..=last].iter().map(|m| (*m).clone()).collect();
        let parsers = match sources_of(read_conn, &sequence).await {
            Ok(parsers) => parsers,
            Err(reason) => {
                let refusals: Vec<_> = sequence.iter().filter(|m| in_scope.contains(&m.id)).map(|m| {
                    json!({"material":m.id, "parser":m.parser, "parser_version":m.parser_version,
                        "reason": if m.binding.as_deref().is_some_and(|b| b.starts_with("ingress:")) { "producer_required" } else { "parser_unavailable" },
                        "detail":reason.to_string()})
                }).collect();
                if !o.compare {
                    writer
                        .record_replay(
                            operation,
                            o.actor.clone(),
                            saved_scope.clone(),
                            refusals.clone(),
                        )
                        .await?;
                }
                report.refused.extend(refusals);
                continue;
            }
        };
        let keys = crate::material_access::sealing::replay_keys(
            read_conn,
            blobs,
            sequence
                .iter()
                .filter(|material| {
                    material.manifest.role != sinex_model::MaterialRole::OriginalArchive
                })
                .map(|material| (material.id, material.blob.clone()))
                .collect(),
            o.unlock.as_ref(),
        )
        .await?;
        let (read, outcomes, diff) = SequenceReader {
            blobs: blobs.clone(),
            sequence,
            parsers,
            in_scope: in_scope.clone(),
            scope: scope.clone(),
            operation,
            keys,
            unlock: o.unlock.clone(),
        }
        .replace(writer, o.actor.clone(), saved_scope.clone(), o.compare)
        .await
        .map_err(|e| e.within(format!("sequence from material {root}")))?;
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

/// Daemon replay uses the saved local binding or its named parser. An `ingress:`
/// binding belongs to its producer, which supplies replacement readings through
/// ReplayScopeQuery and ReplaySequence.
struct NativeReplaySource {
    source: Box<dyn Source>,
    host: Option<String>,
}

async fn sources_of(
    read_conn: &mut sqlx::PgConnection,
    sequence: &[Stored],
) -> Result<Vec<NativeReplaySource>> {
    if sequence.iter().any(|m| {
        m.binding
            .as_deref()
            .is_some_and(|b| b.starts_with("ingress:"))
    }) {
        return Err(Error::bad_request("producer_required"));
    }
    let mut out = Vec::with_capacity(sequence.len());
    let mut bound: HashMap<String, sinex_source_core::Binding> = HashMap::new();
    for m in sequence {
        let stored_binding = match &m.binding {
            Some(name) if !bound.contains_key(name) => {
                let found = bindings::load(read_conn, name).await?;
                if let Some(s) = found {
                    let b = sinex_source_core::Binding {
                        name: s.binding,
                        source: s.source,
                        parser: m.parsed_with().map_err(Error::refused)?.0.to_owned(),
                        input: s.input.into(),
                        settings: serde_json::from_value(s.settings).map_err(|e| {
                            Error::bad_request(format!("binding {name}: settings: {e}"))
                        })?,
                    };
                    bound.insert(name.clone(), b);
                }
                bound.get(name).cloned()
            }
            Some(name) => bound.get(name).cloned(),
            None => None,
        };
        // The material names the parser it was read with.
        let host = stored_binding
            .as_ref()
            .map(|binding| binding.settings.host.clone());
        let (parser, _) = m.parsed_with().map_err(Error::refused)?;
        let binding = match stored_binding {
            Some(b) => sinex_source_core::Binding {
                parser: parser.to_owned(),
                ..b
            },
            None => sources::binding_for_parser(parser, m.host.as_deref().unwrap_or_default())
                .ok_or_else(|| {
                    Error::bad_request(format!("material {}: no source has parser {parser}", m.id))
                })?,
        };
        out.push(NativeReplaySource {
            source: sources::source_for(&binding)
                .map_err(|e| Error::bad_request(format!("material {}: {e}", m.id)))?,
            host,
        });
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

/// What reading one sequence counted.
#[derive(Debug, Default)]
struct Read {
    materials: usize,
    events_read: usize,
    failures: usize,
}

struct SequenceReader {
    blobs: Blobs,
    sequence: Vec<Stored>,
    parsers: Vec<NativeReplaySource>,
    in_scope: BTreeSet<Uuid>,
    scope: Scope,
    operation: OperationId,
    keys: HashMap<Uuid, sinex_sealing::DataKey>,
    unlock: Option<crate::material_access::sealing::Unlock>,
}

impl SequenceReader {
    async fn replace(
        self,
        writer: &WriterHandle,
        actor: String,
        scope: Value,
        compare: bool,
    ) -> Result<(Read, Vec<sinex_store::Outcome>, Diff)> {
        let root = self.sequence[0].id;
        let operation = self.operation;
        let unlock = self.unlock.clone();
        // Dropping the request closes this receiver, including while the
        // blocking parser is waiting on stored input. Its reader checks it at
        // each read and each prepared batch.
        let (caller, pending) = tokio::sync::oneshot::channel::<()>();
        let reading = tokio::task::spawn_blocking(move || -> Result<_> {
            let check: crate::writer::AdmissionCheck = std::sync::Arc::new(move || {
                if caller.is_closed() {
                    Err(Error::refused("replay preparation was cancelled"))
                } else {
                    Ok(())
                }
            });
            let mut prepared = prepared::Builder::new(self.blobs.root())?;
            let mut read = Read::default();
            read_sequence_into(
                &self.blobs,
                &self.sequence,
                &self.parsers,
                &self.in_scope,
                &self.scope,
                operation,
                &mut |batch| prepared.push(batch),
                &mut read,
                &self.keys,
                self.unlock.as_ref(),
                &check,
            )?;
            Ok((prepared.finish()?, read))
        });
        let (batches, read) = reading.await??;
        drop(pending);
        let (outcomes, diff) = writer
            .replay(ReplaySequence {
                operation,
                actor,
                scope,
                root,
                compare,
                batches,
                ingress: None,
                check: std::sync::Arc::new(move || {
                    if let Some(unlock) = &unlock {
                        unlock.check()?;
                    }
                    Ok(())
                }),
            })
            .await?;
        Ok((read, outcomes, diff))
    }
}

/// Drain and prepare every material in order, carrying parser state. A read
/// failure discards preparation before any replacement reaches admission.
#[allow(clippy::too_many_arguments)]
fn read_sequence_into(
    blobs: &sinex_store::Blobs,
    sequence: &[Stored],
    parsers: &[NativeReplaySource],
    in_scope: &BTreeSet<Uuid>,
    scope: &Scope,
    operation: OperationId,
    out: &mut impl FnMut(Batch) -> Result<()>,
    report: &mut Read,
    keys: &HashMap<Uuid, sinex_sealing::DataKey>,
    unlock: Option<&crate::material_access::sealing::Unlock>,
    check: &crate::writer::AdmissionCheck,
) -> Result<()> {
    let mut send = |batch: Batch| {
        check()?;
        if let Some(unlock) = unlock {
            unlock.check()?;
        }
        out(batch)
    };
    let whole = scope.event_types.is_none() && scope.from.is_none() && scope.until.is_none();
    let narrowing = ReplayScope {
        materials: Vec::new(),
        event_types: scope.event_types.clone(),
        from: scope.from,
        until: scope.until,
    };
    send(Batch {
        run: RunId(operation.0),
        kind: BatchKind::Replay {
            operation,
            source: None,
            host: None,
        },
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
    for (position, (m, native)) in sequence.iter().zip(parsers).enumerate() {
        let source = &native.source;
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
                .map_err(|_| Error::internal(format!("material {}: negative length", m.id)))?,
            part_index: u32::try_from(m.part_index)
                .map_err(|_| Error::internal(format!("material {}: negative part", m.id)))?,
            previous_part: m.previous.map(MaterialId),
            parser: m.parsed_with().map_err(Error::refused)?.0.to_owned(),
            parser_version: u32::try_from(m.parsed_with().map_err(Error::refused)?.1).map_err(
                |_| Error::internal(format!("material {}: negative parser version", m.id)),
            )?,
            manifest: m.manifest.clone(),
        };
        let stored = crate::material_access::open_checked(
            blobs,
            reference,
            key.cloned(),
            unlock,
            check.clone(),
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
                bytes: sinex_source_core::drain::RecordBytes::Reader(&*stored),
                records: &records,
                carry: carry.clone(),
                reading_order: &*order,
                limits: match scope.batch_events {
                    Some(n) => Limits {
                        max_events: usize::try_from(n).unwrap_or(usize::MAX),
                        ..Limits::default()
                    },
                    None => Limits::default(),
                },
                manifest: &m.manifest,
            })
            .map_err(|e| Error::internal(format!("material {}: {e}", m.id)))?;
        let scoped = in_scope.contains(&m.id);
        if scoped {
            report.materials += 1;
        }
        for chunk in chunks {
            let chunk = chunk.map_err(|e| Error::internal(format!("material {}: {e}", m.id)))?;
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
            crate::material_access::sealing::seal_records(&mut records, key)?;
            send(Batch {
                run: RunId(operation.0),
                kind: BatchKind::Replay {
                    operation,
                    source: Some(sinex_model::SourceRef {
                        name: source.spec().name.into(),
                        version: source.spec().parser_version,
                    }),
                    host: native.host.clone(),
                },
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
