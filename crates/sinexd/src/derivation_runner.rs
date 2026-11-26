//! The derivation runner. It runs outside the writer task: it reads with its
//! own connection, computes, and hands the writer one `Derived` batch per key
//! (or per run of committed events), which admission stores under the same
//! privacy, schema and identity rules as captured events.
//!
//! - **Keyed derivations:** admission marks keys dirty; the runner takes dirty
//!   keys oldest first, reads each key's current inputs, computes, and submits
//!   the outputs as the key's complete output set. The dirty mark clears in
//!   the same transaction unless the key was dirtied again meanwhile. Keys whose
//!   outputs wait on records outside their window are recomputed whenever
//!   their derivation runs again.
//! - **Per-event derivations:** read committed events after their position, in
//!   commit order, and commit the new position with their outputs. One that
//!   reads stored bytes (screen text from a screen still) gets the bytes of
//!   the material its input cites; an input that fails is counted and passed.
//!
//! Derivations run in the order their inputs and outputs give: a derivation
//! whose inputs another one produces runs after it, so a day's figures are
//! computed once the commands they count have been fused, not again for every
//! intermediate state. A pass works on the lowest rank that has work; one pass
//! in [`EVERY_LEVEL_EVERY`] works every rank, so a busy upstream cannot keep a
//! downstream derivation from running.
//!
//! The runner wakes on `wake`, which the daemon signals after every commit, and
//! also looks for work every so often, for keys that become final with time.
use crate::writer::{Priority, WriteError, WriterHandle};
use crate::{Error, Result};
use serde_json::Value;
use sinex_derivations::{AnyKeyed, AnyPerEvent, Env, Input, InputQuery, KeyView, Neighbour, Reads};
use sinex_model::time_settings::TimeSettings;
use sinex_model::{Batch, BatchKind, Event, ReadingOrder, RecordOutcome, RunId, Timestamp};
use sinex_store::blobs::Blobs;
use sinex_store::derivations::{self, Nearest, Window};
use sinex_store::registry::{self, Derivation};
use sinex_store::{
    ComputedKey, DerivedScope, ReadMark, RefusedInput, RefusedWork, material_bytes, runs,
};
use sqlx::{Connection, PgConnection};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Dirty keys taken per pass.
const DIRTY_PER_PASS: i64 = 512;
/// Committed events a per-event derivation reads per pass.
const EVENTS_PER_PASS: i64 = 2_000;
/// Keys computed per writer batch: one transaction for many keys, so a
/// derivation's write cost is per batch, not per key.
const KEYS_PER_BATCH: usize = 256;
/// Target outputs per writer batch. One indivisible key can exceed it.
const OUTPUTS_PER_BATCH: usize = 20_000;
/// The most previous outputs a batch of keys archives in its own
/// transaction; more are archived in transactions before it.
const ARCHIVED_PER_BATCH: usize = 4096;
/// Outputs in the first transaction of an archive that takes several; later
/// transactions follow how long the last one held the writer.
const FIRST_ARCHIVED: usize = 256;
/// Payload bytes per writer batch: outputs are held in memory until their
/// batch is admitted, so large outputs (document text) end a batch early.
const OUTPUT_BYTES_PER_BATCH: usize = 32 << 20;
/// Input rows read per query: keys are computed as their rows complete, so
/// memory holds a page and the keys still open, not every key's inputs.
const ROWS_PER_PAGE: i64 = 10_000;
/// A key and the dirty mark it was computed for (0 when only refreshed).
type KeyWork = (Value, i64);

/// Keys marked dirty per recompute request.
const KEYS_PER_REQUEST: usize = 5_000;
/// One pass in this many works every rank, not only the lowest with work.
const EVERY_LEVEL_EVERY: u32 = 8;

/// The configured derivations, including their initialized readers. Startup
/// shares these objects with execution and status so reported versions name
/// the readers the daemon actually runs.
#[derive(Clone)]
pub(crate) struct Catalog {
    pub keyed: Vec<Arc<dyn AnyKeyed>>,
    pub per_event: Vec<Arc<dyn AnyPerEvent>>,
}

impl Catalog {
    pub(crate) fn new(configuration: &sinex_derivations::Configuration) -> Result<Self> {
        Ok(Self {
            keyed: crate::analysis::keyed_with(configuration)
                .into_iter()
                .map(Arc::from)
                .collect(),
            per_event: per_event_with(configuration)?
                .into_iter()
                .map(Arc::from)
                .collect(),
        })
    }
}

pub(crate) struct DerivationRunner {
    keyed: Vec<Arc<dyn AnyKeyed>>,
    per_event: Vec<Arc<dyn AnyPerEvent>>,
    /// The content store, for per-event derivations that read stored bytes.
    blobs: Option<Blobs>,
    /// Rank of each keyed and per-event derivation, by name: 0 for those
    /// reading only captured events, one more than the highest rank of any
    /// derivation producing an input.
    ranks: BTreeMap<String, u32>,
    passes: u32,
    /// The time derivations see; the current time unless a test sets it.
    pub(crate) clock: Option<Timestamp>,
    time_settings: TimeSettings,
    read: PgConnection,
    writer: WriterHandle,
    runs: BTreeMap<String, RunId>,
    /// The source revision of the code this runner computes with. Its runs'
    /// manifests and its comparisons record it; a selection does not.
    build: &'static str,
    /// Largest stored reading order before this runner writes per-event outputs.
    previous_orders: Option<BTreeMap<String, i64>>,
    /// Where each per-event derivation's last pass that found no input ended.
    empty_passes: BTreeMap<String, EmptyPass>,
    pub counts: BTreeMap<String, Counts>,
}

/// A per-event pass that found no event of its kinds after `stored` up to the
/// snapshot's `head`. The next pass reads from `head` while the stored
/// position and the kinds are the ones it started from; a version switch that
/// resets the position, or new kinds, make it start from the stored position
/// again.
struct EmptyPass {
    kinds: Vec<String>,
    stored: i64,
    head: i64,
}

pub(crate) fn to_window(q: &InputQuery, now: Timestamp) -> Window {
    let kinds = |kinds: &[sinex_derivations::Kind]| {
        kinds
            .iter()
            .map(|k| derivations::Kind {
                event_type: k.event_type.to_owned(),
                contains: Value::Object(
                    k.equal
                        .iter()
                        .map(|(f, v)| ((*f).to_owned(), v.clone()))
                        .collect(),
                ),
            })
            .collect()
    };
    let nearest = |n: &Neighbour| Nearest {
        kinds: kinds(&n.kinds),
        per: n.per,
        count: i64::from(n.count),
        lifetime: n.lifetime,
    };
    Window {
        unknown_times: q.unknown_times,
        range: q.range.as_ref().map(|range| match range {
            sinex_derivations::InputRange::Connected {
                kinds: selected,
                at,
                end_field,
                gap_ns,
            } => derivations::InputRange::Connected {
                kinds: kinds(selected),
                at: at.0,
                end_field,
                gap_ns: *gap_ns,
            },
            sinex_derivations::InputRange::Extent { kinds: selected } => {
                derivations::InputRange::Extent {
                    kinds: kinds(selected),
                }
            }
            sinex_derivations::InputRange::StateTransitions {
                start,
                end,
                kinds: selected,
                subjects,
                interruptions,
            } => derivations::InputRange::StateTransitions {
                start: start.0,
                end: end.0,
                kinds: kinds(selected),
                subjects,
                interruptions: interruptions.iter().map(nearest).collect(),
            },
            sinex_derivations::InputRange::UntilNext {
                start,
                after,
                kinds: selected,
                field,
            } => derivations::InputRange::UntilNext {
                start: start.0,
                after: after.map(|at| at.0),
                kinds: kinds(selected),
                field,
                now: now.0,
            },
        }),
        kinds: kinds(&q.kinds),
        start: q.window.map(|(start, _)| start.0),
        end: q.window.map(|(_, end)| end.0),
        before: q.before.iter().map(nearest).collect(),
        intervals: q.intervals.clone(),
        after: q.after.iter().map(nearest).collect(),
    }
}

impl DerivationRunner {
    /// Register available producers and start runs, as `build`, without
    /// changing selected versions or progress.
    pub(crate) async fn start(
        mut read: PgConnection,
        writer: WriterHandle,
        keyed: Vec<Arc<dyn AnyKeyed>>,
        per_event: Vec<Arc<dyn AnyPerEvent>>,
        host: &str,
        time_settings: TimeSettings,
        build: &'static str,
    ) -> Result<Self> {
        let catalog: Vec<Derivation> = keyed
            .iter()
            .map(|d| Derivation {
                name: d.name().to_owned(),
                kind: "keyed",
                version: i64::from(d.version_for(&time_settings)),
                inputs: d.inputs_types().iter().map(|s| (*s).to_owned()).collect(),
                outputs: d.output_types().iter().map(|s| (*s).to_owned()).collect(),
            })
            .chain(per_event.iter().map(|d| Derivation {
                name: d.name().to_owned(),
                kind: "per_event",
                version: i64::from(d.version()),
                inputs: d.inputs_types().iter().map(|s| (*s).to_owned()).collect(),
                outputs: d.output_types().iter().map(|s| (*s).to_owned()).collect(),
            }))
            .collect();
        let mut runs = BTreeMap::new();
        for d in &catalog {
            let name = d.name.clone();
            let version = d.version.to_string();
            let emits: Vec<&str> = d.outputs.iter().map(String::as_str).collect();
            let manifest = runs::Manifest {
                name: &name,
                kind: "derivation",
                version: &version,
                build,
                emits: &emits,
            };
            let run = runs::start(&mut read, &manifest, host).await?;
            runs.insert(name.clone(), run);
        }
        let ranks = ranks(&catalog);
        let mut runner = Self {
            keyed,
            per_event,
            blobs: None,
            ranks,
            passes: 0,
            clock: None,
            time_settings,
            read,
            writer,
            runs,
            build,
            previous_orders: None,
            empty_passes: BTreeMap::new(),
            counts: BTreeMap::new(),
        };
        for d in &catalog {
            registry::register_derivation(&mut runner.read, d).await?;
        }
        runner.archive_removed_derivation_outputs().await?;
        runner.finish_switches(None).await?;
        Ok(runner)
    }

    /// Archive, through the lifecycle, the outputs of every registered
    /// derivation the build no longer has, such as the undefined fusion
    /// families, then end its registration. One archive operation per removed
    /// producer, once: a later start finds no registration, so an undo of the
    /// archive stands. A refused or interrupted archive keeps the registration
    /// and is resumed at the next start.
    async fn archive_removed_derivation_outputs(&mut self) -> Result<()> {
        // Every derivation the build has, and any this runner was started
        // with beyond them, is current.
        let mut known = crate::catalog::derivation_names();
        known.extend(self.runs.keys().cloned());
        let removed = registry::removed_derivations(&mut self.read, &known).await?;
        for producer in removed {
            match crate::lifecycle::archive_removed_source(
                &mut self.read,
                &self.writer,
                producer.clone(),
            )
            .await?
            {
                crate::lifecycle::Answer::Done(_) => {
                    registry::forget_derivation(&mut self.read, &producer).await?;
                }
                // A refusal names what blocked it and leaves the operation
                // resumable; the daemon still starts.
                crate::lifecycle::Answer::Refused(reason)
                | crate::lifecycle::Answer::StoppedEarly(reason, _) => {
                    eprintln!("sinexd: archiving a removed derivation's outputs stopped: {reason}");
                }
            }
        }
        Ok(())
    }

    fn env(&self) -> Env {
        Env {
            now: self.clock.unwrap_or_else(Timestamp::now),
            time_settings: self.time_settings.clone(),
            settings: Value::Null,
        }
    }

    /// Every key the inputs of `name` touch, optionally only for inputs whose
    /// `ts_orig` is in `range`. Without a range, also every key the
    /// derivation has outputs under: a version that keys differently
    /// computes an old key to nothing, so its outputs are archived and no
    /// input stays counted under both versions.
    async fn all_keys(
        &mut self,
        name: &str,
        range: Option<(Timestamp, Timestamp)>,
    ) -> Result<Vec<Value>> {
        let env = self.env();
        let d = self
            .keyed
            .iter()
            .find(|d| d.name() == name)
            .ok_or_else(|| Error::internal(format!("no keyed derivation {name}")))?;
        let types: Vec<String> = d.inputs_types().iter().map(|s| (*s).to_owned()).collect();
        let d = d.clone();
        let mut keys: Vec<Value> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        // Inputs of keys read by their stored lists. Events admitted before
        // a key shape declared its list are added here, for every input
        // paged through: a key in the range reads all of its inputs.
        let mut stored: Vec<(Value, uuid::Uuid)> = Vec::new();
        let mut after = 0i64;
        loop {
            let page = derivations::after_seq(
                &mut self.read,
                &types,
                after,
                10_000,
                sinex_store::PAGE_BYTES,
            )
            .await?;
            let Some(last) = page.last() else { break };
            after = last.commit_seq;
            for c in &page {
                let t = c.event.ts_orig.at;
                let view = KeyView {
                    event_type: &c.event.event_type,
                    ts_orig: t,
                    payload: &c.event.payload,
                };
                let in_range =
                    range.is_none_or(|(from, to)| t.is_some_and(|t| t >= from && t < to));
                for k in d.keys_for(&view, &env) {
                    if d.stores_inputs(&k) {
                        stored.push((k.clone(), c.event.id.0));
                    }
                    if in_range && seen.insert(k.to_string()) {
                        keys.push(k);
                    }
                }
            }
            if stored.len() >= KEYS_PER_REQUEST {
                self.store_inputs(name, std::mem::take(&mut stored)).await?;
            }
        }
        self.store_inputs(name, stored).await?;
        if range.is_none() {
            for k in derivations::output_keys(&mut self.read, name).await? {
                if seen.insert(k.to_string()) {
                    keys.push(k);
                }
            }
        }
        Ok(keys)
    }

    /// Add to the stored inputs of keys, through the writer.
    async fn store_inputs(&mut self, name: &str, inputs: Vec<(Value, uuid::Uuid)>) -> Result<()> {
        if inputs.is_empty() {
            return Ok(());
        }
        let batch = Batch {
            run: self.run_of(name)?,
            kind: BatchKind::Derived {
                derivation: name.to_owned(),
                scope: DerivedScope::Inputs { inputs }.to_value(),
            },
            records: vec![],
            archive: vec![],
        };
        self.writer.submit(Priority::Derived, batch).await?;
        Ok(())
    }

    /// A runner for one operation on one keyed derivation: a recompute or a
    /// comparison asked for through the command table. It registers nothing
    /// and starts one run, for that derivation.
    pub(crate) async fn for_one(
        mut read: crate::request_deadline::ReadConnection,
        writer: WriterHandle,
        name: &str,
        host: &str,
        catalog: &Catalog,
        time_settings: TimeSettings,
        build: &'static str,
    ) -> Result<Self> {
        let keyed: Vec<_> = catalog
            .keyed
            .iter()
            .filter(|d| d.name() == name)
            .cloned()
            .collect();
        let per_event: Vec<_> = catalog
            .per_event
            .iter()
            .filter(|d| d.name() == name)
            .cloned()
            .collect();
        let (version, emits) = if let Some(d) = keyed.first() {
            (d.version_for(&time_settings), d.output_types())
        } else if let Some(d) = per_event.first() {
            (d.version(), d.output_types())
        } else {
            return Err(Error::internal(format!("no derivation {name}")));
        };
        let version = version.to_string();
        let manifest = runs::Manifest {
            name,
            kind: "derivation",
            version: &version,
            build,
            emits,
        };
        crate::request_deadline::finish_read(&mut read).await?;
        let mut read = read.into_detached();
        let run = runs::start(&mut read, &manifest, host).await?;
        let runs = BTreeMap::from([(name.to_owned(), run)]);
        Ok(Self {
            keyed,
            per_event,
            blobs: None,
            ranks: BTreeMap::new(),
            passes: 0,
            clock: None,
            time_settings,
            read,
            writer,
            runs,
            build,
            previous_orders: Some(BTreeMap::new()),
            empty_passes: BTreeMap::new(),
            counts: BTreeMap::new(),
        })
    }

    /// The content store per-event derivations read stored bytes from.
    #[must_use]
    pub(crate) fn with_blobs(mut self, blobs: Blobs) -> Self {
        self.blobs = Some(blobs);
        self
    }

    /// Mark every key of `name` dirty, through the writer. While another
    /// version is selected its history is recomputed only by the switch, so
    /// the request is refused. Which build runs the selected version makes
    /// no difference.
    pub(crate) async fn recompute_all(
        &mut self,
        name: &str,
        range: Option<(Timestamp, Timestamp)>,
    ) -> Result<usize> {
        let _lock = self.writer.lock_derivations().await;
        let d = self
            .keyed
            .iter()
            .find(|d| d.name() == name)
            .ok_or_else(|| Error::internal(format!("no keyed derivation {name}")))?;
        let version = i64::from(d.version_for(&self.time_settings));
        if let Some(selected) = registry::selected(&mut self.read, name).await?
            && selected != version
        {
            return Err(Error::conflict(format!(
                "{name}: version {selected} is selected and this build runs version {version}; compare it and switch using its comparison ID"
            )));
        }
        let keys = self.all_keys(name, range).await?;
        let total = keys.len();
        let run = self.run_of(name)?;
        for chunk in keys.chunks(KEYS_PER_REQUEST) {
            let batch = Batch {
                run,
                kind: BatchKind::Derived {
                    derivation: name.to_owned(),
                    scope: DerivedScope::Recompute {
                        keys: chunk.to_vec(),
                    }
                    .to_value(),
                },
                records: vec![],
                archive: vec![],
            };
            self.writer.submit(Priority::Derived, batch).await?;
        }
        Ok(total)
    }

    fn run_of(&self, name: &str) -> Result<RunId> {
        self.runs
            .get(name)
            .copied()
            .ok_or_else(|| Error::internal(format!("no run for derivation {name}")))
    }

    /// Compute a key and read what is stored for it, in one snapshot and
    /// without writing anything: the key's computed outputs, its stored
    /// outputs, and whether it was dirty, which means its stored outputs were
    /// made before its newest inputs.
    async fn compare_key(
        &mut self,
        index: usize,
        key: &Value,
        env: &Env,
    ) -> Result<(Vec<Event>, Vec<Event>, bool)> {
        sinex_store::query::initialize_calendar(&mut self.read).await?;
        let mut snapshot = self.read.begin().await?;
        sinex_store::query::read_one_snapshot(&mut snapshot).await?;
        let d = &self.keyed[index];
        let name = d.name();
        let t = Instant::now();
        let inputs = if let Some(ids) = d.exact_inputs(key, env) {
            Some(exact_inputs(&mut snapshot, ids).await?)
        } else if d.stores_inputs(key) {
            Some(
                stored_inputs(&mut snapshot, d.as_ref(), &[key], env)
                    .await?
                    .pop()
                    .unwrap_or_default(),
            )
        } else if let Some(q) = d.inputs(key, env) {
            Some(
                derivations::read_window(&mut snapshot, &to_window(&q, env.now))
                    .await?
                    .into_iter()
                    .map(|input| input.event)
                    .collect(),
            )
        } else {
            None
        };
        let read = t.elapsed();
        let t = Instant::now();
        let derivation = self.keyed[index].as_ref();
        // A key the declaration has no query for computes to nothing, as in
        // the runner's own pass.
        let mut computed = match &inputs {
            Some(inputs) => off_worker(|| derivation.compute(key, inputs, env)),
            None => sinex_derivations::Computed::default(),
        };
        let position = derivations::head(&mut snapshot).await?;
        provenance::stamp(
            &mut snapshot,
            self.keyed[index].as_ref(),
            key,
            position,
            &mut computed,
        )
        .await?;
        if let Some(error) = &computed.error {
            return Err(Error::internal(error));
        }
        let compute = t.elapsed();
        let t = Instant::now();
        let dirty = sinex_store::query::key_dirty(&mut snapshot, name, key).await?;
        let stored = derivations::outputs(&mut snapshot, name, key).await?;
        snapshot.commit().await?;
        let c = self.counts.entry(name.to_owned()).or_default();
        c.read += read + t.elapsed();
        c.compute += compute;
        c.inputs += inputs.map_or(0, |inputs| inputs.len());
        Ok((computed.outputs, stored, dirty))
    }

    /// Recompute keys through the shared batched computation engine.
    async fn compute_keys(&mut self, index: usize, work: Vec<KeyWork>, env: &Env) -> Result<()> {
        let name = self.keyed[index].name();
        let run = self.run_of(name)?;
        KeyComputation {
            read: &mut self.read,
            writer: &self.writer,
            derivation: self.keyed[index].as_ref(),
            time_settings: self.time_settings.clone(),
            run,
            counts: self.counts.entry(name.to_owned()).or_default(),
        }
        .compute(work, env)
        .await
    }

    /// One pass over the lowest rank with work (every rank, one pass in
    /// [`EVERY_LEVEL_EVERY`]): its dirty keys, then the waiting keys of the
    /// derivations that ran, then its per-event derivations. Returns how much
    /// work it did.
    pub(crate) async fn pass(&mut self) -> Result<usize> {
        self.seed_reading_orders().await?;
        let env = self.env();
        self.passes = self.passes.wrapping_add(1);
        let every_rank = self.passes.is_multiple_of(EVERY_LEVEL_EVERY);
        let mut levels: Vec<u32> = self.ranks.values().copied().collect();
        levels.sort_unstable();
        levels.dedup();
        let mut work = 0;
        for level in levels {
            work += self.rank_pass(level, &env).await?;
            if work > 0 && !every_rank {
                break;
            }
        }
        Ok(work)
    }

    async fn seed_reading_orders(&mut self) -> Result<()> {
        if self.previous_orders.is_none() {
            let producers = self
                .per_event
                .iter()
                .map(|derivation| derivation.name().to_owned())
                .collect::<Vec<_>>();
            self.previous_orders =
                Some(derivations::reading_orders(&mut self.read, &producers).await?);
        }
        Ok(())
    }

    fn rank_of(&self, name: &str) -> u32 {
        self.ranks.get(name).copied().unwrap_or(0)
    }

    async fn rank_pass(&mut self, level: u32, env: &Env) -> Result<usize> {
        let names: Vec<String> = self
            .keyed
            .iter()
            .filter(|d| self.rank_of(d.name()) == level)
            .map(|d| d.name().to_owned())
            .collect();
        let mut work = 0;
        if !names.is_empty() {
            let dirty = derivations::dirty(&mut self.read, &names, DIRTY_PER_PASS).await?;
            let mut touched: BTreeMap<usize, (HashSet<String>, Vec<KeyWork>)> = BTreeMap::new();
            for k in dirty {
                let Some(index) = self.keyed.iter().position(|d| d.name() == k.derivation) else {
                    continue;
                };
                let entry = touched.entry(index).or_default();
                entry.0.insert(k.key.to_string());
                entry.1.push((k.key, k.last_dirty_seq));
                work += 1;
            }
            let indices: Vec<_> = (0..self.keyed.len())
                .filter(|index| {
                    self.rank_of(self.keyed[*index].name()) == level
                        && names.iter().any(|name| name == self.keyed[*index].name())
                })
                .collect();
            for index in indices {
                let name = self.keyed[index].name();
                let (done, mut keys) = touched.remove(&index).unwrap_or_default();
                let dirty: Vec<Value> = keys.iter().map(|(key, _)| key.clone()).collect();
                let waiting = derivations::waiting(&mut self.read, name, &dirty, env.now.0).await?;
                // A key that woke others keeps its dirty mark until they are
                // computed: a stop in between leaves it dirty, and the next
                // pass wakes them again.
                let wakers: HashSet<String> = waiting
                    .iter()
                    .filter_map(|waiting| waiting.woken_by.as_ref())
                    .map(Value::to_string)
                    .filter(|waker| done.contains(waker))
                    .collect();
                let (last, mut keys): (Vec<KeyWork>, Vec<KeyWork>) = keys
                    .drain(..)
                    .partition(|(key, _)| wakers.contains(&key.to_string()));
                for waiting in waiting {
                    if done.contains(&waiting.key.to_string()) {
                        continue;
                    }
                    keys.push((waiting.key, 0));
                    work += 1;
                }
                for keys in [keys, last] {
                    if !keys.is_empty() {
                        self.compute_keys(index, keys, env).await?;
                    }
                }
            }
        }
        for index in 0..self.per_event.len() {
            if self.rank_of(self.per_event[index].name()) == level {
                work += self.per_event_pass(index, env).await?;
            }
        }
        Ok(work)
    }

    async fn per_event_pass(&mut self, index: usize, env: &Env) -> Result<usize> {
        let _lock = self.writer.lock_derivations().await;
        let d = &self.per_event[index];
        let name = d.name();
        let version = d.version();
        let types: Vec<String> = d.inputs_types().iter().map(|s| (*s).to_owned()).collect();
        let stored = derivations::position(&mut self.read, name).await?;
        let position = match self.empty_passes.get(name) {
            Some(empty) if empty.stored == stored && empty.kinds == types => empty.head,
            _ => stored,
        };
        sinex_store::query::initialize_calendar(&mut self.read).await?;
        let mut snapshot = self.read.begin().await?;
        sinex_store::query::read_one_snapshot(&mut snapshot).await?;
        let read = ReadMark::now(&mut snapshot).await?;
        let events = derivations::after_seq(
            &mut snapshot,
            &types,
            position,
            EVENTS_PER_PASS,
            sinex_store::PAGE_BYTES,
        )
        .await?;
        let last = events.last().map(|c| c.commit_seq);
        let ids: Vec<_> = events.iter().map(|c| c.event.id.0).collect();
        let refused = derivations::refused_inputs(&mut snapshot, name, &ids).await?;
        let head = derivations::head(&mut snapshot).await?;
        snapshot.commit().await?;
        let Some(last) = last else {
            // Nothing of these kinds up to the head this snapshot saw: without
            // this, every pass would read the same history again.
            self.empty_passes.insert(
                name.to_owned(),
                EmptyPass {
                    kinds: types,
                    stored,
                    head: head.max(position),
                },
            );
            return Ok(0);
        };
        self.empty_passes.remove(name);
        let reading_order = self
            .previous_orders
            .as_ref()
            .expect("the pass seeded reading orders")
            .get(name)
            .copied()
            .unwrap_or(0)
            .checked_add(head)
            .ok_or_else(|| Error::internal("derived reading order exhausted"))?;
        // Outputs go to the writer in batches of about one writer batch, each
        // advancing the position to its last input: a pass over large outputs
        // (document text) holds one batch in memory, never the whole pass.
        let mut batch = PositionBatch::default();
        let mut failed = 0;
        for (i, c) in events.iter().enumerate() {
            let outcome = if refused.contains(&c.event.id.0) {
                PositionOutcome::RecordedRefusal
            } else {
                let reads = self.per_event[index].reads(&c.event);
                let derived = match self.bytes_of(reads, &c.event).await {
                    Ok(bytes) => {
                        let input = Input {
                            event: &c.event,
                            bytes: bytes.as_deref(),
                        };
                        let d = &self.per_event[index];
                        off_worker(|| d.derive(input, env))
                    }
                    Err(error) => Err(error.into()),
                };
                match derived {
                    Ok(derived) => {
                        let start = batch.outputs.len();
                        for mut event in derived {
                            hash_version(&mut event, name, version);
                            event.reading_order = ReadingOrder(reading_order);
                            batch.bytes += payload_bytes(&event.payload);
                            batch.outputs.push(event);
                        }
                        PositionOutcome::Outputs(start..batch.outputs.len())
                    }
                    Err(error @ sinex_derivations::ComputationError::Unavailable(_)) => {
                        return Err(
                            Error::from(error).within(format!("{name}: input {}", c.event.id))
                        );
                    }
                    Err(error) => {
                        failed += 1;
                        PositionOutcome::Refused(error.failure_reason())
                    }
                }
            };
            batch.items.push(PositionInput {
                id: c.event.id.0,
                position: c.commit_seq,
                outcome,
            });
            if batch.full() || i + 1 == events.len() {
                let batch = std::mem::take(&mut batch);
                self.submit_position(index, batch, c.commit_seq, read)
                    .await?;
            }
        }
        debug_assert_eq!(events.last().map(|c| c.commit_seq), Some(last));
        let c = self.counts.entry(name.to_owned()).or_default();
        c.inputs += events.len();
        c.failed += failed;
        Ok(events.len())
    }

    /// Admit one batch of a per-event pass, its position the commit sequence
    /// of the last input it covers.
    async fn submit_position(
        &mut self,
        index: usize,
        batch: PositionBatch,
        position: i64,
        read: ReadMark,
    ) -> Result<()> {
        let name = self.per_event[index].name();
        let run = self.run_of(name)?;
        let PositionBatch { items, outputs, .. } = batch;
        let n = outputs.len();
        let failures: Vec<_> = items
            .iter()
            .filter_map(|item| match &item.outcome {
                PositionOutcome::Refused(reason) => Some(RefusedWork {
                    input: RefusedInput::Input(item.id),
                    input_seq: item.position,
                    reason: reason.clone(),
                }),
                _ => None,
            })
            .collect();
        // Diagnose failed inputs before their position can cover them. If a
        // later output write is interrupted, its retry recognizes these IDs.
        if !failures.is_empty() {
            admit_refusals(&self.writer, name, run, failures, None, read).await?;
        }
        let inputs = items
            .iter()
            .filter_map(|item| {
                matches!(&item.outcome, PositionOutcome::Outputs(_)).then_some(item.id)
            })
            .collect();
        let batch = Arc::new(Batch {
            run,
            kind: BatchKind::Derived {
                derivation: name.to_owned(),
                scope: DerivedScope::Position {
                    inputs,
                    position,
                    read,
                }
                .to_value(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: outputs,
            }],
            archive: vec![],
        });
        let mut failed = 0;
        let out = match self.writer.submit(Priority::Derived, batch.clone()).await {
            Ok(out) => out,
            Err(initial @ WriteError::InvalidData(_)) => {
                // No part of the failed batch committed. Retain source order:
                // a later input cannot move progress over an unhandled write.
                let RecordOutcome::Events { events, .. } = &batch.records[0] else {
                    unreachable!()
                };
                let mut out = sinex_store::Outcome::default();
                for item in &items {
                    match &item.outcome {
                        PositionOutcome::Outputs(range) => {
                            if items.len() == 1 {
                                admit_refusals(
                                    &self.writer,
                                    name,
                                    run,
                                    vec![RefusedWork {
                                        input: RefusedInput::Input(item.id),
                                        input_seq: item.position,
                                        reason: initial
                                            .content_refusal()
                                            .expect("data refusal has a diagnosis"),
                                    }],
                                    Some(item.position),
                                    read,
                                )
                                .await?;
                                failed += 1;
                                continue;
                            }
                            let result = self
                                .writer
                                .submit(
                                    Priority::Derived,
                                    Batch {
                                        run,
                                        kind: BatchKind::Derived {
                                            derivation: name.to_owned(),
                                            scope: DerivedScope::Position {
                                                inputs: vec![item.id],
                                                position: item.position,
                                                read,
                                            }
                                            .to_value(),
                                        },
                                        records: vec![RecordOutcome::Events {
                                            location: None,
                                            events: events[range.clone()].to_vec(),
                                        }],
                                        archive: vec![],
                                    },
                                )
                                .await;
                            match result {
                                Ok(admitted) => out += &admitted,
                                Err(error @ WriteError::InvalidData(_)) => {
                                    admit_refusals(
                                        &self.writer,
                                        name,
                                        run,
                                        vec![RefusedWork {
                                            input: RefusedInput::Input(item.id),
                                            input_seq: item.position,
                                            reason: error
                                                .content_refusal()
                                                .expect("data refusal has a diagnosis"),
                                        }],
                                        Some(item.position),
                                        read,
                                    )
                                    .await?;
                                    failed += 1;
                                }
                                Err(error) => return Err(Error::from(error).within(name)),
                            }
                        }
                        PositionOutcome::Refused(_) | PositionOutcome::RecordedRefusal => {
                            // These IDs have already committed their diagnosis.
                            admit_refusals(
                                &self.writer,
                                name,
                                run,
                                vec![],
                                Some(item.position),
                                read,
                            )
                            .await?;
                        }
                    }
                }
                out
            }
            Err(error) => return Err(Error::from(error).within(name)),
        };
        let c = self.counts.entry(name.to_owned()).or_default();
        c.outputs += n;
        c.failed += failed;
        c.admitted += out.admitted;
        c.repeats += out.repeats;
        c.superseded_input += out.superseded_input;
        Ok(())
    }

    /// The stored bytes a derivation that `reads` them reads for `event`.
    async fn bytes_of(&mut self, reads: Reads, event: &Event) -> Result<Option<Vec<u8>>> {
        match reads {
            Reads::Nothing => Ok(None),
            Reads::Material => self.material_of(event).await.map(Some),
            Reads::Blob(field) => {
                let Some(blob) = event.payload.get(field).and_then(Value::as_str) else {
                    return Ok(None);
                };
                let blobs = self.blobs.as_ref().ok_or_else(|| {
                    Error::not_configured("no content store was given to the runner")
                })?;
                if !material_bytes::blob_stored(&mut self.read, blob).await? {
                    return Ok(None);
                }
                let located = blobs.locate(&mut self.read, blob).await?;
                blobs.read(&located).map(Some).map_err(Error::from)
            }
        }
    }

    /// The stored bytes of the material `event` cites.
    async fn material_of(&mut self, event: &Event) -> Result<Vec<u8>> {
        let sinex_model::Origin::Record { location, .. } = &event.origin else {
            return Err(Error::internal("a derived event cites no material"));
        };
        let blobs = self
            .blobs
            .as_ref()
            .ok_or_else(|| Error::not_configured("no content store was given to the runner"))?;
        let blob = material_bytes::material_blob(&mut self.read, location.material)
            .await?
            .ok_or_else(|| Error::not_found("its material is not stored"))?;
        let located = blobs.locate(&mut self.read, &blob).await?;
        blobs.read(&located).map_err(Error::from)
    }

    /// Run passes until one does no work.
    pub(crate) async fn catch_up(&mut self) -> Result<()> {
        while self.pass().await? > 0 {}
        Ok(())
    }

    /// Finish the current pass on shutdown, then release the writer handle. Already submitted batches keep their acknowledgments.
    pub(crate) async fn run(
        mut self,
        wake: Arc<Notify>,
        idle: Duration,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        // A read-only seed can be abandoned on shutdown. Once a pass can
        // submit writes, finish that pass and keep its acknowledgments.
        tokio::select! {
            biased;
            _ = stop.wait_for(|stopped| *stopped) => return Ok(()),
            result = self.seed_reading_orders() => result?,
        }
        loop {
            if *stop.borrow() || stop.has_changed().is_err() {
                break;
            }
            let work = self.pass().await?;
            if *stop.borrow() || stop.has_changed().is_err() {
                break;
            }
            if work == 0 {
                tokio::select! {
                    () = wake.notified() => {}
                    () = tokio::time::sleep(idle) => {}
                    _ = stop.wait_for(|stopped| *stopped) => {}
                }
            }
        }
        self.end_runs().await
    }

    pub(crate) async fn end_runs(&mut self) -> Result<()> {
        for run in self.runs.values() {
            runs::end(&mut self.read, *run).await?;
        }
        Ok(())
    }

    /// Compute `name` over its keys (optionally only keys touched by inputs in
    /// `range`) and compare with the stored outputs, writing nothing: the
    /// comparison for a derivation change.
    ///
    /// Inputs keep arriving while it runs, and it holds nothing still. Each
    /// key is compared in one snapshot of its own, so its two sides agree on
    /// the inputs they saw; a key that was dirty in that snapshot is counted
    /// as behind its inputs, because its difference may come from an input
    /// the stored outputs had not been computed from yet. The comparison
    /// records the commit positions it ran between.
    pub(crate) async fn compare(
        &mut self,
        name: &str,
        range: Option<(Timestamp, Timestamp)>,
        examples: usize,
    ) -> Result<Comparison> {
        if !self.keyed.iter().any(|d| d.name() == name) {
            if range.is_some() {
                return Err(Error::internal(
                    "per-event comparisons cover the full producer history",
                ));
            }
            return self.compare_per_event(name, examples).await;
        }
        let index = self
            .keyed
            .iter()
            .position(|d| d.name() == name)
            .ok_or_else(|| Error::internal(format!("no keyed derivation {name}")))?;
        let input_types: Vec<String> = self.keyed[index]
            .inputs_types()
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect();
        let from = derivations::head(&mut self.read).await?;
        let keys = self.all_keys(name, range).await?;
        let env = self.env();
        let mut cmp = Comparison {
            keys: keys.len(),
            ..Comparison::default()
        };
        let version = self.keyed[index].version_for(&self.time_settings);
        for key in keys {
            let (computed, stored, dirty) = self.compare_key(index, &key, &env).await?;
            cmp.behind += usize::from(dirty);
            compare_outputs(&mut cmp, &key, &stored, &computed, examples);
        }
        let through = derivations::head(&mut self.read).await?;
        let count = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        cmp.id = Some(
            derivations::store_comparison(
                &mut self.read,
                &derivations::StoredComparison {
                    id: 0,
                    derivation: name.to_owned(),
                    version: i64::from(version),
                    range: range.map(|(from, until)| (Some(from), Some(until))),
                    made_at: Timestamp::now(),
                    keys: count(cmp.keys),
                    unchanged: count(cmp.same),
                    added: count(cmp.added),
                    changed: count(cmp.changed),
                    removed: count(cmp.removed),
                    context: serde_json::json!({"build": self.build, "positions": {"from": from, "through": through}, "behind": cmp.behind, "input_types": input_types, "output_types": self.keyed[index].output_types(), "kind": "keyed", "history": self.keyed[index].history_policy(), "now": env.now, "settings": self.time_settings.label()}),
                    examples: cmp.stored_examples(),
                },
            )
            .await?,
        );
        Ok(cmp)
    }

    /// Select the compared version of `name` and, when it recomputes
    /// history, archive the previous outputs. Returns how many keys the
    /// selection scheduled.
    pub(crate) async fn switch(
        &mut self,
        name: &str,
        comparison: i64,
        actor: &str,
    ) -> Result<usize> {
        let scheduled = self.select(name, comparison, actor).await?;
        // The version is selected. Other keyed work need not wait for the
        // archive of the previous outputs, which holds this switch's keys.
        self.finish_switches(Some(name)).await?;
        Ok(scheduled)
    }

    /// The selection of a switch, in one transaction through the writer. For
    /// a `recompute` keyed derivation every key its inputs or outputs name is
    /// marked dirty with the selection, and admission marks the keys of
    /// whatever is committed from then on, so the selected version computes
    /// the whole history as it stands, including inputs newer than the
    /// comparison.
    async fn select(&mut self, name: &str, comparison: i64, actor: &str) -> Result<usize> {
        let _lock = self.writer.lock_derivations().await;
        let (version, inputs, outputs, kind, history) =
            if let Some(d) = self.keyed.iter().find(|d| d.name() == name) {
                (
                    d.version_for(&self.time_settings),
                    d.inputs_types(),
                    d.output_types(),
                    "keyed",
                    d.history_policy(),
                )
            } else if let Some(d) = self.per_event.iter().find(|d| d.name() == name) {
                (
                    d.version(),
                    d.inputs_types(),
                    d.output_types(),
                    "per_event",
                    d.history_policy(),
                )
            } else {
                return Err(Error::internal(format!("no derivation {name}")));
            };
        let selection = derivations::selection::Selection {
            comparison,
            operation: uuid::Uuid::now_v7(),
            actor: actor.to_owned(),
            history,
            version: i64::from(version),
            build: self.build.to_owned(),
            settings: self.time_settings.label(),
            inputs: inputs.iter().map(|kind| (*kind).to_owned()).collect(),
            outputs: outputs.iter().map(|kind| (*kind).to_owned()).collect(),
            kind: kind.into(),
        };
        let keys = if kind == "keyed" && history == sinex_model::DerivationHistory::Recompute {
            self.all_keys(name, None).await?
        } else {
            vec![]
        };
        let total = keys.len();
        self.writer
            .submit(
                Priority::Derived,
                Batch {
                    run: self.run_of(name)?,
                    kind: BatchKind::Derived {
                        derivation: name.to_owned(),
                        scope: DerivedScope::Select { selection, keys }.to_value(),
                    },
                    records: vec![],
                    archive: vec![],
                },
            )
            .await?;
        Ok(total)
    }

    /// Archive the previous outputs of every switch of this runner's
    /// derivations that selected its version and has not ended, then end it:
    /// the switch just made (`only`), or those a stopped daemon left. The
    /// outputs go in transactions of their own, so live batches are admitted
    /// between them; until the switch ends its keys are held and a per-event
    /// derivation keeps its position.
    async fn finish_switches(&mut self, only: Option<&str>) -> Result<()> {
        for switch in derivations::selection::unfinished_switches(&mut self.read).await? {
            let name = switch.derivation.as_str();
            if only.is_some_and(|only| only != name) || !self.runs.contains_key(name) {
                continue;
            }
            let run = self.run_of(name)?;
            let outputs = derivations::selection::live_outputs_through(
                &mut self.read,
                name,
                switch.selected_at,
            )
            .await?;
            archive_outputs(
                &self.writer,
                run,
                name,
                &outputs,
                &[],
                Some(switch.operation),
                usize::MAX,
            )
            .await?;
            // A per-event pass holds this lock from reading its position
            // until its last batch commits. Ending the switch under it means
            // no batch computed from the old position commits after the
            // position is reset and moves it past the history again.
            let _lock = self.writer.lock_derivations().await;
            let ended = !derivations::selection::unfinished_switches(&mut self.read)
                .await?
                .iter()
                .any(|unfinished| unfinished.operation == switch.operation);
            if ended {
                // Another runner of this daemon finished it meanwhile.
                continue;
            }
            self.writer
                .submit(
                    Priority::Derived,
                    Batch {
                        run,
                        kind: BatchKind::Derived {
                            derivation: name.to_owned(),
                            scope: DerivedScope::Switched {
                                operation: switch.operation,
                            }
                            .to_value(),
                        },
                        records: vec![],
                        archive: vec![],
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn compare_per_event(&mut self, name: &str, examples: usize) -> Result<Comparison> {
        let index = self
            .per_event
            .iter()
            .position(|d| d.name() == name)
            .ok_or_else(|| Error::internal(format!("no derivation {name}")))?;
        let inputs: Vec<String> = self.per_event[index]
            .inputs_types()
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect();
        let outputs: Vec<String> = self.per_event[index]
            .output_types()
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect();
        let version = self.per_event[index].version();
        let from = derivations::head(&mut self.read).await?;
        let mut cmp = Comparison::default();
        let env = self.env();
        let mut after = 0;
        loop {
            // An input after the saved position has no stored outputs yet,
            // whatever the version: it is behind, like a dirty key.
            let derived_to = derivations::position(&mut self.read, name).await?;
            let page = derivations::after_seq(
                &mut self.read,
                &inputs,
                after,
                EVENTS_PER_PASS,
                sinex_store::PAGE_BYTES,
            )
            .await?;
            let Some(last) = page.last() else { break };
            after = last.commit_seq;
            for row in page {
                let reads = self.per_event[index].reads(&row.event);
                let bytes = self.bytes_of(reads, &row.event).await?;
                let d = &self.per_event[index];
                let input = Input {
                    event: &row.event,
                    bytes: bytes.as_deref(),
                };
                let new = off_worker(|| d.derive(input, &env)).map_err(Error::internal)?;
                let old = derivations::children_outputs(&mut self.read, name, row.event.id).await?;
                compare_outputs(
                    &mut cmp,
                    &serde_json::json!({"input": row.event.id}),
                    &old,
                    &new,
                    examples,
                );
                cmp.keys += 1;
                cmp.behind += usize::from(row.commit_seq > derived_to);
            }
        }
        let through = derivations::head(&mut self.read).await?;
        cmp.id = Some(derivations::store_comparison(&mut self.read, &derivations::StoredComparison {
            id: 0, derivation: name.to_owned(), version: i64::from(version), range: None, made_at: Timestamp::now(),
            keys: i64::try_from(cmp.keys)?, unchanged: i64::try_from(cmp.same)?,
            added: i64::try_from(cmp.added)?, changed: i64::try_from(cmp.changed)?, removed: i64::try_from(cmp.removed)?,
            context: serde_json::json!({"build": self.build, "positions": {"from": from, "through": through}, "behind": cmp.behind, "input_types": inputs, "output_types": outputs, "kind": "per_event", "history": self.per_event[index].history_policy(), "now": env.now, "settings": self.time_settings.label()}),
            examples: cmp.stored_examples(),
        }).await?);
        Ok(cmp)
    }
}

fn compare_outputs(
    cmp: &mut Comparison,
    key: &Value,
    old: &[Event],
    new: &[Event],
    examples: usize,
) {
    let by_key = |events: &[Event]| -> BTreeMap<String, Event> {
        events
            .iter()
            .enumerate()
            .map(|(index, event)| {
                (
                    event
                        .equivalence_key
                        .as_ref()
                        .map(|key| key.0.clone())
                        .unwrap_or_else(|| format!("{}:{index}", event.event_type)),
                    event.clone(),
                )
            })
            .collect()
    };
    let (old, new) = (by_key(old), by_key(new));
    for (key_id, event) in &new {
        match old.get(key_id) {
            None => cmp.note(Change::Added, key, None, Some(event), examples),
            Some(previous) if previous.payload == event.payload => cmp.same += 1,
            Some(previous) => cmp.note(Change::Changed, key, Some(previous), Some(event), examples),
        }
    }
    for (key_id, event) in &old {
        if !new.contains_key(key_id) {
            cmp.note(Change::Removed, key, Some(event), None, examples);
        }
    }
}

// Equal results from different versions must keep their producer attribution.
fn hash_version(event: &mut Event, name: &str, version: u32) {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinex.derivation\0");
    hash.update(name.as_bytes());
    hash.update(&[0]);
    hash.update(&version.to_le_bytes());
    hash.update(&event.record_hash);
    event.record_hash = *hash.finalize().as_bytes();
}

/// Each derivation's rank: 0 when no derivation produces any of its inputs,
/// otherwise one more than the highest rank among those that do. A cycle
/// (which no catalog should have) stops at the catalog's size.
fn ranks(catalog: &[Derivation]) -> BTreeMap<String, u32> {
    let limit = u32::try_from(catalog.len()).unwrap_or(u32::MAX);
    let mut rank: BTreeMap<&str, u32> = catalog.iter().map(|d| (d.name.as_str(), 0)).collect();
    for _ in 0..catalog.len() {
        let mut changed = false;
        for d in catalog {
            let upstream = catalog
                .iter()
                .filter(|u| u.name != d.name && u.outputs.iter().any(|o| d.inputs.contains(o)))
                .map(|u| rank[u.name.as_str()] + 1)
                .max()
                .unwrap_or(0)
                .min(limit);
            if upstream > rank[d.name.as_str()] {
                rank.insert(d.name.as_str(), upstream);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    rank.into_iter()
        .map(|(name, r)| (name.to_owned(), r))
        .collect()
}

mod comparison;
mod counts;

mod provenance;
pub(crate) use comparison::{Change, Comparison};
pub(crate) use counts::Counts;

#[cfg(test)]
mod tests;

/// Every available per-event derivation with the declared rules.
pub(crate) fn per_event_with(
    configuration: &sinex_derivations::Configuration,
) -> Result<Vec<Box<dyn AnyPerEvent>>> {
    let mut all = sinex_derivations::per_event_with(configuration).map_err(Error::bad_value)?;
    let pdf_reader = crate::document_text::pdf_reader()
        .inspect_err(|reason| eprintln!("sinexd: document text reads no PDFs: {reason}"))
        .ok();
    all.push(Box::new(crate::document_text::DocumentText { pdf_reader }));
    let engine = crate::screen_text::engine()
        .inspect_err(|reason| eprintln!("sinexd: screen OCR off: {reason}"))
        .ok();
    all.push(Box::new(crate::screen_text::ScreenText { engine }));
    Ok(all)
}

/// Every available per-event derivation with empty rules.
pub(crate) fn per_event() -> Vec<Box<dyn AnyPerEvent>> {
    per_event_with(&sinex_derivations::Configuration::default()).expect("empty rules are valid")
}

/// Key computations serialize before reading, so a command and the
/// background runner cannot overwrite newer outputs with older inputs.
struct KeyComputation<'a> {
    read: &'a mut PgConnection,
    writer: &'a WriterHandle,
    derivation: &'a dyn AnyKeyed,
    time_settings: TimeSettings,
    run: RunId,
    counts: &'a mut Counts,
}

/// Keys computed in one input snapshot, submitted once that snapshot has
/// ended.
struct Segment {
    /// Indices into the work of the keys computed.
    computed: Vec<usize>,
    scopes: Vec<ComputedKey>,
    refused: Vec<RefusedWork>,
    records: Vec<RecordOutcome>,
    outputs: usize,
    /// Payload bytes of the outputs.
    bytes: usize,
    read: ReadMark,
    /// The commit head the snapshot saw.
    position: i64,
}

impl Segment {
    fn full(&self) -> bool {
        self.computed.len() >= KEYS_PER_BATCH
            || self.outputs >= OUTPUTS_PER_BATCH
            || self.bytes >= OUTPUT_BYTES_PER_BATCH
    }
}

/// Outputs of a per-event pass not yet submitted, and the inputs computed for
/// them.
#[derive(Default)]
struct PositionBatch {
    items: Vec<PositionInput>,
    outputs: Vec<Event>,
    bytes: usize,
}

/// Keep true input boundaries for deterministic refusal isolation. Payloads
/// stay in one flat vector; normal batches never clone them for a retry.
struct PositionInput {
    id: uuid::Uuid,
    position: i64,
    outcome: PositionOutcome,
}
enum PositionOutcome {
    Outputs(std::ops::Range<usize>),
    Refused(String),
    RecordedRefusal,
}

impl PositionBatch {
    fn full(&self) -> bool {
        self.outputs.len() >= OUTPUTS_PER_BATCH || self.bytes >= OUTPUT_BYTES_PER_BATCH
    }
}

/// About how many bytes `payload` takes stored: its strings, keys and a word
/// per scalar. Counted without serializing.
fn payload_bytes(payload: &Value) -> usize {
    match payload {
        Value::String(s) => s.len() + 2,
        Value::Array(items) => 2 + items.iter().map(payload_bytes).sum::<usize>(),
        Value::Object(fields) => {
            2 + fields
                .iter()
                .map(|(k, v)| k.len() + 3 + payload_bytes(v))
                .sum::<usize>()
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => 8,
    }
}

/// Run a derivation's computation off the runtime's worker threads. One
/// computation can take minutes; on a worker it would also hold the task that
/// worker runs next (the writer's, the read models'), which no other worker
/// takes over, so admission would wait for the computation.
fn off_worker<T>(compute: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(compute)
    } else {
        compute()
    }
}

/// Store terminal input failures through the same writer as outputs. A failed
/// write of this state cannot move its position or masquerade as success.
async fn admit_refusals(
    writer: &WriterHandle,
    name: &str,
    run: RunId,
    inputs: Vec<RefusedWork>,
    position: Option<i64>,
    read: ReadMark,
) -> Result<()> {
    writer
        .submit(
            Priority::Derived,
            Batch {
                run,
                kind: BatchKind::Derived {
                    derivation: name.to_owned(),
                    scope: DerivedScope::Refused {
                        inputs,
                        position,
                        read,
                    }
                    .to_value(),
                },
                records: vec![],
                archive: vec![],
            },
        )
        .await?;
    Ok(())
}

impl KeyComputation<'_> {
    /// Compute `work` in segments of about one writer batch, each read in its
    /// own snapshot that ends before the batch goes to the writer. A snapshot
    /// held across a wait on the writer keeps its locks on event chunks; a
    /// request for an exclusive chunk lock (compression) queues behind them,
    /// and the writer's own chunk locks then queue behind that request, a
    /// wait cycle PostgreSQL cannot see because one edge is inside the daemon.
    async fn compute(&mut self, mut work: Vec<KeyWork>, env: &Env) -> Result<()> {
        let _lock = self.writer.lock_derivations().await;
        // A command or background pass may have selected work before waiting
        // here. Another computation can diagnose it while this one is queued.
        let keys: Vec<_> = work.iter().map(|(key, _)| key.clone()).collect();
        let refused = derivations::refused_keys(self.read, self.derivation.name(), &keys).await?;
        let blocked: HashSet<_> = refused.iter().map(|row| row.key.to_string()).collect();
        self.counts.failed += blocked.len();
        work.retain(|(key, _)| !blocked.contains(&key.to_string()));
        if work.is_empty() {
            return Ok(());
        }
        sinex_store::query::initialize_calendar(self.read).await?;
        let mut remaining: Vec<usize> = (0..work.len()).collect();
        while !remaining.is_empty() {
            let mut snapshot = self.read.begin().await?;
            sinex_store::query::read_one_snapshot(&mut snapshot).await?;
            let segment = compute_segment(
                &mut snapshot,
                self.derivation,
                self.counts,
                &work,
                &remaining,
                env,
            )
            .await;
            let segment = match segment {
                Ok(segment) => {
                    snapshot.commit().await?;
                    segment
                }
                Err(error) => {
                    return match snapshot.rollback().await {
                        Ok(()) => Err(error),
                        Err(rollback) => Err(Error::internal(format!(
                            "{error}; rolling back the input snapshot: {rollback}"
                        ))),
                    };
                }
            };
            if segment.computed.is_empty() {
                return Err(Error::internal(format!(
                    "{}: a snapshot computed none of {} keys",
                    self.derivation.name(),
                    remaining.len()
                )));
            }
            let computed: HashSet<usize> = segment.computed.iter().copied().collect();
            remaining.retain(|index| !computed.contains(index));
            self.submit(segment, OUTPUTS_PER_BATCH, OUTPUT_BYTES_PER_BATCH)
                .await?;
        }
        Ok(())
    }

    async fn submit(
        &mut self,
        mut segment: Segment,
        max_outputs: usize,
        max_bytes: usize,
    ) -> Result<()> {
        // The snapshot stops after the first key that reaches a limit. Only its
        // final key can overflow a preceding group. Keep that key's whole output
        // set and original snapshot when it goes to a separate transaction.
        let tail = if segment.scopes.len() > 1
            && (segment.outputs > max_outputs || segment.bytes > max_bytes)
        {
            let last = segment.scopes.len() - 1;
            Some((
                segment.scopes.split_off(last),
                segment.records.split_off(last),
            ))
        } else {
            None
        };
        let Segment {
            scopes,
            refused,
            records,
            read,
            position,
            ..
        } = segment;
        if !refused.is_empty() {
            let failed = refused.len();
            let started = Instant::now();
            admit_refusals(
                self.writer,
                self.derivation.name(),
                self.run,
                refused,
                None,
                read,
            )
            .await?;
            self.counts.write += started.elapsed();
            self.counts.keys += failed;
            self.counts.failed += failed;
        }
        for (keys, records) in std::iter::once((scopes, records)).chain(tail) {
            if !keys.is_empty() {
                self.submit_keys(
                    keys,
                    records,
                    read,
                    position,
                    max_outputs.min(ARCHIVED_PER_BATCH),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn submit_keys(
        &mut self,
        scopes: Vec<ComputedKey>,
        mut records: Vec<RecordOutcome>,
        read: ReadMark,
        position: i64,
        replaces_at_most: usize,
    ) -> Result<()> {
        let name = self.derivation.name();
        let mut n = 0;
        for record in &mut records {
            if let RecordOutcome::Events { events, .. } = record {
                n += events.len();
                for e in events {
                    hash_version(e, name, self.derivation.version_for(&self.time_settings));
                    e.reading_order = ReadingOrder(position);
                }
            }
        }
        let keys = scopes.len();
        let t = Instant::now();
        let scope = DerivedScope::Keys {
            keys: scopes,
            read,
            replaces_at_most: Some(replaces_at_most),
        };
        let batch = Arc::new(Batch {
            run: self.run,
            kind: BatchKind::Derived {
                derivation: name.to_owned(),
                scope: scope.to_value(),
            },
            records,
            archive: vec![],
        });
        let mut failed = 0;
        let out = match self.replace(batch.clone()).await {
            Ok(out) => out,
            Err(initial @ WriteError::InvalidData(_)) => {
                // The full transaction rolled back. Diagnose complete keys,
                // never individual outputs of a key. Ordinary batches retain
                // one write and do not clone their payloads for this path.
                let DerivedScope::Keys { keys, .. } = &scope else {
                    unreachable!()
                };
                let mut out = sinex_store::Outcome::default();
                for (key, record) in keys.iter().zip(&batch.records) {
                    if keys.len() == 1 {
                        admit_refusals(
                            self.writer,
                            name,
                            self.run,
                            vec![RefusedWork {
                                input: RefusedInput::Key(key.key.clone()),
                                input_seq: position,
                                reason: initial
                                    .content_refusal()
                                    .expect("data refusal has a diagnosis"),
                            }],
                            None,
                            read,
                        )
                        .await?;
                        failed += 1;
                        break;
                    }
                    let unit = Batch {
                        run: self.run,
                        kind: BatchKind::Derived {
                            derivation: name.to_owned(),
                            scope: DerivedScope::Keys {
                                keys: vec![key.clone()],
                                read,
                                replaces_at_most: Some(replaces_at_most),
                            }
                            .to_value(),
                        },
                        records: vec![record.clone()],
                        archive: vec![],
                    };
                    match self.replace(Arc::new(unit)).await {
                        Ok(admitted) => out += &admitted,
                        Err(error @ WriteError::InvalidData(_)) => {
                            admit_refusals(
                                self.writer,
                                name,
                                self.run,
                                vec![RefusedWork {
                                    input: RefusedInput::Key(key.key.clone()),
                                    input_seq: position,
                                    reason: error
                                        .content_refusal()
                                        .expect("data refusal has a diagnosis"),
                                }],
                                None,
                                read,
                            )
                            .await?;
                            failed += 1;
                        }
                        Err(error) => return Err(Error::from(error).within(name)),
                    }
                }
                out
            }
            Err(error) => return Err(Error::from(error).within(name)),
        };
        let c = &mut *self.counts;
        c.write += t.elapsed();
        c.keys += keys;
        c.failed += failed;
        c.outputs += n;
        c.admitted += out.admitted;
        c.repeats += out.repeats;
        c.replaced += out.replaced;
        c.superseded_input += out.superseded_input;
        Ok(())
    }

    /// Admit a batch of complete keys. A batch that would archive more
    /// previous outputs than one transaction may is refused with their
    /// identities; they are archived in transactions of their own, with live
    /// batches admitted between them, and the batch is submitted again. Its
    /// keys stay dirty until it commits, so an interruption computes them
    /// again.
    async fn replace(&self, batch: Arc<Batch>) -> Result<sinex_store::Outcome, WriteError> {
        loop {
            let outputs = match self.writer.submit(Priority::Derived, batch.clone()).await {
                Err(WriteError::ArchiveFirst(outputs)) => outputs,
                answer => return answer,
            };
            let BatchKind::Derived { derivation, scope } = &batch.kind else {
                unreachable!("a batch of keys is derived")
            };
            let Ok(DerivedScope::Keys {
                keys,
                replaces_at_most,
                ..
            }) = serde_json::from_value(scope.clone())
            else {
                unreachable!("only a batch of keys is told to archive first")
            };
            let replacing: Vec<_> = keys
                .into_iter()
                .map(|key| (key.key, key.dirty_seq))
                .collect();
            archive_outputs(
                self.writer,
                batch.run,
                derivation,
                &outputs,
                &replacing,
                None,
                replaces_at_most.unwrap_or(usize::MAX),
            )
            .await?;
        }
    }
}

/// Archive `outputs` of `derivation` in transactions of their own, at most
/// `most` in each. The first takes [`FIRST_ARCHIVED`]; each later one is
/// sized by how long the last held the writer, so a live batch waits behind
/// one bounded transaction however many outputs there are.
async fn archive_outputs(
    writer: &WriterHandle,
    run: RunId,
    derivation: &str,
    outputs: &[uuid::Uuid],
    replacing: &[(Value, i64)],
    switch: Option<uuid::Uuid>,
    most: usize,
) -> Result<(), WriteError> {
    let most = most.max(1);
    let mut size = FIRST_ARCHIVED.min(most);
    let mut rest = outputs;
    while !rest.is_empty() {
        let (part, later) = rest.split_at(size.min(rest.len()));
        rest = later;
        let archived = writer
            .submit(
                Priority::Derived,
                Batch {
                    run,
                    kind: BatchKind::Derived {
                        derivation: derivation.to_owned(),
                        scope: DerivedScope::Archive {
                            outputs: part.to_vec(),
                            replacing: replacing.to_vec(),
                            switch,
                        }
                        .to_value(),
                    },
                    records: vec![],
                    archive: vec![],
                },
            )
            .await?;
        if let Some(held) = archived.timings.get("service") {
            size = crate::writer::next_bulk_size(size, *held).min(most);
        }
    }
    Ok(())
}

/// Read and compute the `remaining` keys of `work` in the snapshot `conn` is
/// in, until about one writer batch is ready. Keys whose rows are complete are
/// computed as their pages arrive; reading stops after the key that fills the
/// batch, and keys not computed are read again in the next snapshot.
async fn compute_segment(
    conn: &mut PgConnection,
    derivation: &dyn AnyKeyed,
    counts: &mut Counts,
    work: &[KeyWork],
    remaining: &[usize],
    env: &Env,
) -> Result<Segment> {
    let mut segment = Segment {
        computed: Vec::new(),
        scopes: Vec::new(),
        refused: Vec::new(),
        records: Vec::new(),
        outputs: 0,
        bytes: 0,
        read: ReadMark::now(&mut *conn).await?,
        position: derivations::head(&mut *conn).await?,
    };
    // Keys with exact inputs or no query first; the rest are read page by
    // page.
    let mut windows: Vec<Window> = Vec::new();
    let mut window_keys: Vec<usize> = Vec::new();
    let mut stored: Vec<usize> = Vec::new();
    for &index in remaining {
        let key = &work[index].0;
        let inputs = if let Some(ids) = derivation.exact_inputs(key, env) {
            Some(exact_inputs(&mut *conn, ids).await?)
        } else if derivation.stores_inputs(key) {
            stored.push(index);
            continue;
        } else if let Some(q) = derivation.inputs(key, env) {
            windows.push(to_window(&q, env.now));
            window_keys.push(index);
            continue;
        } else {
            None
        };
        compute_one(
            conn,
            derivation,
            counts,
            &mut segment,
            work,
            index,
            inputs,
            env,
        )
        .await?;
        if segment.full() {
            return Ok(segment);
        }
    }
    // Keys read by their stored inputs: one list read and one event read
    // for each group of keys.
    for group in stored.chunks(KEYS_PER_BATCH) {
        let keys: Vec<&Value> = group.iter().map(|&index| &work[index].0).collect();
        let t = Instant::now();
        let inputs = stored_inputs(&mut *conn, derivation, &keys, env).await?;
        counts.read += t.elapsed();
        for (&index, inputs) in group.iter().zip(inputs) {
            compute_one(
                conn,
                derivation,
                counts,
                &mut segment,
                work,
                index,
                Some(inputs),
                env,
            )
            .await?;
            if segment.full() {
                return Ok(segment);
            }
        }
    }
    let mut reader = derivations::Windows::new(windows, ROWS_PER_PAGE, sinex_store::PAGE_BYTES);
    loop {
        let t = Instant::now();
        let next = reader.next(&mut *conn).await?;
        counts.read += t.elapsed();
        let Some(done) = next else { break };
        for (window, rows) in done {
            let inputs = rows.into_iter().map(|c| c.event).collect();
            compute_one(
                conn,
                derivation,
                counts,
                &mut segment,
                work,
                window_keys[window],
                Some(inputs),
                env,
            )
            .await?;
            if segment.full() {
                return Ok(segment);
            }
        }
    }
    Ok(segment)
}

/// Compute one key from its inputs (`None` when it has no query) and add it
/// to the segment.
#[allow(clippy::too_many_arguments)]
async fn compute_one(
    conn: &mut PgConnection,
    derivation: &dyn AnyKeyed,
    counts: &mut Counts,
    segment: &mut Segment,
    work: &[KeyWork],
    index: usize,
    inputs: Option<Vec<Event>>,
    env: &Env,
) -> Result<()> {
    let (key, dirty_seq) = &work[index];
    let t = Instant::now();
    let input_count = inputs.as_ref().map_or(0, Vec::len);
    let mut computed = match &inputs {
        Some(inputs) => off_worker(|| derivation.compute(key, inputs, env)),
        None => sinex_derivations::Computed::default(),
    };
    provenance::stamp(&mut *conn, derivation, key, segment.position, &mut computed).await?;
    counts.compute += t.elapsed();
    counts.inputs += input_count;
    if let Some(error) = &computed.error {
        segment.computed.push(index);
        segment.refused.push(RefusedWork {
            input: RefusedInput::Key(key.clone()),
            input_seq: segment.position,
            reason: error.failure_reason(),
        });
        return Ok(());
    }
    segment.outputs += computed.outputs.len();
    segment.bytes += computed
        .outputs
        .iter()
        .map(|e| payload_bytes(&e.payload))
        .sum::<usize>();
    segment.computed.push(index);
    segment.scopes.push(ComputedKey {
        key: key.clone(),
        dirty_seq: *dirty_seq,
        waiting: computed.waiting,
        woken_by: computed.woken_by.take().filter(|_| computed.waiting),
        refresh_at: derivation.refresh_at(
            inputs.as_deref().unwrap_or_default(),
            &computed.outputs,
            env,
        ),
    });
    segment.records.push(RecordOutcome::Events {
        location: None,
        events: computed.outputs,
    });
    Ok(())
}

/// Compute requested keys with the background runner's batching and ordering.
/// Commands wait for their outputs before returning reads.
pub(crate) async fn compute_now(
    conn: &mut PgConnection,
    writer: &WriterHandle,
    derivation: &dyn AnyKeyed,
    mut keys: Vec<Value>,
    host: &str,
    time_settings: TimeSettings,
) -> Result<Counts> {
    let available = Derivation {
        name: derivation.name().to_owned(),
        kind: "keyed",
        version: i64::from(derivation.version_for(&time_settings)),
        inputs: derivation
            .inputs_types()
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect(),
        outputs: derivation
            .output_types()
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect(),
    };
    crate::request_deadline::finish_read(conn).await?;
    registry::register_derivation(conn, &available).await?;
    // While another version is selected, this one computes only the keys
    // that are already dirty; the switch recomputes the rest.
    if registry::selected(conn, derivation.name()).await? != Some(available.version) {
        let mut dirty = Vec::new();
        for key in keys {
            if sinex_store::query::key_dirty(conn, derivation.name(), &key).await? {
                dirty.push(key);
            }
        }
        if dirty.is_empty() {
            return Err(Error::conflict(
                "producer history changed; compare and switch before recomputing unchanged keys",
            ));
        }
        keys = dirty;
    }
    let refused = derivations::refused_keys(conn, derivation.name(), &keys).await?;
    let blocked: HashSet<String> = refused.iter().map(|row| row.key.to_string()).collect();
    keys.retain(|key| !blocked.contains(&key.to_string()));
    if keys.is_empty() && !refused.is_empty() {
        return Err(Error::refused(format!(
            "{}: {}; change its inputs or request an explicit recompute",
            derivation.name(),
            refused[0].reason
        )));
    }
    let version = available.version.to_string();
    let manifest = runs::Manifest {
        name: derivation.name(),
        kind: "derivation",
        version: &version,
        build: crate::build_identity::current()?,
        emits: derivation.output_types(),
    };
    let run = runs::start(conn, &manifest, host).await?;
    let result: Result<Counts> = async {
        let env = Env::at(Timestamp::now(), time_settings);
        let mut counts = Counts::default();
        let head = derivations::head(conn).await?;
        let work = keys.into_iter().map(|key| (key, head)).collect();
        KeyComputation {
            read: conn,
            writer,
            derivation,
            time_settings: env.time_settings.clone(),
            run,
            counts: &mut counts,
        }
        .compute(work, &env)
        .await?;
        Ok(counts)
    }
    .await;
    let ended = runs::end(conn, run).await.map_err(Error::from);
    let counts = result?;
    ended?;
    if counts.failed > 0 || !refused.is_empty() {
        return Err(Error::refused(format!(
            "{}: {} selected keys refused; other keys completed; see status",
            derivation.name(),
            counts.failed + refused.len()
        )));
    }
    Ok(counts)
}

/// The inputs of keys read by their stored lists, in the order of `keys`:
/// each key's live stored events and what its query adds, by `ts_orig` then
/// commit order.
async fn stored_inputs(
    conn: &mut PgConnection,
    derivation: &dyn AnyKeyed,
    keys: &[&Value],
    env: &Env,
) -> Result<Vec<Vec<Event>>> {
    let owned: Vec<Value> = keys.iter().map(|key| (*key).clone()).collect();
    let listed = derivations::stored_inputs(conn, derivation.name(), &owned).await?;
    let mut ids_of: std::collections::HashMap<String, Vec<uuid::Uuid>> =
        std::collections::HashMap::new();
    for (key, event) in &listed {
        ids_of.entry(key.to_string()).or_default().push(*event);
    }
    let ids = listed
        .iter()
        .map(|(_, event)| sinex_model::EventId(*event))
        .collect();
    let mut live = std::collections::HashMap::new();
    let mut reader = derivations::ListedInputs::new(ids, ROWS_PER_PAGE, sinex_store::PAGE_BYTES)?;
    while let Some(page) = reader.next(conn).await? {
        for row in page {
            live.insert(row.event.id.0, row);
        }
    }
    let mut inputs = Vec::with_capacity(keys.len());
    // Keys of one group often add the same query (the boot records of one
    // host): it is read once.
    let mut added: std::collections::HashMap<Window, Vec<derivations::Committed>> =
        std::collections::HashMap::new();
    for key in keys {
        let mut rows: Vec<derivations::Committed> = ids_of
            .get(&key.to_string())
            .into_iter()
            .flatten()
            .filter_map(|id| live.get(id).cloned())
            .collect();
        if let Some(query) = derivation.inputs(key, env) {
            let window = to_window(&query, env.now);
            let read = match added.remove(&window) {
                Some(read) => read,
                None => derivations::read_window(conn, &window).await?,
            };
            let listed: HashSet<_> = rows.iter().map(|row| row.event.id).collect();
            rows.extend(
                read.iter()
                    .filter(|row| !listed.contains(&row.event.id))
                    .cloned(),
            );
            added.insert(window, read);
        }
        rows.sort_by_key(|row| (row.event.ts_orig.at, row.commit_seq));
        inputs.push(rows.into_iter().map(|row| row.event).collect());
    }
    Ok(inputs)
}

/// Exact targets use the same snapshot as the key's read mark, including
/// unknown-time records. Transfer budgets never substitute a nearby record.
async fn exact_inputs(
    conn: &mut PgConnection,
    ids: Vec<sinex_model::EventId>,
) -> Result<Vec<Event>> {
    let mut reader = derivations::ListedInputs::new(ids, ROWS_PER_PAGE, sinex_store::PAGE_BYTES)?;
    let mut inputs = Vec::new();
    while let Some(page) = reader.next(conn).await? {
        inputs.extend(page.into_iter().map(|row| row.event));
    }
    Ok(inputs)
}
