//! The derivation runner. It runs outside the writer task: it reads with its
//! own connection, computes, and hands the writer one `Derived` batch per key
//! (or per run of committed events), which admission stores under the same
//! privacy, schema and identity rules as captured events.
//!
//! - **Keyed derivations:** admission marks keys dirty; the runner takes dirty
//!   keys oldest first, reads each key's current inputs, computes, and submits
//!   the outputs as the key's complete output set. The dirty mark clears in
//!   the same transaction unless the key was dirtied again meanwhile. Keys whose
//!   outputs wait on evidence outside their window are recomputed whenever
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
//! also looks for work every so often, for keys that settle with time.
use crate::writer::{Priority, WriterHandle};
use serde_json::Value;
use sinex_derivations::{AnyKeyed, AnyPerEvent, Env, Input, InputQuery, KeyView, Neighbour, Reads};
use sinex_model::time_settings::TimeSettings;
use sinex_model::{Batch, BatchKind, Event, ReadingOrder, RecordOutcome, RunId, Timestamp};
use sinex_store::blobs::Blobs;
use sinex_store::derivations::{self, Nearest, Window};
use sinex_store::registry::{self, Derivation};
use sinex_store::{ComputedKey, DerivedScope, ReadMark, material_bytes, runs};
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
/// Outputs per writer batch, whatever the number of keys.
const OUTPUTS_PER_BATCH: usize = 20_000;
/// Input rows read per query: keys are computed as their rows complete, so
/// memory holds a page and the keys still open, not every key's inputs.
const ROWS_PER_PAGE: i64 = 10_000;
/// A key and the dirty mark it was computed for (0 when only refreshed).
type KeyWork = (Value, i64);

/// Keys marked dirty per recompute request.
const KEYS_PER_REQUEST: usize = 5_000;
/// One pass in this many works every rank, not only the lowest with work.
const EVERY_LEVEL_EVERY: u32 = 8;

pub(crate) struct DerivationRunner {
    keyed: Vec<Box<dyn AnyKeyed>>,
    per_event: Vec<Box<dyn AnyPerEvent>>,
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
    /// Largest stored reading order when each per-event run began.
    previous_orders: BTreeMap<String, i64>,
    pub counts: BTreeMap<String, Counts>,
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
                scope_field,
                subject_fields,
            } => derivations::InputRange::StateTransitions {
                start: start.0,
                end: end.0,
                kinds: kinds(selected),
                scope_field,
                subject_fields: subject_fields.clone(),
                now: now.0,
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
    /// Register available producers and start runs without changing selected versions or progress.
    pub(crate) async fn start(
        mut read: PgConnection,
        writer: WriterHandle,
        keyed: Vec<Box<dyn AnyKeyed>>,
        per_event: Vec<Box<dyn AnyPerEvent>>,
        host: &str,
        time_settings: TimeSettings,
    ) -> Result<Self, String> {
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
        for d in &keyed {
            sinex_store::privacy::estimability::register(
                &mut read,
                d.name(),
                d.version_for(&time_settings),
                crate::build_identity::current()?,
                d.input_requirements().as_deref(),
            )
            .await
            .map_err(|e| e.to_string())?;
        }
        for d in &per_event {
            sinex_store::privacy::estimability::register(
                &mut read,
                d.name(),
                d.version(),
                crate::build_identity::current()?,
                d.input_requirements().as_deref(),
            )
            .await
            .map_err(|e| e.to_string())?;
        }
        let mut runs = BTreeMap::new();
        let mut previous_orders = BTreeMap::new();
        for d in &catalog {
            let name = d.name.clone();
            let version = d.version.to_string();
            let emits: Vec<&str> = d.outputs.iter().map(String::as_str).collect();
            let manifest = runs::Manifest {
                name: &name,
                kind: "derivation",
                version: &version,
                build: crate::build_identity::current()?,
                emits: &emits,
            };
            let run = runs::start(&mut read, &manifest, host)
                .await
                .map_err(|e| e.to_string())?;
            runs.insert(name.clone(), run);
            if d.kind == "per_event" {
                previous_orders.insert(
                    name.clone(),
                    derivations::latest_reading_order(&mut read, &name)
                        .await
                        .map_err(|e| e.to_string())?,
                );
            }
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
            previous_orders,
            counts: BTreeMap::new(),
        };
        for d in &catalog {
            registry::register_derivation(&mut runner.read, d, crate::build_identity::current()?)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(runner)
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
    ) -> Result<Vec<Value>, String> {
        let env = self.env();
        let d = self
            .keyed
            .iter()
            .find(|d| d.name() == name)
            .ok_or_else(|| format!("no keyed derivation {name}"))?;
        let types: Vec<String> = d.inputs_types().iter().map(|s| (*s).to_owned()).collect();
        let mut keys: Vec<Value> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut after = 0i64;
        loop {
            let page = derivations::after_seq(
                &mut self.read,
                &types,
                after,
                10_000,
                sinex_store::PAGE_BYTES,
            )
            .await
            .map_err(|e| e.to_string())?;
            let Some(last) = page.last() else { break };
            after = last.commit_seq;
            for c in &page {
                let t = c.event.ts_orig.at;
                if let Some((from, to)) = range
                    && !t.is_some_and(|t| t >= from && t < to)
                {
                    continue;
                }
                let view = KeyView {
                    event_type: &c.event.event_type,
                    ts_orig: t,
                    payload: &c.event.payload,
                };
                for k in d.keys_for(&view, &env) {
                    if seen.insert(k.to_string()) {
                        keys.push(k);
                    }
                }
            }
        }
        if range.is_none() {
            for k in derivations::output_keys(&mut self.read, name)
                .await
                .map_err(|e| e.to_string())?
            {
                if seen.insert(k.to_string()) {
                    keys.push(k);
                }
            }
        }
        Ok(keys)
    }

    /// A runner for one operation on one keyed derivation: a recompute or a
    /// comparison asked for through the command table. It registers nothing
    /// and starts one run, for that derivation.
    pub(crate) async fn for_one(
        mut read: PgConnection,
        writer: WriterHandle,
        name: &str,
        host: &str,
        configuration: &sinex_derivations::Configuration,
        time_settings: TimeSettings,
    ) -> Result<Self, String> {
        let keyed: Vec<_> = crate::analysis::keyed_with(configuration)
            .into_iter()
            .filter(|d| d.name() == name)
            .collect();
        let per_event: Vec<_> = per_event_with(configuration)?
            .into_iter()
            .filter(|d| d.name() == name)
            .collect();
        let (version, emits) = if let Some(d) = keyed.first() {
            (d.version_for(&time_settings), d.output_types())
        } else if let Some(d) = per_event.first() {
            (d.version(), d.output_types())
        } else {
            return Err(format!("no derivation {name}"));
        };
        let version = version.to_string();
        let manifest = runs::Manifest {
            name,
            kind: "derivation",
            version: &version,
            build: crate::build_identity::current()?,
            emits,
        };
        let run = runs::start(&mut read, &manifest, host)
            .await
            .map_err(|e| e.to_string())?;
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
            previous_orders: BTreeMap::new(),
            counts: BTreeMap::new(),
        })
    }

    /// The content store per-event derivations read stored bytes from.
    #[must_use]
    pub(crate) fn with_blobs(mut self, blobs: Blobs) -> Self {
        self.blobs = Some(blobs);
        self
    }

    /// Mark every key of `name` dirty, through the writer.
    pub(crate) async fn recompute_all(
        &mut self,
        name: &str,
        range: Option<(Timestamp, Timestamp)>,
    ) -> Result<usize, String> {
        let d = self
            .keyed
            .iter()
            .find(|d| d.name() == name)
            .ok_or_else(|| format!("no keyed derivation {name}"))?;
        if registry::selected(&mut self.read, name)
            .await
            .map_err(|e| e.to_string())?
            != Some((
                i64::from(d.version_for(&self.time_settings)),
                crate::build_identity::current()?.to_owned(),
            ))
        {
            return Err(
                "producer version changed; compare it and switch using its comparison ID".into(),
            );
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
                        policy_refresh: None,
                    }
                    .to_value(),
                },
                records: vec![],
                archive: vec![],
            };
            self.writer
                .submit(Priority::Derived, batch)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(total)
    }

    fn run_of(&self, name: &str) -> Result<RunId, String> {
        self.runs
            .get(name)
            .copied()
            .ok_or_else(|| format!("no run for derivation {name}"))
    }

    /// Read a key's inputs and compute it, without writing anything.
    async fn compute_key(
        &mut self,
        index: usize,
        key: &Value,
        env: &Env,
    ) -> Result<sinex_derivations::Computed, String> {
        sinex_store::query::initialize_calendar(&mut self.read)
            .await
            .map_err(|e| e.to_string())?;
        let mut snapshot = self.read.begin().await.map_err(|e| e.to_string())?;
        sinex_store::query::read_one_snapshot(&mut snapshot)
            .await
            .map_err(|e| e.to_string())?;
        let d = &self.keyed[index];
        let name = d.name();
        let t = Instant::now();
        let inputs = if let Some(ids) = d.exact_inputs(key, env) {
            exact_inputs(&mut snapshot, ids).await?
        } else {
            let Some(q) = d.inputs(key, env) else {
                return Ok(sinex_derivations::Computed::default());
            };
            derivations::read_window(&mut snapshot, &to_window(&q, env.now))
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|input| input.event)
                .collect()
        };
        sinex_store::privacy::comparisons::require_grants(&mut snapshot, &inputs)
            .await
            .map_err(|error| error.to_string())?;
        let read = t.elapsed();
        let t = Instant::now();
        let selected = selected::read(
            &mut snapshot,
            self.keyed[index].as_ref(),
            key,
            &inputs,
            env,
            true,
        )
        .await?;
        let issues = input_issues(
            &mut snapshot,
            &inputs,
            selected.as_ref(),
            self.keyed[index].input_requirements().as_deref(),
        )
        .await?;
        if !issues.is_empty() {
            return Err("comparison_required_input_unavailable".into());
        }
        let mut computed = match &selected {
            Some(selected) => self.keyed[index].compute_selected(key, &inputs, selected, env),
            None => self.keyed[index].compute(key, &inputs, env),
        };
        let position = derivations::head(&mut snapshot)
            .await
            .map_err(|error| error.to_string())?;
        selected::stamp(
            &mut snapshot,
            self.keyed[index].as_ref(),
            key,
            &inputs,
            selected.as_ref(),
            position,
            &mut computed,
        )
        .await?;
        if let Some(error) = &computed.error {
            return Err(error.to_string());
        }
        snapshot.commit().await.map_err(|e| e.to_string())?;
        let c = self.counts.entry(name.to_owned()).or_default();
        c.read += read;
        c.compute += t.elapsed();
        c.inputs += inputs.len();
        Ok(computed)
    }

    /// Recompute keys through the shared batched computation engine.
    async fn compute_keys(
        &mut self,
        index: usize,
        work: Vec<KeyWork>,
        env: &Env,
    ) -> Result<(), String> {
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
    pub(crate) async fn pass(&mut self) -> Result<usize, String> {
        let env = self.env();
        let policy = sinex_store::privacy::projection::epoch(&mut self.read)
            .await
            .map_err(|error| error.to_string())?;
        if !policy.ready() {
            return Ok(0);
        }
        let refreshed = self.refresh_policy(policy.active, &env).await?;
        self.passes = self.passes.wrapping_add(1);
        let every_rank = self.passes.is_multiple_of(EVERY_LEVEL_EVERY);
        let mut levels: Vec<u32> = self.ranks.values().copied().collect();
        levels.sort_unstable();
        levels.dedup();
        let mut work = refreshed;
        for level in levels {
            work += self.rank_pass(level, &env).await?;
            if work > 0 && !every_rank {
                break;
            }
        }
        Ok(work)
    }

    fn rank_of(&self, name: &str) -> u32 {
        self.ranks.get(name).copied().unwrap_or(0)
    }

    async fn rank_pass(&mut self, level: u32, env: &Env) -> Result<usize, String> {
        let names: Vec<String> = self
            .keyed
            .iter()
            .filter(|d| self.rank_of(d.name()) == level)
            .map(|d| d.name().to_owned())
            .collect();
        let mut work = 0;
        if !names.is_empty() {
            let dirty = derivations::dirty(&mut self.read, &names, DIRTY_PER_PASS)
                .await
                .map_err(|e| e.to_string())?;
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
                let had_dirty = touched.contains_key(&index);
                let (done, mut keys) = touched.remove(&index).unwrap_or_default();
                let waiting =
                    derivations::waiting(&mut self.read, name, (!had_dirty).then_some(env.now.0))
                        .await
                        .map_err(|e| e.to_string())?;
                for (key, _) in waiting {
                    if done.contains(&key.to_string()) {
                        continue;
                    }
                    keys.push((key, 0));
                    work += 1;
                }
                if !keys.is_empty() {
                    self.compute_keys(index, keys, env).await?;
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

    async fn per_event_pass(&mut self, index: usize, env: &Env) -> Result<usize, String> {
        let d = &self.per_event[index];
        let name = d.name();
        let version = d.version();
        let types: Vec<String> = d.inputs_types().iter().map(|s| (*s).to_owned()).collect();
        let position = derivations::position(&mut self.read, name)
            .await
            .map_err(|e| e.to_string())?;
        sinex_store::query::initialize_calendar(&mut self.read)
            .await
            .map_err(|error| error.to_string())?;
        let mut snapshot = self.read.begin().await.map_err(|error| error.to_string())?;
        sinex_store::query::read_one_snapshot(&mut snapshot)
            .await
            .map_err(|error| error.to_string())?;
        let read = ReadMark::now(&mut snapshot)
            .await
            .map_err(|e| e.to_string())?;
        let events = derivations::after_seq(
            &mut snapshot,
            &types,
            position,
            EVENTS_PER_PASS,
            sinex_store::PAGE_BYTES,
        )
        .await
        .map_err(|e| e.to_string())?;
        let last = events.last().map(|c| c.commit_seq);
        let head = derivations::head(&mut snapshot)
            .await
            .map_err(|e| e.to_string())?;
        let issues = sinex_store::privacy::estimability::unavailable(
            &mut snapshot,
            &events
                .iter()
                .map(|row| row.event.clone())
                .collect::<Vec<_>>(),
            d.input_requirements().as_deref(),
        )
        .await
        .map_err(|e| e.to_string())?;
        snapshot.commit().await.map_err(|error| error.to_string())?;
        let Some(last) = last else { return Ok(0) };
        let reading_order = self
            .previous_orders
            .get(name)
            .copied()
            .unwrap_or(0)
            .checked_add(head)
            .ok_or("derived reading order exhausted")?;
        let mut outputs = Vec::new();
        let mut computed_inputs = Vec::new();
        let mut failed = 0;
        for c in &events {
            let unavailable: Vec<_> = issues
                .iter()
                .filter(|issue| issue.event == c.event.id)
                .cloned()
                .collect();
            if !unavailable.is_empty() {
                match self.per_event[index].unavailable_inputs(
                    Input {
                        event: &c.event,
                        bytes: None,
                    },
                    &unavailable,
                    env,
                ) {
                    Ok(derived) => {
                        computed_inputs.push(c.event.id.0);
                        for mut e in derived {
                            hash_version(&mut e, name, version);
                            e.reading_order = ReadingOrder(reading_order);
                            outputs.push(e);
                        }
                    }
                    Err(_) => {
                        failed += 1;
                    }
                }
                continue;
            }
            let reads = self.per_event[index].reads(&c.event);
            if reads != Reads::Nothing {
                let Some(basis) = read.policy_epoch else {
                    failed += 1;
                    continue;
                };
                if !sinex_store::privacy::projection::raw_event_allowed(
                    &mut self.read,
                    c.event.id.0,
                    basis,
                )
                .await
                .map_err(|error| error.to_string())?
                {
                    failed += 1;
                    continue;
                }
            }
            let bytes = match self.bytes_of(reads, &c.event).await {
                Ok(bytes) => bytes,
                Err(reason) => {
                    eprintln!("{name}: event {}: {reason}", c.event.id);
                    failed += 1;
                    continue;
                }
            };
            let input = Input {
                event: &c.event,
                bytes: bytes.as_deref(),
            };
            let d = &self.per_event[index];
            // A derivation that reads stored bytes runs a local tool (OCR, PDF
            // text) and blocks for a while; keep it off the runtime's other
            // tasks where the runtime allows.
            let derived = if reads != Reads::Nothing
                && tokio::runtime::Handle::current().runtime_flavor()
                    == tokio::runtime::RuntimeFlavor::MultiThread
            {
                tokio::task::block_in_place(|| d.derive(input, env))
            } else {
                d.derive(input, env)
            };
            match derived {
                Ok(derived) => {
                    computed_inputs.push(c.event.id.0);
                    for mut e in derived {
                        // A new derivation version reads every input again; its
                        // outputs must be newer readings than the old version's.
                        hash_version(&mut e, name, version);
                        e.reading_order = ReadingOrder(reading_order);
                        outputs.push(e);
                    }
                }
                Err(reason) => {
                    eprintln!("{name}: event {}: {reason}", c.event.id);
                    failed += 1;
                }
            }
        }
        let n = outputs.len();
        let batch = Batch {
            run: self.run_of(name)?,
            kind: BatchKind::Derived {
                derivation: name.to_owned(),
                scope: DerivedScope::Position {
                    inputs: computed_inputs,
                    position: last,
                    read,
                }
                .to_value(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: outputs,
            }],
            archive: vec![],
        };
        let out = self
            .writer
            .submit(Priority::Derived, batch)
            .await
            .map_err(|e| format!("{name}: {e}"))?;
        let c = self.counts.entry(name.to_owned()).or_default();
        c.inputs += events.len();
        c.failed += failed;
        c.outputs += n;
        c.admitted += out.admitted;
        c.repeats += out.repeats;
        c.superseded_input += out.superseded_input;
        Ok(events.len())
    }

    /// The stored bytes a derivation that `reads` them reads for `event`.
    async fn bytes_of(&mut self, reads: Reads, event: &Event) -> Result<Option<Vec<u8>>, String> {
        match reads {
            Reads::Nothing => Ok(None),
            Reads::Material => self.material_of(event).await.map(Some),
            Reads::Blob(field) => {
                let Some(blob) = event.payload.get(field).and_then(Value::as_str) else {
                    return Ok(None);
                };
                let blobs = self
                    .blobs
                    .as_ref()
                    .ok_or("no content store was given to the runner")?;
                if !material_bytes::blob_stored(&mut self.read, blob)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    return Ok(None);
                }
                blobs.read(blob).map(Some).map_err(|e| e.to_string())
            }
        }
    }

    /// The stored bytes of the material `event` cites.
    async fn material_of(&mut self, event: &Event) -> Result<Vec<u8>, String> {
        let sinex_model::Origin::Record { location, .. } = &event.origin else {
            return Err("a derived event cites no material".into());
        };
        let blobs = self
            .blobs
            .as_ref()
            .ok_or("no content store was given to the runner")?;
        let blob = material_bytes::material_blob(&mut self.read, location.material)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("its material is not stored")?;
        blobs.read(&blob).map_err(|e| e.to_string())
    }

    /// Run passes until one does no work.
    pub(crate) async fn catch_up(&mut self) -> Result<(), String> {
        while self.pass().await? > 0 {}
        Ok(())
    }

    /// Finish the current pass on shutdown, then release the writer handle. Already submitted batches keep their acknowledgments.
    pub(crate) async fn run(
        mut self,
        wake: Arc<Notify>,
        idle: Duration,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), String> {
        loop {
            if *stop.borrow() || stop.has_changed().is_err() {
                break;
            }
            let work = match self.pass().await {
                Ok(work) => work,
                Err(error)
                    if error.contains("privacy_projection_stale")
                        || error.contains("derivation_policy_epoch_changed")
                        || error.contains("policy_refresh_changed") =>
                {
                    tokio::select! {
                        ()=tokio::time::sleep(idle)=>{}
                        _=stop.changed()=>{}
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
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

    pub(crate) async fn end_runs(&mut self) -> Result<(), String> {
        for run in self.runs.values() {
            runs::end(&mut self.read, *run)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Compute `name` over its keys (optionally only keys touched by inputs in
    /// `range`) and compare with the stored outputs, writing nothing: the
    /// comparison for a derivation change.
    pub(crate) async fn compare(
        &mut self,
        name: &str,
        range: Option<(Timestamp, Timestamp)>,
        examples: usize,
    ) -> Result<Comparison, String> {
        if !self.keyed.iter().any(|d| d.name() == name) {
            if range.is_some() {
                return Err("per-event comparisons cover the full producer history".into());
            }
            return self.compare_per_event(name, examples).await;
        }
        let index = self
            .keyed
            .iter()
            .position(|d| d.name() == name)
            .ok_or_else(|| format!("no keyed derivation {name}"))?;
        let input_types: Vec<String> = self.keyed[index]
            .inputs_types()
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect();
        let read_scope = sinex_store::privacy::comparisons::scope(&mut self.read)
            .await
            .map_err(|error| error.to_string())?;
        let input_state = derivations::selection::input_state(&mut self.read, &input_types)
            .await
            .map_err(|e| e.to_string())?;
        let keys = self.all_keys(name, range).await?;
        let env = self.env();
        let mut cmp = Comparison {
            keys: keys.len(),
            ..Comparison::default()
        };
        let version = self.keyed[index].version_for(&self.time_settings);
        for key in keys {
            let mut new = self.compute_key(index, &key, &env).await?.outputs;
            let new_changed =
                sinex_store::privacy::comparisons::project_new(&mut self.read, &mut new)
                    .await
                    .map_err(|error| error.to_string())?;
            let old = derivations::outputs(&mut self.read, name, &key)
                .await
                .map_err(|e| e.to_string())?;
            sinex_store::privacy::comparisons::require_grants(&mut self.read, &old)
                .await
                .map_err(|error| error.to_string())?;
            let key = sinex_store::privacy::comparisons::public_key(
                &mut self.read,
                &key,
                &old,
                new_changed,
            )
            .await
            .map_err(|error| error.to_string())?;
            let by_key = |events: &[Event]| -> BTreeMap<String, Event> {
                events
                    .iter()
                    .map(|e| {
                        (
                            e.equivalence_key.clone().map(|k| k.0).unwrap_or_default(),
                            e.clone(),
                        )
                    })
                    .collect()
            };
            let (new, old) = (by_key(&new), by_key(&old));
            for (k, n) in &new {
                match old.get(k) {
                    None => cmp.note(Change::Added, &key, None, Some(n), examples),
                    Some(o) if o.payload == n.payload => cmp.same += 1,
                    Some(o) => cmp.note(Change::Changed, &key, Some(o), Some(n), examples),
                }
            }
            for (k, o) in &old {
                if !new.contains_key(k) {
                    cmp.note(Change::Removed, &key, Some(o), None, examples);
                }
            }
        }
        if derivations::selection::input_state(&mut self.read, &input_types)
            .await
            .map_err(|e| e.to_string())?
            != input_state
        {
            return Err("producer inputs changed during comparison; compare again".into());
        }
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
                    context: serde_json::json!({"read_scope":read_scope,"build": crate::build_identity::current()?, "inputs": input_state, "input_types": input_types, "output_types": self.keyed[index].output_types(), "kind": "keyed", "history": self.keyed[index].history_policy(), "now": env.now, "settings": self.time_settings.label()}),
                    examples: serde_json::json!(
                        cmp.examples
                            .iter()
                            .map(|(kind, key, old, new)| serde_json::json!({
                                "kind": format!("{kind:?}").to_lowercase(),
                                "key": key,
                                "old": old,
                                "new": new,
                            }))
                            .collect::<Vec<_>>()
                    ),
                },
            )
            .await
            .map_err(|e| e.to_string())?,
        );
        Ok(cmp)
    }
    pub(crate) async fn switch(
        &mut self,
        name: &str,
        comparison: i64,
        actor: &str,
    ) -> Result<usize, String> {
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
                return Err(format!("no derivation {name}"));
            };
        let selection = derivations::selection::Selection {
            comparison,
            operation: uuid::Uuid::now_v7(),
            actor: actor.to_owned(),
            history,
            version: i64::from(version),
            build: crate::build_identity::current()?.to_owned(),
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
            .await
            .map_err(|e| e.to_string())?;
        Ok(total)
    }

    async fn compare_per_event(
        &mut self,
        name: &str,
        examples: usize,
    ) -> Result<Comparison, String> {
        let index = self
            .per_event
            .iter()
            .position(|d| d.name() == name)
            .ok_or_else(|| format!("no derivation {name}"))?;
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
        let read_scope = sinex_store::privacy::comparisons::scope(&mut self.read)
            .await
            .map_err(|error| error.to_string())?;
        let snapshot = derivations::selection::input_state(&mut self.read, &inputs)
            .await
            .map_err(|e| e.to_string())?;
        let mut cmp = Comparison::default();
        let env = self.env();
        let mut after = 0;
        loop {
            let page = derivations::after_seq(
                &mut self.read,
                &inputs,
                after,
                EVENTS_PER_PASS,
                sinex_store::PAGE_BYTES,
            )
            .await
            .map_err(|e| e.to_string())?;
            let Some(last) = page.last() else { break };
            after = last.commit_seq;
            for row in page {
                sinex_store::privacy::comparisons::require_grants(
                    &mut self.read,
                    std::slice::from_ref(&row.event),
                )
                .await
                .map_err(|error| error.to_string())?;
                let issues = sinex_store::privacy::estimability::unavailable(
                    &mut self.read,
                    std::slice::from_ref(&row.event),
                    self.per_event[index].input_requirements().as_deref(),
                )
                .await
                .map_err(|e| e.to_string())?;
                if !issues.is_empty() {
                    return Err("comparison_required_input_unavailable".into());
                }
                let reads = self.per_event[index].reads(&row.event);
                if reads != Reads::Nothing
                    && !sinex_store::privacy::projection::raw_event_allowed(
                        &mut self.read,
                        row.event.id.0,
                        read_scope.policy_epoch,
                    )
                    .await
                    .map_err(|error| error.to_string())?
                {
                    return Err("comparison_raw_input_unavailable".into());
                }
                let bytes = self.bytes_of(reads, &row.event).await?;
                let mut new = self.per_event[index].derive(
                    Input {
                        event: &row.event,
                        bytes: bytes.as_deref(),
                    },
                    &env,
                )?;
                sinex_store::privacy::comparisons::project_new(&mut self.read, &mut new)
                    .await
                    .map_err(|error| error.to_string())?;
                let old = derivations::children_outputs(&mut self.read, name, row.event.id)
                    .await
                    .map_err(|e| e.to_string())?;
                sinex_store::privacy::comparisons::require_grants(&mut self.read, &old)
                    .await
                    .map_err(|error| error.to_string())?;
                compare_outputs(
                    &mut cmp,
                    &serde_json::json!({"input": row.event.id}),
                    &old,
                    &new,
                    examples,
                );
                cmp.keys += 1;
            }
        }
        // An input removed after the read cannot leave a comparison eligible to switch.
        if derivations::selection::input_state(&mut self.read, &inputs)
            .await
            .map_err(|e| e.to_string())?
            != snapshot
        {
            return Err("producer inputs changed during comparison; compare again".into());
        }
        cmp.id = Some(derivations::store_comparison(&mut self.read, &derivations::StoredComparison {
            id: 0, derivation: name.to_owned(), version: i64::from(version), range: None, made_at: Timestamp::now(),
            keys: i64::try_from(cmp.keys).map_err(|e| e.to_string())?, unchanged: i64::try_from(cmp.same).map_err(|e| e.to_string())?,
            added: i64::try_from(cmp.added).map_err(|e| e.to_string())?, changed: i64::try_from(cmp.changed).map_err(|e| e.to_string())?, removed: i64::try_from(cmp.removed).map_err(|e| e.to_string())?,
            context: serde_json::json!({"read_scope":read_scope,"build": crate::build_identity::current()?, "inputs": snapshot, "input_types": inputs, "output_types": outputs, "kind": "per_event", "history": self.per_event[index].history_policy(), "now": env.now, "settings": self.time_settings.label()}),
            examples: serde_json::json!(cmp.examples.iter().map(|(kind,key,old,new)| serde_json::json!({"kind":format!("{kind:?}"),"key":key,"old":old,"new":new})).collect::<Vec<_>>()),
        }).await.map_err(|e| e.to_string())?);
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
/// (which no catalog should have) settles at the catalog's size.
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

mod policy;
mod selected;
pub(crate) use comparison::{Change, Comparison};
pub(crate) use counts::Counts;

#[cfg(test)]
mod tests;

/// Every available per-event derivation with the declared rules.
pub(crate) fn per_event_with(
    configuration: &sinex_derivations::Configuration,
) -> Result<Vec<Box<dyn AnyPerEvent>>, String> {
    let mut all = sinex_derivations::per_event_with(configuration)?;
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

/// Derivations enabled without configuration.
pub(crate) fn per_event() -> Vec<Box<dyn AnyPerEvent>> {
    per_event_with(&sinex_derivations::Configuration::default())
        .expect("empty rules are valid")
        .into_iter()
        .filter(|d| {
            d.name() == sinex_derivations::tags::TagApplier::NAME
                || sinex_derivations::enabled_by_default(d.name())
        })
        .collect()
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

impl KeyComputation<'_> {
    async fn compute(&mut self, work: Vec<KeyWork>, env: &Env) -> Result<(), String> {
        sinex_store::query::initialize_calendar(&mut self.read)
            .await
            .map_err(|error| error.to_string())?;
        let mut snapshot = self.read.begin().await.map_err(|error| error.to_string())?;
        sinex_store::query::read_one_snapshot(&mut snapshot)
            .await
            .map_err(|error| error.to_string())?;
        let result = KeyComputation {
            read: &mut snapshot,
            writer: self.writer,
            derivation: self.derivation,
            time_settings: self.time_settings.clone(),
            run: self.run,
            counts: &mut *self.counts,
        }
        .compute_snapshot(work, env)
        .await;
        match result {
            Ok(()) => snapshot.commit().await.map_err(|error| error.to_string()),
            Err(error) => match snapshot.rollback().await {
                Ok(()) => Err(error),
                Err(rollback) => Err(format!(
                    "{error}; rolling back the input snapshot: {rollback}"
                )),
            },
        }
    }

    async fn compute_snapshot(&mut self, work: Vec<KeyWork>, env: &Env) -> Result<(), String> {
        let mut scopes: Vec<ComputedKey> = Vec::new();
        let mut records: Vec<RecordOutcome> = Vec::new();
        let mut outputs = 0usize;
        let _lock = self.writer.lock_derivations().await;
        let read = ReadMark::now(&mut *self.read)
            .await
            .map_err(|e| e.to_string())?;
        let input_position = derivations::head(self.read)
            .await
            .map_err(|e| e.to_string())?;
        // Keys without a query have no inputs; the rest are read page by
        // page and computed as soon as their rows are complete.
        let mut ready: Vec<(usize, Vec<Event>, bool)> = Vec::new();
        let mut windows: Vec<Window> = Vec::new();
        let mut window_keys: Vec<usize> = Vec::new();
        for (i, (key, _)) in work.iter().enumerate() {
            if let Some(ids) = self.derivation.exact_inputs(key, env) {
                let inputs = exact_inputs(self.read, ids).await?;
                ready.push((i, inputs, true));
                continue;
            }
            match self.derivation.inputs(key, env) {
                Some(q) => {
                    windows.push(to_window(&q, env.now));
                    window_keys.push(i);
                }
                None => ready.push((i, Vec::new(), false)),
            }
        }
        let mut reader = derivations::Windows::new(windows, ROWS_PER_PAGE, sinex_store::PAGE_BYTES);
        loop {
            if ready.is_empty() {
                let t = Instant::now();
                let next = reader
                    .next(&mut *self.read)
                    .await
                    .map_err(|e| e.to_string())?;
                self.counts.read += t.elapsed();
                let Some(done) = next else { break };
                ready.extend(done.into_iter().map(|(w, rows)| {
                    (
                        window_keys[w],
                        rows.into_iter().map(|c| c.event).collect(),
                        true,
                    )
                }));
            }
            for (i, input, queried) in std::mem::take(&mut ready) {
                let (key, dirty_seq) = &work[i];
                let t = Instant::now();
                let selected =
                    selected::read(&mut *self.read, self.derivation, key, &input, env, true)
                        .await?;
                let issues = input_issues(
                    self.read,
                    &input,
                    selected.as_ref(),
                    self.derivation.input_requirements().as_deref(),
                )
                .await?;
                let mut computed = if !issues.is_empty() {
                    self.derivation
                        .unavailable_inputs(key, &input, &issues, env)
                } else {
                    match (&selected, queried) {
                        (Some(selected), _) => {
                            self.derivation.compute_selected(key, &input, selected, env)
                        }
                        (None, true) => self.derivation.compute(key, &input, env),
                        (None, false) => sinex_derivations::Computed::default(),
                    }
                };
                selected::stamp(
                    &mut *self.read,
                    self.derivation,
                    key,
                    &input,
                    selected.as_ref(),
                    input_position,
                    &mut computed,
                )
                .await?;
                if let Some(error) = &computed.error {
                    return Err(error.to_string());
                }
                let c = &mut *self.counts;
                c.compute += t.elapsed();
                c.inputs += input.len();
                c.failed += issues.len();
                outputs += computed.outputs.len();
                scopes.push(ComputedKey {
                    key: key.clone(),
                    dirty_seq: *dirty_seq,
                    waiting: computed.waiting,
                    refresh_at: self.derivation.refresh_at(&computed.outputs, env),
                });
                records.push(RecordOutcome::Events {
                    location: None,
                    events: computed.outputs,
                });
                if scopes.len() >= KEYS_PER_BATCH || outputs >= OUTPUTS_PER_BATCH {
                    self.submit(
                        std::mem::take(&mut scopes),
                        std::mem::take(&mut records),
                        read,
                    )
                    .await?;
                    outputs = 0;
                }
            }
        }
        if !scopes.is_empty() {
            self.submit(scopes, records, read).await?;
        }
        Ok(())
    }

    async fn submit(
        &mut self,
        scopes: Vec<ComputedKey>,
        mut records: Vec<RecordOutcome>,
        read: ReadMark,
    ) -> Result<(), String> {
        let name = self.derivation.name();
        let head = derivations::head(&mut *self.read)
            .await
            .map_err(|e| e.to_string())?;
        let mut n = 0;
        for record in &mut records {
            if let RecordOutcome::Events { events, .. } = record {
                n += events.len();
                for e in events {
                    // A recompute is a newer reading than any before it.
                    hash_version(e, name, self.derivation.version_for(&self.time_settings));
                    e.reading_order = ReadingOrder(head);
                }
            }
        }
        let keys = scopes.len();
        let batch = Batch {
            run: self.run,
            kind: BatchKind::Derived {
                derivation: name.to_owned(),
                scope: DerivedScope::Keys { keys: scopes, read }.to_value(),
            },
            records,
            archive: vec![],
        };
        let t = Instant::now();
        let out = self
            .writer
            .submit(Priority::Derived, batch)
            .await
            .map_err(|e| format!("{name}: {e}"))?;
        let c = &mut *self.counts;
        c.write += t.elapsed();
        c.keys += keys;
        c.outputs += n;
        c.admitted += out.admitted;
        c.repeats += out.repeats;
        c.replaced += out.replaced;
        c.superseded_input += out.superseded_input;
        Ok(())
    }
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
) -> Result<Counts, String> {
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
    let build = crate::build_identity::current()?;
    registry::register_derivation(conn, &available, build)
        .await
        .map_err(|error| error.to_string())?;
    sinex_store::privacy::estimability::register(
        conn,
        derivation.name(),
        derivation.version_for(&time_settings),
        build,
        derivation.input_requirements().as_deref(),
    )
    .await
    .map_err(|e| e.to_string())?;
    if registry::selected(conn, derivation.name())
        .await
        .map_err(|error| error.to_string())?
        != Some((available.version, build.to_owned()))
    {
        let mut dirty = Vec::new();
        for key in keys {
            if sinex_store::query::key_dirty(conn, derivation.name(), &key)
                .await
                .map_err(|error| error.to_string())?
            {
                dirty.push(key);
            }
        }
        if dirty.is_empty() {
            return Err(
                "producer history changed; compare and switch before recomputing unchanged keys"
                    .into(),
            );
        }
        keys = dirty;
    }
    let version = available.version.to_string();
    let manifest = runs::Manifest {
        name: derivation.name(),
        kind: "derivation",
        version: &version,
        build: crate::build_identity::current()?,
        emits: derivation.output_types(),
    };
    let run = runs::start(conn, &manifest, host)
        .await
        .map_err(|e| e.to_string())?;
    let result: Result<Counts, String> = async {
        let env = Env::at(Timestamp::now(), time_settings);
        let mut counts = Counts::default();
        let head = derivations::head(conn).await.map_err(|e| e.to_string())?;
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
    let ended = runs::end(conn, run).await.map_err(|e| e.to_string());
    let counts = result?;
    ended?;
    Ok(counts)
}

/// Exact targets use the same snapshot as the key's read mark, including
/// unknown-time evidence. Transfer budgets never substitute a nearby record.
async fn exact_inputs(
    conn: &mut PgConnection,
    ids: Vec<sinex_model::EventId>,
) -> Result<Vec<Event>, String> {
    let mut reader = derivations::ExactInputs::new(ids, ROWS_PER_PAGE, sinex_store::PAGE_BYTES)
        .map_err(|error| error.to_string())?;
    let mut inputs = Vec::new();
    while let Some(page) = reader.next(conn).await.map_err(|error| error.to_string())? {
        inputs.extend(page.into_iter().map(|row| row.event));
    }
    Ok(inputs)
}

async fn input_issues(
    conn: &mut PgConnection,
    inputs: &[Event],
    selected: Option<&sinex_derivations::selected::SelectedInputs>,
    requirements: Option<&[sinex_model::InputRequirement]>,
) -> Result<Vec<sinex_model::UnavailableInput>, String> {
    let mut issues = sinex_store::privacy::estimability::unavailable(conn, inputs, requirements)
        .await
        .map_err(|e| e.to_string())?;
    if let Some(selected) = selected {
        let ids: Vec<_> = selected
            .parents
            .iter()
            .filter(|id| !inputs.iter().any(|event| event.id == **id))
            .copied()
            .collect();
        let parents = exact_inputs(conn, ids.clone()).await?;
        if ids
            .iter()
            .any(|id| !parents.iter().any(|event| event.id == *id))
        {
            return Err("selected_required_input_projection_unavailable".into());
        }
        issues.extend(
            sinex_store::privacy::estimability::unavailable(conn, &parents, requirements)
                .await
                .map_err(|e| e.to_string())?,
        );
    }
    Ok(issues)
}
