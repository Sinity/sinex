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
//! A key waiting on evidence outside its window stops waiting once it has
//! waited [`WAITING_HORIZON`] (a systemd unit that never stops would otherwise
//! be recomputed forever); its last outputs stand, and a later input in its
//! window still dirties it as usual.
//!
//! The runner wakes on `wake`, which the daemon signals after every commit, and
//! also looks for work every so often, for keys that settle with time.
use crate::writer::{Priority, WriterHandle};
use serde_json::Value;
use sinex_derivations::{AnyKeyed, AnyPerEvent, Env, Input, InputQuery, KeyView, Neighbour};
use sinex_model::time_settings::TimeSettings;
use sinex_model::{Batch, BatchKind, Event, ReadingOrder, RecordOutcome, RunId, Timestamp};
use sinex_store::blobs::Blobs;
use sinex_store::derivations::{self, Nearest, Window};
use sinex_store::registry::{self, Derivation};
use sinex_store::{ComputedKey, DerivedScope, material_bytes, runs};
use sqlx::PgConnection;
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
/// A key to compute: the key, the dirty mark it was computed for (0 when it
/// is a waiting key being refreshed), and whether it stops waiting.
type KeyWork = (Value, i64, bool);

/// Keys marked dirty per recompute request.
const KEYS_PER_REQUEST: usize = 5_000;
/// One pass in this many works every rank, not only the lowest with work.
const EVERY_LEVEL_EVERY: u32 = 8;
/// A key that has waited this long stops waiting.
const WAITING_HORIZON: time::Duration = time::Duration::days(14);

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
    pub counts: BTreeMap<String, Counts>,
}

fn to_window(q: &InputQuery) -> Window {
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
        kinds: kinds(&q.kinds),
        start: q.window.0.0,
        end: q.window.1.0,
        before: q.before.iter().map(nearest).collect(),
        after: q.after.iter().map(nearest).collect(),
    }
}

impl DerivationRunner {
    /// Register the derivation catalog, start one run per derivation, and
    /// queue a full recompute for every derivation whose version changed.
    pub(crate) async fn start(
        mut read: PgConnection,
        writer: WriterHandle,
        keyed: Vec<Box<dyn AnyKeyed>>,
        per_event: Vec<Box<dyn AnyPerEvent>>,
        host: &str,
    ) -> Result<Self, String> {
        let catalog: Vec<Derivation> = keyed
            .iter()
            .map(|d| Derivation {
                name: d.name().to_owned(),
                kind: "keyed",
                version: i32::try_from(d.version()).unwrap_or(i32::MAX),
                inputs: d.inputs_types().iter().map(|s| (*s).to_owned()).collect(),
                outputs: d.output_types().iter().map(|s| (*s).to_owned()).collect(),
            })
            .chain(per_event.iter().map(|d| Derivation {
                name: d.name().to_owned(),
                kind: "per_event",
                version: i32::try_from(d.version()).unwrap_or(i32::MAX),
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
                build: option_env!("SINEX_BUILD").unwrap_or("dev"),
                emits: &emits,
            };
            let run = runs::start(&mut read, &manifest, host)
                .await
                .map_err(|e| e.to_string())?;
            runs.insert(name, run);
        }
        let ranks = ranks(&catalog);
        let mut runner = Self {
            keyed,
            per_event,
            blobs: None,
            ranks,
            passes: 0,
            clock: None,
            time_settings: TimeSettings::operator_default(),
            read,
            writer,
            runs,
            counts: BTreeMap::new(),
        };
        // A changed derivation's version is written once its recompute is
        // marked (dirty keys, or a per-event position reset), so a stop
        // part-way marks it all again at the next start.
        for d in &catalog {
            let current = registry::derivation_current(&mut runner.read, d)
                .await
                .map_err(|e| e.to_string())?;
            if !current {
                if d.kind == "keyed" {
                    runner.recompute_all(&d.name, None).await?;
                } else {
                    derivations::reset_position(&mut runner.read, &d.name)
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
            registry::write_derivation(&mut runner.read, d)
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
    /// `ts_orig` is in `range`.
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
    ) -> Result<Self, String> {
        let d = sinex_derivations::keyed()
            .into_iter()
            .find(|d| d.name() == name)
            .ok_or_else(|| format!("no keyed derivation {name}"))?;
        let version = d.version().to_string();
        let manifest = runs::Manifest {
            name: d.name(),
            kind: "derivation",
            version: &version,
            build: option_env!("SINEX_BUILD").unwrap_or("dev"),
            emits: d.output_types(),
        };
        let run = runs::start(&mut read, &manifest, host)
            .await
            .map_err(|e| e.to_string())?;
        let runs = BTreeMap::from([(d.name().to_string(), run)]);
        Ok(Self {
            keyed: vec![d],
            per_event: Vec::new(),
            blobs: None,
            ranks: BTreeMap::new(),
            passes: 0,
            clock: None,
            time_settings: TimeSettings::operator_default(),
            read,
            writer,
            runs,
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
        let d = &self.keyed[index];
        let name = d.name();
        let Some(q) = d.inputs(key, env) else {
            return Ok(sinex_derivations::Computed::default());
        };
        let t = Instant::now();
        let inputs: Vec<Event> = derivations::read_window(&mut self.read, &to_window(&q))
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|c| c.event)
            .collect();
        let read = t.elapsed();
        let t = Instant::now();
        let computed = self.keyed[index].compute(key, &inputs, env);
        if let Some(error) = &computed.error {
            return Err(error.to_string());
        }
        let c = self.counts.entry(name.to_owned()).or_default();
        c.read += read;
        c.compute += t.elapsed();
        c.inputs += inputs.len();
        Ok(computed)
    }

    /// Recompute these keys of one derivation and submit their complete
    /// output sets, several keys per writer batch.
    async fn compute_keys(
        &mut self,
        index: usize,
        work: Vec<KeyWork>,
        env: &Env,
    ) -> Result<(), String> {
        let mut scopes: Vec<ComputedKey> = Vec::new();
        let mut records: Vec<RecordOutcome> = Vec::new();
        let mut outputs = 0usize;
        let name = self.keyed[index].name();
        // Keys without a query have no inputs; the rest are read page by
        // page and computed as soon as their rows are complete.
        let mut ready: Vec<(usize, Vec<Event>, bool)> = Vec::new();
        let mut windows: Vec<Window> = Vec::new();
        let mut window_keys: Vec<usize> = Vec::new();
        for (i, (key, _, _)) in work.iter().enumerate() {
            match self.keyed[index].inputs(key, env) {
                Some(q) => {
                    windows.push(to_window(&q));
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
                    .next(&mut self.read)
                    .await
                    .map_err(|e| e.to_string())?;
                self.counts.entry(name.to_owned()).or_default().read += t.elapsed();
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
                let (key, dirty_seq, give_up) = &work[i];
                let t = Instant::now();
                let mut computed = if queried {
                    self.keyed[index].compute(key, &input, env)
                } else {
                    sinex_derivations::Computed::default()
                };
                if let Some(error) = &computed.error {
                    return Err(error.to_string());
                }
                let c = self.counts.entry(name.to_owned()).or_default();
                c.compute += t.elapsed();
                c.inputs += input.len();
                if *give_up {
                    computed.waiting = false;
                }
                outputs += computed.outputs.len();
                scopes.push(ComputedKey {
                    key: key.clone(),
                    dirty_seq: *dirty_seq,
                    waiting: computed.waiting,
                });
                records.push(RecordOutcome::Events {
                    location: None,
                    events: computed.outputs,
                });
                if scopes.len() >= KEYS_PER_BATCH || outputs >= OUTPUTS_PER_BATCH {
                    self.submit_keys(
                        index,
                        std::mem::take(&mut scopes),
                        std::mem::take(&mut records),
                    )
                    .await?;
                    outputs = 0;
                }
            }
        }
        if !scopes.is_empty() {
            self.submit_keys(index, scopes, records).await?;
        }
        Ok(())
    }

    async fn submit_keys(
        &mut self,
        index: usize,
        scopes: Vec<ComputedKey>,
        mut records: Vec<RecordOutcome>,
    ) -> Result<(), String> {
        let name = self.keyed[index].name();
        let head = derivations::head(&mut self.read)
            .await
            .map_err(|e| e.to_string())?;
        let mut n = 0;
        for record in &mut records {
            if let RecordOutcome::Events { events, .. } = record {
                n += events.len();
                for e in events {
                    // A recompute is a newer reading than any before it.
                    e.reading_order = ReadingOrder(head);
                }
            }
        }
        let keys = scopes.len();
        let batch = Batch {
            run: self.run_of(name)?,
            kind: BatchKind::Derived {
                derivation: name.to_owned(),
                scope: DerivedScope::Keys { keys: scopes }.to_value(),
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
        let c = self.counts.entry(name.to_owned()).or_default();
        c.write += t.elapsed();
        c.keys += keys;
        c.outputs += n;
        c.admitted += out.admitted;
        c.repeats += out.repeats;
        c.replaced += out.replaced;
        Ok(())
    }

    /// One pass over the lowest rank with work (every rank, one pass in
    /// [`EVERY_LEVEL_EVERY`]): its dirty keys, then the waiting keys of the
    /// derivations that ran, then its per-event derivations. Returns how much
    /// work it did.
    pub(crate) async fn pass(&mut self) -> Result<usize, String> {
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
                entry.1.push((k.key, k.last_dirty_seq, false));
                work += 1;
            }
            for (index, (done, mut keys)) in touched {
                let name = self.keyed[index].name();
                let waiting = derivations::waiting(&mut self.read, name)
                    .await
                    .map_err(|e| e.to_string())?;
                for (key, since) in waiting {
                    if done.contains(&key.to_string()) {
                        continue;
                    }
                    let give_up = since.0 < env.now.0 - WAITING_HORIZON;
                    // Not a dirty mark: clear nothing, only refresh the outputs.
                    keys.push((key, 0, give_up));
                    work += 1;
                }
                self.compute_keys(index, keys, env).await?;
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
        let version = i64::from(d.version());
        let types: Vec<String> = d.inputs_types().iter().map(|s| (*s).to_owned()).collect();
        let position = derivations::position(&mut self.read, name)
            .await
            .map_err(|e| e.to_string())?;
        let events = derivations::after_seq(
            &mut self.read,
            &types,
            position,
            EVENTS_PER_PASS,
            sinex_store::PAGE_BYTES,
        )
        .await
        .map_err(|e| e.to_string())?;
        let Some(last) = events.last().map(|c| c.commit_seq) else {
            return Ok(0);
        };
        let reads_material = self.per_event[index].reads_material();
        let mut outputs = Vec::new();
        let mut failed = 0;
        for c in &events {
            let material = if reads_material {
                match self.material_of(&c.event).await {
                    Ok(bytes) => Some(bytes),
                    Err(reason) => {
                        eprintln!("{name}: event {}: {reason}", c.event.id);
                        failed += 1;
                        continue;
                    }
                }
            } else {
                None
            };
            let input = Input {
                event: &c.event,
                material: material.as_deref(),
            };
            let d = &self.per_event[index];
            // A derivation that runs a local tool (OCR) blocks for a while;
            // keep it off the runtime's other tasks where the runtime allows.
            let derived = if reads_material
                && tokio::runtime::Handle::current().runtime_flavor()
                    == tokio::runtime::RuntimeFlavor::MultiThread
            {
                tokio::task::block_in_place(|| d.derive(input, env))
            } else {
                d.derive(input, env)
            };
            match derived {
                Ok(derived) => {
                    for mut e in derived {
                        // A new derivation version reads every input again; its
                        // outputs must be newer readings than the old version's.
                        e.reading_order = ReadingOrder((version << 40) | c.commit_seq);
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
                scope: DerivedScope::Position { position: last }.to_value(),
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
        Ok(events.len())
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
        let index = self
            .keyed
            .iter()
            .position(|d| d.name() == name)
            .ok_or_else(|| format!("no keyed derivation {name}"))?;
        let keys = self.all_keys(name, range).await?;
        let env = self.env();
        let mut cmp = Comparison {
            keys: keys.len(),
            ..Comparison::default()
        };
        let version = self.keyed[index].version();
        for key in keys {
            let new = self.compute_key(index, &key, &env).await?.outputs;
            let old = derivations::outputs(&mut self.read, name, &key)
                .await
                .map_err(|e| e.to_string())?;
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
        let count = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        cmp.id = Some(
            derivations::store_comparison(
                &mut self.read,
                &derivations::StoredComparison {
                    id: 0,
                    derivation: name.to_owned(),
                    version: i32::try_from(version).unwrap_or(i32::MAX),
                    range: range.map(|(from, until)| (Some(from), Some(until))),
                    made_at: Timestamp::now(),
                    keys: count(cmp.keys),
                    unchanged: count(cmp.same),
                    added: count(cmp.added),
                    changed: count(cmp.changed),
                    removed: count(cmp.removed),
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
        .map(|(name, r)| (name.to_string(), r))
        .collect()
}

mod comparison;
mod counts;

pub(crate) use comparison::{Change, Comparison};
pub(crate) use counts::Counts;

#[cfg(test)]
mod tests;

/// Every per-event derivation `sinexd` runs: the pure ones, plus screen text
/// when a local OCR engine is installed (said once when it is not).
pub(crate) fn per_event() -> Vec<Box<dyn AnyPerEvent>> {
    let mut all = sinex_derivations::per_event();
    match crate::screen_text::engine() {
        Ok(engine) => all.push(Box::new(crate::screen_text::ScreenText { engine })),
        Err(reason) => eprintln!("sinexd: screen text off: {reason}"),
    }
    all
}
