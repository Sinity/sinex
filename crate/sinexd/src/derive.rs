//! The automaton driver (architecture §6.2). It runs outside the writer task:
//! it reads with its own connection, computes, and hands the writer one
//! `Derived` batch per key (or per run of committed events), which admission
//! stores under the same privacy, schema and identity rules as captured
//! events.
//!
//! - **Keyed derivations:** admission marks keys dirty; the driver takes dirty
//!   keys oldest first, reads each key's current inputs, computes, and submits
//!   the outputs as the key's complete output set. The dirty mark clears in
//!   the same transaction unless the key was dirtied again meanwhile. Keys whose
//!   outputs wait on evidence outside their window are recomputed whenever
//!   their derivation runs again.
//! - **Per-event derivations:** read committed events after their position, in
//!   commit order, and commit the new position with their outputs.
//!
//! Derivations run in the order their inputs and outputs give: a derivation
//! whose inputs another one produces runs after it, so a daily summary is
//! computed once the commands it counts have been fused, not again for every
//! intermediate state. A pass works on the lowest rank that has work; one pass
//! in [`FAIR_EVERY`] works every rank, so a busy upstream cannot keep a
//! downstream derivation from running.
//!
//! A key waiting on evidence outside its window stops waiting once it has
//! waited [`WAITING_HORIZON`] (a systemd unit that never stops would otherwise
//! be recomputed forever); its last outputs stand, and a later input in its
//! window still dirties it as usual.
//!
//! The driver wakes on `wake`, which the daemon signals after every commit, and
//! also looks for work every so often, for keys that settle with time.
use crate::writer::{Priority, WriterHandle};
use serde_json::Value;
use sinex_automata::{AnyKeyed, AnyPerEvent, Env, InputQuery, KeyView, Neighbour};
use sinex_model::civil::Civil;
use sinex_model::{Batch, BatchKind, Event, ReadingOrder, RecordOutcome, RunId, Timestamp};
use sinex_store::derived::{self, Match, Nearest, Registered, Window};
use sinex_store::{DerivedScope, runs};
use sqlx::PgConnection;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Dirty keys taken per pass.
const DIRTY_PER_PASS: i64 = 512;
/// Committed events a per-event derivation reads per pass.
const EVENTS_PER_PASS: i64 = 2_000;
/// Keys marked dirty per recompute request.
const KEYS_PER_REQUEST: usize = 5_000;
/// One pass in this many works every rank, not only the lowest with work.
pub const FAIR_EVERY: u32 = 8;
/// A key that has waited this long stops waiting.
pub const WAITING_HORIZON: time::Duration = time::Duration::days(14);

/// Counts for one derivation.
#[derive(Clone, Debug, Default)]
pub struct Counts {
    pub keys: usize,
    pub inputs: usize,
    pub outputs: usize,
    pub admitted: usize,
    pub repeats: usize,
    pub replaced: usize,
    pub compute: Duration,
    pub read: Duration,
    pub write: Duration,
}

impl std::fmt::Display for Counts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "keys {} | inputs read {} | outputs {} | admitted {} | repeats {} | replaced {} | read {:.2}s | compute {:.2}s | write {:.2}s",
            self.keys,
            self.inputs,
            self.outputs,
            self.admitted,
            self.repeats,
            self.replaced,
            self.read.as_secs_f64(),
            self.compute.as_secs_f64(),
            self.write.as_secs_f64(),
        )
    }
}

pub struct Driver {
    keyed: Vec<Box<dyn AnyKeyed>>,
    per_event: Vec<Box<dyn AnyPerEvent>>,
    /// Rank of each keyed and per-event derivation, by name: 0 for those
    /// reading only captured events, one more than the highest rank of any
    /// derivation producing an input.
    ranks: BTreeMap<&'static str, u32>,
    passes: u32,
    /// The time derivations see; the current time unless a test sets it.
    pub(crate) clock: Option<Timestamp>,
    civil: Civil,
    read: PgConnection,
    writer: WriterHandle,
    runs: BTreeMap<&'static str, RunId>,
    pub counts: BTreeMap<&'static str, Counts>,
}

fn to_window(q: &InputQuery) -> Window {
    let matching = |equal: &[(&'static str, Value)]| {
        equal
            .iter()
            .map(|(f, v)| match v {
                Value::Array(values) => Match::OneOf(
                    f,
                    values
                        .iter()
                        .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_owned))
                        .collect(),
                ),
                other => Match::Equals(f, other.clone()),
            })
            .collect()
    };
    let nearest = |n: &Neighbour| Nearest {
        event_types: n.event_types.iter().map(|s| (*s).to_owned()).collect(),
        matching: matching(&n.equal),
        per: n.per,
        count: i64::from(n.count),
    };
    Window {
        event_types: q.event_types.iter().map(|s| (*s).to_owned()).collect(),
        start: q.window.0.0,
        end: q.window.1.0,
        matching: matching(&q.equal),
        before: q.before.iter().map(nearest).collect(),
        after: q.after.iter().map(nearest).collect(),
    }
}

impl Driver {
    /// Register the derivation catalog, start one run per derivation, and
    /// queue a full recompute for every derivation whose version changed.
    pub async fn start(
        mut read: PgConnection,
        writer: WriterHandle,
        keyed: Vec<Box<dyn AnyKeyed>>,
        per_event: Vec<Box<dyn AnyPerEvent>>,
        host: &str,
    ) -> Result<Self, String> {
        let catalog: Vec<Registered> = keyed
            .iter()
            .map(|d| Registered {
                name: d.name().to_owned(),
                kind: "keyed",
                version: i32::try_from(d.version()).unwrap_or(i32::MAX),
                inputs: d.inputs_types().iter().map(|s| (*s).to_owned()).collect(),
                outputs: d.output_types().iter().map(|s| (*s).to_owned()).collect(),
            })
            .chain(per_event.iter().map(|d| Registered {
                name: d.name().to_owned(),
                kind: "per_event",
                version: i32::try_from(d.version()).unwrap_or(i32::MAX),
                inputs: d.inputs_types().iter().map(|s| (*s).to_owned()).collect(),
                outputs: d.output_types().iter().map(|s| (*s).to_owned()).collect(),
            }))
            .collect();
        let changed = derived::register(&mut read, &catalog)
            .await
            .map_err(|e| e.to_string())?;
        let mut runs = BTreeMap::new();
        for d in &catalog {
            let name = derived::static_type(&d.name);
            let version = d.version.to_string();
            let emits: Vec<&str> = d.outputs.iter().map(String::as_str).collect();
            let manifest = runs::Manifest {
                name,
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
        let mut driver = Self {
            keyed,
            per_event,
            ranks,
            passes: 0,
            clock: None,
            civil: Civil::operator_default(),
            read,
            writer,
            runs,
            counts: BTreeMap::new(),
        };
        for name in changed {
            if driver.keyed.iter().any(|d| d.name() == name) {
                driver.recompute_all(&name, None).await?;
            } else {
                derived::reset_position(&mut driver.read, &name)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        Ok(driver)
    }

    fn env(&self) -> Env {
        Env {
            now: self.clock.unwrap_or_else(Timestamp::now),
            civil: self.civil.clone(),
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
            let page = derived::after_seq(&mut self.read, &types, after, 10_000)
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
                    event_type: c.event.event_type,
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

    /// Mark every key of `name` dirty, through the writer.
    pub async fn recompute_all(
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
    ) -> Result<sinex_automata::Computed, String> {
        let d = &self.keyed[index];
        let name = d.name();
        let Some(q) = d.inputs(key, env) else {
            return Ok(sinex_automata::Computed::default());
        };
        let t = Instant::now();
        let inputs: Vec<Event> = derived::read_window(&mut self.read, &to_window(&q))
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|c| c.event)
            .collect();
        let read = t.elapsed();
        let t = Instant::now();
        let computed = self.keyed[index].compute(key, &inputs, env);
        let c = self.counts.entry(name).or_default();
        c.read += read;
        c.compute += t.elapsed();
        c.inputs += inputs.len();
        Ok(computed)
    }

    /// Recompute one key and submit its complete output set.
    async fn settle_key(
        &mut self,
        index: usize,
        key: Value,
        dirty_seq: i64,
        give_up: bool,
        env: &Env,
    ) -> Result<(), String> {
        let name = self.keyed[index].name();
        let mut computed = self.compute_key(index, &key, env).await?;
        if give_up {
            computed.waiting = false;
        }
        let head = derived::head(&mut self.read)
            .await
            .map_err(|e| e.to_string())?;
        let mut outputs = computed.outputs;
        for e in &mut outputs {
            // A recompute is a newer reading than any before it.
            e.reading_order = ReadingOrder(head);
        }
        let n = outputs.len();
        let batch = Batch {
            run: self.run_of(name)?,
            kind: BatchKind::Derived {
                derivation: name.to_owned(),
                scope: DerivedScope::Key {
                    key,
                    dirty_seq,
                    waiting: computed.waiting,
                }
                .to_value(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: outputs,
            }],
            archive: vec![],
        };
        let t = Instant::now();
        let out = self
            .writer
            .submit(Priority::Derived, batch)
            .await
            .map_err(|e| format!("{name}: {e}"))?;
        let c = self.counts.entry(name).or_default();
        c.write += t.elapsed();
        c.keys += 1;
        c.outputs += n;
        c.admitted += out.admitted;
        c.repeats += out.repeats;
        c.replaced += out.replaced;
        Ok(())
    }

    /// One pass over the lowest rank with work (every rank, one pass in
    /// [`FAIR_EVERY`]): its dirty keys, then the waiting keys of the
    /// derivations that ran, then its per-event derivations. Returns how much
    /// work it did.
    pub async fn pass(&mut self) -> Result<usize, String> {
        let env = self.env();
        self.passes = self.passes.wrapping_add(1);
        let every_rank = self.passes % FAIR_EVERY == 0;
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
            let dirty = derived::dirty(&mut self.read, &names, DIRTY_PER_PASS)
                .await
                .map_err(|e| e.to_string())?;
            let mut touched: BTreeMap<usize, HashSet<String>> = BTreeMap::new();
            for k in dirty {
                let Some(index) = self.keyed.iter().position(|d| d.name() == k.derivation) else {
                    continue;
                };
                touched.entry(index).or_default().insert(k.key.to_string());
                self.settle_key(index, k.key, k.last_dirty_seq, false, env)
                    .await?;
                work += 1;
            }
            for (index, done) in touched {
                let name = self.keyed[index].name();
                let waiting = derived::waiting(&mut self.read, name)
                    .await
                    .map_err(|e| e.to_string())?;
                for (key, since) in waiting {
                    if done.contains(&key.to_string()) {
                        continue;
                    }
                    let give_up = since.0 < env.now.0 - WAITING_HORIZON;
                    // Not a dirty mark: clear nothing, only refresh the outputs.
                    self.settle_key(index, key, 0, give_up, env).await?;
                    work += 1;
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
        let version = i64::from(d.version());
        let types: Vec<String> = d.inputs_types().iter().map(|s| (*s).to_owned()).collect();
        let position = derived::position(&mut self.read, name)
            .await
            .map_err(|e| e.to_string())?;
        let events = derived::after_seq(&mut self.read, &types, position, EVENTS_PER_PASS)
            .await
            .map_err(|e| e.to_string())?;
        let Some(last) = events.last().map(|c| c.commit_seq) else {
            return Ok(0);
        };
        let mut outputs = Vec::new();
        for c in &events {
            for mut e in self.per_event[index].derive(&c.event, env) {
                // A new derivation version reads every input again; its
                // outputs must be newer readings than the old version's.
                e.reading_order = ReadingOrder((version << 40) | c.commit_seq);
                outputs.push(e);
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
        let c = self.counts.entry(name).or_default();
        c.inputs += events.len();
        c.outputs += n;
        c.admitted += out.admitted;
        c.repeats += out.repeats;
        Ok(events.len())
    }

    /// Run passes until one does no work.
    pub async fn settle(&mut self) -> Result<(), String> {
        while self.pass().await? > 0 {}
        Ok(())
    }

    /// The daemon's driver task: a pass whenever woken, or every `idle`. The
    /// daemon signals `wake` after each commit.
    pub async fn run(mut self, wake: Arc<Notify>, idle: Duration) -> Result<(), String> {
        loop {
            if self.pass().await? == 0 {
                tokio::select! {
                    () = wake.notified() => {}
                    () = tokio::time::sleep(idle) => {}
                }
            }
        }
    }

    pub async fn end_runs(&mut self) -> Result<(), String> {
        for run in self.runs.values() {
            runs::end(&mut self.read, *run)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Compute `name` over its keys (optionally only keys touched by inputs in
    /// `range`) and compare with the stored outputs, writing nothing: the
    /// comparison lane for a derivation change.
    pub async fn compare(
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
            let old = derived::outputs(&mut self.read, name, &key)
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
                    None => cmp.note(Kind::Added, &key, None, Some(n), examples),
                    Some(o) if o.payload == n.payload => cmp.same += 1,
                    Some(o) => cmp.note(Kind::Changed, &key, Some(o), Some(n), examples),
                }
            }
            for (k, o) in &old {
                if !new.contains_key(k) {
                    cmp.note(Kind::Removed, &key, Some(o), None, examples);
                }
            }
        }
        let count = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        cmp.id = Some(
            derived::store_comparison(
                &mut self.read,
                &derived::StoredComparison {
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
fn ranks(catalog: &[Registered]) -> BTreeMap<&'static str, u32> {
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
        .map(|(name, r)| (derived::static_type(name), r))
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Added,
    Changed,
    Removed,
}

/// What a recompute would change.
#[derive(Debug, Default)]
pub struct Comparison {
    /// The stored comparison's ID.
    pub id: Option<i64>,
    pub keys: usize,
    pub same: usize,
    pub added: usize,
    pub changed: usize,
    pub removed: usize,
    /// (kind, key, old payload, new payload) for the first few of each kind.
    pub examples: Vec<(Kind, Value, Option<Value>, Option<Value>)>,
}

impl Comparison {
    fn note(
        &mut self,
        kind: Kind,
        key: &Value,
        old: Option<&Event>,
        new: Option<&Event>,
        examples: usize,
    ) {
        let count = match kind {
            Kind::Added => &mut self.added,
            Kind::Changed => &mut self.changed,
            Kind::Removed => &mut self.removed,
        };
        *count += 1;
        if self.examples.iter().filter(|e| e.0 == kind).count() < examples {
            self.examples.push((
                kind,
                key.clone(),
                old.map(|e| e.payload.clone()),
                new.map(|e| e.payload.clone()),
            ));
        }
    }
}

impl std::fmt::Display for Comparison {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(id) = self.id {
            writeln!(f, "comparison {id}")?;
        }
        writeln!(
            f,
            "keys {} | outputs unchanged {} | added {} | changed {} | removed {}",
            self.keys, self.same, self.added, self.changed, self.removed
        )?;
        for (kind, key, old, new) in &self.examples {
            writeln!(f, "{kind:?} at {key}")?;
            if let Some(old) = old {
                writeln!(f, "  old {old}")?;
            }
            if let Some(new) = new {
                writeln!(f, "  new {new}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "derive_tests.rs"]
mod tests;
