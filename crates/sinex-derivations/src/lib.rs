//! Derivations as pure functions. Two kinds:
//!
//! - a **per-event derivation** reads committed events in commit order and
//!   writes zero or more outputs for each (document text, OCR, mark linking);
//! - a **keyed derivation** keeps each key's outputs equal to a function of the
//!   key's current inputs (fused records, sessions, activity spans, intervals,
//!   figures).
//!
//! Admission marks keys dirty when an input of a keyed derivation is admitted
//! or archived; the driver in `sinexd` recomputes only dirty keys and submits
//! the outputs as a `Derived` batch that replaces the key's previous outputs.
//! Order independence holds by construction: `compute` always sees the key's
//! full current input set, sorted by `ts_orig`.
use serde_json::Value;
use sinex_model::time_settings::TimeSettings;
use sinex_model::{Event, EventId, Timestamp};

pub mod activity;
pub mod attribution;
pub mod calendar;
pub mod conversation;
pub mod derived_event;
pub mod embedding;
pub mod figures;
pub mod fusion;
pub mod health;
pub mod library;
pub mod portfolio;
pub mod presence;
pub mod selected;
pub mod session;
pub mod state_interval;
pub mod tags;
pub mod work;

/// What a key function sees of an admitted or archived event.
#[derive(Clone, Copy, Debug)]
pub struct KeyView<'a> {
    pub event_type: &'a str,
    pub ts_orig: Option<Timestamp>,
    pub payload: &'a Value,
}

/// Which events `compute` reads for one key: live events of `kinds` whose
/// `ts_orig` is in `window`, plus the neighbours a key needs from outside its
/// window.
#[derive(Clone, Debug, PartialEq)]
pub struct InputQuery {
    /// Resolve time bounds from the selected live events before reading inputs.
    pub range: Option<InputRange>,
    pub unknown_times: bool,
    pub kinds: Vec<Kind>,
    /// `ts_orig` window, inclusive start, exclusive end; none reads all known times.
    pub window: Option<(Timestamp, Timestamp)>,
    /// The latest events before the window: the state a key starts in.
    pub before: Vec<Neighbour>,
    /// Earlier intervals that still overlap the window.
    pub intervals: Vec<(&'static str, sinex_model::clock::IntervalEnd)>,
    /// The earliest events after the window: what ends a state the key opened.
    pub after: Vec<Neighbour>,
}

impl InputQuery {
    /// Every event of `event_types` in the window.
    pub fn window(event_types: &[&str], start: Timestamp, end: Timestamp) -> Self {
        Self::of(
            event_types.iter().map(|t| Kind::all(t)).collect(),
            start,
            end,
        )
    }

    pub fn of(kinds: Vec<Kind>, start: Timestamp, end: Timestamp) -> Self {
        Self {
            range: None,
            unknown_times: false,
            kinds,
            window: Some((start, end)),
            before: Vec::new(),
            intervals: Vec::new(),
            after: Vec::new(),
        }
    }

    /// Every known-time event of these kinds, with no time boundary.
    pub fn all(kinds: Vec<Kind>) -> Self {
        Self {
            range: None,
            unknown_times: false,
            kinds,
            window: None,
            before: Vec::new(),
            intervals: Vec::new(),
            after: Vec::new(),
        }
    }

    /// Only events whose payload `field` equals `value`, whatever their type.
    #[must_use]
    pub fn only(mut self, field: &'static str, value: Value) -> Self {
        for k in &mut self.kinds {
            k.equal.push((field.into(), value.clone()));
        }
        self
    }

    /// Whether `event` is one the window part reads (neighbours aside): what
    /// the store's query selects, for code that filters events in memory.
    pub fn admits(&self, event: &Event) -> bool {
        ((self.unknown_times && self.window.is_none() && event.ts_orig.at.is_none())
            || event.ts_orig.at.is_some_and(|t| {
                self.window.is_none_or(|(start, end)| {
                    t < end
                        && (t >= start
                            || self.intervals.iter().any(|(kind, interval)| {
                                *kind == event.event_type
                                    && interval
                                        .at(t, &event.payload)
                                        .is_none_or(|end| end >= start)
                            }))
                })
            }))
            && self
                .kinds
                .iter()
                .any(|k| k.admits(&event.event_type, &event.payload))
    }

    /// The event types read, without repeats.
    pub fn event_types(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for k in &self.kinds {
            if !out.contains(&k.event_type) {
                out.push(k.event_type.clone());
            }
        }
        out
    }
}

/// A semantic range, resolved against the same snapshot as the inputs.
#[derive(Clone, Debug, PartialEq)]
pub enum InputRange {
    /// The connected run of ordered, non-overlapping windows containing `at`.
    Connected {
        kinds: Vec<Kind>,
        at: Timestamp,
        end_field: &'static str,
        gap_ns: i128,
    },
    /// From the first through the last selected event, including both.
    Extent { kinds: Vec<Kind> },
    /// A day's transitions with the last prior state and the first different
    /// state after it, per captured scope. Same-state refreshes stay in range.
    StateTransitions {
        start: Timestamp,
        end: Timestamp,
        kinds: Vec<Kind>,
        scope_field: &'static str,
        subject_fields: Vec<&'static str>,
    },
    /// From `start` until the next selected payload time, or `now` while open.
    UntilNext {
        start: Timestamp,
        /// Search for the boundary after this time, while reading from start.
        after: Option<Timestamp>,
        kinds: Vec<Kind>,
        field: &'static str,
    },
}

/// An input can change a key whose existing output overlaps this range.
#[derive(Clone, Debug)]
pub struct AffectedRange {
    /// A true field means the stored output is still open after its recorded end.
    pub open_field: Option<&'static str>,
    pub output_type: &'static str,
    pub start_field: &'static str,
    pub end_field: &'static str,
    pub start: Timestamp,
    pub end: Timestamp,
}

/// Events of one type, only those whose payload fields equal the given
/// values. Several kinds of one type read the events any of them admits.
#[derive(Clone, Debug, PartialEq)]
pub struct Kind {
    pub event_type: String,
    pub equal: Vec<(String, Value)>,
}

impl Kind {
    pub fn all(event_type: &str) -> Self {
        Self {
            event_type: event_type.into(),
            equal: Vec::new(),
        }
    }

    pub fn only(event_type: &str, equal: &[(&'static str, &'static str)]) -> Self {
        Self {
            event_type: event_type.into(),
            equal: equal
                .iter()
                .map(|(f, v)| ((*f).into(), Value::String((*v).to_owned())))
                .collect(),
        }
    }

    pub fn admits(&self, event_type: &str, payload: &Value) -> bool {
        self.event_type == event_type
            && self
                .equal
                .iter()
                .all(|(f, v)| payload.get(f).is_some_and(|p| p == v))
    }
}

/// One neighbour request: of `kinds`, the `count` nearest events to the
/// window edge; with `per` set, the `count` nearest for each value of that
/// payload field seen in the window.
#[derive(Clone, Debug, PartialEq)]
pub struct Neighbour {
    pub kinds: Vec<Kind>,
    pub per: Option<&'static str>,
    pub count: u32,
}

/// Whether a key can still receive inputs. Joins take closed, settled inputs
/// only; open keys are visible as provisional snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyState {
    Open,
    Closed,
}

/// Configuration and clocks a derivation may read, so `compute` stays pure.
#[derive(Clone, Debug)]
pub struct Env {
    pub now: Timestamp,
    /// The operator's zone and logical-day start, one setting.
    pub time_settings: TimeSettings,
    pub settings: Value,
}

impl Env {
    pub fn at(now: Timestamp, time_settings: TimeSettings) -> Self {
        Self {
            now,
            time_settings,
            settings: Value::Null,
        }
    }
}

/// What one key's computation produced.
#[derive(Clone, Debug, Default)]
pub struct Computed {
    pub error: Option<sinex_model::BuildError>,
    pub outputs: Vec<Event>,
    /// Some output still waits for evidence that may arrive later and outside
    /// the key's window (an interval still open, a session still growing).
    /// The driver recomputes such keys whenever the derivation runs again.
    pub waiting: bool,
}

pub trait Keyed: Send + Sync {
    const HISTORY: sinex_model::DerivationHistory = sinex_model::DerivationHistory::Recompute;
    /// Registered name, also the `derivation.dirty` key.
    const NAME: &'static str;
    const VERSION: u32;
    const USES_TIME_SETTINGS: bool = false;
    const INPUTS: &'static [&'static str];
    const OUTPUTS: &'static [&'static str];

    /// Exact input kind/version and computational payload footprint. None
    /// conservatively refuses inputs touched by transforming policy actions.
    fn input_requirements(&self) -> Option<Vec<sinex_model::InputRequirement>> {
        None
    }
    fn unavailable_inputs(
        &self,
        _: &Value,
        _: &[Event],
        _: &[sinex_model::UnavailableInput],
        _: &Env,
    ) -> Computed {
        Computed {
            waiting: true,
            ..Default::default()
        }
    }
    fn version(&self) -> u32 {
        Self::VERSION
    }

    /// The keys an input event affects. Windows are keyed by `ts_orig`, so a
    /// late or historical event lands in the right key.
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value>;

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery>;

    fn selected_query(
        &self,
        _: &Value,
        _: &Env,
    ) -> Option<sinex_model::event_types::analysis::QuerySelection> {
        None
    }
    /// Exact live readings required by a receipt, including readings with unknown time.
    fn exact_inputs(&self, _: &Value, _: &Env) -> Option<Vec<EventId>> {
        None
    }
    fn reads_type(&self, event_type: &str) -> bool {
        Self::INPUTS.contains(&event_type)
    }
    fn compute_selected(
        &self,
        key: &Value,
        inputs: &[Event],
        _: &selected::SelectedInputs,
        env: &Env,
    ) -> Computed {
        Keyed::compute(self, key, inputs, env)
    }

    fn affected_ranges(&self, _: &KeyView<'_>, _: &Env) -> Vec<AffectedRange> {
        Vec::new()
    }
    fn nearby_inputs(&self, _: &KeyView<'_>, _: &Env) -> Option<InputQuery> {
        None
    }
    fn refresh_at(&self, _: &[Event], _: &Env) -> Option<Timestamp> {
        None
    }

    /// The key's outputs, as events derived from `inputs` (each names its
    /// parents). `inputs` are sorted by `ts_orig`, then commit order.
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed;

    fn key_state(&self, key: &Value, env: &Env) -> KeyState;
}

/// One input of a per-event derivation: the committed event, and the stored
/// bytes the derivation reads for it ([`Reads`]), when there are any.
#[derive(Clone, Copy, Debug)]
pub struct Input<'a> {
    pub event: &'a Event,
    pub bytes: Option<&'a [u8]>,
}

/// Which stored bytes a per-event derivation reads for each input. Stored
/// bytes are content-addressed and never change, so the output is still a
/// function of the input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reads {
    Nothing,
    /// The material the input cites (a screen still).
    Material,
    /// The stored blob a payload field names (a library copy's `content`);
    /// none when the field is absent or the bytes were not stored.
    Blob(&'static str),
}

pub trait PerEvent: Send + Sync {
    const HISTORY: sinex_model::DerivationHistory = sinex_model::DerivationHistory::Recompute;
    const NAME: &'static str;
    const VERSION: u32;
    const INPUTS: &'static [&'static str];
    const OUTPUTS: &'static [&'static str];
    /// Exact input kind/version and computational payload footprint. None
    /// conservatively refuses inputs touched by transforming policy actions.
    fn input_requirements(&self) -> Option<Vec<sinex_model::InputRequirement>> {
        None
    }
    fn unavailable_inputs(
        &self,
        _: Input<'_>,
        _: &[sinex_model::UnavailableInput],
        _: &Env,
    ) -> Result<Vec<Event>, String> {
        Err("required_input_unavailable".into())
    }
    /// A tool's availability and version may change what this run can read.
    fn version(&self) -> u32 {
        Self::VERSION
    }
    /// The stored bytes `derive` reads for each input.
    const READS: Reads = Reads::Nothing;
    /// Select bytes before the runner reads them: a document reader does not
    /// read an image or video simply because both are library copies.
    fn reads_for(&self, _: &Event) -> Reads {
        Self::READS
    }

    /// The input's outputs. An error fails this input alone; the driver counts
    /// it and moves on.
    fn derive(&self, input: Input<'_>, env: &Env) -> Result<Vec<Event>, String>;
}

/// A keyed derivation with its type erased, for the registry, admission's
/// dirty-key marking and the driver.
pub trait AnyKeyed: Send + Sync {
    fn history_policy(&self) -> sinex_model::DerivationHistory {
        sinex_model::DerivationHistory::Recompute
    }
    /// Exact input kind/version and computational payload footprint. None
    /// conservatively refuses inputs touched by transforming policy actions.
    fn input_requirements(&self) -> Option<Vec<sinex_model::InputRequirement>> {
        None
    }
    fn unavailable_inputs(
        &self,
        _: &Value,
        _: &[Event],
        _: &[sinex_model::UnavailableInput],
        _: &Env,
    ) -> Computed {
        Computed {
            waiting: true,
            ..Default::default()
        }
    }
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;
    fn version_for(&self, _: &TimeSettings) -> u32 {
        self.version()
    }
    fn inputs_types(&self) -> &'static [&'static str];
    fn output_types(&self) -> &'static [&'static str];
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value>;
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery>;
    fn selected_query(
        &self,
        _: &Value,
        _: &Env,
    ) -> Option<sinex_model::event_types::analysis::QuerySelection> {
        None
    }
    fn exact_inputs(&self, _: &Value, _: &Env) -> Option<Vec<EventId>> {
        None
    }
    fn reads_type(&self, event_type: &str) -> bool {
        self.inputs_types().contains(&event_type)
    }
    fn compute_selected(
        &self,
        key: &Value,
        inputs: &[Event],
        _: &selected::SelectedInputs,
        env: &Env,
    ) -> Computed {
        self.compute(key, inputs, env)
    }

    fn affected_ranges(&self, _: &KeyView<'_>, _: &Env) -> Vec<AffectedRange> {
        Vec::new()
    }
    fn nearby_inputs(&self, _: &KeyView<'_>, _: &Env) -> Option<InputQuery> {
        None
    }
    fn refresh_at(&self, _: &[Event], _: &Env) -> Option<Timestamp> {
        None
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed;
    fn key_state(&self, key: &Value, env: &Env) -> KeyState;
}

impl<K: Keyed> AnyKeyed for K {
    fn input_requirements(&self) -> Option<Vec<sinex_model::InputRequirement>> {
        Keyed::input_requirements(self)
    }
    fn unavailable_inputs(
        &self,
        key: &Value,
        inputs: &[Event],
        issues: &[sinex_model::UnavailableInput],
        env: &Env,
    ) -> Computed {
        Keyed::unavailable_inputs(self, key, inputs, issues, env)
    }

    fn history_policy(&self) -> sinex_model::DerivationHistory {
        K::HISTORY
    }
    fn name(&self) -> &'static str {
        K::NAME
    }
    fn version(&self) -> u32 {
        Keyed::version(self)
    }
    fn version_for(&self, time_settings: &TimeSettings) -> u32 {
        if !K::USES_TIME_SETTINGS {
            return self.version();
        }
        let mut hash = blake3::Hasher::new();
        hash.update(&self.version().to_le_bytes());
        hash.update(time_settings.label().as_bytes());
        1 + (u32::from_le_bytes(hash.finalize().as_bytes()[..4].try_into().unwrap()) >> 2)
    }
    fn inputs_types(&self) -> &'static [&'static str] {
        K::INPUTS
    }
    fn output_types(&self) -> &'static [&'static str] {
        K::OUTPUTS
    }
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        Keyed::keys_for(self, input, env)
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        Keyed::inputs(self, key, env)
    }
    fn selected_query(
        &self,
        key: &Value,
        env: &Env,
    ) -> Option<sinex_model::event_types::analysis::QuerySelection> {
        Keyed::selected_query(self, key, env)
    }
    fn exact_inputs(&self, key: &Value, env: &Env) -> Option<Vec<EventId>> {
        Keyed::exact_inputs(self, key, env)
    }
    fn reads_type(&self, event_type: &str) -> bool {
        Keyed::reads_type(self, event_type)
    }
    fn compute_selected(
        &self,
        key: &Value,
        inputs: &[Event],
        selected: &selected::SelectedInputs,
        env: &Env,
    ) -> Computed {
        Keyed::compute_selected(self, key, inputs, selected, env)
    }
    fn affected_ranges(&self, input: &KeyView<'_>, env: &Env) -> Vec<AffectedRange> {
        Keyed::affected_ranges(self, input, env)
    }
    fn nearby_inputs(&self, input: &KeyView<'_>, env: &Env) -> Option<InputQuery> {
        Keyed::nearby_inputs(self, input, env)
    }
    fn refresh_at(&self, outputs: &[Event], env: &Env) -> Option<Timestamp> {
        Keyed::refresh_at(self, outputs, env)
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        Keyed::compute(self, key, inputs, env)
    }
    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        Keyed::key_state(self, key, env)
    }
}

/// A per-event derivation with its type erased.
pub trait AnyPerEvent: Send + Sync {
    fn input_requirements(&self) -> Option<Vec<sinex_model::InputRequirement>> {
        None
    }
    fn unavailable_inputs(
        &self,
        _: Input<'_>,
        _: &[sinex_model::UnavailableInput],
        _: &Env,
    ) -> Result<Vec<Event>, String> {
        Err("required_input_unavailable".into())
    }

    fn history_policy(&self) -> sinex_model::DerivationHistory {
        sinex_model::DerivationHistory::Recompute
    }
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;
    fn inputs_types(&self) -> &'static [&'static str];
    fn output_types(&self) -> &'static [&'static str];
    fn reads(&self, event: &Event) -> Reads;
    fn derive(&self, input: Input<'_>, env: &Env) -> Result<Vec<Event>, String>;
}

impl<P: PerEvent> AnyPerEvent for P {
    fn input_requirements(&self) -> Option<Vec<sinex_model::InputRequirement>> {
        PerEvent::input_requirements(self)
    }
    fn unavailable_inputs(
        &self,
        input: Input<'_>,
        issues: &[sinex_model::UnavailableInput],
        env: &Env,
    ) -> Result<Vec<Event>, String> {
        PerEvent::unavailable_inputs(self, input, issues, env)
    }

    fn history_policy(&self) -> sinex_model::DerivationHistory {
        P::HISTORY
    }
    fn name(&self) -> &'static str {
        P::NAME
    }
    fn version(&self) -> u32 {
        PerEvent::version(self)
    }
    fn inputs_types(&self) -> &'static [&'static str] {
        P::INPUTS
    }
    fn output_types(&self) -> &'static [&'static str] {
        P::OUTPUTS
    }
    fn reads(&self, event: &Event) -> Reads {
        PerEvent::reads_for(self, event)
    }
    fn derive(&self, input: Input<'_>, env: &Env) -> Result<Vec<Event>, String> {
        PerEvent::derive(self, input, env)
    }
}

/// What the configuration declares for derivations: the tag rules.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Configuration {
    #[serde(default)]
    pub tags: Vec<tags::TagRule>,
    #[serde(default)]
    pub activity: activity::ActivityProfile,
    #[serde(default)]
    pub project_attribution: attribution::AttributionProfile,
    #[serde(default)]
    pub rhythm: portfolio::rhythm::RhythmProfile,
}

/// Every keyed derivation, in dependency order.
pub fn keyed() -> Vec<Box<dyn AnyKeyed>> {
    keyed_with(&Configuration::default())
}

pub fn keyed_with(configuration: &Configuration) -> Vec<Box<dyn AnyKeyed>> {
    builtin(configuration)
}

fn builtin(configuration: &Configuration) -> Vec<Box<dyn AnyKeyed>> {
    let mut all: Vec<Box<dyn AnyKeyed>> = fusion::rules::RULES
        .iter()
        .map(|rule| Box::new(fusion::Fusion(rule)) as Box<dyn AnyKeyed>)
        .collect();
    all.extend::<[Box<dyn AnyKeyed>; 22]>([
        Box::new(library::LibraryItems),
        Box::new(session::ShellSessions),
        Box::new(activity::InputCoverage),
        Box::new(portfolio::input::InputWorkloads),
        Box::new(portfolio::rhythm::RhythmDeviations(
            configuration.rhythm.clone(),
        )),
        Box::new(work::WorkActs),
        Box::new(activity::ActivitySpans),
        Box::new(attribution::ProjectSpans(
            configuration.project_attribution.clone(),
        )),
        Box::new(activity::ActivityWindows),
        Box::new(activity::ActivitySessions(configuration.activity.clone())),
        Box::new(conversation::ConversationDays),
        Box::new(state_interval::StateIntervals),
        Box::new(figures::HourlyMetrics),
        Box::new(figures::DailyMetrics),
        Box::new(presence::Presence),
        Box::new(work::WorkEpisodes),
        Box::new(work::ContextPacks),
        Box::new(work::ActivityDays),
        Box::new(health::HourlyFigures),
        Box::new(health::DailyFigures),
        Box::new(calendar::Instances),
        Box::new(portfolio::marks::MarkLinks),
    ]);
    all
}

/// Every pure per-event derivation. Those that need a local tool (screen text
/// through an OCR engine) are added by `sinexd`, which may run them.
pub fn per_event() -> Vec<Box<dyn AnyPerEvent>> {
    per_event_with(&Configuration::default()).expect("empty rules are valid")
}

/// Pure per-event derivations using the declared rules.
pub fn per_event_with(configuration: &Configuration) -> Result<Vec<Box<dyn AnyPerEvent>>, String> {
    Ok(vec![
        Box::new(embedding::Embeddings),
        Box::new(library::Hints),
        Box::new(tags::TagApplier::new(configuration.tags.clone())?),
    ])
}

/// The tag applier runs only when explicitly enabled.
pub fn enabled_by_default(name: &str) -> bool {
    name != tags::TagApplier::NAME
}

/// The parent IDs of a set of input events, in input order, without repeats.
pub fn parents_of<'a>(inputs: impl IntoIterator<Item = &'a Event>) -> Vec<EventId> {
    let mut out: Vec<EventId> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for e in inputs {
        if seen.insert(e.id) {
            out.push(e.id);
        }
    }
    out
}
