//! Derivations as pure functions. Two kinds:
//!
//! - a **per-event derivation** reads committed events in commit order and
//!   writes zero or more outputs for each (document text, OCR, mark linking);
//! - a **keyed derivation** keeps each key's outputs equal to a function of the
//!   key's current inputs (fused records, sessions, intervals, figures).
//!
//! Admission marks keys dirty when an input of a keyed derivation is admitted
//! or archived; the driver in `sinexd` recomputes only dirty keys and submits
//! the outputs as a `Derived` batch that replaces the key's previous outputs.
//! Order independence holds by construction: `compute` always sees the key's
//! full current input set, sorted by `ts_orig`.
use serde_json::Value;
use sinex_model::time_settings::TimeSettings;
use sinex_model::{Event, EventId, Timestamp};

pub mod calendar;
pub mod derived_event;
pub mod figures;
pub mod fusion;
pub mod health;
pub mod library;
pub mod presence;
pub mod session;
pub mod state_interval;

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
    pub kinds: Vec<Kind>,
    /// `ts_orig` window, inclusive start, exclusive end.
    pub window: (Timestamp, Timestamp),
    /// The latest events before the window: the state a key starts in.
    pub before: Vec<Neighbour>,
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
            kinds,
            window: (start, end),
            before: Vec::new(),
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
        event
            .ts_orig
            .at
            .is_some_and(|t| t >= self.window.0 && t < self.window.1)
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
    pub fn at(now: Timestamp) -> Self {
        Self {
            now,
            time_settings: TimeSettings::operator_default(),
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
    /// Registered name, also the `derivation.dirty` key.
    const NAME: &'static str;
    const VERSION: u32;
    const INPUTS: &'static [&'static str];
    const OUTPUTS: &'static [&'static str];

    /// The keys an input event affects. Windows are keyed by `ts_orig`, so a
    /// late or historical event lands in the right key.
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value>;

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery>;

    /// The key's outputs, as events derived from `inputs` (each names its
    /// parents). `inputs` are sorted by `ts_orig`, then commit order.
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed;

    fn key_state(&self, key: &Value, env: &Env) -> KeyState;
}

/// One input of a per-event derivation: the committed event, and the stored
/// bytes of the material it cites when the derivation reads them.
#[derive(Clone, Copy, Debug)]
pub struct Input<'a> {
    pub event: &'a Event,
    pub material: Option<&'a [u8]>,
}

pub trait PerEvent: Send + Sync {
    const NAME: &'static str;
    const VERSION: u32;
    const INPUTS: &'static [&'static str];
    const OUTPUTS: &'static [&'static str];
    /// Whether `derive` reads the bytes of the material the input cites
    /// (screen text from a screen still). The bytes are content-addressed and
    /// never change, so the output is still a function of the input.
    const READS_MATERIAL: bool = false;

    /// The input's outputs. An error fails this input alone; the driver counts
    /// it and moves on.
    fn derive(&self, input: Input<'_>, env: &Env) -> Result<Vec<Event>, String>;
}

/// A keyed derivation with its type erased, for the registry, admission's
/// dirty-key marking and the driver.
pub trait AnyKeyed: Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;
    fn inputs_types(&self) -> &'static [&'static str];
    fn output_types(&self) -> &'static [&'static str];
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value>;
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery>;
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed;
    fn key_state(&self, key: &Value, env: &Env) -> KeyState;
}

impl<K: Keyed> AnyKeyed for K {
    fn name(&self) -> &'static str {
        K::NAME
    }
    fn version(&self) -> u32 {
        K::VERSION
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
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        Keyed::compute(self, key, inputs, env)
    }
    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        Keyed::key_state(self, key, env)
    }
}

/// A per-event derivation with its type erased.
pub trait AnyPerEvent: Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;
    fn inputs_types(&self) -> &'static [&'static str];
    fn output_types(&self) -> &'static [&'static str];
    fn reads_material(&self) -> bool;
    fn derive(&self, input: Input<'_>, env: &Env) -> Result<Vec<Event>, String>;
}

impl<P: PerEvent> AnyPerEvent for P {
    fn name(&self) -> &'static str {
        P::NAME
    }
    fn version(&self) -> u32 {
        P::VERSION
    }
    fn inputs_types(&self) -> &'static [&'static str] {
        P::INPUTS
    }
    fn output_types(&self) -> &'static [&'static str] {
        P::OUTPUTS
    }
    fn reads_material(&self) -> bool {
        P::READS_MATERIAL
    }
    fn derive(&self, input: Input<'_>, env: &Env) -> Result<Vec<Event>, String> {
        PerEvent::derive(self, input, env)
    }
}

/// Every keyed derivation, in one list. Order matters only for reading the
/// list: the driver runs whatever is dirty.
pub fn keyed() -> Vec<Box<dyn AnyKeyed>> {
    let mut all: Vec<Box<dyn AnyKeyed>> = fusion::rules::RULES
        .iter()
        .map(|rule| Box::new(fusion::Fusion(rule)) as Box<dyn AnyKeyed>)
        .collect();
    all.extend::<[Box<dyn AnyKeyed>; 9]>([
        Box::new(library::LibraryItems),
        Box::new(session::ShellSessions),
        Box::new(state_interval::StateIntervals),
        Box::new(figures::HourlyMetrics),
        Box::new(figures::DailyMetrics),
        Box::new(presence::Presence),
        Box::new(health::HourlyFigures),
        Box::new(health::DailyFigures),
        Box::new(calendar::Instances),
    ]);
    all
}

/// Every pure per-event derivation. Those that need a local tool (screen text
/// through an OCR engine) are added by `sinexd`, which may run them.
pub fn per_event() -> Vec<Box<dyn AnyPerEvent>> {
    vec![Box::new(library::Hints)]
}

/// The parent IDs of a set of input events, in input order, without repeats.
pub fn parents_of<'a>(inputs: impl IntoIterator<Item = &'a Event>) -> Vec<EventId> {
    let mut out: Vec<EventId> = Vec::new();
    for e in inputs {
        if !out.contains(&e.id) {
            out.push(e.id);
        }
    }
    out
}
