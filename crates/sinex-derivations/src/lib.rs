//! Derivations as pure functions. Two kinds:
//!
//! - a **per-event derivation** reads committed events in commit order and
//!   writes zero or more outputs for each (document text, OCR, mark linking);
//! - a **keyed derivation** keeps each key's outputs equal to a function of the
//!   key's current inputs (fused commands, sessions, intervals, summaries).
//!
//! Admission marks keys dirty when an input of a keyed derivation is admitted
//! or archived; the driver in `sinexd` recomputes only dirty keys and submits
//! the outputs as a `Derived` batch that replaces the key's previous outputs.
//! Order independence holds by construction: `compute` always sees the key's
//! full current input set, sorted by `ts_orig`.
use serde_json::Value;
use sinex_model::civil::Civil;
use sinex_model::{Event, EventId, Timestamp};

pub mod derived_event;
pub mod fusion;
pub mod health;
pub mod presence;
pub mod session;
pub mod state_interval;
pub mod summary;

/// What a key function sees of an admitted or archived event.
#[derive(Clone, Copy, Debug)]
pub struct KeyView<'a> {
    pub event_type: &'a str,
    pub ts_orig: Option<Timestamp>,
    pub payload: &'a Value,
}

/// Which events `compute` reads for one key: live events of `event_types`
/// whose `ts_orig` is in `window` and whose payload contains `equal`, plus the
/// neighbours a key needs from outside its window.
#[derive(Clone, Debug, PartialEq)]
pub struct InputQuery {
    pub event_types: Vec<&'static str>,
    /// `ts_orig` window, inclusive start, exclusive end.
    pub window: (Timestamp, Timestamp),
    /// Payload fields that must equal these values.
    pub equal: Vec<(&'static str, Value)>,
    /// The latest events before the window: the state a key starts in.
    pub before: Vec<Neighbour>,
    /// The earliest events after the window: what ends a state the key opened.
    pub after: Vec<Neighbour>,
}

impl InputQuery {
    pub fn window(event_types: &[&'static str], start: Timestamp, end: Timestamp) -> Self {
        Self {
            event_types: event_types.to_vec(),
            window: (start, end),
            equal: Vec::new(),
            before: Vec::new(),
            after: Vec::new(),
        }
    }
}

/// One neighbour request: of `event_types` (and payload `equal`; an array
/// value means "one of"), the `count` nearest events to the window edge; with
/// `per` set, the `count` nearest for each value of that payload field seen
/// in the window.
#[derive(Clone, Debug, PartialEq)]
pub struct Neighbour {
    pub event_types: Vec<&'static str>,
    pub equal: Vec<(&'static str, Value)>,
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
    pub civil: Civil,
    pub settings: Value,
}

impl Env {
    pub fn at(now: Timestamp) -> Self {
        Self {
            now,
            civil: Civil::operator_default(),
            settings: Value::Null,
        }
    }
}

/// What one key's computation produced.
#[derive(Clone, Debug, Default)]
pub struct Computed {
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
    vec![
        Box::new(fusion::CommandFusion),
        Box::new(session::ShellSessions),
        Box::new(state_interval::StateIntervals),
        Box::new(summary::HourlySummary),
        Box::new(summary::DailySummary),
        Box::new(presence::Presence),
        Box::new(health::HourlyFigures),
        Box::new(health::DailyFigures),
    ]
}

/// Every pure per-event derivation. Those that need a local tool (screen text
/// through an OCR engine) are added by `sinexd`, which may run them.
pub fn per_event() -> Vec<Box<dyn AnyPerEvent>> {
    Vec::new()
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
