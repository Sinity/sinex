//! Derivations as pure functions (architecture §6). Two kinds:
//!
//! - a **per-event derivation** reads committed events in commit order and
//!   writes zero or more outputs for each (document text, OCR, mark linking);
//! - a **keyed derivation** keeps each key's outputs equal to a function of the
//!   key's current inputs (fused commands, sessions, intervals, daily summaries).
//!
//! Admission marks keys dirty when an input of a keyed derivation is admitted
//! or archived; the runner in `sinexd` recomputes only dirty keys and submits
//! the outputs as a `Derived` batch that replaces the key's previous outputs.
//! Order independence holds by construction: `compute` always sees the key's
//! full current input set.
use serde_json::Value;
use sinex_model::{Event, Timestamp};

pub mod fusion;

/// What a key function sees of an admitted or archived event.
#[derive(Clone, Copy, Debug)]
pub struct KeyView<'a> {
    pub event_type: &'a str,
    pub ts_orig: Option<Timestamp>,
    pub payload: &'a Value,
}

/// Which events `compute` reads for one key.
#[derive(Clone, Debug, PartialEq)]
pub struct InputQuery {
    pub event_types: &'static [&'static str],
    /// `ts_orig` window, inclusive start, exclusive end.
    pub window: Option<(Timestamp, Timestamp)>,
    /// Payload fields that must equal these values.
    pub equal: Vec<(&'static str, Value)>,
}

/// Whether a key can still receive inputs. Joins take closed, settled inputs
/// only; open keys are visible as provisional snapshots (`pq2`, `599`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyState {
    Open,
    Closed,
}

/// Configuration and clocks a derivation may read, so `compute` stays pure.
#[derive(Clone, Debug)]
pub struct Env {
    pub now: Timestamp,
    /// The logical day boundary, one setting (architecture §13).
    pub day_starts_at: time::Time,
    pub settings: Value,
}

pub trait Keyed: Send + Sync {
    /// Registered name, also the `derivation.dirty` key.
    const NAME: &'static str;
    const VERSION: u32;
    const INPUTS: &'static [&'static str];

    /// The keys an input event affects. Windows are keyed by `ts_orig`, so a
    /// late or historical event lands in the right key.
    fn keys_for(&self, input: &KeyView<'_>) -> Vec<Value>;

    fn inputs(&self, key: &Value) -> InputQuery;

    /// The key's outputs, as events derived from `inputs` (each names its parents).
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Vec<Event>;

    fn key_state(&self, key: &Value, now: Timestamp) -> KeyState;
}

pub trait PerEvent: Send + Sync {
    const NAME: &'static str;
    const VERSION: u32;
    const INPUTS: &'static [&'static str];

    fn derive(&self, input: &Event, env: &Env) -> Vec<Event>;
}

/// A keyed derivation with its type erased, for the registry and for
/// admission's dirty-key marking.
pub trait AnyKeyed: Send + Sync {
    fn name(&self) -> &'static str;
    fn inputs_types(&self) -> &'static [&'static str];
    fn keys_for(&self, input: &KeyView<'_>) -> Vec<Value>;
}

impl<K: Keyed> AnyKeyed for K {
    fn name(&self) -> &'static str {
        K::NAME
    }
    fn inputs_types(&self) -> &'static [&'static str] {
        K::INPUTS
    }
    fn keys_for(&self, input: &KeyView<'_>) -> Vec<Value> {
        Keyed::keys_for(self, input)
    }
}

/// Every keyed derivation, in one list.
pub fn keyed() -> Vec<Box<dyn AnyKeyed>> {
    vec![Box::new(fusion::CommandFusion)]
}
