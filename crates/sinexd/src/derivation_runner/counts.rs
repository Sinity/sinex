//! What a derivation run did: keys, inputs, outputs and time spent.
use std::time::Duration;

/// Counts for one derivation.
#[derive(Clone, Debug, Default)]
pub(crate) struct Counts {
    pub keys: usize,
    pub inputs: usize,
    pub outputs: usize,
    pub admitted: usize,
    pub repeats: usize,
    pub replaced: usize,
    /// Outputs dropped because an input they cite stopped being live before
    /// they were stored.
    pub superseded_input: usize,
    /// Inputs a per-event derivation could not derive (their outputs are
    /// missing until a new version reads them again).
    pub failed: usize,
    pub compute: Duration,
    pub read: Duration,
    pub write: Duration,
}

impl Counts {
    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "keys": self.keys,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "admitted": self.admitted,
            "repeats": self.repeats,
            "replaced": self.replaced,
            "superseded_input": self.superseded_input,
            "failed": self.failed,
            "read_s": self.read.as_secs_f64(),
            "compute_s": self.compute.as_secs_f64(),
            "write_s": self.write.as_secs_f64(),
            "outputs_per_write_s": self.outputs as f64 / self.write.as_secs_f64().max(1e-9),
        })
    }
}

impl std::fmt::Display for Counts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "keys {} | inputs read {} | outputs {} | admitted {} | repeats {} | replaced {} | superseded input {} | failed {} | read {:.2}s | compute {:.2}s | write {:.2}s",
            self.keys,
            self.inputs,
            self.outputs,
            self.admitted,
            self.repeats,
            self.replaced,
            self.superseded_input,
            self.failed,
            self.read.as_secs_f64(),
            self.compute.as_secs_f64(),
            self.write.as_secs_f64(),
        )
    }
}
