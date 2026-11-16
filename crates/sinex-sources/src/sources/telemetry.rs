//! Sinex's own state: the daemon samples its parts every minute (how far the
//! publisher and each derivation are behind, how many keys wait, each source's
//! events) and spools one JSON line per part, which this source reads like any
//! live capture. Samples use the same event admission and replay as other sources.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::InputRef;
use sinex_model::event_types::telemetry::SinexTelemetry;
use sinex_source_core::RecordError;
use sinex_source_core::RecordResult;
use sinex_source_core::discovery::{CaptureStream, CapturedBy, DiscoverySpec};
use sinex_source_core::drivers::live_stream::{self, LiveStream};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Stateless, StatelessParser};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "sinex.telemetry",
    origin: Origin::Input(Shape::LiveStream),
    expect: Expectation::continuous_hours(1),
    purpose: "Sinex's sampled state",
    parser: "sinex-telemetry",
    event_types: sinex_model::event_types::telemetry::TYPES,
    framer: sinex_source_core::Framer::LINES,
    ..SourceSpec::DEFAULT
};

/// One spooled sample of one part.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub component: String,
    #[serde(default)]
    pub name: String,
    pub measures: BTreeMap<String, f64>,
}

pub struct TelemetryParser;

impl StatelessParser for TelemetryParser {
    type Record<'a> = Map<String, Value>;
    type Payload = SinexTelemetry;

    fn parse(
        &self,
        record: &Map<String, Value>,

        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<Vec<SinexTelemetry>, RecordError> {
        let (sampled_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let sample: Sample = serde_json::from_slice(&bytes).record("telemetry sample")?;
        Ok(vec![SinexTelemetry {
            component: sample.component,
            name: sample.name,
            sampled_at,
            measures: sample.measures,
        }])
    }
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "spool".into(),
            locator: "sinexd:telemetry".into(),
            host,
        },
    }
}

/// `stage --parser sinex-telemetry SPOOL`: the samples a daemon spooled into SPOOL.
/// The daemon reads its own spool without configuration.
super::input_format! { pub FORMAT = "sinex-telemetry" {
    specs: [SPEC],
    parser: |_b| Stateless(TelemetryParser),
    discovery: DiscoverySpec {
        captured_by: CapturedBy::System,
        streams: &[CaptureStream { spool: "telemetry", options: &[], capabilities: &[] }],
        ..DiscoverySpec::DEFAULT
    },
    plan: single,
    reader: polled(driver),
} }

#[cfg(test)]
mod tests;
