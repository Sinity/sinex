//! Sinex's own state: the daemon samples its parts every minute (how far the
//! publisher and each derivation are behind, how many keys wait, each source's
//! events) and spools one JSON line per part, which this source reads like any
//! live capture. Samples use ordinary event admission and replay.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::event_types::telemetry::SinexTelemetry;
use sinex_model::{Class, InputRef};
use sinex_source_core::drivers::live_stream::{self, LiveStream};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "sinex.telemetry",
    driver: Driver::LiveStream,
    excisable: false,
    purpose: "Sinex's sampled state",
    class: Class::Open,
    parser: "sinex-telemetry",
    parser_version: 1,
    event_types: sinex_model::event_types::telemetry::TYPES,
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
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

impl Parser for TelemetryParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = SinexTelemetry;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<SinexTelemetry>, ()), RecordError> {
        let (sampled_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let sample: Sample = serde_json::from_slice(&bytes)
            .map_err(|e| RecordError::new(format!("telemetry sample: {e}")))?;
        Ok((
            vec![SinexTelemetry {
                component: sample.component,
                name: sample.name,
                sampled_at,
                measures: sample.measures,
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<TelemetryParser> {
    DeclaredSource {
        spec: SPEC,
        parser: TelemetryParser,
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

/// `--read sinex-telemetry=SPOOL`: the samples a daemon spooled into SPOOL.
/// The daemon reads its own spool without configuration.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "sinex-telemetry",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[],
            capabilities: &[],
            credentials: &[],
            streams: &[sinex_source_core::discovery::CaptureStream {
                spool: "telemetry",
                options: &[],
                capabilities: &[],
            }],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|spool, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, spool, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::polled(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
