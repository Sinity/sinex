//! A read outcome is evidence even when no source record changed. Each binding
//! on each host has one current coverage reading; a newer one is admitted only
//! when the binding's state or its reason changes, so a binding read every few
//! seconds writes when it starts failing or recovers, not on every read.
use crate::material_access::Access;
use crate::stage::Totals;
use crate::writer::{Priority, WriterHandle};
use crate::{Error, Result};
use sinex_model::event_types::coverage::{CoverageCompleteness, CoverageState, SourceCoverage};
use sinex_model::{Class, RunId, Timestamp};
use sinex_source_core::Binding;
use sinex_sources::sources::coverage;
use sinex_store::{Blobs, bindings};
use sqlx::PgConnection;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Why a successful read establishes no observed coverage. One reason for
/// every successful read, so a binding that alternates between new records
/// and none stays in one state.
const READ_SUCCEEDED: &str = "input read successfully; temporal coverage is not declared";

pub(super) fn outcome(
    binding: &Binding,
    started_at: Timestamp,
    result: &Result<Totals>,
) -> SourceCoverage {
    let (state, reason, events, failures) = match result {
        Ok(totals) if totals.accounting.failures == 0 => (
            CoverageState::RecordedUnknownCoverage,
            Some(READ_SUCCEEDED.into()),
            totals.accounting.readings,
            0,
        ),
        Ok(totals) => (
            CoverageState::Partial,
            Some("some source records failed to parse".into()),
            totals.accounting.readings,
            totals.accounting.failures,
        ),
        Err(error) => (CoverageState::Missing, Some(error.to_string()), 0, 1),
    };
    SourceCoverage {
        observed_run: None,
        observed_class: binding.settings.class,
        observed_source: binding.source.clone(),
        binding: binding.name.clone(),
        host: binding.settings.host.clone(),
        started_at,
        ended_at: Timestamp::now(),
        state,
        reason,
        completeness: CoverageCompleteness::Unknown,
        bounded_by: "read attempt; source coverage interval is unknown".into(),
        events,
        failures,
    }
}

/// A binding's coverage identity: its name and host.
type Key = (String, String);

/// The coverage a reader has admitted, the observations waiting to be
/// admitted, and the one open run per host that admits them.
#[derive(Default)]
pub(crate) struct Coverage {
    current: BTreeMap<Key, (CoverageState, Option<String>)>,
    pending: Vec<SourceCoverage>,
    runs: BTreeMap<String, RunId>,
}

impl Coverage {
    /// Coverage that continues from the current readings stored for `host`, so
    /// a restarted reader writes nothing for a binding whose state is unchanged.
    pub(crate) async fn stored(conn: &mut PgConnection, host: &str) -> Result<Self> {
        let events = sinex_store::live::of_types(conn, &["source.coverage".into()]).await?;
        let mut current = BTreeMap::new();
        for payload in events.into_iter().filter_map(|event| event.payload) {
            let row: SourceCoverage = serde_json::from_value(payload)?;
            if row.host == host {
                current.insert((row.binding, row.host), (row.state, row.reason));
            }
        }
        Ok(Self {
            current,
            ..Self::default()
        })
    }

    /// Queue `observation` when its state or reason differs from its binding's
    /// current reading. A later observation of the same binding replaces one
    /// still waiting.
    pub(crate) fn observe(&mut self, observation: SourceCoverage) {
        self.pending.retain(|waiting| {
            (&waiting.binding, &waiting.host) != (&observation.binding, &observation.host)
        });
        let key = (observation.binding.clone(), observation.host.clone());
        if self.current.get(&key) != Some(&(observation.state, observation.reason.clone())) {
            self.pending.push(observation);
        }
    }

    /// Whether `binding` on `host` has a current reading.
    pub(crate) fn has_reading(&self, binding: &str, host: &str) -> bool {
        self.current
            .contains_key(&(binding.to_owned(), host.to_owned()))
    }

    /// Admit every waiting observation. One that fails is reported in
    /// `results` and observed again on the binding's next read.
    pub(crate) async fn flush(
        &mut self,
        conn: &mut PgConnection,
        blobs: &Blobs,
        writer: &WriterHandle,
        access: Access<'_>,
        results: &mut Vec<(String, Result<Totals>)>,
    ) {
        for observation in std::mem::take(&mut self.pending) {
            let source = observation.observed_source.clone();
            let key = (observation.binding.clone(), observation.host.clone());
            let state = (observation.state, observation.reason.clone());
            match record(conn, blobs, writer, access, &mut self.runs, observation).await {
                Ok(()) => {
                    self.current.insert(key, state);
                }
                Err(error) => {
                    eprintln!("sinexd: source coverage evidence for {source}: {error}");
                    results.push((
                        source,
                        Err(Error::internal(format!(
                            "source coverage evidence: {error}"
                        ))),
                    ));
                }
            }
        }
    }

    /// End the runs that admitted coverage.
    pub(crate) async fn end(self, conn: &mut PgConnection) -> Result<()> {
        for run in self.runs.into_values() {
            sinex_store::runs::end(conn, run).await?;
        }
        Ok(())
    }
}

async fn record(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    access: Access<'_>,
    runs: &mut BTreeMap<String, RunId>,
    observation: SourceCoverage,
) -> Result<()> {
    let binding = format!("source.observation:{}", observation.binding);
    let acquired = coverage::observation_material(observation.clone());
    bindings::save(
        conn,
        &bindings::Stored {
            binding: binding.clone(),
            source: coverage::OBSERVATION.name.into(),
            input: acquired.input.locator.clone(),
            settings: serde_json::json!({"host": observation.host}),
        },
    )
    .await?;
    let floor = sinex_store::privacy::load(
        conn,
        [
            observation.observed_source.as_str(),
            coverage::OBSERVATION.name,
        ]
        .into_iter(),
    )
    .await?
    .floors
    .values()
    .copied()
    .max()
    .unwrap_or_default();
    let declared = sinex_sources::sources::specs()
        .into_iter()
        .filter(|spec| spec.name == observation.observed_source)
        .map(|spec| spec.class)
        .max()
        .unwrap_or_default();
    let settings = bindings::load(conn, &observation.binding)
        .await?
        .map(|saved| {
            serde_json::from_value::<sinex_source_core::Settings>(saved.settings).map_err(|error| {
                Error::from(error).within(format!(
                    "binding {} has invalid settings",
                    observation.binding
                ))
            })
        })
        .transpose()?
        .and_then(|settings| settings.class)
        .unwrap_or_default();
    let class = Class::Open
        .max(floor)
        .max(declared)
        .max(settings)
        .max(observation.observed_class.unwrap_or_default());
    let material = crate::material_access::MaterialAccess::new(blobs.clone(), class, access)?;
    let run = match runs.get(&observation.host) {
        Some(run) => *run,
        None => {
            let version = coverage::OBSERVATION.parser_version.to_string();
            let run = sinex_store::runs::start(
                conn,
                &sinex_store::runs::Manifest {
                    name: coverage::OBSERVATION.name,
                    kind: "source",
                    version: &version,
                    build: crate::build_identity::current()?,
                    emits: coverage::OBSERVATION.event_types,
                },
                &observation.host,
            )
            .await?;
            runs.insert(observation.host.clone(), run);
            run
        }
    };
    let result = crate::intake::Intake {
        host: Some(&observation.host),
        writer,
        source: Arc::new(coverage::observation_source()),
        binding: &binding,
        run,
        material,
        check: Arc::new(|| Ok(())),
        priority: Priority::Live,
        atomic: true,
        batch_events: None,
        registered: None,
    }
    .submit(acquired, serde_json::Value::Null, &mut Totals::default())
    .await;
    let admitted = result?;
    if admitted.outcome.failures != 0 || admitted.outcome.admitted != 1 {
        return Err(Error::internal(
            "read outcome was not admitted as one coverage event",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
