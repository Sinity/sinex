//! Discover source inputs every minute, reuse persisted bindings and read them
//! continuously through the writer. An unavailable input is retried independently;
//! each input is read independently.
use crate::binding_reader::{self, BindingReader};
use crate::material_access::Sink;
use crate::writer::WriterHandle;
use crate::{Error, Result};
use sinex_source_core::{Binding, InputFormat, Settings};
use sinex_sources::sources;
use sinex_store::Blobs;
use sqlx::{Connection, PgConnection};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::watch;

pub(crate) use sinex_sources::discovery::SourceConfig;

/// A binding's current read failure: the error of its latest attempt and when
/// its failing run of attempts began.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ReadFailure {
    pub error: String,
    pub since: String,
}

/// Every binding whose latest read attempt failed, by binding name. The binding
/// runner keeps it and status shows it: a binding that cannot read admits
/// nothing and has no record to count as failed, so without this status shows
/// an input that fails every round as a quiet source with zero failures.
pub(crate) type ReadFailures = std::sync::Arc<std::sync::Mutex<BTreeMap<String, ReadFailure>>>;

/// Record the outcome of one read attempt of `binding`.
fn note(failures: &ReadFailures, binding: &str, error: Option<&str>) {
    let mut failures = failures.lock().unwrap();
    match error {
        Some(error) => {
            failures
                .entry(binding.to_owned())
                .and_modify(|failure| error.clone_into(&mut failure.error))
                .or_insert_with(|| ReadFailure {
                    error: error.to_owned(),
                    since: jiff::Timestamp::now().to_string(),
                });
        }
        None => {
            failures.remove(binding);
        }
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Config {
    pub sources: BTreeMap<String, SourceConfig>,
    pub exclude: BTreeMap<String, Option<sinex_sources::discovery::Exclusion>>,
    pub discovery: sinex_sources::discovery::DiscoveryContext,
    pub derivations: BTreeMap<String, bool>,
    pub tags: Vec<sinex_derivations::tags::TagRule>,
    pub activity: sinex_derivations::activity::ActivityProfile,
    #[serde(default)]
    pub project_attribution: sinex_derivations::attribution::AttributionProfile,
    pub rhythm: sinex_derivations::portfolio::rhythm::RhythmProfile,
    pub signals: Option<crate::signals::Config>,
    /// Directories the operator lets agents stage from, besides the declared
    /// source locations.
    pub staging_roots: Vec<std::path::PathBuf>,
}
impl Config {
    pub(crate) fn load(path: &std::path::Path) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|error| Error::file(path, error))?;
        let config: Self = serde_json::from_slice(&bytes)
            .map_err(|error| Error::from(error).within(path.display()))?;
        if let Some(signals) = &config.signals {
            signals.validate()?;
        }
        Ok(config)
    }
    pub(crate) fn runs(&self, name: &str) -> bool {
        self.derivations
            .get(name)
            .copied()
            .unwrap_or_else(|| sinex_derivations::enabled_by_default(name))
    }
    pub(crate) fn context(&self, host: &str) -> sinex_sources::discovery::DiscoveryContext {
        let mut context = self.discovery.clone();
        context.host = host.into();
        context.sources.clone_from(&self.sources);
        context.exclude.clone_from(&self.exclude);
        context
    }
    /// Where an agent may stage from on `host`: the declared source
    /// locations and the operator's staging roots.
    pub(crate) fn agent_stage_roots(&self, host: &str) -> Vec<std::path::PathBuf> {
        let mut roots = discovery::declared_locations(&self.context(host));
        roots.extend(self.staging_roots.iter().cloned());
        roots
    }

    pub(crate) fn derivation_configuration(&self) -> sinex_derivations::Configuration {
        sinex_derivations::Configuration {
            tags: if self.runs(sinex_derivations::tags::TagApplier::NAME) {
                self.tags.clone()
            } else {
                vec![]
            },
            activity: self.activity.clone(),
            project_attribution: self.project_attribution.clone(),
            rhythm: self.rhythm.clone(),
        }
    }
}
use sinex_sources::discovery::{self, Availability, Cause};

pub(crate) fn plan(config: &Config, host: &str) -> Result<Vec<(&'static InputFormat, Binding)>> {
    let inputs = discovery::discover(&config.context(host)).map_err(Error::bad_value)?;
    Ok(discovery::reconcile(
        inputs
            .into_iter()
            .flat_map(|input| {
                let format = sources::format(&input.format).expect("discovered format");
                input
                    .bindings
                    .into_iter()
                    .filter(move |_| !input.capture)
                    .map(move |binding| (format, binding))
            })
            .collect(),
        &[],
    )
    .bindings)
}

/// Native spool obligations at the exported core cut, plus configured spools
/// whose acquired bytes have not yet produced a database checkpoint.
pub(crate) async fn backup_bindings(
    conn: &mut PgConnection,
    config: &Config,
    host: &str,
) -> Result<Vec<Binding>> {
    use sinex_source_core::registry::{Origin, Shape};
    let specs = sources::specs();
    let mut bindings = plan(config, host)?
        .into_iter()
        .map(|(_, binding)| binding)
        .collect::<Vec<_>>();
    for row in sinex_store::bindings::all(conn).await? {
        let settings: Settings = serde_json::from_value(row.settings)
            .map_err(|e| Error::internal(format!("saved binding {}: {e}", row.binding)))?;
        if settings.host != host {
            continue;
        }
        if !specs.iter().any(|spec| spec.name == row.source) {
            return Err(Error::internal(format!(
                "saved native binding {} has unknown source {}",
                row.binding, row.source
            )));
        }
        for spec in specs.iter().filter(|spec| spec.name == row.source) {
            bindings.push(Binding {
                name: row.binding.clone(),
                source: row.source.clone(),
                input: row.input.clone().into(),
                parser: spec.parser.into(),
                settings: settings.clone(),
            });
        }
    }
    bindings.retain(|binding| {
        specs.iter().any(|spec| {
            spec.name == binding.source
                && matches!(spec.origin, Origin::Input(Shape::LiveStream | Shape::Media))
        })
    });
    bindings.sort_by(|a, b| a.name.cmp(&b.name));
    bindings.dedup_by(|a, b| a.name == b.name);
    Ok(bindings)
}

/// A saved name belongs to the source and input, independent of which current
/// parser discovers it. Reconciliation compares only the proposed parser.
async fn saved(conn: &mut PgConnection, database: &str, host: &str) -> Result<Vec<Binding>> {
    let rows = match sinex_store::bindings::all(conn).await {
        Ok(rows) => rows,
        Err(_) => {
            *conn = PgConnection::connect(database).await?;
            sinex_store::bindings::all(conn).await?
        }
    };
    let mut out = Vec::new();
    for row in rows {
        let settings: Settings = match serde_json::from_value(row.settings) {
            Ok(settings) => settings,
            Err(error) => {
                eprintln!("sinexd: binding {}: unavailable: {error}", row.binding);
                continue;
            }
        };
        if settings.host != host {
            continue;
        }
        let parsers: std::collections::BTreeSet<_> = sources::specs()
            .into_iter()
            .filter(|spec| spec.name == row.source)
            .map(|spec| spec.parser.to_owned())
            .collect();
        for parser in parsers {
            out.push(Binding {
                name: row.binding.clone(),
                source: row.source.clone(),
                input: std::path::PathBuf::from(&row.input),
                parser,
                settings: settings.clone(),
            });
        }
    }
    Ok(out)
}

/// The bindings being read, and what was last said about each path and
/// binding, so a path that stays empty or a binding that cannot start is
/// reported once, not every round.
#[derive(Default)]
struct Bindings {
    readers: Vec<BindingReader>,
    said: BTreeMap<String, String>,
    failures: ReadFailures,
}

impl Bindings {
    /// Plan `config` again and start the bindings not read yet: files a source
    /// writes later (a new day's file, a new recording) are read without a
    /// restart. An input that cannot be read is observed as missing coverage;
    /// one that is not present yet (an export never staged) is waiting, not
    /// failing, and is observed only to replace a missing reading it had.
    fn extend(
        &mut self,
        config: &Config,
        host: &str,
        saved: &[Binding],
        coverage: &mut binding_reader::coverage::Coverage,
    ) {
        let inputs = match discovery::status(&config.context(host)) {
            Ok(inputs) => inputs,
            Err(error) => {
                self.say("plan", format!("sinexd: sources: {error}"));
                return;
            }
        };
        let mut proposed = Vec::new();
        for input in inputs {
            let key = format!(
                "input {} {} {} {}",
                input.source,
                input.format,
                if input.capture { "capture" } else { "read" },
                input
                    .input
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default()
            );
            // An input that exists but cannot be read is missing coverage; one
            // that is absent or inapplicable on this host is not.
            let unavailable = matches!(
                input.availability,
                Availability::Unavailable(cause)
                    if !matches!(cause, Cause::NotPresent | Cause::NotApplicable)
            );
            if unavailable || coverage.has_reading(&key, host) {
                use sinex_model::event_types::coverage::{
                    CoverageCompleteness, CoverageState, SourceCoverage,
                };
                let at = sinex_model::Timestamp::now();
                coverage.observe(SourceCoverage {
                    observed_run: None,
                    observed_class: input.settings.class,
                    observed_source: input.source.clone(),
                    binding: key.clone(),
                    host: host.into(),
                    started_at: at,
                    ended_at: at,
                    state: if unavailable {
                        CoverageState::Missing
                    } else {
                        CoverageState::RecordedUnknownCoverage
                    },
                    reason: Some(input.reason.clone().unwrap_or_else(|| {
                        if unavailable {
                            "no input"
                        } else {
                            "input is readable"
                        }
                        .into()
                    })),
                    completeness: CoverageCompleteness::Unknown,
                    bounded_by: "discovery; no read attempt".into(),
                    events: 0,
                    failures: 0,
                });
            }
            if matches!(input.availability, Availability::Unavailable(_)) {
                self.say(
                    &key,
                    format!(
                        "sinexd: {}: {:?}: {}",
                        input.source,
                        input.availability,
                        input.reason.as_deref().unwrap_or("no input")
                    ),
                );
            } else {
                self.said.remove(&key);
            }
            let format = sources::format(&input.format).expect("discovered format");
            if !input.capture {
                proposed.extend(input.bindings.into_iter().map(|binding| (format, binding)));
            }
        }
        let planned = discovery::reconcile(proposed, saved);
        for (binding, error) in planned.unavailable {
            self.readers
                .retain(|reader| reader.binding.name != binding.name);
            note(&self.failures, &binding.name, Some(&error.to_string()));
            self.say(
                &format!("binding {}", binding.name),
                format!("sinexd: {}: unavailable: {error}", binding.input.display()),
            );
        }
        for (format, binding) in planned.bindings {
            if let Some(old) = self.readers.iter_mut().find(|reader| {
                reader.binding.name == binding.name && reader.binding.parser == binding.parser
            }) {
                if old.binding.settings == binding.settings {
                    continue;
                }
                match BindingReader::new(format, binding) {
                    Ok(reader) => *old = reader,
                    Err(error) => self.say("plan", format!("sinexd: {error}")),
                }
                continue;
            }
            let name = binding.name.clone();
            match BindingReader::new(format, binding) {
                Ok(reader) => {
                    self.said.remove(&format!("binding {name}"));
                    note(&self.failures, &name, None);
                    self.readers.push(reader);
                }
                Err(error) => {
                    note(&self.failures, &name, Some(&error.to_string()));
                    self.say(
                        &format!("binding {name}"),
                        format!("sinexd: binding {name}: {error}"),
                    );
                }
            }
        }
    }

    fn say(&mut self, key: &str, message: String) {
        if self.said.get(key) != Some(&message) {
            eprintln!("{message}");
            self.said.insert(key.to_owned(), message);
        }
    }
}

/// How often the sources are planned again for new files.
const PLAN_EVERY: Duration = Duration::from_secs(60);

/// Read the bindings `config` enables, in rounds every `interval`, until
/// `stop`, keeping each binding's current read failure in `failures`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    connection: (PgConnection, String),
    blobs: Blobs,
    writer: WriterHandle,
    host: String,
    config: Config,
    interval: Duration,
    failures: ReadFailures,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let (mut read_conn, database) = connection;
    discovery::validate(&config.context("")).map_err(Error::bad_value)?;
    let sink = Sink(blobs.clone());
    let mut rounds = binding_reader::Rounds::resume(&mut read_conn, &host).await?;
    let mut bindings = Bindings {
        failures: failures.clone(),
        ..Bindings::default()
    };
    let mut planned_at: Option<std::time::Instant> = None;
    loop {
        if planned_at.is_none_or(|at| at.elapsed() >= PLAN_EVERY) {
            let before = bindings.readers.len();
            match saved(&mut read_conn, &database, &host).await {
                Ok(saved) => {
                    bindings.said.remove("saved");
                    bindings.extend(&config, &host, &saved, &mut rounds.coverage);
                }
                Err(error) => bindings.say(
                    "saved",
                    format!("sinexd: saved bindings unavailable: {error}"),
                ),
            }
            if bindings.readers.len() != before {
                eprintln!("sinexd: reading {} bindings", bindings.readers.len());
            }
            planned_at = Some(std::time::Instant::now());
        }
        let said = &mut bindings.said;
        binding_reader::read(
            &mut read_conn,
            &blobs,
            &writer,
            &sink,
            &mut bindings.readers,
            &mut rounds,
            stop.clone(),
            crate::material_access::Access::default(),
            &mut |binding, result| {
                let key = format!("binding {binding}");
                note(
                    &failures,
                    binding,
                    result.as_ref().err().map(ToString::to_string).as_deref(),
                );
                match result {
                    Err(error) => {
                        let message = format!("sinexd: binding {binding}: {error}");
                        if said.get(&key) != Some(&message) {
                            eprintln!("{message}");
                            said.insert(key, message);
                        }
                    }
                    Ok(_) => {
                        said.remove(&key);
                    }
                }
            },
        )
        .await;
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = stop.wait_for(|s| *s) => break,
        }
    }
    rounds.end(&mut read_conn).await
}

#[cfg(test)]
mod tests;
