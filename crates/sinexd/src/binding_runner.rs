//! The binding runner: every binding the sources configuration enables, read
//! continuously through the writer. Bindings are planned by their source's format
//! from the configured paths, recorded with their settings, and read in turn; the
//! paths are planned again every minute, so files a source starts later are
//! read too. A binding that fails is reported and retried on the next round,
//! never stopping the others or the daemon. Private mode pauses every read
//! until it is off again.
use crate::binding_reader::{self, BindingReader};
use crate::sealing::Sink;
use crate::writer::WriterHandle;
use serde::Deserialize;
use serde_json::Value;
use sinex_source_core::{Binding, InputFormat, Settings};
use sinex_sources::sources;
use sinex_store::{Blobs, runs};
use sqlx::PgConnection;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tokio::sync::watch;

/// `sources.json` as the NixOS module writes it: each source's options by its
/// registered name, and which derivations run.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct Config {
    #[serde(default)]
    pub sources: BTreeMap<String, SourceConfig>,
    /// Derivations by name; one set to false does not run. Its keys are still
    /// marked as inputs arrive, so it catches up when it is enabled again.
    #[serde(default)]
    pub derivations: BTreeMap<String, bool>,
    /// The tag applier's rules.
    #[serde(default)]
    pub tags: Vec<sinex_derivations::tags::TagRule>,
    #[serde(default)]
    pub activity: sinex_derivations::activity::ActivityProfile,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct SourceConfig {
    #[serde(default)]
    pub enable: bool,
    /// Where the source reads from on this host.
    #[serde(default)]
    pub paths: Vec<String>,
    /// The zone its local times are in; the system zone when unset.
    #[serde(default)]
    pub zone: Option<String>,
    /// The source's own options.
    #[serde(default)]
    pub options: Value,
    /// The format to read it with, when several read this source name (the
    /// live clipboard spool and the Sinnix lake's clipboard lane); the first
    /// registered otherwise.
    #[serde(default)]
    pub format: Option<String>,
}

impl Config {
    /// Whether the derivation runs: an explicit choice, or its catalog default.
    pub(crate) fn runs(&self, derivation: &str) -> bool {
        self.derivations
            .get(derivation)
            .copied()
            .unwrap_or_else(|| sinex_derivations::enabled_by_default(derivation))
    }

    /// What the configuration declares for derivations.
    pub(crate) fn derivation_configuration(&self) -> sinex_derivations::Configuration {
        sinex_derivations::Configuration {
            tags: if self.runs(sinex_derivations::tags::TagApplier::NAME) {
                self.tags.clone()
            } else {
                vec![]
            },
            activity: self.activity.clone(),
        }
    }

    pub(crate) fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
        serde_json::from_slice(&text).map_err(|e| format!("{}: {e}", path.display()))
    }
}

/// The bindings `config` enables on `host`: each enabled source planned by
/// its format from each of its paths, keeping the bindings of that source.
pub(crate) fn plan(
    config: &Config,
    host: &str,
) -> Result<Vec<(&'static InputFormat, Binding)>, String> {
    Ok(plan_by_path(config, host)?
        .into_iter()
        .flat_map(|planned| planned.bindings)
        .collect())
}

/// The bindings one configured path of one source gives.
struct PathPlan {
    source: String,
    path: String,
    bindings: Vec<(&'static InputFormat, Binding)>,
}

fn plan_by_path(config: &Config, host: &str) -> Result<Vec<PathPlan>, String> {
    let mut out = Vec::new();
    for (name, source) in config.sources.iter().filter(|(_, s)| s.enable) {
        let format = format_of(name, source)?;
        let plan = format
            .plan
            .ok_or_else(|| format!("{name}: not read from a configured path"))?;
        let settings = Settings {
            host: host.to_owned(),
            zone: source.zone.clone(),
            options: source.options.clone(),
        };
        for path in &source.paths {
            out.push(PathPlan {
                source: name.clone(),
                path: path.clone(),
                bindings: plan(Path::new(path), &settings)?
                    .into_iter()
                    .filter(|b| b.source == *name)
                    .map(|b| (format, b))
                    .collect(),
            });
        }
    }
    Ok(out)
}

fn format_of(name: &str, source: &SourceConfig) -> Result<&'static InputFormat, String> {
    match &source.format {
        Some(k) => sources::format(k)
            .filter(|k| k.reads(name))
            .ok_or_else(|| format!("{name}: format {k} does not read it")),
        None => sources::format_of(name).ok_or_else(|| format!("{name}: no such source")),
    }
}

/// The event types the enabled sources can emit, whether or not their paths
/// hold anything yet.
fn emitted(config: &Config) -> Result<Vec<&'static str>, String> {
    let mut types = Vec::new();
    for (name, source) in config.sources.iter().filter(|(_, s)| s.enable) {
        let format = format_of(name, source)?;
        for spec in (format.specs)().into_iter().filter(|s| s.name == name) {
            types.extend(spec.event_types.iter().copied());
        }
    }
    types.sort_unstable();
    types.dedup();
    Ok(types)
}

/// The bindings being read, and what was last said about each path and
/// binding, so a path that stays empty or a binding that cannot start is
/// reported once, not every round.
#[derive(Default)]
struct Bindings {
    readers: Vec<BindingReader>,
    said: BTreeMap<String, String>,
}

impl Bindings {
    /// Plan `config` again and start the bindings not read yet: files a source
    /// writes later (a new day's file, a new recording) are read without a
    /// restart.
    fn extend(&mut self, config: &Config, host: &str) {
        let plans = match plan_by_path(config, host) {
            Ok(plans) => plans,
            Err(e) => {
                self.say("plan", format!("sinexd: sources: {e}"));
                return;
            }
        };
        for planned in plans {
            let key = format!("path {} {}", planned.source, planned.path);
            if planned.bindings.is_empty() {
                self.say(
                    &key,
                    format!(
                        "sinexd: source {}: nothing to read at {} (missing, unreadable or empty)",
                        planned.source, planned.path
                    ),
                );
                continue;
            }
            self.said.remove(&key);
            for (format, binding) in planned.bindings {
                if self
                    .readers
                    .iter()
                    .any(|b| b.binding.name == binding.name && b.binding.parser == binding.parser)
                {
                    continue;
                }
                let name = binding.name.clone();
                match BindingReader::new(format, binding) {
                    Ok(b) => {
                        self.said.remove(&format!("binding {name}"));
                        self.readers.push(b);
                    }
                    Err(e) => self.say(
                        &format!("binding {name}"),
                        format!("sinexd: binding {name}: {e}"),
                    ),
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
/// `stop`.
pub(crate) async fn run(
    mut read_conn: PgConnection,
    blobs: Blobs,
    writer: WriterHandle,
    host: String,
    config: Config,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let emits = emitted(&config)?;
    let manifest = runs::Manifest {
        name: "sinexd bindings",
        kind: "source",
        version: env!("CARGO_PKG_VERSION"),
        build: option_env!("SINEX_BUILD").unwrap_or("dev"),
        emits: &emits,
    };
    let run = runs::start(&mut read_conn, &manifest, &host)
        .await
        .map_err(|e| e.to_string())?;
    let sink = Sink(blobs.clone());
    let private = sinex_transport::private_mode::PrivateMode::default();
    let mut bindings = Bindings::default();
    let mut planned_at: Option<std::time::Instant> = None;
    loop {
        if !private.on() {
            if planned_at.is_none_or(|at| at.elapsed() >= PLAN_EVERY) {
                let before = bindings.readers.len();
                bindings.extend(&config, &host);
                if bindings.readers.len() != before {
                    eprintln!("sinexd: reading {} bindings", bindings.readers.len());
                }
                planned_at = Some(std::time::Instant::now());
            }
            // Each failure is logged as it happens, once until it changes or
            // the binding reads again.
            let said = &mut bindings.said;
            binding_reader::read(
                &mut read_conn,
                &blobs,
                &writer,
                run,
                &sink,
                &mut bindings.readers,
                stop.clone(),
                binding_reader::Access::default(),
                &mut |binding, result| {
                    let key = format!("binding {binding}");
                    match result {
                        Err(e) => {
                            let message = format!("sinexd: binding {binding}: {e}");
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
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = stop.wait_for(|s| *s) => break,
        }
    }
    runs::end(&mut read_conn, run)
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests;
