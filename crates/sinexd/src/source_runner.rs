//! The binding runner (architecture §7.3, §7.4): every binding the sources
//! configuration enables, read continuously through the writer. Bindings are
//! planned by their source's kind from the configured paths, recorded with
//! their settings, and read in turn; a binding that fails is reported and
//! retried on the next round, never stopping the others or the daemon.
//! Private mode pauses every read until it is off again.
use crate::binding_reader::{self, Bound};
use crate::one_shot::Sink;
use crate::writer::WriterHandle;
use serde::Deserialize;
use serde_json::Value;
use sinex_sources::{Binding, InputKind, Settings, sources};
use sinex_store::{Blobs, runs};
use sqlx::PgConnection;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tokio::sync::watch;

/// `sources.json` as the NixOS module writes it: each source's options by its
/// registered name.
#[derive(Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub sources: BTreeMap<String, SourceConfig>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SourceConfig {
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
    /// The kind to read it with, when several read this source name (the
    /// live clipboard spool and the Sinnix lake's clipboard lane); the first
    /// registered otherwise.
    #[serde(default)]
    pub kind: Option<String>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
        serde_json::from_slice(&text).map_err(|e| format!("{}: {e}", path.display()))
    }
}

/// The bindings `config` enables on `host`: each enabled source planned by
/// its kind from each of its paths, keeping the bindings of that source.
pub fn plan(config: &Config, host: &str) -> Result<Vec<(&'static InputKind, Binding)>, String> {
    let mut out = Vec::new();
    for (name, source) in config.sources.iter().filter(|(_, s)| s.enable) {
        let kind = match &source.kind {
            Some(k) => sources::input_kind(k)
                .filter(|k| k.reads(name))
                .ok_or_else(|| format!("{name}: kind {k} does not read it"))?,
            None => {
                sources::input_kind_of(name).ok_or_else(|| format!("{name}: no such source"))?
            }
        };
        let plan = kind
            .plan
            .ok_or_else(|| format!("{name}: not read from a configured path"))?;
        let settings = Settings {
            host: host.to_owned(),
            zone: source.zone.clone(),
            options: source.options.clone(),
        };
        for path in &source.paths {
            out.extend(
                plan(Path::new(path), &settings)?
                    .into_iter()
                    .filter(|b| b.source == *name)
                    .map(|b| (kind, b)),
            );
        }
    }
    Ok(out)
}

/// Read `planned` in rounds every `interval` until `stop`.
pub async fn run(
    mut read_conn: PgConnection,
    blobs: Blobs,
    writer: WriterHandle,
    host: String,
    planned: Vec<(&'static InputKind, Binding)>,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut bound = Vec::new();
    for (kind, binding) in planned {
        let name = binding.name.clone();
        match Bound::new(kind, binding) {
            Ok(b) => bound.push(b),
            Err(e) => eprintln!("sinexd: binding {name}: {e}"),
        }
    }
    if bound.is_empty() {
        return Ok(());
    }
    let emits: Vec<&'static str> = {
        let mut types: Vec<&'static str> = bound
            .iter()
            .flat_map(|b| b.source.spec().event_types.iter().copied())
            .collect();
        types.sort_unstable();
        types.dedup();
        types
    };
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
    let private = sinex_protocol::private_mode::PrivateMode::default();
    eprintln!("sinexd: reading {} bindings", bound.len());
    loop {
        if !private.on() {
            for (source, result) in binding_reader::read(
                &mut read_conn,
                &blobs,
                &writer,
                run,
                &sink,
                &mut bound,
                stop.clone(),
            )
            .await
            {
                if let Err(e) = result {
                    eprintln!("sinexd: source {source}: {e}");
                }
            }
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
