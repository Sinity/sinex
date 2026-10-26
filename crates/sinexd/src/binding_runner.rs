//! The binding runner: every binding the sources configuration enables, read
//! continuously through the writer. Bindings are planned by their source's format
//! from the configured paths, recorded with their settings, and read in turn; a
//! binding that fails is reported and retried on the next round, never stopping
//! the others or the daemon. Private mode pauses every read until it is off
//! again.
use crate::binding_reader::{self, Bound};
use crate::material_sink::Sink;
use crate::writer::WriterHandle;
use serde::Deserialize;
use serde_json::Value;
use sinex_sources::{Binding, InputFormat, Settings, sources};
use sinex_store::{Blobs, runs};
use sqlx::PgConnection;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tokio::sync::watch;

/// `sources.json` as the NixOS module writes it: each source's options by its
/// registered name.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct Config {
    #[serde(default)]
    pub sources: BTreeMap<String, SourceConfig>,
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
    let mut out = Vec::new();
    for (name, source) in config.sources.iter().filter(|(_, s)| s.enable) {
        let format = match &source.format {
            Some(k) => sources::format(k)
                .filter(|k| k.reads(name))
                .ok_or_else(|| format!("{name}: format {k} does not read it"))?,
            None => sources::format_of(name).ok_or_else(|| format!("{name}: no such source"))?,
        };
        let plan = format
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
                    .map(|b| (format, b)),
            );
        }
    }
    Ok(out)
}

/// Read `planned` in rounds every `interval` until `stop`.
pub(crate) async fn run(
    mut read_conn: PgConnection,
    blobs: Blobs,
    writer: WriterHandle,
    host: String,
    planned: Vec<(&'static InputFormat, Binding)>,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut bound = Vec::new();
    for (format, binding) in planned {
        let name = binding.name.clone();
        match Bound::new(format, binding) {
            Ok(b) => bound.push(b),
            Err(e) => eprintln!("sinexd: binding {name}: {e}"),
        }
    }
    // The daemon treats a task that returns as stopped, so a runner with
    // nothing to read waits for shutdown like the others.
    if bound.is_empty() {
        let _ = stop.wait_for(|s| *s).await;
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
    let private = sinex_transport::private_mode::PrivateMode::default();
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
                binding_reader::Access::default(),
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
