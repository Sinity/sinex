//! Host facts and format declarations used to find inputs without changing them.
use crate::{Binding, InputFormat, Settings};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Owner {
    System,
    Session,
}

/// A location relative to a host-declared root, or an application environment variable.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Location {
    pub root: &'static str,
    pub path: &'static str,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct CaptureStream {
    pub spool: &'static str,
    pub options: &'static str,
    pub capabilities: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct DiscoverySpec {
    pub owner: Owner,
    pub locations: &'static [Location],
    pub capabilities: &'static [&'static str],
    pub credentials: &'static [&'static str],
    pub request_only: bool,
    pub streams: &'static [CaptureStream],
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SourceConfig {
    pub paths: Vec<String>,
    pub zone: Option<String>,
    pub class: Option<sinex_model::Class>,
    pub options: Value,
    /// Scopes explicit paths; discovery still considers every format.
    pub format: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Exclusion {
    pub reason: String,
}

/// All paths are supplied by the host or derived from the application's declared roots.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscoveryContext {
    pub host: String,
    pub home: Option<PathBuf>,
    pub data_home: Option<PathBuf>,
    pub config_home: Option<PathBuf>,
    pub state_home: Option<PathBuf>,
    pub runtime_dir: Option<PathBuf>,
    pub subject_roots: BTreeMap<String, Vec<PathBuf>>,
    pub inputs: BTreeMap<String, Vec<PathBuf>>,
    pub environment: BTreeMap<String, String>,
    pub spool: PathBuf,
    pub layout_file: Option<PathBuf>,
    pub watch: Vec<PathBuf>,
    pub sources: BTreeMap<String, SourceConfig>,
    pub exclude: BTreeMap<String, Option<Exclusion>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Reading,
    Capturing,
    WaitingForInput,
    Unavailable,
    Excluded,
    RequestOnly,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiscoveredInput {
    pub format: String,
    pub source: String,
    pub input: Option<PathBuf>,
    pub settings: Settings,
    pub availability: Availability,
    pub reason: Option<String>,
    /// Planned bindings preserve each format's distinguishing suffix.
    pub bindings: Vec<Binding>,
}

pub type Discover = fn(&InputFormat, &DiscoveryContext) -> Vec<DiscoveredInput>;

impl DiscoveryContext {
    /// Runtime facts of the account doing discovery. System services receive the
    /// operator's roots explicitly instead of using their own home directory.
    pub fn for_process(host: &str, spool: PathBuf) -> Self {
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let root = |variable, suffix| {
            std::env::var_os(variable)
                .map(PathBuf::from)
                .or_else(|| home.as_ref().map(|h| h.join(suffix)))
        };
        Self {
            host: host.into(),
            data_home: root("XDG_DATA_HOME", ".local/share"),
            config_home: root("XDG_CONFIG_HOME", ".config"),
            state_home: root("XDG_STATE_HOME", ".local/state"),
            home,
            runtime_dir: std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
            environment: std::env::vars().collect(),
            spool,
            ..Self::default()
        }
    }
}

/// The generated host configuration shared by discovery and capture producers.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Configuration {
    pub sources: BTreeMap<String, SourceConfig>,
    pub exclude: BTreeMap<String, Option<Exclusion>>,
    pub discovery: DiscoveryContext,
    pub derivations: BTreeMap<String, bool>,
}

impl Configuration {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
        serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", path.display()))
    }

    pub fn runs(&self, name: &str) -> bool {
        self.derivations.get(name).copied().unwrap_or(true)
    }

    pub fn context(&self, host: &str) -> DiscoveryContext {
        let mut context = self.discovery.clone();
        context.host = host.into();
        context.sources.clone_from(&self.sources);
        context.exclude.clone_from(&self.exclude);
        context
    }
}
