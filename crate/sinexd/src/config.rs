use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AtuinConfig {
    pub path: PathBuf,
    #[serde(default = "poll_interval")]
    pub poll_interval_ms: u64,
}
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ZshConfig {
    pub path: PathBuf,
    pub execution_host: String,
    /// Independently declared protocol premise, not proof supplied by a prefix test.
    pub protocol: String,
    #[serde(default = "poll_interval")]
    pub poll_interval_ms: u64,
}
fn poll_interval() -> u64 {
    30_000
}
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrivacyConfig {
    pub pattern: String,
    pub replacement: String,
    pub created_by: String,
}
impl PrivacyConfig {
    pub fn rule(&self) -> sinex_store::PrivacyRule {
        sinex_store::PrivacyRule {
            pattern: self.pattern.clone(),
            replacement: self.replacement.clone(),
            created_by: self.created_by.clone(),
        }
    }
}
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    pub name: String,
    pub roots: Vec<String>,
}
impl ProjectConfig {
    pub fn model(&self) -> sinex_model::retrieval::Project {
        sinex_model::retrieval::Project {
            name: self.name.clone(),
            roots: self.roots.clone(),
        }
    }
}
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub read_model_connection_file: Option<PathBuf>,
    #[serde(default)]
    pub projects: Vec<ProjectConfig>,
    pub listen: String,
    pub state_dir: PathBuf,
    pub store_dir: PathBuf,
    pub writer_connection_file: PathBuf,
    pub reader_connection_file: PathBuf,
    pub token_file: PathBuf,
    /// Explicit fixed operator profile for replay and exclusion; the ordinary token remains read-only.
    pub operator_token_file: Option<PathBuf>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub automata: Vec<String>,
    pub atuin: Option<AtuinConfig>,
    pub zsh: Option<ZshConfig>,
    #[serde(default)]
    pub privacy_rules: Vec<PrivacyConfig>,
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) input_blake3: String,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|_| "cannot read configuration file".to_string())?;
        let mut config: Self =
            toml::from_str(&text).map_err(|e| format!("invalid configuration: {e}"))?;
        config.input_blake3 = format!("blake3:{}", blake3::hash(text.as_bytes()).to_hex());
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self
            .read_model_connection_file
            .as_ref()
            .is_some_and(|p| !p.is_absolute())
        {
            return Err("read model credential path must be absolute".into());
        }
        sinex_model::retrieval::Project::validate_all(
            &self
                .projects
                .iter()
                .map(ProjectConfig::model)
                .collect::<Vec<_>>(),
        )
        .map_err(str::to_string)?;
        if !self.projects.is_empty() && self.read_model_connection_file.is_none() {
            return Err("projects require indexed retrieval configuration".into());
        }
        if !self.automata.is_empty() && self.read_model_connection_file.is_none() {
            return Err("session requires indexed retrieval configuration".into());
        }
        let address: SocketAddr = self
            .listen
            .parse()
            .map_err(|_| "listen must be a numeric loopback address".to_string())?;
        if address.port() == 0 {
            return Err("listen requires an explicit nonzero port".into());
        }
        if !address.ip().is_loopback() {
            return Err("foundation listens only on loopback".into());
        }
        if self
            .sources
            .iter()
            .any(|s| s != sinex_store::ATUIN_SOURCE && s != sinex_store::ZSH_SOURCE)
            || self
                .sources
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.sources.len()
            || !(self.automata.is_empty() || self.automata == ["session"])
        {
            return Err("only Atuin, append-only extended zsh and the shell fusion interpretation are implemented; only the session automaton is additionally supported".into());
        }
        if self.sources.iter().any(|s| s == sinex_store::ATUIN_SOURCE) != self.atuin.is_some() {
            return Err("Atuin source enablement and [atuin] configuration must agree".into());
        }
        if self.sources.iter().any(|s| s == sinex_store::ZSH_SOURCE) != self.zsh.is_some() {
            return Err("zsh source enablement and [zsh] configuration must agree".into());
        }
        if let Some(zsh) = &self.zsh {
            if !zsh.path.is_absolute()
                || !(100..=3_600_000).contains(&zsh.poll_interval_ms)
                || zsh.execution_host.trim().is_empty()
                || zsh.execution_host.len() > 255
                || zsh.protocol != sinex_store::ZSH_PROTOCOL
            {
                return Err("zsh requires absolute path, execution_host, append_only_extended_v1 protocol and poll interval100..=3600000ms".into());
            }
        }
        if let Some(atuin) = &self.atuin {
            if !atuin.path.is_absolute() || !(100..=3_600_000).contains(&atuin.poll_interval_ms) {
                return Err(
                    "Atuin requires an absolute path and poll interval 100..=3600000 ms".into(),
                );
            }
        }
        if self.zsh.is_some() && !self.privacy_rules.is_empty() {
            return Err("optional ingest privacy transformations are unavailable for the selected zsh/two-source capability; Atuin-only reading and mandatory credential presentation remain supported".into());
        }
        if self.privacy_rules.len() > 32 {
            return Err("at most 32 privacy rules are supported".into());
        }
        for rule in &self.privacy_rules {
            rule.rule().validate()?;
        }
        if self
            .operator_token_file
            .as_ref()
            .is_some_and(|p| !p.is_absolute())
        {
            return Err("operator token path must be absolute".into());
        }
        for path in [
            &self.state_dir,
            &self.store_dir,
            &self.writer_connection_file,
            &self.reader_connection_file,
            &self.token_file,
        ] {
            if !path.is_absolute() {
                return Err("configuration paths must be absolute".into());
            }
        }
        Ok(())
    }
}
pub fn secret(path: &Path) -> Result<String, String> {
    let value = std::fs::read_to_string(path)
        .map_err(|_| "cannot read configured credential file".to_string())?;
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err("configured credential file is empty".into());
    }
    Ok(value)
}
pub fn schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retired_replay_credential_field_is_not_an_alias() {
        let base = r#"listen="127.0.0.1:41037"
state_dir="/synthetic/state"
store_dir="/synthetic/store"
writer_connection_file="/synthetic/writer"
reader_connection_file="/synthetic/reader"
token_file="/synthetic/read-token"
"#;
        let old = format!("{base}replay_token_file=\"/synthetic/operator-token\"\n");
        assert!(toml::from_str::<Config>(&old).is_err());
        let new = format!("{base}operator_token_file=\"/synthetic/operator-token\"\n");
        assert!(toml::from_str::<Config>(&new).unwrap().validate().is_ok());
    }
    #[test]
    fn unknown_configuration_is_rejected_before_startup() {
        assert!(toml::from_str::<Config>("unknown = true").is_err());
    }
}
