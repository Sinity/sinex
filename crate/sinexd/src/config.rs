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
pub struct Config {
    pub listen: String,
    pub state_dir: PathBuf,
    pub store_dir: PathBuf,
    pub writer_connection_file: PathBuf,
    pub reader_connection_file: PathBuf,
    pub token_file: PathBuf,
    /// Explicit fixed operator profile for replay; the ordinary token remains read-only.
    pub replay_token_file: Option<PathBuf>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub automata: Vec<String>,
    pub atuin: Option<AtuinConfig>,
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
        if self.sources.iter().any(|s| s != sinex_store::ATUIN_SOURCE)
            || self.sources.len() > 1
            || !self.automata.is_empty()
        {
            return Err(
                "only the registered Atuin source is supported; automata are unavailable".into(),
            );
        }
        if self.sources.is_empty() != self.atuin.is_none() {
            return Err("Atuin source enablement and [atuin] configuration must agree".into());
        }
        if let Some(atuin) = &self.atuin {
            if !atuin.path.is_absolute() || !(100..=3_600_000).contains(&atuin.poll_interval_ms) {
                return Err(
                    "Atuin requires an absolute path and poll interval 100..=3600000 ms".into(),
                );
            }
        }
        if self.privacy_rules.len() > 32 {
            return Err("at most 32 privacy rules are supported".into());
        }
        for rule in &self.privacy_rules {
            rule.rule().validate()?;
        }
        if self
            .replay_token_file
            .as_ref()
            .is_some_and(|p| !p.is_absolute())
        {
            return Err("replay token path must be absolute".into());
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
    fn unknown_configuration_is_rejected_before_startup() {
        assert!(toml::from_str::<Config>("unknown = true").is_err());
    }
}
