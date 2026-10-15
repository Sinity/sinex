use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: String,
    pub state_dir: PathBuf,
    pub store_dir: PathBuf,
    pub writer_connection_file: PathBuf,
    pub reader_connection_file: PathBuf,
    pub token_file: PathBuf,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub automata: Vec<String>,
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
        if !self.sources.is_empty() || !self.automata.is_empty() {
            return Err(
                "sources and automata are not supported by this foundation candidate".into(),
            );
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
