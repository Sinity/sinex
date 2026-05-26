use color_eyre::Result;
use color_eyre::eyre::eyre;
use serde::de::DeserializeOwned;

pub(super) fn parse_serde_enum<T: DeserializeOwned>(name: &str, raw: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(raw.to_string()))
        .map_err(|error| eyre!("invalid {name} `{raw}`: {error}"))
}
