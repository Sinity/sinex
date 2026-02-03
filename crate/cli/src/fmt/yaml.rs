use serde::Serialize;

use crate::Result;

/// Format output as YAML
pub fn format_yaml<T: Serialize>(value: &T) -> Result<String> {
    serde_yaml::to_string(value).map_err(Into::into)
}
