//! The source revision recorded by every producer run.
use crate::{Error, Result};
pub(crate) fn current() -> Result<&'static str> {
    let build = option_env!("SINEX_BUILD")
        .ok_or_else(|| Error::internal("producer build metadata is absent"))?;
    let (commit, dirty) = build
        .split_once(";dirty=")
        .ok_or_else(|| Error::internal("producer build metadata lacks its dirty flag"))?;
    if commit.len() != 40
        || !commit.bytes().all(|b| b.is_ascii_hexdigit())
        || !matches!(dirty, "true" | "false")
    {
        return Err(Error::internal(
            "producer build metadata requires a full commit and a boolean dirty flag",
        ));
    }
    Ok(build)
}
