//! The source revision recorded by every producer run.
pub(crate) fn current() -> Result<&'static str, String> {
    let build = option_env!("SINEX_BUILD").ok_or("producer build metadata is absent")?;
    let (commit, dirty) = build
        .split_once(";dirty=")
        .ok_or("producer build metadata lacks its dirty flag")?;
    if commit.len() != 40
        || !commit.bytes().all(|b| b.is_ascii_hexdigit())
        || !matches!(dirty, "true" | "false")
    {
        return Err(
            "producer build metadata requires a full commit and a boolean dirty flag".into(),
        );
    }
    Ok(build)
}
