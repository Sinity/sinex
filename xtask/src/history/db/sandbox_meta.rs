use color_eyre::eyre::{Result, WrapErr};

/// Sandbox infrastructure metadata extracted from slog events in test output.
#[derive(Debug, Default)]
pub(super) struct SandboxMeta {
    pub(super) slot_name: Option<String>,
    pub(super) slot_wait_ms: Option<i64>,
    pub(super) cleanup_ms: Option<i64>,
}

fn parse_sandbox_metric(field: &str, value: &str) -> Result<i64> {
    value
        .parse()
        .wrap_err_with(|| format!("invalid sandbox metadata field {field}={value}"))
}

/// Parse sandbox slog events from test output to extract infrastructure metadata.
///
/// Looks for `[sandbox:*] event=slot_acquired` lines and extracts:
/// - `slot` -> slot_name (e.g., "sinex_test_pool_13")
/// - `duration_ms` -> slot_wait_ms (total acquisition time including cleanup)
/// - `clean_ms` -> cleanup_ms (cleanup time for dirty slots, absent for clean slots)
pub(super) fn parse_sandbox_meta(output: &str) -> Result<SandboxMeta> {
    let mut meta = SandboxMeta::default();

    for line in output.lines() {
        if !line.contains("event=slot_acquired") {
            continue;
        }

        // Parse key=value pairs from the slog line.
        for part in line.split_whitespace() {
            if let Some(val) = part.strip_prefix("slot=") {
                meta.slot_name = Some(val.to_string());
            } else if let Some(val) = part.strip_prefix("duration_ms=") {
                meta.slot_wait_ms = Some(parse_sandbox_metric("duration_ms", val)?);
            } else if let Some(val) = part.strip_prefix("clean_ms=") {
                meta.cleanup_ms = Some(parse_sandbox_metric("clean_ms", val)?);
            }
        }

        // Take the first slot_acquired event (the test's primary database).
        break;
    }

    Ok(meta)
}
