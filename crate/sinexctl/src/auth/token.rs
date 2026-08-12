use std::env;
use std::fs;
use std::path::Path;

use crate::Result;
use sinex_primitives::RuntimeTargetGatewayTokenRole;

/// Load RPC authentication token from environment or file
///
/// Tries in order:
/// 1. Explicit token value (if provided)
/// 2. `SINEX_API_TOKEN` environment variable
/// 3. Token file path (if provided)
/// 4. Default token file (~/.config/sinex/token)
pub fn load_token(
    explicit_token: Option<&str>,
    token_file: Option<&Path>,
    token_role: Option<RuntimeTargetGatewayTokenRole>,
) -> Result<String> {
    // 1. Explicit token
    if let Some(token) = explicit_token {
        return Ok(apply_runtime_role(token, token_role));
    }

    // 2. Environment variable
    if let Ok(token) = env::var("SINEX_API_TOKEN")
        && !token.is_empty()
    {
        return Ok(apply_runtime_role(&token, token_role));
    }

    // 3. Token file
    if let Some(path) = token_file
        && path.exists()
    {
        return fs::read_to_string(path)
            .map(|s| apply_runtime_role(&s, token_role))
            .map_err(|e| color_eyre::eyre::eyre!("Failed to read token from {:?}: {}", path, e));
    }

    // 4. Default token file
    if let Some(home) = env::var_os("HOME") {
        let default_path = Path::new(&home).join(".config/sinex/token");
        if default_path.exists() {
            return fs::read_to_string(&default_path)
                .map(|s| apply_runtime_role(&s, token_role))
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Failed to read token from {:?}: {}", default_path, e)
                });
        }
    }

    Err(color_eyre::eyre::eyre!(
        "No authentication token found. Set SINEX_API_TOKEN environment variable or provide --token"
    ))
}

fn apply_runtime_role(token: &str, role: Option<RuntimeTargetGatewayTokenRole>) -> String {
    role.map_or_else(
        || token.trim().to_string(),
        |role| role.apply_to_token(token),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // sinex-rywp bug 2: an empty explicit `--token ""` is accepted and
    // returned immediately, skipping the SINEX_API_TOKEN env var and token
    // file fallbacks -- an empty explicit token silently wins over a
    // genuinely-configured fallback token instead of falling through.
    //
    // (Bug 1 from the same audit report -- "file contents not trimmed" --
    // does NOT reproduce against current code: apply_runtime_role trims via
    // token.trim() on both the role=None and role=Some branches, and
    // RuntimeTargetGatewayTokenRole::apply_to_token also trims. Not writing
    // a test for it; see the sinex-rywp bd comment.)
    #[test]
    #[ignore = "sinex-rywp open: empty explicit --token silently wins over \
                a configured SINEX_API_TOKEN/token-file fallback instead of \
                falling through (auth/token.rs load_token, explicit_token \
                branch returns unconditionally on Some(_), even Some(\"\"))"]
    fn empty_explicit_token_does_not_fall_back_to_env_or_file() {
        // SAFETY: test-only env mutation, single-threaded within this test
        // body (no other test in this crate touches SINEX_API_TOKEN).
        unsafe {
            std::env::set_var("SINEX_API_TOKEN", "real-configured-token");
        }
        let result = load_token(Some(""), None, None);
        unsafe {
            std::env::remove_var("SINEX_API_TOKEN");
        }
        assert_eq!(
            result.unwrap(),
            "real-configured-token",
            "an empty explicit token must fall through to SINEX_API_TOKEN, \
             not silently win as an empty bearer token"
        );
    }
}
