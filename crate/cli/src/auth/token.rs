use std::env;
use std::fs;
use std::path::Path;

use crate::Result;
use sinex_primitives::RuntimeTargetGatewayTokenRole;

/// Load RPC authentication token from environment or file
///
/// Tries in order:
/// 1. Explicit token value (if provided)
/// 2. `SINEX_RPC_TOKEN` environment variable
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
    if let Ok(token) = env::var("SINEX_RPC_TOKEN")
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
        "No authentication token found. Set SINEX_RPC_TOKEN environment variable or provide --token"
    ))
}

fn apply_runtime_role(token: &str, role: Option<RuntimeTargetGatewayTokenRole>) -> String {
    role.map_or_else(
        || token.trim().to_string(),
        |role| role.apply_to_token(token),
    )
}
