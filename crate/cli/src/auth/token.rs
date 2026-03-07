use std::env;
use std::fs;
use std::path::Path;

use crate::Result;

/// Load RPC authentication token from environment or file
///
/// Tries in order:
/// 1. Explicit token value (if provided)
/// 2. `SINEX_RPC_TOKEN` environment variable
/// 3. Token file path (if provided)
/// 4. Default token file (~/.config/sinex/token)
pub fn load_token(explicit_token: Option<&str>, token_file: Option<&Path>) -> Result<String> {
    // 1. Explicit token
    if let Some(token) = explicit_token {
        return Ok(token.to_string());
    }

    // 2. Environment variable
    if let Ok(token) = env::var("SINEX_RPC_TOKEN")
        && !token.is_empty()
    {
        return Ok(token);
    }

    // 3. Token file
    if let Some(path) = token_file
        && path.exists()
    {
        return fs::read_to_string(path)
            .map(|s| s.trim().to_string())
            .map_err(|e| color_eyre::eyre::eyre!("Failed to read token from {:?}: {}", path, e));
    }

    // 4. Default token file
    if let Some(home) = env::var_os("HOME") {
        let default_path = Path::new(&home).join(".config/sinex/token");
        if default_path.exists() {
            return fs::read_to_string(&default_path)
                .map(|s| s.trim().to_string())
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Failed to read token from {:?}: {}", default_path, e)
                });
        }
    }

    Err(color_eyre::eyre::eyre!(
        "No authentication token found. Set SINEX_RPC_TOKEN environment variable or provide --token"
    ))
}
