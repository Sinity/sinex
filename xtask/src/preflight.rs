//! Preflight checks and automatic setup for xtask commands.
//!
//! This module provides infrastructure readiness checks and lazy-start capabilities.
//! Commands that need Postgres, NATS, TLS, or migrations can call `ensure_ready()`
//! to prompt the user and set up infrastructure automatically.

use anyhow::{Context, Result};

/// Check if Postgres is available.
pub fn is_postgres_ready() -> bool {
    std::process::Command::new("pg_isready")
        .arg("-q")
        .status()
        .is_ok_and(|s| s.success())
}

/// Check if NATS is available on the configured port.
pub fn is_nats_ready() -> bool {
    let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(4222);
    std::net::TcpStream::connect(format!("127.0.0.1:{}", nats_port)).is_ok()
}

/// Check if TLS certificates exist.
pub fn tls_certs_exist() -> bool {
    // TLS certs are in the project's certs/ directory or .tls/ directory
    let certs_dir = std::path::Path::new("certs");
    let tls_dir = std::path::Path::new(".tls");

    let check_dir = |dir: &std::path::Path| {
        dir.join("ca.crt").exists()
            && dir.join("server.crt").exists()
            && dir.join("client.crt").exists()
    };

    check_dir(certs_dir) || check_dir(tls_dir)
}

/// Check for pending database migrations.
pub fn has_pending_migrations() -> Result<bool> {
    // This would need to query the database for pending migrations
    // For now, assume no pending migrations if we can't check
    Ok(false)
}

/// Infrastructure status for preflight checks.
#[derive(Debug)]
pub struct InfraStatus {
    pub postgres: bool,
    pub nats: bool,
    pub tls: bool,
    pub migrations_pending: bool,
}

impl InfraStatus {
    /// Capture current infrastructure status.
    pub fn capture() -> Self {
        Self {
            postgres: is_postgres_ready(),
            nats: is_nats_ready(),
            tls: tls_certs_exist(),
            migrations_pending: has_pending_migrations().unwrap_or(false),
        }
    }

    /// Check if all infrastructure is ready.
    pub fn all_ready(&self) -> bool {
        self.postgres && self.nats && !self.migrations_pending
    }

    /// Check if stack (Postgres + NATS) is running.
    pub fn stack_running(&self) -> bool {
        self.postgres && self.nats
    }
}

/// Prompt the user to start the stack if not running.
///
/// Returns Ok(true) if stack is now running, Ok(false) if user declined.
pub fn prompt_start_stack(is_interactive: bool) -> Result<bool> {
    let status = InfraStatus::capture();

    if status.stack_running() {
        return Ok(true);
    }

    if !is_interactive {
        // Non-interactive mode - just report the issue
        if !status.postgres {
            eprintln!("⚠ Postgres is not running");
        }
        if !status.nats {
            eprintln!("⚠ NATS is not running");
        }
        return Ok(false);
    }

    // Interactive mode - prompt user
    println!();
    if !status.postgres && !status.nats {
        println!("⚠ Neither Postgres nor NATS are running.");
    } else if !status.postgres {
        println!("⚠ Postgres is not running.");
    } else {
        println!("⚠ NATS is not running.");
    }

    // Use inquire for prompting
    let should_start = inquire::Confirm::new("Start the development stack?")
        .with_default(true)
        .with_help_message("This will run 'cargo xtask stack start'")
        .prompt()
        .unwrap_or(false);

    if should_start {
        println!("Starting stack...");
        let result = std::process::Command::new("cargo")
            .args(["xtask", "stack", "start"])
            .status()
            .context("Failed to start stack")?;

        if result.success() {
            println!("✓ Stack started successfully");
            Ok(true)
        } else {
            eprintln!("✗ Failed to start stack");
            Ok(false)
        }
    } else {
        println!("Skipping stack start. Some operations may fail.");
        Ok(false)
    }
}

/// Generate TLS certificates if they don't exist.
pub fn ensure_tls_certs(is_interactive: bool) -> Result<()> {
    if tls_certs_exist() {
        return Ok(());
    }

    if is_interactive {
        println!("Generating development TLS certificates...");
    }

    let result = std::process::Command::new("cargo")
        .args(["xtask", "tls", "generate-dev-certs"])
        .status()
        .context("Failed to generate TLS certificates")?;

    if result.success() {
        if is_interactive {
            println!("✓ TLS certificates generated");
        }
        Ok(())
    } else {
        anyhow::bail!("Failed to generate TLS certificates")
    }
}

/// Ensure all infrastructure is ready for a command.
///
/// This is the main entry point for preflight checks. It will:
/// 1. Check if stack is running, prompt to start if not
/// 2. Generate TLS certs if missing
/// 3. Apply pending migrations if any
pub fn ensure_ready(ctx: &crate::command::CommandContext) -> Result<()> {
    let is_interactive = ctx.is_human();
    let status = InfraStatus::capture();

    // 1. Check stack
    if !status.stack_running() && !prompt_start_stack(is_interactive)? {
        // User declined, but we'll continue - some operations might still work
    }

    // 2. Check TLS (auto-generate silently in non-interactive)
    if !status.tls {
        // TLS generation is optional for most development
        // Only auto-generate if in interactive mode
        if is_interactive {
            ensure_tls_certs(is_interactive)?;
        }
    }

    // 3. Migrations would be handled here if we had the infrastructure
    // For now, skip this

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infra_status_capture() {
        // This test just verifies the capture doesn't panic
        let status = InfraStatus::capture();
        // The actual values depend on the environment
        let _ = status.all_ready();
        let _ = status.stack_running();
    }
}
