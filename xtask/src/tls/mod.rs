//! TLS certificate management utilities for development and production.
//!
//! This module provides commands for:
//! - Generating self-signed certificates for local development
//! - Verifying TLS configurations
//! - Generating client certificates for mTLS
//! - Setting up environment variables for TLS
//!
//! # Certificate Structure
//!
//! Generated certificates follow a CA-signed hierarchy:
//! ```text
//! .tls/
//! ├── ca.pem           # Certificate Authority certificate
//! ├── ca-key.pem       # CA private key (keep secure!)
//! ├── server.pem       # Server certificate (gateway, NATS)
//! ├── server-key.pem   # Server private key
//! ├── client.pem       # Client certificate (mTLS)
//! └── client-key.pem   # Client private key
//! ```

mod generate;
mod verify;

pub use generate::{CertConfig, generate_ca, generate_client_cert, generate_dev_certs};
pub use verify::{TlsCheckOptions, check_tls_config};

use clap::Subcommand;
use color_eyre::eyre::Result;
use std::path::PathBuf;

/// TLS subcommands for certificate management.
#[derive(Debug, Clone, Subcommand)]
pub enum TlsCommand {
    /// Generate a complete dev TLS bundle (CA + server + client).
    /// Hidden — invoked automatically by the devshell hook; use `xtr tls generate-ca`
    /// and `xtr tls generate-client-cert` for manual certificate management.
    #[command(hide = true, name = "generate-dev-certs")]
    GenerateDevCerts {
        /// Output directory for generated certificates
        #[arg(long, default_value = ".tls")]
        output: PathBuf,

        /// Suppress success output (used by shellHook)
        #[arg(long)]
        quiet: bool,

        /// Overwrite existing certificates
        #[arg(long)]
        force: bool,
    },

    /// Generate a client certificate signed by the CA
    GenerateClientCert {
        /// Output directory for the client certificate
        #[arg(long, default_value = ".tls")]
        output: PathBuf,

        /// Common Name for the client certificate
        #[arg(long)]
        name: String,

        /// Path to CA certificate (default: .tls/ca.pem)
        #[arg(long)]
        ca_cert: Option<PathBuf>,

        /// Path to CA private key (default: .tls/ca-key.pem)
        #[arg(long)]
        ca_key: Option<PathBuf>,

        /// Certificate validity in days
        #[arg(long, default_value_t = 365)]
        days: u32,
    },

    /// Generate only a CA certificate
    GenerateCa {
        /// CA name
        #[arg(long, default_value = "Sinex Development CA")]
        name: String,

        /// Certificate validity in days
        #[arg(long, default_value_t = 365)]
        validity_days: u32,

        /// Output directory
        #[arg(long, default_value = ".tls")]
        output_dir: PathBuf,
    },
}

use crate::command::CommandResult;

/// Execute a TLS subcommand.
pub fn run(cmd: TlsCommand, _json: bool) -> Result<CommandResult> {
    match cmd {
        TlsCommand::GenerateDevCerts {
            output,
            quiet,
            force,
        } => {
            let san = vec!["localhost".to_string(), "127.0.0.1".to_string()];
            let config = CertConfig {
                output_dir: output.clone(),
                san,
                ca_name: "Sinex Development CA".to_string(),
                validity_days: 365,
                force,
            };
            let result = generate_dev_certs(&config)?;
            if !quiet {
                println!("Generated TLS certificates in {}", output.display());
            }
            Ok(CommandResult::success()
                .with_message("Development certificates generated")
                .with_data(result))
        }

        TlsCommand::GenerateClientCert {
            output,
            name,
            ca_cert,
            ca_key,
            days,
        } => {
            let ca_cert = ca_cert.unwrap_or_else(|| output.join("ca.pem"));
            let ca_key = ca_key.unwrap_or_else(|| output.join("ca-key.pem"));
            let info = generate_client_cert(&output, &name, &ca_cert, &ca_key, days)?;
            Ok(CommandResult::success()
                .with_message(format!("Client certificate generated for {name}"))
                .with_data(serde_json::to_value(info)?))
        }

        TlsCommand::GenerateCa {
            name,
            validity_days,
            output_dir,
        } => {
            std::fs::create_dir_all(&output_dir)?;
            let (cert_pem, key_pem) = generate_ca(&name, validity_days)?;
            let cert_path = output_dir.join("ca.pem");
            let key_path = output_dir.join("ca-key.pem");
            std::fs::write(&cert_path, &cert_pem)?;
            std::fs::write(&key_path, &key_pem)?;
            Ok(CommandResult::success()
                .with_message("CA certificate generated")
                .with_data(serde_json::json!({
                    "ca_cert": cert_path.display().to_string(),
                    "ca_key": key_path.display().to_string(),
                    "name": name,
                    "validity_days": validity_days,
                })))
        }
    }
}
