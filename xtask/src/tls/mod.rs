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

pub use generate::{generate_ca, generate_client_cert, generate_dev_certs, CertConfig};
pub use verify::{check_tls_config, TlsCheckOptions};

use anyhow::Result;
use clap::Subcommand;
use std::path::PathBuf;

/// TLS subcommands for certificate management.
#[derive(Debug, Clone, Subcommand)]
pub enum TlsCommand {
    /// Generate self-signed certificates for local development
    GenerateDevCerts {
        /// Output directory for certificates (default: .tls)
        #[arg(long, default_value = ".tls")]
        output: PathBuf,

        /// Subject Alternative Names (comma-separated)
        /// Default: localhost,127.0.0.1
        #[arg(long, default_value = "localhost,127.0.0.1")]
        san: String,

        /// Common Name for certificates
        #[arg(long, default_value = "Sinex Dev CA")]
        ca_name: String,

        /// Certificate validity in days
        #[arg(long, default_value_t = 365)]
        days: u32,

        /// Force overwrite existing certificates
        #[arg(long)]
        force: bool,
    },

    /// Verify TLS configuration and certificates
    Check {
        /// Path to certificate file (or reads from `SINEX_GATEWAY_TLS_CERT`)
        #[arg(long)]
        cert: Option<PathBuf>,

        /// Path to private key file (or reads from `SINEX_GATEWAY_TLS_KEY`)
        #[arg(long)]
        key: Option<PathBuf>,

        /// Path to CA certificate for mTLS (or reads from `SINEX_GATEWAY_TLS_CLIENT_CA`)
        #[arg(long)]
        ca: Option<PathBuf>,

        /// Verify certificate chain validity
        #[arg(long)]
        verify_chain: bool,

        /// Check NATS TLS configuration as well
        #[arg(long)]
        nats: bool,
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

    /// Generate .env file with TLS environment variables
    SetupEnv {
        /// TLS directory containing certificates
        #[arg(long, default_value = ".tls")]
        tls_dir: PathBuf,

        /// Output .env file path
        #[arg(long, default_value = ".env.tls")]
        output: PathBuf,

        /// Enable mTLS (include client CA configuration)
        #[arg(long)]
        mtls: bool,

        /// Include NATS TLS configuration
        #[arg(long)]
        nats: bool,

        /// Append to existing file instead of overwriting
        #[arg(long)]
        append: bool,
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
            san,
            ca_name,
            days,
            force,
        } => {
            let sans: Vec<String> = san.split(',').map(|s| s.trim().to_string()).collect();
            let config = CertConfig {
                output_dir: output,
                san: sans,
                ca_name,
                validity_days: days,
                force,
            };
            let info = generate_dev_certs(&config)?;
            Ok(CommandResult::success()
                .with_message("Certificates generated")
                .with_data(serde_json::to_value(info)?))
        }

        TlsCommand::Check {
            cert,
            key,
            ca,
            verify_chain,
            nats,
        } => {
            let options = TlsCheckOptions {
                cert_path: cert,
                key_path: key,
                ca_path: ca,
                verify_chain,
                check_nats: nats,
            };
            let result = check_tls_config(&options)?;
            let mut res = CommandResult::success()
                .with_message(if result.valid {
                    "TLS configuration valid"
                } else {
                    "TLS configuration has issues"
                })
                .with_data(serde_json::to_value(&result)?);

            if !result.valid {
                res.status = crate::output::Status::Failed;
            }

            for issue in &result.issues {
                res = res.with_error(crate::output::StructuredError::new("TLS_ISSUE", issue));
            }
            for warning in &result.warnings {
                res = res.with_warning(warning);
            }

            Ok(res)
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

        TlsCommand::SetupEnv {
            tls_dir,
            output,
            mtls,
            nats,
            append,
        } => {
            let info = setup_env(&tls_dir, &output, mtls, nats, append)?;
            Ok(CommandResult::success()
                .with_message("Environment file generated")
                .with_data(info))
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

fn setup_env(
    tls_dir: &PathBuf,
    output: &PathBuf,
    mtls: bool,
    nats: bool,
    append: bool,
) -> Result<serde_json::Value> {
    use std::fs::OpenOptions;
    use std::io::Write;

    let tls_dir = std::fs::canonicalize(tls_dir).unwrap_or_else(|_| tls_dir.clone());

    // Verify required files exist
    let server_cert = tls_dir.join("server.pem");
    let server_key = tls_dir.join("server-key.pem");

    if !server_cert.exists() {
        anyhow::bail!(
            "Server certificate not found at {}. Run 'cargo xtask xtr tls generate-dev-certs' first.",
            server_cert.display()
        );
    }
    if !server_key.exists() {
        anyhow::bail!(
            "Server key not found at {}. Run 'cargo xtask xtr tls generate-dev-certs' first.",
            server_key.display()
        );
    }

    let mut content = String::new();
    content.push_str("# Sinex TLS Configuration\n");
    content.push_str("# Generated by: cargo xtask xtr tls setup-env\n\n");

    // Gateway TLS
    content.push_str("# Gateway TLS\n");
    content.push_str(&format!(
        "SINEX_GATEWAY_TLS_CERT={}\n",
        server_cert.display()
    ));
    content.push_str(&format!("SINEX_GATEWAY_TLS_KEY={}\n", server_key.display()));

    // mTLS configuration
    if mtls {
        let ca_cert = tls_dir.join("ca.pem");
        if !ca_cert.exists() {
            anyhow::bail!(
                "CA certificate not found at {}. Run 'cargo xtask xtr tls generate-dev-certs' first.",
                ca_cert.display()
            );
        }
        content.push_str(&format!(
            "SINEX_GATEWAY_TLS_CLIENT_CA={}\n",
            ca_cert.display()
        ));
        content.push_str("SINEX_GATEWAY_REQUIRE_CLIENT_TLS=1\n");
    }

    // NATS TLS
    if nats {
        content.push_str("\n# NATS TLS\n");
        content.push_str("SINEX_NATS_REQUIRE_TLS=1\n");
        content.push_str(&format!(
            "SINEX_NATS_CA_CERT={}\n",
            tls_dir.join("ca.pem").display()
        ));

        // Client certs for NATS mTLS
        let client_cert = tls_dir.join("client.pem");
        let client_key = tls_dir.join("client-key.pem");
        if client_cert.exists() && client_key.exists() {
            content.push_str(&format!(
                "SINEX_NATS_CLIENT_CERT={}\n",
                client_cert.display()
            ));
            content.push_str(&format!("SINEX_NATS_CLIENT_KEY={}\n", client_key.display()));
        }
    }

    // Write to file
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(output)?;

    if append {
        content = format!("\n{content}");
    }

    file.write_all(content.as_bytes())?;

    Ok(serde_json::json!({
        "output_file": output.display().to_string(),
        "gateway_tls": true,
        "mtls_enabled": mtls,
        "nats_tls": nats,
    }))
}
