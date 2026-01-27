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

pub use generate::{generate_client_cert, generate_dev_certs, CertConfig};
pub use verify::check_tls_config;

use anyhow::Result;
use clap::Subcommand;
use std::path::PathBuf;

/// TLS subcommands for certificate management.
#[derive(Subcommand, Clone)]
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
        /// Path to certificate file (or reads from SINEX_GATEWAY_TLS_CERT)
        #[arg(long)]
        cert: Option<PathBuf>,

        /// Path to private key file (or reads from SINEX_GATEWAY_TLS_KEY)
        #[arg(long)]
        key: Option<PathBuf>,

        /// Path to CA certificate for mTLS (or reads from SINEX_GATEWAY_TLS_CLIENT_CA)
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
}

/// Execute a TLS subcommand.
pub fn run(cmd: TlsCommand, json: bool) -> Result<()> {
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
            generate_dev_certs(&config, json)
        }

        TlsCommand::Check {
            cert,
            key,
            ca,
            verify_chain,
            nats,
        } => check_tls_config(cert, key, ca, verify_chain, nats, json),

        TlsCommand::GenerateClientCert {
            output,
            name,
            ca_cert,
            ca_key,
            days,
        } => {
            let ca_cert = ca_cert.unwrap_or_else(|| output.join("ca.pem"));
            let ca_key = ca_key.unwrap_or_else(|| output.join("ca-key.pem"));
            generate_client_cert(&output, &name, &ca_cert, &ca_key, days, json)
        }

        TlsCommand::SetupEnv {
            tls_dir,
            output,
            mtls,
            nats,
            append,
        } => setup_env(&tls_dir, &output, mtls, nats, append, json),
    }
}

fn setup_env(
    tls_dir: &PathBuf,
    output: &PathBuf,
    mtls: bool,
    nats: bool,
    append: bool,
    json: bool,
) -> Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;

    let tls_dir = std::fs::canonicalize(tls_dir).unwrap_or_else(|_| tls_dir.clone());

    // Verify required files exist
    let server_cert = tls_dir.join("server.pem");
    let server_key = tls_dir.join("server-key.pem");

    if !server_cert.exists() {
        anyhow::bail!(
            "Server certificate not found at {}. Run 'cargo xtask tls generate-dev-certs' first.",
            server_cert.display()
        );
    }
    if !server_key.exists() {
        anyhow::bail!(
            "Server key not found at {}. Run 'cargo xtask tls generate-dev-certs' first.",
            server_key.display()
        );
    }

    let mut content = String::new();
    content.push_str("# Sinex TLS Configuration\n");
    content.push_str(&format!("# Generated by: cargo xtask tls setup-env\n\n"));

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
                "CA certificate not found at {}. Run 'cargo xtask tls generate-dev-certs' first.",
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
        content = format!("\n{}", content);
    }

    file.write_all(content.as_bytes())?;

    if json {
        let result = serde_json::json!({
            "status": "success",
            "output_file": output.display().to_string(),
            "gateway_tls": true,
            "mtls_enabled": mtls,
            "nats_tls": nats,
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!(
            "TLS environment configuration written to: {}",
            output.display()
        );
        println!();
        println!("To use, either:");
        println!("  1. Source it: source {}", output.display());
        println!(
            "  2. Use with direnv: echo 'dotenv {}' >> .envrc",
            output.display()
        );
        println!();
        if mtls {
            println!("mTLS is enabled. Clients must present valid certificates signed by the CA.");
        }
    }

    Ok(())
}
