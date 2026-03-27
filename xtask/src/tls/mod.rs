//! TLS certificate management utilities for development and production.
//!
//! Library functions only — no CLI commands. Certificate generation is invoked
//! automatically by preflight (`ensure_tls_certs`). Production PKI operations
//! (generate-ca, generate-client-cert) belong in sinexctl.
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

pub use generate::{
    CertConfig, DEFAULT_DEV_CERT_VALIDITY_DAYS, generate_ca, generate_client_cert,
    generate_dev_certs,
};
pub use verify::{TlsCheckOptions, check_tls_config};
