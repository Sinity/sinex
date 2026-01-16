use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;

use crate::Result;

/// Load root CA certificate from PEM file
pub fn load_root_ca(ca_cert_path: &Path) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    let ca_file = File::open(ca_cert_path)?;
    let mut reader = BufReader::new(ca_file);

    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    for cert in certs {
        root_store.add(cert)?;
    }

    Ok(root_store)
}

/// Load client certificate and private key from PEM files
pub fn load_client_cert(
    cert_path: &Path,
    key_path: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    // Load certificate
    let cert_file = File::open(cert_path)?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err(color_eyre::eyre::eyre!(
            "No certificates found in {:?}",
            cert_path
        ));
    }

    // Load private key
    let key_file = File::open(key_path)?;
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| color_eyre::eyre::eyre!("No private key found in {:?}", key_path))?;

    Ok((certs, key))
}
