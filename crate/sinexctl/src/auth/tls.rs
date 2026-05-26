use std::path::Path;

use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::Result;

/// Load root CA certificate from PEM file
pub fn load_root_ca(ca_cert_path: &Path) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    let certs: Vec<CertificateDer> =
        CertificateDer::pem_file_iter(ca_cert_path)?.collect::<std::result::Result<Vec<_>, _>>()?;

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
    let certs: Vec<CertificateDer> =
        CertificateDer::pem_file_iter(cert_path)?.collect::<std::result::Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err(color_eyre::eyre::eyre!(
            "No certificates found in {:?}",
            cert_path
        ));
    }

    let key = PrivateKeyDer::from_pem_file(key_path)
        .map_err(|e| color_eyre::eyre::eyre!("No private key found in {:?}: {e}", key_path))?;

    Ok((certs, key))
}
