//! Test fixtures and helpers for sinex-cli testing

#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Temporary test directory that cleans up on drop
pub struct TestDir {
    _dir: TempDir,
    path: PathBuf,
}

impl TestDir {
    pub fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        Self { _dir: dir, path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Create a file with given content
    pub fn create_file(&self, name: &str, content: &str) -> PathBuf {
        let path = self.path.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        path
    }

    /// Create a file with specific permissions (Unix only)
    #[cfg(unix)]
    pub fn create_file_with_mode(&self, name: &str, content: &str, mode: u32) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = self.create_file(name, content);
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    /// Create a directory
    pub fn create_dir(&self, name: &str) -> PathBuf {
        let path = self.path.join(name);
        fs::create_dir_all(&path).unwrap();
        path
    }
}

/// Config file builder for testing
pub struct ConfigFixture {
    rpc_url: String,
    token: Option<String>,
    token_file: Option<String>,
    ca_cert: Option<String>,
    client_cert: Option<String>,
    client_key: Option<String>,
    insecure: bool,
    timeout: u64,
}

impl ConfigFixture {
    pub fn new() -> Self {
        Self {
            rpc_url: "https://localhost:9999".to_string(),
            token: None,
            token_file: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            insecure: false,
            timeout: 30,
        }
    }

    pub fn rpc_url(mut self, url: &str) -> Self {
        self.rpc_url = url.to_string();
        self
    }

    pub fn token(mut self, token: &str) -> Self {
        self.token = Some(token.to_string());
        self
    }

    pub fn token_file(mut self, path: &str) -> Self {
        self.token_file = Some(path.to_string());
        self
    }

    pub fn insecure(mut self) -> Self {
        self.insecure = true;
        self
    }

    pub fn timeout(mut self, secs: u64) -> Self {
        self.timeout = secs;
        self
    }

    pub fn to_yaml(&self) -> String {
        let mut yaml = format!("rpc_url: \"{}\"\n", self.rpc_url);
        if let Some(ref token) = self.token {
            yaml.push_str(&format!("token: \"{}\"\n", token));
        }
        if let Some(ref token_file) = self.token_file {
            yaml.push_str(&format!("token_file: \"{}\"\n", token_file));
        }
        if let Some(ref ca_cert) = self.ca_cert {
            yaml.push_str(&format!("ca_cert: \"{}\"\n", ca_cert));
        }
        if let Some(ref client_cert) = self.client_cert {
            yaml.push_str(&format!("client_cert: \"{}\"\n", client_cert));
        }
        if let Some(ref client_key) = self.client_key {
            yaml.push_str(&format!("client_key: \"{}\"\n", client_key));
        }
        yaml.push_str(&format!("insecure: {}\n", self.insecure));
        yaml.push_str(&format!("timeout: {}\n", self.timeout));
        yaml
    }

    pub fn to_toml(&self) -> String {
        let mut toml = format!("rpc_url = \"{}\"\n", self.rpc_url);
        if let Some(ref token) = self.token {
            toml.push_str(&format!("token = \"{}\"\n", token));
        }
        if let Some(ref token_file) = self.token_file {
            toml.push_str(&format!("token_file = \"{}\"\n", token_file));
        }
        toml.push_str(&format!("insecure = {}\n", self.insecure));
        toml.push_str(&format!("timeout = {}\n", self.timeout));
        toml
    }
}

impl Default for ConfigFixture {
    fn default() -> Self {
        Self::new()
    }
}

/// Token fixture builder
pub struct TokenFixture;

impl TokenFixture {
    /// Valid bearer token
    pub fn valid() -> &'static str {
        "sinex_test_token_1234567890abcdef"
    }

    /// Token with special characters
    pub fn with_special_chars() -> &'static str {
        "token-with-dashes_and_underscores.dots"
    }

    /// Very long token
    pub fn long() -> String {
        "sinex_".to_string() + &"x".repeat(500)
    }

    /// Empty token
    pub fn empty() -> &'static str {
        ""
    }

    /// Token with newline (invalid)
    pub fn with_newline() -> &'static str {
        "token\nwith\nnewlines"
    }
}

/// TLS certificate fixtures for testing
pub struct TlsFixture;

impl TlsFixture {
    /// Valid self-signed certificate (PEM)
    pub fn valid_cert() -> &'static str {
        "-----BEGIN CERTIFICATE-----\n\
         MIIBkTCB+wIJAKHHCgVZU1W/MA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl\n\
         c3RDQTAeFw0yNDAxMDEwMDAwMDBaFw0yNTAxMDEwMDAwMDBaMBExDzANBgNVBAMM\n\
         BnRlc3RDQTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAwL5kL8qQ8zYxV9Qd\n\
         -----END CERTIFICATE-----"
    }

    /// Invalid certificate (malformed PEM)
    pub fn invalid_cert() -> &'static str {
        "-----BEGIN CERTIFICATE-----\n\
         THIS IS NOT A VALID CERTIFICATE\n\
         -----END CERTIFICATE-----"
    }

    /// Expired certificate marker
    pub fn expired_cert() -> &'static str {
        "-----BEGIN CERTIFICATE-----\n\
         MIIBkTCB+wIJAKHHCgVZU1W/MA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl\n\
         c3RDQTAeFw0yMDAxMDEwMDAwMDBaFw0yMDAxMDIwMDAwMDBaMBExDzANBgNVBAMM\n\
         -----END CERTIFICATE-----"
    }

    /// Valid private key (PEM)
    pub fn valid_key() -> &'static str {
        "-----BEGIN PRIVATE KEY-----\n\
         MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAMC+ZC/KkPM2MVfU\n\
         -----END PRIVATE KEY-----"
    }
}

/// HTTP mock server helpers
#[cfg(feature = "wiremock")]
pub mod http {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    pub async fn mock_gateway() -> MockServer {
        MockServer::start().await
    }

    pub async fn mock_success_response(
        server: &MockServer,
        path_str: &str,
        body: impl Into<String>,
    ) {
        Mock::given(method("POST"))
            .and(path(path_str))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(server)
            .await;
    }

    pub async fn mock_error_response(server: &MockServer, path_str: &str, status: u16) {
        Mock::given(method("POST"))
            .and(path(path_str))
            .respond_with(ResponseTemplate::new(status))
            .mount(server)
            .await;
    }

    pub async fn mock_timeout_response(server: &MockServer, path_str: &str) {
        use std::time::Duration;
        Mock::given(method("POST"))
            .and(path(path_str))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
            .mount(server)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_dir_creates_temp_directory() {
        let dir = TestDir::new();
        assert!(dir.path().exists());
        assert!(dir.path().is_dir());
    }

    #[test]
    fn test_test_dir_cleans_up() {
        let path = {
            let dir = TestDir::new();
            dir.path().to_path_buf()
        };
        // After drop, directory should be gone
        assert!(!path.exists());
    }

    #[test]
    fn test_create_file() {
        let dir = TestDir::new();
        let file = dir.create_file("test.txt", "content");
        assert!(file.exists());
        assert_eq!(fs::read_to_string(&file).unwrap(), "content");
    }

    #[test]
    #[cfg(unix)]
    fn test_create_file_with_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TestDir::new();
        let file = dir.create_file_with_mode("secret.txt", "password", 0o600);
        let perms = fs::metadata(&file).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o600);
    }

    #[test]
    fn test_config_fixture_yaml() {
        let config = ConfigFixture::new()
            .rpc_url("https://example.com")
            .token("test-token")
            .timeout(60);

        let yaml = config.to_yaml();
        assert!(yaml.contains("rpc_url: \"https://example.com\""));
        assert!(yaml.contains("token: \"test-token\""));
        assert!(yaml.contains("timeout: 60"));
    }

    #[test]
    fn test_config_fixture_toml() {
        let config = ConfigFixture::new().insecure().timeout(120);

        let toml = config.to_toml();
        assert!(toml.contains("insecure = true"));
        assert!(toml.contains("timeout = 120"));
    }

    #[test]
    fn test_token_fixtures() {
        assert!(!TokenFixture::valid().is_empty());
        assert!(TokenFixture::long().len() > 500);
        assert!(TokenFixture::empty().is_empty());
    }
}
