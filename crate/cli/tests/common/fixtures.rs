//! Test fixtures and helpers for sinex-cli testing

#![allow(dead_code, clippy::expect_used)]

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
    pub(crate) fn new() -> Self {
        let dir = TempDir::new().expect("failed to create temp directory");
        let path = dir.path().to_path_buf();
        Self { _dir: dir, path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Create a file with given content
    pub(crate) fn create_file(&self, name: &str, content: &str) -> PathBuf {
        let path = self.path.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create parent directories");
        }
        let mut file = fs::File::create(&path).expect("failed to create file");
        file.write_all(content.as_bytes())
            .expect("failed to write file content");
        path
    }

    /// Create a file with specific permissions (Unix only)
    #[cfg(unix)]
    pub(crate) fn create_file_with_mode(&self, name: &str, content: &str, mode: u32) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = self.create_file(name, content);
        fs::set_permissions(&path, fs::Permissions::from_mode(mode))
            .expect("failed to set file permissions");
        path
    }

    /// Create a directory
    pub(crate) fn create_dir(&self, name: &str) -> PathBuf {
        let path = self.path.join(name);
        fs::create_dir_all(&path).expect("failed to create directory");
        path
    }
}

/// Config file builder for testing
pub struct ConfigFixture {
    default_format: String,
    editor: String,
    table_style: String,
}

impl ConfigFixture {
    pub(crate) fn new() -> Self {
        Self {
            default_format: "table".to_string(),
            editor: "vim".to_string(),
            table_style: "rounded".to_string(),
        }
    }

    pub(crate) fn default_format(mut self, format: &str) -> Self {
        self.default_format = format.to_string();
        self
    }

    pub(crate) fn editor(mut self, editor: &str) -> Self {
        self.editor = editor.to_string();
        self
    }

    pub(crate) fn table_style(mut self, table_style: &str) -> Self {
        self.table_style = table_style.to_string();
        self
    }

    pub(crate) fn to_yaml(&self) -> String {
        let mut yaml = format!("default_format: \"{}\"\n", &self.default_format);
        yaml.push_str(&format!("editor: \"{}\"\n", &self.editor));
        yaml.push_str("theme:\n");
        yaml.push_str(&format!("  table_style: \"{}\"\n", &self.table_style));
        yaml
    }

    pub(crate) fn to_toml(&self) -> String {
        let mut toml = format!("default_format = \"{}\"\n", &self.default_format);
        toml.push_str(&format!("editor = \"{}\"\n", &self.editor));
        toml.push_str("[theme]\n");
        toml.push_str(&format!("table_style = \"{}\"\n", &self.table_style));
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
    pub(crate) fn valid() -> &'static str {
        "sinex_test_token_1234567890abcdef"
    }

    /// Token with special characters
    pub(crate) fn with_special_chars() -> &'static str {
        "token-with-dashes_and_underscores.dots"
    }

    /// Very long token
    pub(crate) fn long() -> String {
        "sinex_".to_string() + &"x".repeat(500)
    }

    /// Empty token
    pub(crate) fn empty() -> &'static str {
        ""
    }

    /// Token with newline (invalid)
    pub(crate) fn with_newline() -> &'static str {
        "token\nwith\nnewlines"
    }
}

/// TLS certificate fixtures for testing
pub struct TlsFixture;

impl TlsFixture {
    /// Valid self-signed certificate (PEM)
    pub(crate) fn valid_cert() -> &'static str {
        "-----BEGIN CERTIFICATE-----\n\
         MIIBkTCB+wIJAKHHCgVZU1W/MA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl\n\
         c3RDQTAeFw0yNDAxMDEwMDAwMDBaFw0yNTAxMDEwMDAwMDBaMBExDzANBgNVBAMM\n\
         BnRlc3RDQTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAwL5kL8qQ8zYxV9Qd\n\
         -----END CERTIFICATE-----"
    }

    /// Invalid certificate (malformed PEM)
    pub(crate) fn invalid_cert() -> &'static str {
        "-----BEGIN CERTIFICATE-----\n\
         THIS IS NOT A VALID CERTIFICATE\n\
         -----END CERTIFICATE-----"
    }

    /// Expired certificate marker
    pub(crate) fn expired_cert() -> &'static str {
        "-----BEGIN CERTIFICATE-----\n\
         MIIBkTCB+wIJAKHHCgVZU1W/MA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl\n\
         c3RDQTAeFw0yMDAxMDEwMDAwMDBaFw0yMDAxMDIwMDAwMDBaMBExDzANBgNVBAMM\n\
         -----END CERTIFICATE-----"
    }

    /// Valid private key (PEM)
    pub(crate) fn valid_key() -> &'static str {
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
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_mins(1)))
            .mount(server)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_test_dir_creates_temp_directory() -> TestResult<()> {
        let dir = TestDir::new();
        assert!(dir.path().exists());
        assert!(dir.path().is_dir());
        Ok(())
    }

    #[sinex_test]
    async fn test_test_dir_cleans_up() -> TestResult<()> {
        let path = {
            let dir = TestDir::new();
            dir.path().to_path_buf()
        };
        // After drop, directory should be gone
        assert!(!path.exists());
        Ok(())
    }

    #[sinex_test]
    async fn test_create_file() -> TestResult<()> {
        let dir = TestDir::new();
        let file = dir.create_file("test.txt", "content");
        assert!(file.exists());
        assert_eq!(
            fs::read_to_string(&file).expect("failed to read file"),
            "content"
        );
        Ok(())
    }

    #[sinex_test]
    #[cfg(unix)]
    async fn test_create_file_with_mode() -> TestResult<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = TestDir::new();
        let file = dir.create_file_with_mode("secret.txt", "password", 0o600);
        let perms = fs::metadata(&file)
            .expect("failed to get file metadata")
            .permissions();
        assert_eq!(perms.mode() & 0o777, 0o600);
        Ok(())
    }

    #[sinex_test]
    async fn test_config_fixture_yaml() -> TestResult<()> {
        let config = ConfigFixture::new()
            .default_format("json")
            .editor("helix")
            .table_style("minimal");

        let yaml = config.to_yaml();
        assert!(yaml.contains("default_format: \"json\""));
        assert!(yaml.contains("editor: \"helix\""));
        assert!(yaml.contains("table_style: \"minimal\""));
        Ok(())
    }

    #[sinex_test]
    async fn test_config_fixture_toml() -> TestResult<()> {
        let config = ConfigFixture::new()
            .default_format("yaml")
            .table_style("ascii");

        let toml = config.to_toml();
        assert!(toml.contains("default_format = \"yaml\""));
        assert!(toml.contains("table_style = \"ascii\""));
        Ok(())
    }

    #[sinex_test]
    async fn test_token_fixtures() -> TestResult<()> {
        assert!(!TokenFixture::valid().is_empty());
        assert!(TokenFixture::long().len() > 500);
        assert!(TokenFixture::empty().is_empty());
        Ok(())
    }
}
