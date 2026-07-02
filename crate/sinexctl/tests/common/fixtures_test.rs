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
