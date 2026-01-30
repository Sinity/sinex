use camino::Utf8Path;
use sinex_node_sdk::annex::{AnnexKey, GitAnnex};
use tempfile::TempDir;
use xtask::sandbox::sinex_test;

#[sinex_test]
fn annex_key_parse_extracts_components() -> TestResult<()> {
    let key = AnnexKey::parse("SHA256E-s12345--abcdef123456.dat")?;
    assert_eq!(key.backend, "SHA256E");
    assert_eq!(key.size, 12345);
    assert!(key.hash.contains("abcdef123456"));
    Ok(())
}

#[sinex_test]
async fn compute_blake3_hash_produces_hex_digest() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("test.txt");
    tokio::fs::write(&test_file, b"hello world").await?;
    let test_path = Utf8Path::from_path(&test_file).expect("tempfile path must be UTF-8");

    let hash = GitAnnex::compute_blake3_hash(test_path).await?;
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    Ok(())
}
