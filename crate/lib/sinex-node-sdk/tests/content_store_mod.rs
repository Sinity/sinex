use camino::Utf8Path;
use sinex_node_sdk::content_store::{ContentStoreKey, MaterialContentStore};
use tempfile::TempDir;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn content_key_parse_extracts_components() -> TestResult<()> {
    let key = ContentStoreKey::parse("SHA256E-s12345--abcdef123456.dat")?;
    assert_eq!(key.storage_backend(), "SHA256E");
    assert_eq!(key.size, 12345);
    assert!(key.digest.contains("abcdef123456"));

    assert!(ContentStoreKey::parse("SHA256E--abcdef123456.dat").is_err());
    assert!(ContentStoreKey::parse("SHA256E-snope--abcdef123456.dat").is_err());
    assert!(ContentStoreKey::parse("SHA256E-s1--abc--def.dat").is_err());
    Ok(())
}

#[sinex_test]
async fn compute_blake3_hash_produces_hex_digest() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("test.txt");
    tokio::fs::write(&test_file, b"hello world").await?;
    let test_path = Utf8Path::from_path(&test_file).expect("tempfile path must be UTF-8");

    let hash = MaterialContentStore::compute_blake3_hash(test_path).await?;
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    Ok(())
}
