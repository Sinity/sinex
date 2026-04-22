use camino::Utf8Path;
use sinex_node_sdk::content_store::manager::ContentStoreManager;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn detect_mime_type_matches_extension() -> TestResult<()> {
    assert_eq!(
        ContentStoreManager::detect_mime_type(Utf8Path::new("test.txt"))?,
        "text/plain"
    );
    assert_eq!(
        ContentStoreManager::detect_mime_type(Utf8Path::new("image.jpg"))?,
        "image/jpeg"
    );
    Ok(())
}
