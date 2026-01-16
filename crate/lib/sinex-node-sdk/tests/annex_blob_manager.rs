use camino::Utf8Path;
use sinex_node_sdk::annex::blob_manager::BlobManager;
use sinex_test_utils::sinex_test;
use sinex_test_utils::TestResult;

#[sinex_test]
fn detect_mime_type_matches_extension() -> TestResult<()> {
    assert_eq!(
        BlobManager::detect_mime_type(Utf8Path::new("test.txt"))?,
        "text/plain"
    );
    assert_eq!(
        BlobManager::detect_mime_type(Utf8Path::new("image.jpg"))?,
        "image/jpeg"
    );
    Ok(())
}
