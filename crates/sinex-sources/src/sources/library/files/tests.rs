use super::*;

#[devkit::test]
fn structured_text_files_keep_their_media_type_and_original_bytes() {
    let directory = tempfile::tempdir().unwrap();
    for (name, expected) in [
        ("records.JSON", "application/json"),
        ("records.jsonl", "application/x-ndjson"),
        ("records.NDJSON", "application/x-ndjson"),
        ("records.csv", "text/csv"),
        ("records.TSV", "text/tab-separated-values"),
        ("notes.txt", "text/plain"),
        ("notes.md", "text/plain"),
    ] {
        let path = directory.path().join(name);
        let bytes = b"{\"text\":\"original\"}\n";
        std::fs::write(&path, bytes).unwrap();
        let result = digest(&path).unwrap();
        assert_eq!(result.media_type, expected);
        assert_eq!(result.blake3, blake3::hash(bytes).to_hex().as_str());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    assert_eq!(
        media_type_for_path("application/pdf", "misnamed.jsonl"),
        "application/pdf"
    );
    assert_eq!(
        media_type_for_path("text/plain", "/folder.jsonl/notes"),
        "text/plain"
    );
}
