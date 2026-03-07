//! Integration tests for clipboard logic in sinex-desktop-ingestor.
//!
//! The clipboard module's core logic (hash computation, deduplication, history
//! management, content validation) is private to the crate. These tests exercise
//! the publicly accessible surface: payload types, serde roundtrips, `EventPayload`
//! trait implementations, and BLAKE3 hash determinism (verified independently).
//!
//! For internal logic tests (dedup decisions, history eviction, content analysis),
//! see the `#[cfg(test)] mod tests` block in `src/clipboard.rs`.

use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{ClipboardCopiedPayload, ClipboardSelectedPayload};
use xtask::sandbox::prelude::*;

// ---------------------------------------------------------------------------
// BLAKE3 hash determinism (independent verification)
// ---------------------------------------------------------------------------

#[sinex_test]
async fn blake3_same_content_produces_same_hash() -> TestResult<()> {
    let content = "hello clipboard world";
    let hash1 = blake3::hash(content.as_bytes()).to_hex().to_string();
    let hash2 = blake3::hash(content.as_bytes()).to_hex().to_string();

    assert_eq!(
        hash1, hash2,
        "identical content must produce identical BLAKE3 hashes"
    );
    Ok(())
}

#[sinex_test]
async fn blake3_different_content_produces_different_hash() -> TestResult<()> {
    let hash_a = blake3::hash(b"content A").to_hex().to_string();
    let hash_b = blake3::hash(b"content B").to_hex().to_string();

    assert_ne!(
        hash_a, hash_b,
        "different content must produce different BLAKE3 hashes"
    );
    Ok(())
}

#[sinex_test]
async fn blake3_empty_string_has_stable_hash() -> TestResult<()> {
    let hash = blake3::hash(b"").to_hex().to_string();

    // BLAKE3 of empty input is a well-known constant
    assert!(!hash.is_empty());
    // Verify determinism across calls
    let hash2 = blake3::hash(b"").to_hex().to_string();
    assert_eq!(hash, hash2);
    Ok(())
}

// ---------------------------------------------------------------------------
// ClipboardCopiedPayload: serde roundtrip and trait impls
// ---------------------------------------------------------------------------

#[sinex_test]
async fn clipboard_copied_payload_serde_roundtrip() -> TestResult<()> {
    let hash = blake3::hash(b"test content").to_hex().to_string();

    let original = ClipboardCopiedPayload {
        operation: "copy".to_string(),
        content_type: "text".to_string(),
        content_size: 42,
        text_preview: Some("test content".to_string()),
        file_count: None,
        file_paths: None,
        source_app: Some("firefox".to_string()),
        window_title: Some("Example Page - Mozilla Firefox".to_string()),
        content_hash: hash.clone(),
        original_hash: None,
        annex_key: None,
        blob_id: None,
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: ClipboardCopiedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.operation, "copy");
    assert_eq!(deserialized.content_type, "text");
    assert_eq!(deserialized.content_size, 42);
    assert_eq!(deserialized.text_preview.as_deref(), Some("test content"));
    assert_eq!(deserialized.source_app.as_deref(), Some("firefox"));
    assert_eq!(
        deserialized.window_title.as_deref(),
        Some("Example Page - Mozilla Firefox")
    );
    assert_eq!(deserialized.content_hash, hash);
    assert!(deserialized.file_count.is_none());
    assert!(deserialized.file_paths.is_none());
    assert!(deserialized.original_hash.is_none());
    assert!(deserialized.annex_key.is_none());
    assert!(deserialized.blob_id.is_none());

    Ok(())
}

#[sinex_test]
async fn clipboard_copied_payload_with_files() -> TestResult<()> {
    let hash = blake3::hash(b"/home/user/doc.pdf\n/home/user/img.png")
        .to_hex()
        .to_string();

    let payload = ClipboardCopiedPayload {
        operation: "copy".to_string(),
        content_type: "files".to_string(),
        content_size: 38,
        text_preview: None,
        file_count: Some(2),
        file_paths: Some(vec![
            "/home/user/doc.pdf".to_string(),
            "/home/user/img.png".to_string(),
        ]),
        source_app: Some("nautilus".to_string()),
        window_title: Some("Files".to_string()),
        content_hash: hash,
        original_hash: None,
        annex_key: None,
        blob_id: None,
    };

    let json = serde_json::to_string(&payload)?;
    let roundtripped: ClipboardCopiedPayload = serde_json::from_str(&json)?;

    assert_eq!(roundtripped.file_count, Some(2));
    let paths = roundtripped
        .file_paths
        .expect("file_paths should be present");
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0], "/home/user/doc.pdf");
    assert_eq!(paths[1], "/home/user/img.png");

    Ok(())
}

#[sinex_test]
async fn clipboard_copied_payload_event_source_and_type() -> TestResult<()> {
    let payload = ClipboardCopiedPayload::test_default("abc123");

    assert_eq!(
        payload.event_source().as_ref(),
        "clipboard",
        "ClipboardCopiedPayload source must be 'clipboard'"
    );
    assert_eq!(
        payload.event_type().as_ref(),
        "clipboard.copied",
        "ClipboardCopiedPayload event_type must be 'clipboard.copied'"
    );

    Ok(())
}

#[sinex_test]
async fn clipboard_copied_test_default_has_correct_defaults() -> TestResult<()> {
    let payload = ClipboardCopiedPayload::test_default("hash_value");

    assert_eq!(payload.operation, "copy");
    assert_eq!(payload.content_type, "text/plain");
    assert_eq!(payload.content_size, 0);
    assert_eq!(payload.content_hash, "hash_value");
    assert!(payload.text_preview.is_none());
    assert!(payload.file_count.is_none());
    assert!(payload.source_app.is_none());

    Ok(())
}

// ---------------------------------------------------------------------------
// ClipboardSelectedPayload: serde roundtrip and trait impls
// ---------------------------------------------------------------------------

#[sinex_test]
async fn clipboard_selected_payload_serde_roundtrip() -> TestResult<()> {
    let hash = blake3::hash(b"selected text").to_hex().to_string();

    let original = ClipboardSelectedPayload {
        selection_type: "primary".to_string(),
        content_type: "text/plain".to_string(),
        content_size: 13,
        text_preview: Some("selected text".to_string()),
        source_app: Some("alacritty".to_string()),
        content_hash: hash.clone(),
        original_hash: None,
        annex_key: None,
        blob_id: None,
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: ClipboardSelectedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.selection_type, "primary");
    assert_eq!(deserialized.content_type, "text/plain");
    assert_eq!(deserialized.content_size, 13);
    assert_eq!(deserialized.text_preview.as_deref(), Some("selected text"));
    assert_eq!(deserialized.source_app.as_deref(), Some("alacritty"));
    assert_eq!(deserialized.content_hash, hash);

    Ok(())
}

#[sinex_test]
async fn clipboard_selected_payload_event_source_and_type() -> TestResult<()> {
    let payload = ClipboardSelectedPayload::test_default("abc123");

    assert_eq!(
        payload.event_source().as_ref(),
        "clipboard",
        "ClipboardSelectedPayload source must be 'clipboard'"
    );
    assert_eq!(
        payload.event_type().as_ref(),
        "clipboard.selected",
        "ClipboardSelectedPayload event_type must be 'clipboard.selected'"
    );

    Ok(())
}

#[sinex_test]
async fn clipboard_selected_test_default_has_correct_defaults() -> TestResult<()> {
    let payload = ClipboardSelectedPayload::test_default("my_hash");

    assert_eq!(payload.selection_type, "primary");
    assert_eq!(payload.content_type, "text/plain");
    assert_eq!(payload.content_size, 0);
    assert_eq!(payload.content_hash, "my_hash");
    assert!(payload.text_preview.is_none());
    assert!(payload.source_app.is_none());

    Ok(())
}

// ---------------------------------------------------------------------------
// Content size and preview edge cases in payload construction
// ---------------------------------------------------------------------------

#[sinex_test]
async fn clipboard_payload_zero_size_content() -> TestResult<()> {
    let hash = blake3::hash(b"").to_hex().to_string();

    let payload = ClipboardCopiedPayload {
        operation: "copy".to_string(),
        content_type: "text".to_string(),
        content_size: 0,
        text_preview: Some(String::new()),
        file_count: None,
        file_paths: None,
        source_app: None,
        window_title: None,
        content_hash: hash,
        original_hash: None,
        annex_key: None,
        blob_id: None,
    };

    let json = serde_json::to_string(&payload)?;
    let roundtripped: ClipboardCopiedPayload = serde_json::from_str(&json)?;

    assert_eq!(roundtripped.content_size, 0);
    assert_eq!(roundtripped.text_preview.as_deref(), Some(""));

    Ok(())
}

#[sinex_test]
async fn clipboard_payload_large_content_size_recorded() -> TestResult<()> {
    // The payload just records the size as metadata; the actual content is in
    // source material. Even very large sizes should serialize fine.
    let payload = ClipboardCopiedPayload {
        operation: "copy".to_string(),
        content_type: "text".to_string(),
        content_size: 10 * 1024 * 1024, // 10 MB
        text_preview: Some("start of very long content...".to_string()),
        file_count: None,
        file_paths: None,
        source_app: None,
        window_title: None,
        content_hash: "abcdef".to_string(),
        original_hash: None,
        annex_key: None,
        blob_id: None,
    };

    let json = serde_json::to_string(&payload)?;
    let roundtripped: ClipboardCopiedPayload = serde_json::from_str(&json)?;

    assert_eq!(roundtripped.content_size, 10 * 1024 * 1024);

    Ok(())
}

// ---------------------------------------------------------------------------
// Duplicate hash tracking (independent verification of dedup concept)
// ---------------------------------------------------------------------------

#[sinex_test]
async fn clipboard_dedup_concept_same_content_same_hash() -> TestResult<()> {
    // Verifies the dedup invariant: if two clipboard grabs produce the same text,
    // the BLAKE3 hash matches, so the watcher's dedup logic would suppress the
    // second event.
    let text = "identical paste content";
    let first_grab = blake3::hash(text.as_bytes()).to_hex().to_string();
    let second_grab = blake3::hash(text.as_bytes()).to_hex().to_string();

    assert_eq!(
        first_grab, second_grab,
        "dedup relies on identical content producing identical BLAKE3 hashes"
    );

    Ok(())
}

#[sinex_test]
async fn clipboard_dedup_concept_whitespace_matters() -> TestResult<()> {
    // Trailing whitespace or newlines should produce different hashes, which means
    // the watcher treats them as distinct clipboard contents (correct behavior).
    let hash_no_trailing = blake3::hash(b"text").to_hex().to_string();
    let hash_trailing_newline = blake3::hash(b"text\n").to_hex().to_string();
    let hash_trailing_space = blake3::hash(b"text ").to_hex().to_string();

    assert_ne!(hash_no_trailing, hash_trailing_newline);
    assert_ne!(hash_no_trailing, hash_trailing_space);
    assert_ne!(hash_trailing_newline, hash_trailing_space);

    Ok(())
}

// ---------------------------------------------------------------------------
// Original hash / dedup reference field
// ---------------------------------------------------------------------------

#[sinex_test]
async fn clipboard_payload_original_hash_tracks_dedup_reference() -> TestResult<()> {
    // When content was seen before, original_hash points to the first occurrence
    let first_hash = blake3::hash(b"reused content").to_hex().to_string();

    let payload = ClipboardCopiedPayload {
        operation: "copy".to_string(),
        content_type: "text".to_string(),
        content_size: 14,
        text_preview: Some("reused content".to_string()),
        file_count: None,
        file_paths: None,
        source_app: None,
        window_title: None,
        content_hash: first_hash.clone(),
        original_hash: Some(first_hash.clone()),
        annex_key: None,
        blob_id: None,
    };

    let json = serde_json::to_string(&payload)?;
    let roundtripped: ClipboardCopiedPayload = serde_json::from_str(&json)?;

    assert_eq!(
        roundtripped.original_hash.as_deref(),
        Some(first_hash.as_str()),
        "original_hash should survive serde roundtrip"
    );

    Ok(())
}
