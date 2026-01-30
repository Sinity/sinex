use std::str::FromStr;

use color_eyre::eyre::eyre;
use sinex_primitives::domain::{
    AbsoluteUri, AnnexKey, Blake3Hash, EventSource, EventType, JobId, NatsSubject, RelativePath,
    SanitizedPath, SchemaVersion, ServiceName, Sha256Hash,
};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::events::payloads::{
    desktop::DesktopMonitoringStartedPayload, filesystem::FileCreatedPayload,
    shell::TerminalMonitoringStartedPayload,
};
use sinex_primitives::events::EventPayload;
use xtask::sandbox::sinex_test;
use xtask::sandbox::TestResult;

#[sinex_test]
fn string_wrappers_retain_values() -> TestResult<()> {
    let source = FileCreatedPayload::SOURCE;
    assert_eq!(source.as_str(), "fs-watcher");
    assert_eq!(source.to_string(), "fs-watcher");

    let event_type = EventType::from("file.created");
    assert_eq!(event_type.as_str(), "file.created");
    Ok(())
}

#[sinex_test]
fn event_type_validation_enforces_format() -> TestResult<()> {
    assert!(EventType::new("file.created").validate().is_ok());
    assert!(EventType::new("command.executed").validate().is_ok());
    assert!(EventType::new("window.focus-changed").validate().is_ok());

    assert!(EventType::new("").validate().is_err());
    assert!(EventType::new(".file").validate().is_err());
    assert!(EventType::new("file.").validate().is_err());
    assert!(EventType::new("file..created").validate().is_err());
    assert!(EventType::new("File.Created").validate().is_err());
    Ok(())
}

#[sinex_test]
fn event_source_validation_preserves_rules() -> TestResult<()> {
    assert!(FileCreatedPayload::SOURCE.validate().is_ok());
    assert!(TerminalMonitoringStartedPayload::SOURCE.validate().is_ok());
    assert!(DesktopMonitoringStartedPayload::SOURCE.validate().is_ok());

    assert!(EventSource::new("").validate().is_err());
    assert!(EventSource::new("FS-Watcher").validate().is_err());
    assert!(EventSource::new("fs watcher").validate().is_err());
    Ok(())
}

#[sinex_test]
fn schema_version_validation_matches_semver() -> TestResult<()> {
    assert!(SchemaVersion::new("1.0.0").validate().is_ok());
    assert!(SchemaVersion::new("0.1.0").validate().is_ok());
    assert!(SchemaVersion::new("10.20.30").validate().is_ok());

    assert!(SchemaVersion::new("").validate().is_err());
    assert!(SchemaVersion::new("1.0").validate().is_err());
    assert!(SchemaVersion::new("1.0.0.0").validate().is_err());
    assert!(SchemaVersion::new("1.0.alpha").validate().is_err());
    Ok(())
}

#[sinex_test]
fn domain_types_remain_distinct() -> TestResult<()> {
    let source = EventSource::new("test");
    let event_type = EventType::new("test");
    assert_eq!(source.as_str(), event_type.as_str());
    Ok(())
}

#[sinex_test]
fn sanitized_path_validation_blocks_traversal() -> TestResult<()> {
    assert!(SanitizedPath::from_str("").is_err());
    assert!(SanitizedPath::from_str("../etc/passwd").is_err());
    assert!(SanitizedPath::from_str("/path/with/../traversal").is_err());
    Ok(())
}

#[sinex_test]
fn relative_path_validation_restricts_absolute_inputs() -> TestResult<()> {
    assert!(RelativePath::from_str("file.txt").is_ok());
    assert!(RelativePath::from_str("dir/file.txt").is_ok());
    assert!(RelativePath::from_str("./file.txt").is_ok());

    assert!(RelativePath::from_str("").is_err());
    assert!(RelativePath::from_str("/absolute/path").is_err());
    assert!(RelativePath::from_str("../parent").is_err());
    Ok(())
}

#[sinex_test]
fn absolute_uri_validation_checks_scheme() -> TestResult<()> {
    assert!(AbsoluteUri::from_str("https://example.com").is_ok());
    assert!(AbsoluteUri::from_str("file:///path/to/file").is_ok());
    assert!(AbsoluteUri::from_str("postgresql://user:pass@host:5432/db").is_ok());

    assert!(AbsoluteUri::from_str("").is_err());
    assert!(AbsoluteUri::from_str("not-a-uri").is_err());
    assert!(AbsoluteUri::from_str("relative/path").is_err());
    Ok(())
}

#[sinex_test]
fn blake3_hash_validation_enforces_length_and_hex() -> TestResult<()> {
    let valid_hash = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
    assert!(Blake3Hash::from_str(valid_hash).is_ok());
    assert!(Blake3Hash::from_str(&valid_hash.to_uppercase()).is_ok());

    assert!(Blake3Hash::from_str("").is_err());
    assert!(Blake3Hash::from_str("too_short").is_err());
    assert!(Blake3Hash::from_str(
        "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3X"
    )
    .is_err());
    assert!(Blake3Hash::from_str(
        "g665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3"
    )
    .is_err());

    let hash = Blake3Hash::from_str(&valid_hash.to_uppercase()).unwrap();
    assert_eq!(hash.as_str(), valid_hash);
    Ok(())
}

#[sinex_test]
fn sha256_hash_validation_matches_expectations() -> TestResult<()> {
    let valid_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    assert!(Sha256Hash::from_str(valid_hash).is_ok());
    assert!(Sha256Hash::from_str(&valid_hash.to_uppercase()).is_ok());

    assert!(Sha256Hash::from_str("").is_err());
    assert!(Sha256Hash::from_str("too_short").is_err());
    assert!(Sha256Hash::from_str(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855X"
    )
    .is_err());
    assert!(Sha256Hash::from_str(
        "g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    .is_err());

    let hash = Sha256Hash::from_str(&valid_hash.to_uppercase()).unwrap();
    assert_eq!(hash.as_str(), valid_hash);
    Ok(())
}

#[sinex_test]
fn annex_key_validation_and_parsing() -> TestResult<()> {
    assert!(AnnexKey::from_str("SHA256E-s12345--filename.txt").is_ok());
    assert!(AnnexKey::from_str("BLAKE2B--somefile").is_ok());
    assert!(AnnexKey::from_str("SHA1-s1024-m1234567890--document.pdf").is_ok());

    assert!(AnnexKey::from_str("").is_err());
    assert!(AnnexKey::from_str("no-double-dash").is_err());
    assert!(AnnexKey::from_str("--no-prefix").is_err());
    assert!(AnnexKey::from_str("prefix--").is_err());
    assert!(AnnexKey::from_str("multiple--double--dashes").is_err());

    let key = AnnexKey::from_str("SHA256E-s12345-m1234567890--filename.txt")
        .map_err(|err| eyre!("invalid annex key: {err}"))?;
    let (backend, size, mtime, filename) = key
        .parse_components()
        .ok_or_else(|| color_eyre::eyre::eyre!("invalid annex key"))?;
    assert_eq!(backend, "SHA256E");
    assert_eq!(size, Some(12345));
    assert_eq!(mtime, Some(1234567890));
    assert_eq!(filename, "filename.txt");

    let simple_key = AnnexKey::from_str("BLAKE2B--document.pdf")
        .map_err(|err| eyre!("invalid annex key: {err}"))?;
    let (backend, size, mtime, filename) = simple_key
        .parse_components()
        .ok_or_else(|| color_eyre::eyre::eyre!("invalid annex key"))?;
    assert_eq!(backend, "BLAKE2B");
    assert_eq!(size, None);
    assert_eq!(mtime, None);
    assert_eq!(filename, "document.pdf");
    Ok(())
}

#[sinex_test]
fn nats_subject_validation_rejects_invalid_patterns() -> TestResult<()> {
    assert!(NatsSubject::from_str("events").is_ok());
    assert!(NatsSubject::from_str("events.filesystem").is_ok());
    assert!(NatsSubject::from_str("events.filesystem.file-created").is_ok());
    assert!(NatsSubject::from_str("system_monitor.cpu_usage").is_ok());

    assert!(NatsSubject::from_str("").is_err());
    assert!(NatsSubject::from_str(".events").is_err());
    assert!(NatsSubject::from_str("events.").is_err());
    assert!(NatsSubject::from_str("events..filesystem").is_err());
    assert!(NatsSubject::from_str("events.file system").is_err());
    assert!(NatsSubject::from_str("events.file@system").is_err());
    Ok(())
}

#[sinex_test]
fn service_name_and_job_id_accept_standard_inputs() -> TestResult<()> {
    assert!(ServiceName::from_str("sinex-ingestd").is_ok());
    assert!(ServiceName::from_str("fs-watcher").is_ok());
    assert!(JobId::from_str("job_12345").is_ok());
    assert!(JobId::from_str("background-task-001").is_ok());
    Ok(())
}
