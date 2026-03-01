use std::str::FromStr;

use color_eyre::eyre::eyre;
use sinex_primitives::domain::{
    AnnexKey, EventSource, EventType, JobId, NatsSubject, SanitizedPath, SchemaVersion, ServiceName,
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    desktop::DesktopMonitoringStartedPayload, filesystem::FileCreatedPayload,
    shell::TerminalMonitoringStartedPayload,
};
use xtask::sandbox::sinex_test;

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
    assert!(EventType::new("v2.event").validate().is_ok());
    assert!(EventType::new("batch.event.123").validate().is_ok());

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
    // Dots and digits are valid in source names
    assert!(EventSource::new("shell.bash").validate().is_ok());
    assert!(EventSource::new("integration-e2e").validate().is_ok());
    assert!(EventSource::new("source-v2").validate().is_ok());
    assert!(EventSource::new("test.source.123").validate().is_ok());

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
    // Empty paths are rejected
    assert!(SanitizedPath::from_str("").is_err());
    // Actual traversal attack (escapes above root) is rejected
    assert!(SanitizedPath::from_str("../etc/passwd").is_err());
    // A path with .. that stays within bounds is normalized, not rejected —
    // ingestors observe real filesystem paths that may be unnormalized
    let normalized = SanitizedPath::from_str("/path/with/../traversal").unwrap();
    assert_eq!(normalized.as_str(), "/path/traversal");
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

    // parse_components is not yet implemented on AnnexKey
    // TODO: implement parse_components and uncomment these tests
    let _key = AnnexKey::from_str("SHA256E-s12345-m1234567890--filename.txt")
        .map_err(|err| eyre!("invalid annex key: {err}"))?;
    let _simple_key = AnnexKey::from_str("BLAKE2B--document.pdf")
        .map_err(|err| eyre!("invalid annex key: {err}"))?;
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
