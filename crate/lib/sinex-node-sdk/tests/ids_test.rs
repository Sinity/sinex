use sinex_node_sdk::ids::{deterministic_event_id, deterministic_material_event_id};
use sinex_primitives::Timestamp;
use xtask::sandbox::prelude::*;

fn fixed_timestamp() -> TestResult<Timestamp> {
    Timestamp::from_unix_timestamp_millis(1_710_000_000_123)
        .ok_or_else(|| color_eyre::eyre::eyre!("test timestamp should be valid"))
}

#[sinex_test]
async fn deterministic_event_id_is_stable_for_same_occurrence() -> TestResult<()> {
    let first = deterministic_event_id("journal", "cursor=same", fixed_timestamp()?);
    let second = deterministic_event_id("journal", "cursor=same", fixed_timestamp()?);

    assert_eq!(first, second);
    assert_eq!(first.get_version_num(), 7);
    assert_eq!(first.get_variant(), uuid::Variant::RFC4122);
    Ok(())
}

#[sinex_test]
async fn deterministic_event_id_separates_source_anchor_and_timestamp() -> TestResult<()> {
    let baseline = deterministic_event_id("journal", "cursor=same", fixed_timestamp()?);

    assert_ne!(
        baseline,
        deterministic_event_id("systemd", "cursor=same", fixed_timestamp()?)
    );
    assert_ne!(
        baseline,
        deterministic_event_id("journal", "cursor=other", fixed_timestamp()?)
    );
    assert_ne!(
        baseline,
        deterministic_event_id(
            "journal",
            "cursor=same",
            Timestamp::from_unix_timestamp_millis(1_710_000_000_124)
                .ok_or_else(|| color_eyre::eyre::eyre!("test timestamp should be valid"))?
        )
    );
    Ok(())
}

#[sinex_test]
async fn deterministic_event_id_carries_timestamp_millis() -> TestResult<()> {
    let uuid = deterministic_event_id("journal", "cursor=same", fixed_timestamp()?);
    let (seconds, nanos) = uuid
        .get_timestamp()
        .ok_or_else(|| {
            color_eyre::eyre::eyre!("deterministic event id should carry UUIDv7 timestamp")
        })?
        .to_unix();

    assert_eq!(seconds, 1_710_000_000);
    assert_eq!(nanos, 123_000_000);
    Ok(())
}

#[sinex_test]
async fn deterministic_material_event_id_uses_material_anchor() -> TestResult<()> {
    let material_id = uuid::Uuid::now_v7();
    let baseline = deterministic_material_event_id(
        "terminal.history",
        "shell.command",
        material_id,
        16,
        Some(16),
        Some(48),
        fixed_timestamp()?,
    );

    assert_eq!(
        baseline,
        deterministic_material_event_id(
            "terminal.history",
            "shell.command",
            material_id,
            16,
            Some(16),
            Some(48),
            fixed_timestamp()?
        )
    );
    assert_ne!(
        baseline,
        deterministic_material_event_id(
            "terminal.history",
            "shell.command",
            material_id,
            17,
            Some(17),
            Some(48),
            fixed_timestamp()?
        )
    );
    assert_eq!(baseline.get_version_num(), 7);
    assert_eq!(baseline.get_variant(), uuid::Variant::RFC4122);
    Ok(())
}
