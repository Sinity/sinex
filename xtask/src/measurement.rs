//! Pure calculators for bounded pre-wipe measurement reports.
//!
//! These functions deliberately consume recorded fixture deltas. They never
//! start an import, replay, compression job, or provider request, so the same
//! calculation can be rerun from an operator's captured measurements.

use color_eyre::eyre::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ManifestProjectionInput {
    pub measured_days: f64,
    pub target_days: f64,
    pub measured_material_bytes: u64,
    pub measured_manifest_bytes: u64,
    pub measured_core_event_bytes: u64,
    pub measured_cas_bytes: u64,
    pub measured_staging_bytes: u64,
    pub measured_nats_bytes: u64,
    pub measured_events: u64,
    pub copy_events_per_second: f64,
    pub available_bytes: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ManifestProjectionReport {
    pub input: ManifestProjectionInput,
    pub projected_material_bytes: u64,
    pub projected_manifest_bytes: u64,
    pub projected_core_event_bytes: u64,
    pub projected_cas_bytes: u64,
    pub projected_staging_bytes: u64,
    pub projected_nats_bytes: u64,
    pub projected_events: u64,
    pub projected_persistent_bytes: u64,
    pub staging_plus_cas_duplication_factor: Option<f64>,
    pub import_eta_seconds: f64,
    pub available_capacity_fraction: Option<f64>,
    pub capacity_headroom_bytes: Option<i128>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplayCostInput {
    pub fixture_events: u64,
    pub archived_events: u64,
    pub archive_wall_ms: u64,
    pub replay_wall_ms: u64,
    pub operation_duration_ms: Option<u64>,
    pub wal_bytes: u64,
    pub compressed_bytes_before: u64,
    pub uncompressed_bytes_before: u64,
    pub compressed_bytes_after: u64,
    pub uncompressed_bytes_after: u64,
    pub pathological_threshold_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplayCostReport {
    pub input: ReplayCostInput,
    pub archive_events_per_second: Option<f64>,
    pub replay_events_per_second: Option<f64>,
    pub archive_wall_share: Option<f64>,
    pub compressed_ratio_before: Option<f64>,
    pub compressed_ratio_after: Option<f64>,
    pub pathological_cost: bool,
}

pub fn project_manifest_storage(
    input: ManifestProjectionInput,
) -> Result<ManifestProjectionReport> {
    if !input.measured_days.is_finite() || input.measured_days <= 0.0 {
        bail!("measured_days must be finite and greater than zero");
    }
    if !input.target_days.is_finite() || input.target_days <= 0.0 {
        bail!("target_days must be finite and greater than zero");
    }
    if !input.copy_events_per_second.is_finite() || input.copy_events_per_second <= 0.0 {
        bail!("copy_events_per_second must be finite and greater than zero");
    }

    let scale = input.target_days / input.measured_days;
    let project = |value: u64| -> Result<u64> {
        let projected = value as f64 * scale;
        if !projected.is_finite() || projected > u64::MAX as f64 {
            bail!("projection overflows u64");
        }
        Ok(projected.round() as u64)
    };

    let projected_material_bytes = project(input.measured_material_bytes)?;
    let projected_manifest_bytes = project(input.measured_manifest_bytes)?;
    let projected_core_event_bytes = project(input.measured_core_event_bytes)?;
    let projected_cas_bytes = project(input.measured_cas_bytes)?;
    let projected_staging_bytes = project(input.measured_staging_bytes)?;
    let projected_nats_bytes = project(input.measured_nats_bytes)?;
    let projected_events = project(input.measured_events)?;
    let projected_persistent_bytes = projected_material_bytes
        .saturating_add(projected_manifest_bytes)
        .saturating_add(projected_core_event_bytes)
        .saturating_add(projected_cas_bytes)
        .saturating_add(projected_staging_bytes);
    let import_eta_seconds = projected_events as f64 / input.copy_events_per_second;
    let staging_plus_cas_duplication_factor = (input.measured_material_bytes > 0).then(|| {
        input
            .measured_staging_bytes
            .saturating_add(input.measured_cas_bytes) as f64
            / input.measured_material_bytes as f64
    });
    let (available_capacity_fraction, capacity_headroom_bytes) = input
        .available_bytes
        .map(|available| {
            (
                Some(projected_persistent_bytes as f64 / available.max(1) as f64),
                Some(available as i128 - projected_persistent_bytes as i128),
            )
        })
        .unwrap_or((None, None));

    Ok(ManifestProjectionReport {
        input,
        projected_material_bytes,
        projected_manifest_bytes,
        projected_core_event_bytes,
        projected_cas_bytes,
        projected_staging_bytes,
        projected_nats_bytes,
        projected_events,
        projected_persistent_bytes,
        staging_plus_cas_duplication_factor,
        import_eta_seconds,
        available_capacity_fraction,
        capacity_headroom_bytes,
    })
}

pub fn report_replay_cost(input: ReplayCostInput) -> Result<ReplayCostReport> {
    if input.archive_wall_ms == 0 || input.replay_wall_ms == 0 {
        bail!("archive_wall_ms and replay_wall_ms must be greater than zero");
    }
    if input.pathological_threshold_ms == 0 {
        bail!("pathological_threshold_ms must be greater than zero");
    }

    let rate = |events: u64, wall_ms: u64| {
        (wall_ms > 0).then(|| events as f64 / (wall_ms as f64 / 1_000.0))
    };
    let ratio = |uncompressed: u64, compressed: u64| {
        (compressed > 0).then(|| uncompressed as f64 / compressed as f64)
    };

    Ok(ReplayCostReport {
        archive_wall_share: (input.replay_wall_ms > 0)
            .then(|| input.archive_wall_ms as f64 / input.replay_wall_ms as f64),
        archive_events_per_second: rate(input.archived_events, input.archive_wall_ms),
        replay_events_per_second: rate(input.fixture_events, input.replay_wall_ms),
        compressed_ratio_before: ratio(
            input.uncompressed_bytes_before,
            input.compressed_bytes_before,
        ),
        compressed_ratio_after: ratio(input.uncompressed_bytes_after, input.compressed_bytes_after),
        pathological_cost: input.replay_wall_ms >= input.pathological_threshold_ms,
        input,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_projection_keeps_duplicate_storage_explicit() {
        let report = project_manifest_storage(ManifestProjectionInput {
            measured_days: 30.0,
            target_days: 365.0,
            measured_material_bytes: 1_000,
            measured_manifest_bytes: 200,
            measured_core_event_bytes: 3_000,
            measured_cas_bytes: 1_500,
            measured_staging_bytes: 1_000,
            measured_nats_bytes: 4_000,
            measured_events: 600,
            copy_events_per_second: 2.0,
            available_bytes: Some(100_000),
        })
        .expect("valid projection");

        assert_eq!(report.projected_events, 7_300);
        assert_eq!(report.projected_persistent_bytes, 81_517);
        assert_eq!(report.staging_plus_cas_duplication_factor, Some(2.5));
        assert_eq!(report.capacity_headroom_bytes, Some(18_483));
        assert!((report.import_eta_seconds - 3_650.0).abs() < f64::EPSILON);
    }

    #[test]
    fn replay_report_marks_hour_scale_cost_and_compression_ratio() {
        let report = report_replay_cost(ReplayCostInput {
            fixture_events: 100,
            archived_events: 100,
            archive_wall_ms: 2_000,
            replay_wall_ms: 3_600_000,
            operation_duration_ms: Some(3_700_000),
            wal_bytes: 9_000,
            compressed_bytes_before: 2_000,
            uncompressed_bytes_before: 10_000,
            compressed_bytes_after: 2_500,
            uncompressed_bytes_after: 12_500,
            pathological_threshold_ms: 3_600_000,
        })
        .expect("valid replay report");

        assert!(report.pathological_cost);
        assert_eq!(report.compressed_ratio_before, Some(5.0));
        assert_eq!(report.archive_events_per_second, Some(50.0));
    }
}
