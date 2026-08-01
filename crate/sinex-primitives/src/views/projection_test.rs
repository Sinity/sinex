use super::*;
use xtask::sandbox::sinex_test;

fn input(status: ProjectionStatus, read_time_computed: bool) -> ProjectionReadinessInput {
    ProjectionReadinessInput {
        projection_kind: "email_mailbox".to_string(),
        scope_key: "mode-1".to_string(),
        semantics_version: "v1".to_string(),
        status,
        freshness_class: ProjectionFreshnessClass::Hours,
        built_at: None,
        stale_reason: None,
        last_error: None,
        verification_command: "xtask test -p sinexd".to_string(),
        read_time_computed,
    }
}

#[sinex_test]
async fn ready_row_has_no_caveats() -> xtask::TestResult<()> {
    let view = ProjectionReadinessView::from_rows([input(ProjectionStatus::Ready, false)]);
    assert_eq!(view.count, 1);
    assert_eq!(view.ready_count, 1);
    assert_eq!(view.degraded_count, 0);
    assert!(view.projections[0].caveats.is_empty());
    Ok(())
}

#[sinex_test]
async fn absent_row_renders_readmodel_absent_caveat() -> xtask::TestResult<()> {
    let view = ProjectionReadinessView::from_rows([input(ProjectionStatus::Absent, false)]);
    assert_eq!(view.degraded_count, 1);
    let caveat = &view.projections[0].caveats[0];
    assert_eq!(caveat.id, ReadinessCaveatId::ReadmodelAbsent.as_str());
    Ok(())
}

#[sinex_test]
async fn building_row_renders_readmodel_building_caveat() -> xtask::TestResult<()> {
    let view = ProjectionReadinessView::from_rows([input(ProjectionStatus::Building, false)]);
    let caveat = &view.projections[0].caveats[0];
    assert_eq!(caveat.id, ReadinessCaveatId::ReadmodelBuilding.as_str());
    Ok(())
}

#[sinex_test]
async fn stale_row_includes_stale_reason_in_message() -> xtask::TestResult<()> {
    let mut row = input(ProjectionStatus::Stale, false);
    row.stale_reason = Some("acceptable staleness exceeded".to_string());
    let view = ProjectionReadinessView::from_rows([row]);
    let caveat = &view.projections[0].caveats[0];
    assert_eq!(caveat.id, ReadinessCaveatId::ReadmodelStaleBy.as_str());
    assert!(caveat.message.contains("acceptable staleness exceeded"));
    Ok(())
}

#[sinex_test]
async fn failed_row_includes_last_error_in_message() -> xtask::TestResult<()> {
    let mut row = input(ProjectionStatus::Failed, false);
    row.last_error = Some("connection refused".to_string());
    let view = ProjectionReadinessView::from_rows([row]);
    let caveat = &view.projections[0].caveats[0];
    assert_eq!(caveat.id, ReadinessCaveatId::ReadmodelFailed.as_str());
    assert!(caveat.message.contains("connection refused"));
    Ok(())
}

#[sinex_test]
async fn partial_row_renders_readmodel_partial_caveat() -> xtask::TestResult<()> {
    let mut row = input(ProjectionStatus::Partial, false);
    row.stale_reason = Some("half the scope covered".to_string());
    let view = ProjectionReadinessView::from_rows([row]);
    let caveat = &view.projections[0].caveats[0];
    assert_eq!(caveat.id, ReadinessCaveatId::ReadmodelPartial.as_str());
    Ok(())
}

#[sinex_test]
async fn read_time_computed_flag_is_preserved() -> xtask::TestResult<()> {
    let view = ProjectionReadinessView::from_rows([input(ProjectionStatus::Ready, true)]);
    assert!(view.projections[0].read_time_computed);
    Ok(())
}

#[sinex_test]
async fn mixed_rows_split_ready_and_degraded_counts() -> xtask::TestResult<()> {
    let view = ProjectionReadinessView::from_rows([
        input(ProjectionStatus::Ready, false),
        input(ProjectionStatus::Stale, false),
        input(ProjectionStatus::Ready, false),
    ]);
    assert_eq!(view.count, 3);
    assert_eq!(view.ready_count, 2);
    assert_eq!(view.degraded_count, 1);
    Ok(())
}
