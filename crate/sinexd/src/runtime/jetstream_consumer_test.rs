use super::{
    ConfirmedConsumerRetirementAction, JetStreamEventConsumerConfig,
    confirmed_filter_subject_for,
};
use crate::runtime::automaton::traits::InputProvenanceFilter;
use async_nats::jetstream::consumer::DeliverPolicy;
use sinex_primitives::environment::SinexEnvironment;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn default_consumer_config_targets_confirmed_firehose() -> xtask::sandbox::TestResult<()>
{
    let config = JetStreamEventConsumerConfig::default();
    assert!(config.event_type_filters.is_empty());
    assert_eq!(config.deliver_policy, DeliverPolicy::All);
    Ok(())
}

#[sinex_test]
async fn confirmed_filter_subject_composes_provenance_and_type()
-> xtask::sandbox::TestResult<()> {
    let env = SinexEnvironment::new("dev")?;

    assert_eq!(
        confirmed_filter_subject_for(&env, None, InputProvenanceFilter::Any, None),
        "dev.events.confirmed.>"
    );
    assert_eq!(
        confirmed_filter_subject_for(&env, None, InputProvenanceFilter::MaterialOnly, None),
        "dev.events.confirmed.material.>"
    );
    assert_eq!(
        confirmed_filter_subject_for(
            &env,
            None,
            InputProvenanceFilter::SynthesizedOnly,
            Some("entity.resolved")
        ),
        "dev.events.confirmed.synthesized.*.entity_d_resolved"
    );
    assert_eq!(
        confirmed_filter_subject_for(
            &env,
            Some("agent"),
            InputProvenanceFilter::Any,
            Some("command.executed")
        ),
        "dev.agent.events.confirmed.*.*.command_d_executed"
    );
    Ok(())
}

#[sinex_test]
async fn confirmed_filter_subjects_compose_multiple_event_types()
-> xtask::sandbox::TestResult<()> {
    let env = SinexEnvironment::new("dev")?;
    let filters = super::confirmed_filter_subjects_for(
        &env,
        None,
        InputProvenanceFilter::MaterialOnly,
        &["command.executed".to_string(), "command.canonical".to_string()],
    );

    assert_eq!(
        filters,
        vec![
            "dev.events.confirmed.material.*.command_d_executed",
            "dev.events.confirmed.material.*.command_d_canonical",
        ]
    );
    Ok(())
}

#[sinex_test]
async fn confirmed_consumer_retirement_deletes_same_service_stale_filters()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_interval-lift-confirmed-events-filter-window_d_focused_or_window_d_active_or_afk_d_changed_or_unit_d_started_or_unit_d_stopped",
            "sinex_interval-lift-confirmed-events-filter-window_d_focused"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_interval-lift-confirmed-events-filter-window_d_focused_or_window_d_active_or_afk_d_changed_or_unit_d_started_or_unit_d_stopped",
            "sinex_interval-lift-confirmed-events-filter-window_d_focused_or_window_d_active"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_interval-lift-confirmed-events-filter-window_d_focused_or_window_d_active_or_afk_d_changed_or_unit_d_started_or_unit_d_stopped",
            "sinex_interval-lift-confirmed-events"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    Ok(())
}

#[sinex_test]
async fn confirmed_consumer_retirement_keeps_current_and_unrelated()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_analytics-confirmed-events-material-filter-command_d_executed",
            "sinex_analytics-confirmed-events-material-filter-command_d_executed"
        ),
        ConfirmedConsumerRetirementAction::KeepCurrent
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_analytics-confirmed-events-material-filter-command_d_executed",
            "sinex-tag-applier-confirmed-events-material"
        ),
        ConfirmedConsumerRetirementAction::IgnoreUnrelated
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_analytics-confirmed-events-material-filter-command_d_executed",
            "event-engine-dev"
        ),
        ConfirmedConsumerRetirementAction::IgnoreUnrelated
    );
    Ok(())
}

#[sinex_test]
async fn confirmed_consumer_retirement_deletes_old_provenance_shape()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex-tag-applier-confirmed-events-material",
            "sinex-tag-applier-confirmed-events"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex-tag-applier-confirmed-events",
            "sinex-tag-applier-confirmed-events-synthesized"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    Ok(())
}
