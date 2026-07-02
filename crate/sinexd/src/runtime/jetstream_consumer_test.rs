use super::{JetStreamEventConsumerConfig, confirmed_filter_subject_for};
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
async fn legacy_filter_consumer_names_cover_broader_siblings()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::legacy_filter_consumer_names(
            "sinex_entity-extractor-confirmed-events-filter-document_d_chunked"
        ),
        vec![
            "sinex_entity-extractor-confirmed-events",
            "sinex_entity-extractor-confirmed-events-material",
            "sinex_entity-extractor-confirmed-events-synthesized",
        ]
    );
    assert_eq!(
        super::legacy_filter_consumer_names(
            "sinex_analytics-confirmed-events-material-filter-command_d_executed"
        ),
        vec![
            "sinex_analytics-confirmed-events",
            "sinex_analytics-confirmed-events-material",
        ]
    );
    assert_eq!(
        super::legacy_filter_consumer_names("sinex-tag-applier-confirmed-events-material"),
        vec!["sinex-tag-applier-confirmed-events"]
    );
    assert!(super::legacy_filter_consumer_names("sinex-tag-applier-confirmed-events").is_empty());
    Ok(())
}
