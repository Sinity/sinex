use super::{JetStreamEventConsumerConfig, confirmed_filter_subject_for};
use crate::runtime::automaton::traits::InputProvenanceFilter;
use async_nats::jetstream::consumer::DeliverPolicy;
use sinex_primitives::environment::SinexEnvironment;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn default_consumer_config_targets_confirmed_firehose() -> xtask::sandbox::TestResult<()>
{
    let config = JetStreamEventConsumerConfig::default();
    assert!(config.event_type_filter.is_none());
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
