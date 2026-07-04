use super::{
    JETSTREAM_BOOTSTRAP_MAX_BYTES, REFLECTION_STREAM_MAX_BYTES, RawStreamConsumerState,
    RawStreamWorkQueueRecreationDecision, raw_events_stream_config,
    raw_stream_workqueue_recreation_decision,
};
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::nats::{JetStreamEventLane, JetStreamTopology};
use std::time::Duration;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn raw_stream_caps_follow_topology_lane() -> xtask::sandbox::TestResult<()> {
    let env = SinexEnvironment::new("dev")?;
    let activity = JetStreamTopology::new(
        &env,
        env.nats_stream_name_with_namespace(None, "SINEX_RAW_EVENTS"),
        "event-engine-dev".to_string(),
        None,
    );
    let reflection = JetStreamTopology::reflection(
        &env,
        env.nats_stream_name_with_namespace(None, "SINEX_REFLECTION_EVENTS"),
        "event-engine-dev-reflection".to_string(),
        None,
    );

    let activity_config = raw_events_stream_config(&activity);
    let reflection_config = raw_events_stream_config(&reflection);

    assert_eq!(activity.lane, JetStreamEventLane::Activity);
    assert_eq!(activity_config.max_bytes, JETSTREAM_BOOTSTRAP_MAX_BYTES);
    assert_eq!(activity_config.max_age, Duration::from_secs(72 * 60 * 60));
    assert_eq!(reflection.lane, JetStreamEventLane::Reflection);
    assert_eq!(reflection_config.max_bytes, REFLECTION_STREAM_MAX_BYTES);
    assert_eq!(reflection_config.max_age, Duration::from_secs(24 * 60 * 60));
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_recreation_allows_empty_stream() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        raw_stream_workqueue_recreation_decision(0, 0, "event-engine-dev", &[]),
        RawStreamWorkQueueRecreationDecision::AlreadyWorkQueueOrEmpty
    );
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_recreation_allows_drained_single_consumer() -> xtask::sandbox::TestResult<()>
{
    let consumers = vec![RawStreamConsumerState {
        name: "event-engine-dev".to_string(),
        pending: 0,
        ack_pending: 0,
        redelivered: 0,
        ack_floor_sequence: 42,
    }];

    assert_eq!(
        raw_stream_workqueue_recreation_decision(10, 42, "event-engine-dev", &consumers),
        RawStreamWorkQueueRecreationDecision::Recreate
    );
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_recreation_rejects_unexpected_consumers() -> xtask::sandbox::TestResult<()> {
    let consumers = vec![
        RawStreamConsumerState {
            name: "event-engine-dev".to_string(),
            pending: 0,
            ack_pending: 0,
            redelivered: 0,
            ack_floor_sequence: 42,
        },
        RawStreamConsumerState {
            name: "old-automaton".to_string(),
            pending: 0,
            ack_pending: 0,
            redelivered: 0,
            ack_floor_sequence: 42,
        },
    ];

    assert_eq!(
        raw_stream_workqueue_recreation_decision(10, 42, "event-engine-dev", &consumers),
        RawStreamWorkQueueRecreationDecision::Reject {
            reason: "unexpected raw consumer(s) still exist: old-automaton".to_string()
        }
    );
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_recreation_rejects_in_flight_event_engine() -> xtask::sandbox::TestResult<()>
{
    let consumers = vec![RawStreamConsumerState {
        name: "event-engine-dev".to_string(),
        pending: 1,
        ack_pending: 2,
        redelivered: 0,
        ack_floor_sequence: 40,
    }];

    assert_eq!(
        raw_stream_workqueue_recreation_decision(10, 42, "event-engine-dev", &consumers),
        RawStreamWorkQueueRecreationDecision::Reject {
            reason: "consumer event-engine-dev is not fully drained: pending=1, ack_pending=2, redelivered=0, ack_floor=40, stream_last=42".to_string()
        }
    );
    Ok(())
}
