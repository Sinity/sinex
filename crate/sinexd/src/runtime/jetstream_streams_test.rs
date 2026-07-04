use super::{
    RawStreamConsumerState, RawStreamWorkQueueRecreationDecision,
    raw_stream_workqueue_recreation_decision,
};
use xtask::sandbox::sinex_test;

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
