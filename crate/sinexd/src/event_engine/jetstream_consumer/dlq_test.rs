use super::dlq_event_id;
use serde_json::json;
use xtask::sandbox::prelude::sinex_test;

/// sinex-x444: `dlq_event_id` (and therefore `dlq_publish_msg_id`'s dedupe
/// identity) derives from `events[0].id` only. Two distinct multi-event
/// intents that happen to share a leading event id -- exactly what
/// sinex-w4i's re-emit mechanism produces -- collapse onto the same DLQ
/// dedupe identity, so the second intent's other sibling event is DLQ'd,
/// ACKed, and lost with no trace.
#[sinex_test]
#[ignore = "sinex-x444 open: dlq_event_id only reads events[0].id, so two distinct \
            multi-event intents sharing a leading event id collide on DLQ dedupe identity"]
async fn dlq_event_id_distinguishes_intents_sharing_a_leading_event() -> xtask::sandbox::TestResult<()> {
    // intent A = [X, Y]
    let intent_a = json!({
        "events": [
            {"id": "00000000-0000-7000-8000-000000000001"},
            {"id": "00000000-0000-7000-8000-000000000002"},
        ]
    });
    // intent B = [X, Z] -- same leading event id X, different second sibling
    let intent_b = json!({
        "events": [
            {"id": "00000000-0000-7000-8000-000000000001"},
            {"id": "00000000-0000-7000-8000-000000000003"},
        ]
    });

    let id_a = dlq_event_id(&intent_a);
    let id_b = dlq_event_id(&intent_b);

    assert_ne!(
        id_a, id_b,
        "two distinct multi-event intents produced the same DLQ dedupe identity ({id_a:?}) \
         because dlq_event_id only looks at events[0] -- the second intent's sibling event \
         (Z) would be silently lost when this collides in JetStream's dupeWindow"
    );
    Ok(())
}
