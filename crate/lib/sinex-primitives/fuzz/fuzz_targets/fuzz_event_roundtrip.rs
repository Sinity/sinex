#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::Value as JsonValue;
use sinex_primitives::events::Event;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        if let Ok(event) = serde_json::from_str::<Event<JsonValue>>(s) {
            // Roundtrip: serialize back and deserialize again
            let serialized = serde_json::to_string(&event).expect("serialize should not fail for valid Event");
            let roundtripped: Event<JsonValue> =
                serde_json::from_str(&serialized).expect("roundtrip deserialize should not fail");
            // Verify payload equality (the core invariant)
            assert_eq!(
                serde_json::to_value(&event.payload).ok(),
                serde_json::to_value(&roundtripped.payload).ok(),
            );
        }
    }
});
