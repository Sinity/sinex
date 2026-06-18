// This file is intentionally invalid Rust. The EventBuilder typestate contract
// exposes `.build()` only after material or derived provenance is attached.

use sinex_primitives::DynamicPayload;

fn main() {
    let payload = DynamicPayload::new("test-source", "test.event", serde_json::json!({}));

    let _event = payload.into_builder().build();
}
