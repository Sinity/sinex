// This file is intentionally invalid Rust — it is a trybuild compile-error fixture.
//
// Invariant under test: `Id<Event>` and `Id<Checkpoint>` are distinct types.
// Assigning one to the other must be a compile-time error, not a runtime panic.
//
// The trybuild harness (id_type_system_test.rs) compiles this file and asserts
// the expected type error appears in the compiler output.

use sinex_primitives::Id;
use sinex_primitives::events::Event;
use sinex_primitives::rpc::replay::ReplayCheckpoint;
use serde_json::Value;

fn main() {
    let event_id: Id<Event<Value>> = Id::new();
    // This assignment must not compile: Id<ReplayCheckpoint> ≠ Id<Event<Value>>
    let _checkpoint_id: Id<ReplayCheckpoint> = event_id;
}
