//! Property suites that focus on SDK behaviour (checkpointing, queues, config).

// Sync tests that don't use TestContext
pub mod error_handling_property_test;
pub mod validation_invariants_property_test;

// Async property tests using #[sinex_prop] with Result<(), SinexError>
pub mod checkpoint_property_test;
pub mod queue_property_test;

// TODO: These need additional fixes (type inference issues with multiple From<SinexError> impls)
// pub mod node_property_test;

// TODO: These need additional fixes
// pub mod automation_property_test;
