//! Property test modules for Sinex components
//!
//! This module contains property-based tests that verify system invariants
//! and properties that should hold across a wide range of inputs.

pub mod automation_property_test;
pub mod checkpoint_property_test;
pub mod event_model_fuzzing_test;
pub mod event_validation_property_test;
pub mod queue_property_test;
pub mod satellite_property_test;
pub mod schema_property_test;
pub mod ulid_property_test;