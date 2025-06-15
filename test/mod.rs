#[macro_use]
mod common;
mod test_setup;

// Unit tests organized by crate
#[cfg(test)]
mod unit {
    mod core;
    mod db;
}

// System-level tests (temporarily disabled until API is stable)
// #[cfg(test)]
// mod system;

// Temporarily disabled until API compatibility is restored
/*
#[cfg(test)]
mod database {
    // mod database_integration_tests;  // Still has compilation issues
    // mod timescaledb_tests;  // Still has compilation issues
    mod ulid_integration_tests;  // Should work - basic ULID/DB integration
    // mod jsonschema_validation_tests;  // Still has compilation issues
    // mod schema_validation_tests;  // Still has compilation issues
}

#[cfg(test)]
mod agent {
    mod agent_manifest_tests;
    mod heartbeat_tests;
}

#[cfg(test)]
mod collector;

#[cfg(test)]
mod ulid;

#[cfg(test)]
mod model;

#[cfg(test)]
mod validation;

#[cfg(test)]
mod worker;

#[cfg(test)]
mod events;

#[cfg(test)]
mod regression;

#[cfg(test)]
mod adversarial;

#[cfg(test)]
mod pipeline;

#[cfg(test)]
mod property_tests;

// #[cfg(test)]
// mod ingestor;

// #[cfg(test)]
// mod annex;
*/