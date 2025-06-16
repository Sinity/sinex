#[macro_use]
mod common;
mod test_setup;

// Unit tests organized by crate
#[cfg(test)]
mod unit;

// Integration tests
#[cfg(test)]
mod integration;

// System-level tests
#[cfg(test)]
mod system;

// Adversarial and property tests
#[cfg(test)]
mod adversarial;

#[cfg(test)]
mod property_tests;

// Legacy organization (being migrated)
#[cfg(test)]
mod agent {
    mod agent_manifest_tests;
    mod heartbeat_tests;
}

#[cfg(test)]
mod ulid;

#[cfg(test)]
mod model;

#[cfg(test)]
mod validation;

#[cfg(test)]
mod ingestor;