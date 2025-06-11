#[macro_use]
mod common;
mod test_setup;

#[cfg(test)]
mod database {
    mod database_integration_tests;
    mod timescaledb_tests;
    mod ulid_integration_tests;
    mod jsonschema_validation_tests;
    mod schema_validation_tests;
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
mod property_tests;