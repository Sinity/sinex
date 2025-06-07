mod common;

#[cfg(test)]
mod database {
    mod database_integration_tests;
    mod migration_tests;
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
mod pipeline {
    mod end_to_end_pipeline_test;
    mod event_pipeline_integration_tests;
    mod real_pipeline_test;
    mod worker_concurrency_tests;
}

#[cfg(test)]
mod reliability {
    mod assumption_mismatch_tests;
    mod error_handling_tests;
    mod realistic_failure_tests;
}

#[cfg(test)]
mod runtime {
    mod event_sink_test;
    mod runtime_test;
    mod validation_unit_tests;
}

#[cfg(test)]
mod property_tests;