use super::*;

#[test]
fn steady_state_bootstrap_table_sql_does_not_replace_functions_or_triggers() {
    assert!(!BOOTSTRAP_TABLE_SQL.contains("CREATE OR REPLACE FUNCTION"));
    assert!(!BOOTSTRAP_TABLE_SQL.contains("DROP TRIGGER"));
    assert!(!BOOTSTRAP_TABLE_SQL.contains("CREATE TRIGGER"));
}

#[test]
fn privacy_table_sql_does_not_replace_triggers() {
    assert!(!PRIVACY_SCHEMA_TABLE_SQL.contains("DROP TRIGGER"));
    assert!(!PRIVACY_SCHEMA_TABLE_SQL.contains("CREATE TRIGGER"));
}

#[test]
fn guarded_function_sets_cover_boot_sensitive_blocks() {
    assert_eq!(
        OPERATIONS_AND_CASCADE_FUNCTIONS,
        &[
            "core.start_operation(text,text,jsonb,tstzrange)",
            "core.complete_operation(uuid,jsonb)",
            "core.fail_operation(uuid,jsonb)",
            "core.prepare_cascade_session(text,boolean)",
            "core.cascade_populate_roots(text,uuid[])",
            "core.cascade_count_nodes(text)",
            "core.cascade_depth_histogram(text)",
            "core.cascade_find_integrity_violations(text,integer)",
            "core.cascade_find_integrity_violations_paginated(text,integer,integer)",
            "core.cleanup_cascade_session(text)",
            "core.expand_cascade(text,integer)",
        ]
    );
    assert_eq!(
        TOMBSTONE_LIFECYCLE_FUNCTIONS,
        &[
            "core.execute_cascade_tombstone(uuid[],text,uuid)",
            "core.execute_cascade_restore(uuid[],text)",
            "core.lifecycle_tier_status()",
        ]
    );
    assert_eq!(JSONB_MERGE_FUNCTIONS, &["core.jsonb_merge_deep(jsonb,jsonb)"]);
    assert_eq!(
        EMBEDDING_INDEX_MANAGEMENT_FUNCTIONS,
        &[
            "core.create_embedding_model_index(uuid,integer)",
            "core.drop_embedding_model_index(uuid)",
            "core.embedding_model_index_trigger()",
        ]
    );
    assert_eq!(
        HYBRID_SEARCH_FUNCTIONS,
        &[
            "core.hybrid_search(text,vector,uuid,integer,integer,double precision,double precision,double precision)",
        ]
    );
}

#[test]
fn trigger_sql_isolated_in_guarded_blocks() {
    for sql in [
        DLQ_EVENTS_TRIGGER_SQL,
        PRIVACY_RECOGNIZER_BACKENDS_TRIGGER_SQL,
        PRIVACY_DICTIONARIES_TRIGGER_SQL,
        PRIVACY_RULES_TRIGGER_SQL,
    ] {
        assert!(sql.contains("DROP TRIGGER"));
        assert!(sql.contains("CREATE TRIGGER"));
    }
}

#[test]
fn reflection_event_trigger_sql_targets_reflection_table() {
    let sql = reflection_events_trigger_sql();

    assert!(sql.contains("reflection.events"));
    assert!(sql.contains("reflection.fn_events_no_update"));
    assert!(sql.contains("reflection.fn_events_validate_payload"));
    assert!(sql.contains("reflection.fn_events_validate_material_bounds"));
    assert!(!sql.contains(
        "DROP TRIGGER IF EXISTS trg_events_no_update ON core.events"
    ));
    assert!(
        !sql.contains("CREATE TRIGGER trg_events_no_update\n        BEFORE UPDATE ON core.events")
    );
}
