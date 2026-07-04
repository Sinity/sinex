use super::*;
use sinex_primitives::rpc::dlq::{DlqMessageGroup, DlqPressureSignal};
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn dlq_stats_table_renders_structured_pressure_signal() -> xtask::sandbox::TestResult<()> {
    let rendered = format_dlq_stats_table(&DlqListResponse {
        total_messages: 11,
        total_bytes: 4096,
        first_seq: 1,
        last_seq: 11,
        pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
        resource_pressure: DlqPressureSignal {
            pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
            runtime_action: sinex_primitives::RuntimePressureAction::Throttle,
            pending_messages: 11,
            pending_bytes: 4096,
            retry_batch_size: 10,
            recommended_action: "ops dlq peek".to_string(),
            reason: "inspect failures before running paced requeue or purge".to_string(),
        },
        pending_sequence_span: 11,
        recommended_action: "ops dlq peek".to_string(),
        action_reason: "inspect failures before running paced requeue or purge".to_string(),
    });

    assert!(rendered.contains("Pressure: critical"));
    assert!(rendered.contains("Runtime action: throttle"));
    assert!(rendered.contains("Retry batch size: 10"));
    Ok(())
}

#[sinex_test]
async fn dlq_peek_table_renders_grouped_explanations() -> xtask::sandbox::TestResult<()> {
    let rendered = format_dlq_peek_table(&DlqPeekResponse {
        messages: vec![DlqMessagePeek {
            subject: "dev.events.dlq.event_engine".to_string(),
            sequence: 10,
            retry_count: 0,
            original_subject: Some("dev.events.raw.git.commit_d_created".to_string()),
            payload_preview: "{\"error\":\"live event with equivalence_key git|a already exists\"}"
                .to_string(),
            payload_redacted: false,
            privacy_caveats: Vec::new(),
        }],
        groups: vec![DlqMessageGroup {
            original_subject: Some("dev.events.raw.git.commit_d_created".to_string()),
            reason_bucket: "occurrence_duplicate.equivalence_key_exists".to_string(),
            count: 1,
            first_sequence: 10,
            last_sequence: 10,
            sample_previews: vec![
                "{\"error\":\"live event with equivalence_key git|a already exists\"}".to_string(),
            ],
        }],
    });

    assert!(rendered.contains("DLQ Groups:"));
    assert!(rendered.contains("Reason: occurrence_duplicate.equivalence_key_exists"));
    assert!(rendered.contains("Original subject: dev.events.raw.git.commit_d_created"));
    assert!(rendered.contains("DLQ Messages:"));
    Ok(())
}

#[sinex_test]
async fn dlq_tail_start_sequence_uses_retained_window() -> xtask::sandbox::TestResult<()> {
    assert_eq!(tail_start_sequence(5, 1, 195), Some(191));
    assert_eq!(tail_start_sequence(10, 190, 195), Some(190));
    assert_eq!(tail_start_sequence(0, 1, 195), None);
    assert_eq!(tail_start_sequence(5, 0, 0), None);
    Ok(())
}

#[sinex_test]
async fn dlq_all_retained_uses_pending_sequence_span() -> xtask::sandbox::TestResult<()> {
    let stats = DlqListResponse {
        total_messages: 283,
        total_bytes: 4096,
        first_seq: 1,
        last_seq: 303,
        pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
        resource_pressure: DlqPressureSignal {
            pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
            runtime_action: sinex_primitives::RuntimePressureAction::Throttle,
            pending_messages: 283,
            pending_bytes: 4096,
            retry_batch_size: 10,
            recommended_action: "ops dlq peek".to_string(),
            reason: "inspect failures before running paced requeue or purge".to_string(),
        },
        pending_sequence_span: 303,
        recommended_action: "ops dlq peek".to_string(),
        action_reason: "inspect failures before running paced requeue or purge".to_string(),
    };

    assert_eq!(dlq_inspected_tail(20, false, &stats), 20);
    assert_eq!(dlq_inspected_tail(20, true, &stats), 303);
    Ok(())
}

#[sinex_test]
async fn dlq_triage_extracts_material_ids_and_group_commands() -> xtask::sandbox::TestResult<()> {
    let stats = DlqListResponse {
        total_messages: 278,
        total_bytes: 4096,
        first_seq: 1,
        last_seq: 298,
        pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
        resource_pressure: DlqPressureSignal {
            pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
            runtime_action: sinex_primitives::RuntimePressureAction::Throttle,
            pending_messages: 278,
            pending_bytes: 4096,
            retry_batch_size: 10,
            recommended_action: "ops dlq peek".to_string(),
            reason: "inspect failures before running paced requeue or purge".to_string(),
        },
        pending_sequence_span: 298,
        recommended_action: "ops dlq peek".to_string(),
        action_reason: "inspect failures before running paced requeue or purge".to_string(),
    };
    let peek = DlqPeekResponse {
        messages: vec![
            DlqMessagePeek {
                subject: "dev.events.dlq.event_engine".to_string(),
                sequence: 294,
                retry_count: 0,
                original_subject: None,
                payload_preview:
                    "{\"error\":\"material assembly corruption detected\",\"material_id\":\"019f17d7-2ba6-7b01-9f0a-017a3d025d14\",\"context\":{\"reason\":\"assembled_bytes=1825 exceeds expected_bytes=1284\"}}"
                        .to_string(),
                payload_redacted: false,
                privacy_caveats: Vec::new(),
            },
            DlqMessagePeek {
                subject: "dev.events.dlq.event_engine".to_string(),
                sequence: 295,
                retry_count: 0,
                original_subject: None,
                payload_preview:
                    "{\"error\":\"slice arrival timeout\",\"material_id\":\"019f17d7-ffff-7000-8000-000000000000\"}"
                        .to_string(),
                payload_redacted: false,
                privacy_caveats: Vec::new(),
            },
        ],
        groups: vec![DlqMessageGroup {
            original_subject: None,
            reason_bucket: "error_payload.material_assembly_corruption_detected".to_string(),
            count: 1,
            first_sequence: 294,
            last_sequence: 295,
            sample_previews: Vec::new(),
        }],
    };

    let mut report = dlq_triage_report(stats, peek, 12);
    assert_eq!(report.groups.len(), 2);
    report.groups[0]
        .material_statuses
        .push(DlqTriageMaterialStatus {
            material_id: "019f17d7-2ba6-7b01-9f0a-017a3d025d14".to_string(),
            lookup_status: "found".to_string(),
            source_identifier: Some("git-commit-history".to_string()),
            material_status: Some("failed".to_string()),
            failure_reason: Some("material assembly corruption detected".to_string()),
            total_bytes: Some(1284),
            has_blob: Some(false),
            event_count: Some(0),
            start_time: None,
            end_time: None,
        });
    let group = &report.groups[0];
    assert_eq!(
        group.material_ids,
        vec!["019f17d7-2ba6-7b01-9f0a-017a3d025d14"]
    );
    assert_eq!(
        group.inspect_command,
        "sinexctl ops dlq peek --start-sequence 294 -n 1"
    );
    assert_eq!(
        group.purge_command,
        "sinexctl ops dlq purge --start-sequence 294 --end-sequence 294 --confirm"
    );

    let rendered = format_dlq_triage_table(&report);
    assert!(rendered.contains("DLQ Triage:"));
    assert!(rendered.contains("Material IDs: 019f17d7-2ba6-7b01-9f0a-017a3d025d14"));
    assert!(rendered.contains("Material status:"));
    assert!(rendered.contains("source=git-commit-history"));
    assert!(rendered.contains("status=failed"));
    assert!(rendered.contains("failure=material assembly corruption detected"));
    assert!(rendered.contains("Purge if historical:"));
    Ok(())
}

#[sinex_test]
async fn dlq_cleanup_plan_marks_only_terminal_contiguous_groups_as_candidates()
-> xtask::sandbox::TestResult<()> {
    let stats = DlqListResponse {
        total_messages: 278,
        total_bytes: 4096,
        first_seq: 1,
        last_seq: 298,
        pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
        resource_pressure: DlqPressureSignal {
            pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
            runtime_action: sinex_primitives::RuntimePressureAction::Throttle,
            pending_messages: 278,
            pending_bytes: 4096,
            retry_batch_size: 10,
            recommended_action: "ops dlq peek".to_string(),
            reason: "inspect failures before running paced requeue or purge".to_string(),
        },
        pending_sequence_span: 298,
        recommended_action: "ops dlq peek".to_string(),
        action_reason: "inspect failures before running paced requeue or purge".to_string(),
    };
    let peek = DlqPeekResponse {
        messages: vec![
            DlqMessagePeek {
                subject: "dev.events.dlq.event_engine".to_string(),
                sequence: 294,
                retry_count: 0,
                original_subject: None,
                payload_preview:
                    "{\"error\":\"material assembly corruption detected\",\"material_id\":\"019f17d7-2ba6-7b01-9f0a-017a3d025d14\"}"
                        .to_string(),
                payload_redacted: false,
                privacy_caveats: Vec::new(),
            },
            DlqMessagePeek {
                subject: "dev.events.dlq.event_engine".to_string(),
                sequence: 296,
                retry_count: 0,
                original_subject: None,
                payload_preview:
                    "{\"error\":\"slice arrival timeout\",\"material_id\":\"019f17d7-ffff-7000-8000-000000000000\"}"
                        .to_string(),
                payload_redacted: false,
                privacy_caveats: Vec::new(),
            },
        ],
        groups: vec![
            DlqMessageGroup {
                original_subject: None,
                reason_bucket: "error_payload.material_assembly_corruption_detected"
                    .to_string(),
                count: 1,
                first_sequence: 294,
                last_sequence: 294,
                sample_previews: Vec::new(),
            },
            DlqMessageGroup {
                original_subject: None,
                reason_bucket: "error_payload.slice_arrival_timeout".to_string(),
                count: 1,
                first_sequence: 295,
                last_sequence: 296,
                sample_previews: Vec::new(),
            },
        ],
    };

    let mut report = dlq_triage_report(stats, peek, 12);
    report.groups[0]
        .material_statuses
        .push(DlqTriageMaterialStatus {
            material_id: "019f17d7-2ba6-7b01-9f0a-017a3d025d14".to_string(),
            lookup_status: "found".to_string(),
            source_identifier: Some("sinex.self-observation.document-parser".to_string()),
            material_status: Some("failed".to_string()),
            failure_reason: Some("material assembly corruption detected".to_string()),
            total_bytes: None,
            has_blob: Some(false),
            event_count: Some(3),
            start_time: None,
            end_time: None,
        });
    report.groups[1]
        .material_statuses
        .push(DlqTriageMaterialStatus {
            material_id: "019f17d7-ffff-7000-8000-000000000000".to_string(),
            lookup_status: "found".to_string(),
            source_identifier: Some("sinex.self-observation.analytics".to_string()),
            material_status: Some("sensing".to_string()),
            failure_reason: Some("slice_arrival_timeout".to_string()),
            total_bytes: None,
            has_blob: Some(false),
            event_count: Some(0),
            start_time: None,
            end_time: None,
        });

    let plan = dlq_cleanup_plan(report);
    assert_eq!(plan.schema_version, DLQ_CLEANUP_PLAN_SCHEMA_VERSION);
    assert_eq!(plan.candidate_count, 1);
    assert_eq!(plan.blocked_count, 1);
    assert_eq!(plan.purge_candidate_messages, 1);
    assert_eq!(plan.coalesced_actions.len(), 1);
    assert_eq!(plan.coalesced_actions[0].sequence_range, "294..294");
    assert_eq!(plan.coalesced_actions[0].message_count, 1);
    assert_eq!(plan.items[0].decision, "purge_candidate");
    assert_eq!(
        plan.items[0].purge_command.as_deref(),
        Some("sinexctl ops dlq purge --start-sequence 294 --end-sequence 294 --confirm")
    );
    assert_eq!(plan.items[1].decision, "inspect_only");
    assert!(
        plan.items[1]
            .blockers
            .iter()
            .any(|blocker| blocker.contains("status is sensing"))
    );

    let rendered = format_dlq_cleanup_plan_table(&plan);
    assert!(rendered.contains("DLQ Cleanup Plan:"));
    assert!(rendered.contains("Coalesced cleanup actions:"));
    assert!(rendered.contains("purge_candidate: 1 message(s), seq 294..294"));
    assert!(rendered.contains("inspect_only: 1 message(s), seq 296..296"));
    assert!(rendered.contains("Blockers:"));
    Ok(())
}

#[sinex_test]
async fn dlq_cleanup_plan_allows_duplicate_buckets_without_material_ids()
-> xtask::sandbox::TestResult<()> {
    let stats = DlqListResponse {
        total_messages: 2,
        total_bytes: 512,
        first_seq: 1,
        last_seq: 2,
        pressure_level: sinex_primitives::RuntimePressureLevel::Nominal,
        resource_pressure: DlqPressureSignal {
            pressure_level: sinex_primitives::RuntimePressureLevel::Nominal,
            runtime_action: sinex_primitives::RuntimePressureAction::Admit,
            pending_messages: 2,
            pending_bytes: 512,
            retry_batch_size: 10,
            recommended_action: "ops dlq peek".to_string(),
            reason: "inspect failures before running paced requeue or purge".to_string(),
        },
        pending_sequence_span: 2,
        recommended_action: "ops dlq peek".to_string(),
        action_reason: "inspect failures before running paced requeue or purge".to_string(),
    };
    let peek = DlqPeekResponse {
        messages: vec![
            DlqMessagePeek {
                subject: "dev.events.dlq.event_engine".to_string(),
                sequence: 1,
                retry_count: 0,
                original_subject: Some("dev.events.raw.git.commit_d_created".to_string()),
                payload_preview:
                    "{\"error\":\"live event with equivalence_key git|a already exists\"}"
                        .to_string(),
                payload_redacted: false,
                privacy_caveats: Vec::new(),
            },
            DlqMessagePeek {
                subject: "dev.events.dlq.event_engine".to_string(),
                sequence: 2,
                retry_count: 0,
                original_subject: Some("dev.events.raw.git.commit_d_created".to_string()),
                payload_preview:
                    "{\"error\":\"live event with equivalence_key git|b already exists\"}"
                        .to_string(),
                payload_redacted: false,
                privacy_caveats: Vec::new(),
            },
        ],
        groups: vec![DlqMessageGroup {
            original_subject: Some("dev.events.raw.git.commit_d_created".to_string()),
            reason_bucket: "occurrence_duplicate.equivalence_key_exists".to_string(),
            count: 2,
            first_sequence: 1,
            last_sequence: 2,
            sample_previews: Vec::new(),
        }],
    };

    let report = dlq_triage_report(stats, peek, 2);
    assert_eq!(report.groups[0].material_ids.len(), 0);
    let plan = dlq_cleanup_plan(report);
    assert_eq!(plan.candidate_count, 1);
    assert_eq!(plan.blocked_count, 0);
    assert_eq!(plan.purge_candidate_messages, 2);
    assert_eq!(plan.coalesced_actions.len(), 1);
    assert_eq!(plan.coalesced_actions[0].sequence_range, "1..2");
    assert_eq!(plan.coalesced_actions[0].message_count, 2);
    assert_eq!(plan.items[0].decision, "purge_candidate");
    assert!(plan.items[0].blockers.is_empty());
    assert!(
        plan.items[0]
            .evidence
            .iter()
            .any(|item| item == "reason_contract=duplicate_occurrence_suppression")
    );
    assert_eq!(
        plan.items[0].purge_command.as_deref(),
        Some("sinexctl ops dlq purge --start-sequence 1 --end-sequence 2 --confirm")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_cleanup_plan_regroups_messages_when_server_groups_are_stale()
-> xtask::sandbox::TestResult<()> {
    let stats = DlqListResponse {
        total_messages: 2,
        total_bytes: 512,
        first_seq: 2928,
        last_seq: 2929,
        pressure_level: sinex_primitives::RuntimePressureLevel::Warning,
        resource_pressure: DlqPressureSignal {
            pressure_level: sinex_primitives::RuntimePressureLevel::Warning,
            runtime_action: sinex_primitives::RuntimePressureAction::Inspect,
            pending_messages: 2,
            pending_bytes: 512,
            retry_batch_size: 10,
            recommended_action: "ops dlq peek".to_string(),
            reason: "inspect failures before running paced requeue or purge".to_string(),
        },
        pending_sequence_span: 2,
        recommended_action: "ops dlq peek".to_string(),
        action_reason: "inspect failures before running paced requeue or purge".to_string(),
    };
    let peek = DlqPeekResponse {
        messages: vec![
            DlqMessagePeek {
                subject: "dev.events.dlq.event_engine".to_string(),
                sequence: 2928,
                retry_count: 0,
                original_subject: None,
                payload_preview:
                    "{\"error\": \"buffered_slice_limit_exceeded\", \"material_id\":\"019f22f2-23af-7c10-8aa7-87f8ebac75ec\",..."
                        .to_string(),
                payload_redacted: false,
                privacy_caveats: Vec::new(),
            },
            DlqMessagePeek {
                subject: "dev.events.dlq.event_engine".to_string(),
                sequence: 2929,
                retry_count: 0,
                original_subject: None,
                payload_preview:
                    "{\"error\": \"orphaned_sensing_material\", \"material_id\":\"019f22d3-f500-7783-9ed6-a05cd8e91cc3\",..."
                        .to_string(),
                payload_redacted: false,
                privacy_caveats: Vec::new(),
            },
        ],
        groups: vec![DlqMessageGroup {
            original_subject: None,
            reason_bucket: "error_payload.unparsed".to_string(),
            count: 2,
            first_sequence: 2928,
            last_sequence: 2929,
            sample_previews: Vec::new(),
        }],
    };

    let report = dlq_triage_report(stats, peek, 2);
    assert_eq!(report.groups.len(), 2);
    assert_eq!(report.groups.iter().map(|group| group.count).sum::<usize>(), 2);

    let plan = dlq_cleanup_plan(report);
    assert_eq!(plan.items.len(), 2);
    assert_eq!(plan.items.iter().map(|item| item.count).sum::<usize>(), 2);
    assert_eq!(plan.blocked_count, 2);
    assert!(
        plan.items
            .iter()
            .any(|item| item.reason_bucket == "error_payload.buffered_slice_limit_exceeded")
    );
    assert!(
        plan.items
            .iter()
            .any(|item| item.reason_bucket == "error_payload.orphaned_sensing_material")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_cleanup_plan_allows_completed_duplicate_blob_upsert() -> xtask::sandbox::TestResult<()>
{
    let material_id = "019f16cc-dd56-7ab3-8aff-ea3f29e79932";
    let stats = DlqListResponse {
        total_messages: 1,
        total_bytes: 512,
        first_seq: 208,
        last_seq: 208,
        pressure_level: sinex_primitives::RuntimePressureLevel::Warning,
        resource_pressure: DlqPressureSignal {
            pressure_level: sinex_primitives::RuntimePressureLevel::Warning,
            runtime_action: sinex_primitives::RuntimePressureAction::Inspect,
            pending_messages: 1,
            pending_bytes: 512,
            retry_batch_size: 10,
            recommended_action: "ops dlq peek".to_string(),
            reason: "inspect failures before running paced requeue or purge".to_string(),
        },
        pending_sequence_span: 1,
        recommended_action: "ops dlq peek".to_string(),
        action_reason: "inspect failures before running paced requeue or purge".to_string(),
    };
    let preview = format!(
        "{{\"error\":\"material_persist_failed\",\"material_id\":\"{material_id}\",\
         \"context\":{{\"error\":\"Database error: Failed to insert blob metadata \
         (finalization_stage: upsert_blob)\\nCaused by:\\n  1: Database error: \
         Failed to insert blob (backend=SINEXBLAKE3, hash=fb4e): error returned \
         from database: duplicate key value violates unique constraint \
         \\\"uk_blobs_annex_backend_content_hash\\\"\"}}}}"
    );
    let peek = DlqPeekResponse {
        messages: vec![DlqMessagePeek {
            subject: "dev.events.dlq.event_engine".to_string(),
            sequence: 208,
            retry_count: 0,
            original_subject: None,
            payload_preview: preview,
            payload_redacted: false,
            privacy_caveats: Vec::new(),
        }],
        groups: vec![DlqMessageGroup {
            original_subject: None,
            reason_bucket: "error_payload.material_persist_failed".to_string(),
            count: 1,
            first_sequence: 208,
            last_sequence: 208,
            sample_previews: Vec::new(),
        }],
    };

    let mut report = dlq_triage_report(stats, peek, 1);
    report.groups[0].material_statuses = vec![DlqTriageMaterialStatus {
        material_id: material_id.to_string(),
        lookup_status: "found".to_string(),
        source_identifier: Some(
            "/realm/project/sinex/crate/sinexd/src/runtime/source_driver.rs".to_string(),
        ),
        material_status: Some("completed".to_string()),
        failure_reason: None,
        total_bytes: Some(51991),
        has_blob: Some(true),
        event_count: Some(1),
        start_time: Some("2026-06-30 4:32:32.599116 +00:00:00".to_string()),
        end_time: Some("2026-06-30 4:33:46.318072 +00:00:00".to_string()),
    }];

    let plan = dlq_cleanup_plan(report);

    assert_eq!(plan.candidate_count, 1);
    assert_eq!(plan.blocked_count, 0);
    assert_eq!(plan.items[0].decision, "purge_candidate");
    assert!(plan.items[0].blockers.is_empty());
    assert!(
        plan.items[0]
            .evidence
            .iter()
            .any(|item| item == "reason_contract=completed_duplicate_blob_upsert")
    );
    assert_eq!(
        plan.items[0].purge_command.as_deref(),
        Some("sinexctl ops dlq purge --start-sequence 208 --end-sequence 208 --confirm")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_cleanup_coalesces_adjacent_candidate_ranges() -> xtask::sandbox::TestResult<()> {
    let items = vec![
        DlqCleanupPlanItemView {
            decision: "purge_candidate".to_string(),
            reason_bucket: "error_payload.slice_arrival_timeout".to_string(),
            count: 2,
            sequence_range: "292..293".to_string(),
            purge_command: Some(
                "sinexctl ops dlq purge --start-sequence 292 --end-sequence 293 --confirm"
                    .to_string(),
            ),
            requeue_command: None,
            inspect_command: "sinexctl ops dlq peek --start-sequence 292 -n 2".to_string(),
            evidence: Vec::new(),
            blockers: Vec::new(),
        },
        DlqCleanupPlanItemView {
            decision: "purge_candidate".to_string(),
            reason_bucket: "error_payload.material_assembly_corruption_detected".to_string(),
            count: 5,
            sequence_range: "294..298".to_string(),
            purge_command: Some(
                "sinexctl ops dlq purge --start-sequence 294 --end-sequence 298 --confirm"
                    .to_string(),
            ),
            requeue_command: None,
            inspect_command: "sinexctl ops dlq peek --start-sequence 294 -n 5".to_string(),
            evidence: Vec::new(),
            blockers: Vec::new(),
        },
        DlqCleanupPlanItemView {
            decision: "inspect_only".to_string(),
            reason_bucket: "error_payload.material_persist_failed".to_string(),
            count: 1,
            sequence_range: "301..301".to_string(),
            purge_command: None,
            requeue_command: None,
            inspect_command: "sinexctl ops dlq peek --start-sequence 301 -n 1".to_string(),
            evidence: Vec::new(),
            blockers: vec!["not terminal".to_string()],
        },
    ];

    let actions = dlq_cleanup_coalesced_actions(&items);

    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].sequence_range, "292..298");
    assert_eq!(actions[0].message_count, 7);
    assert_eq!(actions[0].group_count, 2);
    assert_eq!(
        actions[0].command,
        "sinexctl ops dlq purge --start-sequence 292 --end-sequence 298 --confirm"
    );
    assert_eq!(actions[0].action, "purge");
    assert_eq!(
        actions[0].purge_command.as_deref(),
        Some("sinexctl ops dlq purge --start-sequence 292 --end-sequence 298 --confirm")
    );
    assert_eq!(
        actions[0].reason_buckets,
        vec![
            "error_payload.material_assembly_corruption_detected".to_string(),
            "error_payload.slice_arrival_timeout".to_string()
        ]
    );
    Ok(())
}

#[sinex_test]
async fn dlq_cleanup_coalesces_adjacent_requeue_ranges() -> xtask::sandbox::TestResult<()> {
    let items = vec![
        DlqCleanupPlanItemView {
            decision: "requeue_candidate".to_string(),
            reason_bucket:
                "error_payload.persistence_error_database_error_persisting_batch_timed_out_after_8_416s"
                    .to_string(),
            count: 2,
            sequence_range: "77..78".to_string(),
            purge_command: None,
            requeue_command: Some(
                "sinexctl ops dlq requeue --start-sequence 77 --end-sequence 78".to_string(),
            ),
            inspect_command: "sinexctl ops dlq peek --start-sequence 77 -n 2".to_string(),
            evidence: Vec::new(),
            blockers: Vec::new(),
        },
        DlqCleanupPlanItemView {
            decision: "requeue_candidate".to_string(),
            reason_bucket:
                "error_payload.persistence_error_database_error_persisting_batch_timed_out_after_8_416s"
                    .to_string(),
            count: 1,
            sequence_range: "79..79".to_string(),
            purge_command: None,
            requeue_command: Some(
                "sinexctl ops dlq requeue --start-sequence 79 --end-sequence 79".to_string(),
            ),
            inspect_command: "sinexctl ops dlq peek --start-sequence 79 -n 1".to_string(),
            evidence: Vec::new(),
            blockers: Vec::new(),
        },
    ];

    let actions = dlq_cleanup_coalesced_actions(&items);

    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].action, "requeue");
    assert_eq!(actions[0].sequence_range, "77..79");
    assert_eq!(actions[0].message_count, 3);
    assert_eq!(
        actions[0].command,
        "sinexctl ops dlq requeue --start-sequence 77 --end-sequence 79"
    );
    assert_eq!(
        actions[0].requeue_command.as_deref(),
        Some("sinexctl ops dlq requeue --start-sequence 77 --end-sequence 79")
    );
    assert!(actions[0].purge_command.is_none());
    Ok(())
}

#[sinex_test]
async fn dlq_purge_selector_requires_complete_valid_range() -> xtask::sandbox::TestResult<()> {
    validate_purge_selector(None, None)?;
    validate_purge_selector(Some(2), Some(4))?;

    assert!(validate_purge_selector(Some(2), None).is_err());
    assert!(validate_purge_selector(None, Some(4)).is_err());
    assert!(validate_purge_selector(Some(0), Some(4)).is_err());
    assert!(validate_purge_selector(Some(4), Some(2)).is_err());
    Ok(())
}

#[sinex_test]
async fn dlq_requeue_selector_requires_exactly_one_valid_selector() -> xtask::sandbox::TestResult<()>
{
    validate_requeue_selector(Some("event"), None, None, false)?;
    validate_requeue_selector(None, Some(2), Some(4), false)?;
    validate_requeue_selector(None, None, None, true)?;

    assert!(validate_requeue_selector(None, None, None, false).is_err());
    assert!(validate_requeue_selector(Some("event"), Some(2), Some(4), false).is_err());
    assert!(validate_requeue_selector(None, Some(2), None, false).is_err());
    assert!(validate_requeue_selector(None, None, Some(4), false).is_err());
    assert!(validate_requeue_selector(None, Some(0), Some(4), false).is_err());
    assert!(validate_requeue_selector(None, Some(4), Some(2), false).is_err());
    Ok(())
}

#[sinex_test]
async fn dlq_purge_target_label_names_ranges() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        purge_target_label(Some(2), Some(4), 10),
        "sequence range 2..4"
    );
    assert_eq!(purge_target_label(Some(4), Some(4), 10), "sequence 4");
    assert_eq!(purge_target_label(None, None, 10), "10 messages");
    Ok(())
}

#[sinex_test]
async fn dlq_purge_table_formatter_renders_operation() -> xtask::sandbox::TestResult<()> {
    let rendered = format_dlq_purge_table(&sinex_primitives::rpc::dlq::DlqPurgeResponse {
        status: "success".to_string(),
        purged_count: 3,
        operation_id: "op-1".to_string(),
    });

    assert_eq!(rendered, "success: 3 messages purged (operation op-1)");
    Ok(())
}

fn fixture_dlq_list(
    total_messages: u64,
    pressure_level: sinex_primitives::RuntimePressureLevel,
) -> DlqListResponse {
    let runtime_action = match pressure_level {
        sinex_primitives::RuntimePressureLevel::Unknown => {
            sinex_primitives::RuntimePressureAction::None
        }
        sinex_primitives::RuntimePressureLevel::Nominal => {
            sinex_primitives::RuntimePressureAction::Admit
        }
        sinex_primitives::RuntimePressureLevel::Warning => {
            sinex_primitives::RuntimePressureAction::Inspect
        }
        sinex_primitives::RuntimePressureLevel::Critical => {
            sinex_primitives::RuntimePressureAction::Throttle
        }
    };
    let recommended_action = if total_messages == 0 {
        "none"
    } else {
        "ops dlq peek"
    };
    let action_reason = match pressure_level {
        sinex_primitives::RuntimePressureLevel::Unknown => "DLQ owner could not be observed",
        sinex_primitives::RuntimePressureLevel::Nominal => "raw-ingest DLQ is empty",
        sinex_primitives::RuntimePressureLevel::Warning => {
            "inspect raw-ingest DLQ before retry"
        }
        sinex_primitives::RuntimePressureLevel::Critical => {
            "throttle ingestion and inspect raw-ingest DLQ"
        }
    };

    DlqListResponse {
        total_messages,
        total_bytes: total_messages * 1024,
        first_seq: if total_messages == 0 { 0 } else { 10 },
        last_seq: if total_messages == 0 {
            0
        } else {
            9 + total_messages
        },
        pressure_level,
        resource_pressure: DlqPressureSignal {
            pressure_level,
            runtime_action,
            pending_messages: total_messages,
            pending_bytes: total_messages * 1024,
            retry_batch_size: 10,
            recommended_action: recommended_action.to_string(),
            reason: action_reason.to_string(),
        },
        pending_sequence_span: total_messages,
        recommended_action: recommended_action.to_string(),
        action_reason: action_reason.to_string(),
    }
}

#[sinex_test]
async fn dlq_list_json_renders_finite_view_envelope() -> xtask::sandbox::TestResult<()> {
    let envelope = dlq_list_envelope(fixture_dlq_list(
        3,
        sinex_primitives::RuntimePressureLevel::Warning,
    ));
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.ops.dlq.list");
    assert_eq!(parsed["payload"]["total_messages"], 3);
    assert_eq!(parsed["payload"]["pressure_level"], "warning");
    assert_eq!(parsed["caveats"][0]["id"], "source.absent");
    Ok(())
}

#[sinex_test]
async fn dlq_list_empty_queue_names_observation_limit() -> xtask::sandbox::TestResult<()> {
    let envelope = dlq_list_envelope(fixture_dlq_list(
        0,
        sinex_primitives::RuntimePressureLevel::Nominal,
    ));

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "coverage.unmeasurable");
    assert!(
        envelope.caveats[0].message.contains("current queue observation"),
        "empty DLQ must not be presented as proof of complete historical ingestion"
    );
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref()),
        Some("sinexctl ops dlq list")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_list_critical_pressure_marks_partial_runtime_window()
-> xtask::sandbox::TestResult<()> {
    let envelope = dlq_list_envelope(fixture_dlq_list(
        42,
        sinex_primitives::RuntimePressureLevel::Critical,
    ));

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "window.partial");
    assert!(
        envelope.caveats[0].message.contains("42 pending messages"),
        "critical DLQ caveat should report the pending message count"
    );
    assert!(
        envelope.caveats[0].message.contains("pressure=critical"),
        "critical DLQ caveat should report the pressure level"
    );
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.rpc_method.as_deref()),
        Some("dlq.list")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_list_unknown_pressure_is_unmeasurable() -> xtask::sandbox::TestResult<()> {
    let envelope = dlq_list_envelope(fixture_dlq_list(
        0,
        sinex_primitives::RuntimePressureLevel::Unknown,
    ));

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "coverage.unmeasurable");
    assert!(
        envelope.caveats[0].message.contains("pressure is unknown"),
        "unknown pressure should be explicit in the caveat message"
    );
    Ok(())
}

fn fixture_dlq_message(sequence: u64) -> DlqMessagePeek {
    DlqMessagePeek {
        subject: "dev.events.dlq.event_engine".to_string(),
        sequence,
        retry_count: 0,
        original_subject: Some("dev.events.raw.git.commit_d_created".to_string()),
        payload_preview: "{\"error\":\"live event with equivalence_key git|a already exists\"}"
            .to_string(),
        payload_redacted: false,
        privacy_caveats: Vec::new(),
    }
}

fn fixture_dlq_triage_report() -> DlqTriageReport {
    DlqTriageReport {
        total_messages: 2,
        pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
        first_seq: 10,
        last_seq: 11,
        inspected_tail: 2,
        groups: vec![DlqTriageGroup {
            reason_bucket: "occurrence_duplicate.equivalence_key_exists".to_string(),
            original_subject: Some("dev.events.raw.git.commit_d_created".to_string()),
            count: 2,
            first_sequence: 10,
            last_sequence: 11,
            sample_previews: vec![
                "{\"error\":\"live event with equivalence_key git|a already exists\"}"
                    .to_string(),
            ],
            material_ids: Vec::new(),
            material_statuses: Vec::new(),
            inspect_command: "sinexctl ops dlq peek --start-sequence 10 -n 2".to_string(),
            purge_command:
                "sinexctl ops dlq purge --start-sequence 10 --end-sequence 11 --confirm"
                    .to_string(),
            caveat: "duplicate occurrence; verify historical residue before purge".to_string(),
        }],
        recommended_next: "Inspect group commands before requeue or purge".to_string(),
    }
}

#[sinex_test]
async fn dlq_peek_json_renders_finite_view_envelope() -> xtask::sandbox::TestResult<()> {
    let response = DlqPeekResponse::from_messages(vec![fixture_dlq_message(10)]);
    let envelope = dlq_peek_envelope(response);
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.ops.dlq.peek");
    assert_eq!(parsed["payload"]["messages"][0]["sequence"], 10);
    assert!(
        parsed.get("caveats").is_none(),
        "non-empty peek should not emit readiness caveats"
    );
    Ok(())
}

#[sinex_test]
async fn dlq_peek_empty_sample_names_bounded_observation()
-> xtask::sandbox::TestResult<()> {
    let envelope = dlq_peek_envelope(DlqPeekResponse::from_messages(Vec::new()));

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "coverage.unmeasurable");
    assert!(
        envelope.caveats[0].message.contains("bounded sample"),
        "empty peek caveat must not imply historical completeness"
    );
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref()),
        Some("sinexctl ops dlq peek")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_triage_json_renders_finite_view_envelope() -> xtask::sandbox::TestResult<()> {
    let envelope = dlq_triage_envelope(fixture_dlq_triage_report());
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.ops.dlq.triage");
    assert_eq!(parsed["payload"]["total_messages"], 2);
    assert_eq!(parsed["payload"]["groups"][0]["first_sequence"], 10);
    assert_eq!(parsed["caveats"][0]["id"], "window.partial");
    Ok(())
}

#[sinex_test]
async fn dlq_triage_empty_queue_names_observation_limit()
-> xtask::sandbox::TestResult<()> {
    let mut report = fixture_dlq_triage_report();
    report.total_messages = 0;
    report.pressure_level = sinex_primitives::RuntimePressureLevel::Nominal;
    report.groups.clear();

    let envelope = dlq_triage_envelope(report);

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "coverage.unmeasurable");
    assert!(
        envelope.caveats[0].message.contains("current queue observation"),
        "empty triage caveat must not imply historical completeness"
    );
    Ok(())
}

#[sinex_test]
async fn dlq_cleanup_plan_json_renders_finite_view_envelope()
-> xtask::sandbox::TestResult<()> {
    let plan = dlq_cleanup_plan(fixture_dlq_triage_report());
    let envelope = dlq_cleanup_plan_envelope(plan);
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.ops.dlq.cleanup-plan");
    assert_eq!(
        parsed["payload"]["schema_version"],
        DLQ_CLEANUP_PLAN_SCHEMA_VERSION
    );
    assert!(
        parsed["caveats"]
            .as_array()
            .is_some_and(|caveats| !caveats.is_empty()),
        "cleanup plans with candidates or blockers must emit caveats"
    );
    Ok(())
}

#[sinex_test]
async fn dlq_cleanup_plan_caveats_name_candidates_and_blockers()
-> xtask::sandbox::TestResult<()> {
    let plan = DlqCleanupPlanView {
        schema_version: DLQ_CLEANUP_PLAN_SCHEMA_VERSION.to_string(),
        total_messages: 3,
        pressure_level: sinex_primitives::RuntimePressureLevel::Warning,
        retained_sequence_span: "10..12".to_string(),
        inspected_tail: 3,
        candidate_count: 1,
        blocked_count: 1,
        purge_candidate_messages: 1,
        requeue_candidate_messages: 0,
        coalesced_actions: Vec::new(),
        items: Vec::new(),
        recommended_next: "Run only candidate cleanup actions".to_string(),
    };
    let envelope = dlq_cleanup_plan_envelope(plan);
    let caveat_ids = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect::<Vec<_>>();

    assert!(
        caveat_ids.contains(&"window.partial"),
        "blocked cleanup groups must mark the sampled window partial"
    );
    assert!(
        caveat_ids.contains(&"source.absent"),
        "cleanup candidates mean failed source material remains unresolved"
    );
    assert!(
        envelope
            .caveats
            .iter()
            .any(|caveat| caveat.message.contains("blocked group")),
        "blocked caveat must name blocked groups"
    );
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref()),
        Some("sinexctl ops dlq cleanup-plan")
    );
    Ok(())
}
