use clap::Subcommand;
use serde::Serialize;
use sinex_primitives::views::{
    DLQ_CLEANUP_PLAN_SCHEMA_VERSION, DlqCleanupActionView, DlqCleanupPlanItemView,
    DlqCleanupPlanView,
};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, Spinner, format_bytes, with_spinner_result};
use crate::model::OutputFormat;
use crate::prompt;

/// Dead letter queue operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Show DLQ statistics
    sinexctl ops dlq list

    # Peek at messages in the DLQ
    sinexctl ops dlq peek -n 5

    # Peek at the newest retained DLQ messages
    sinexctl ops dlq peek -n 5 --tail

    # Requeue a specific message for retry
    sinexctl ops dlq requeue --event-id 01HQ2KM...

    # Requeue a known sequence range from peek/cleanup-plan output
    sinexctl ops dlq requeue --start-sequence 156 --end-sequence 163

    # Requeue all failed messages
    sinexctl ops dlq requeue --all

    # Purge a known sequence range from peek output
    sinexctl ops dlq purge --start-sequence 202 --end-sequence 231 --confirm

    # Triage the newest DLQ buckets with follow-up commands
    sinexctl ops dlq triage --tail 20

    # Plan bounded cleanup for historical DLQ residue without deleting anything
    sinexctl ops dlq cleanup-plan --tail 20

    # Plan cleanup for the full retained DLQ span
    sinexctl ops dlq cleanup-plan --all-retained

    # Purge all messages (requires confirmation)
    sinexctl ops dlq purge --confirm
")]
pub enum DlqCommands {
    /// Show DLQ statistics
    #[command(alias = "ls")]
    List,

    /// Peek at messages in the DLQ
    Peek {
        /// Number of messages to peek
        #[arg(long, short = 'n', default_value = "10")]
        limit: usize,

        /// Start peeking at this DLQ stream sequence
        #[arg(long)]
        start_sequence: Option<u64>,

        /// Peek the newest retained DLQ messages by deriving a start sequence
        /// from dlq.list
        #[arg(long)]
        tail: bool,

        /// Maximum sanitized payload-preview characters per message
        #[arg(long, default_value_t = DEFAULT_DLQ_PREVIEW_CHARS)]
        payload_preview_chars: usize,
    },

    /// Summarize newest DLQ buckets and concrete follow-up commands
    Triage {
        /// Number of newest retained messages to inspect
        #[arg(long, default_value = "20")]
        tail: usize,

        /// Inspect the full retained DLQ sequence span from dlq.list
        #[arg(long)]
        all_retained: bool,
    },

    /// Plan bounded DLQ cleanup without mutating the queue
    CleanupPlan {
        /// Number of newest retained messages to inspect
        #[arg(long, default_value = "20")]
        tail: usize,

        /// Inspect the full retained DLQ sequence span from dlq.list
        #[arg(long)]
        all_retained: bool,
    },

    /// Requeue messages from DLQ back to processing
    Requeue {
        /// Specific event ID to requeue (optional)
        #[arg(long)]
        event_id: Option<String>,

        /// Inclusive first DLQ stream sequence to requeue
        #[arg(long)]
        start_sequence: Option<u64>,

        /// Inclusive last DLQ stream sequence to requeue
        #[arg(long)]
        end_sequence: Option<u64>,

        /// Requeue all messages
        #[arg(long)]
        all: bool,
    },

    /// Purge messages from DLQ
    Purge {
        /// Inclusive first DLQ stream sequence to delete
        #[arg(long)]
        start_sequence: Option<u64>,

        /// Inclusive last DLQ stream sequence to delete
        #[arg(long)]
        end_sequence: Option<u64>,

        /// Confirm purge operation
        #[arg(long)]
        confirm: bool,
    },
}

impl DlqCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List => {
                let stats = client.dlq_list().await?;
                CommandOutput::single(stats, format_dlq_stats_table).display(&format)?;
            }
            Self::Peek {
                limit,
                start_sequence,
                tail,
                payload_preview_chars,
            } => {
                let request = build_dlq_peek_request(
                    client,
                    *limit,
                    *start_sequence,
                    *tail,
                    *payload_preview_chars,
                )
                .await?;
                let response = client.dlq_peek_request(request).await?;
                CommandOutput::single(response, format_dlq_peek_table).display(&format)?;
            }
            Self::Triage { tail, all_retained } => {
                let report = build_dlq_triage_report(client, *tail, *all_retained).await?;
                CommandOutput::single(report, format_dlq_triage_table).display(&format)?;
            }
            Self::CleanupPlan { tail, all_retained } => {
                let report = build_dlq_triage_report(client, *tail, *all_retained).await?;
                let plan = dlq_cleanup_plan(report);
                CommandOutput::single(plan, format_dlq_cleanup_plan_table).display(&format)?;
            }
            Self::Requeue {
                event_id,
                start_sequence,
                end_sequence,
                all,
            } => {
                validate_requeue_selector(
                    event_id.as_deref(),
                    *start_sequence,
                    *end_sequence,
                    *all,
                )?;

                let msg = if *all {
                    "Requeuing all messages...".to_string()
                } else if let (Some(start), Some(end)) = (*start_sequence, *end_sequence) {
                    format!("Requeuing sequence range {start}..{end}...")
                } else {
                    format!(
                        "Requeuing event {}...",
                        event_id.as_deref().unwrap_or("unknown")
                    )
                };

                let response = with_spinner_result(
                    msg,
                    "Messages requeued",
                    client.dlq_requeue(event_id.clone(), *start_sequence, *end_sequence, *all),
                )
                .await?;

                println!(
                    "{}: {} messages requeued (operation {})",
                    response.status, response.requeued_count, response.operation_id
                );
            }
            Self::Purge {
                start_sequence,
                end_sequence,
                confirm,
            } => {
                validate_purge_selector(*start_sequence, *end_sequence)?;
                let table_output = matches!(format, OutputFormat::Table);
                // First, check how many messages would be deleted
                let stats = if table_output {
                    let spinner = Spinner::new("Checking DLQ...");
                    let stats = client.dlq_list().await?;
                    spinner.finish_and_clear();
                    stats
                } else {
                    client.dlq_list().await?
                };

                if stats.total_messages == 0 {
                    println!("DLQ is already empty");
                    return Ok(());
                }

                let target =
                    purge_target_label(*start_sequence, *end_sequence, stats.total_messages);
                // Require confirmation flag
                if !confirm {
                    eprintln!("Purge would delete {target} from DLQ");
                    eprintln!();
                    eprintln!("Use --confirm to proceed with purge");
                    std::process::exit(1);
                }

                // Interactive confirmation for human table output. Machine formats rely on
                // the explicit --confirm flag so stdout remains parseable.
                let proceed = if table_output {
                    let prompt_msg = format!("Delete {target} from DLQ? This cannot be undone.");
                    prompt::confirm(&prompt_msg, false)?
                } else {
                    true
                };

                if !proceed {
                    println!("Cancelled");
                    return Ok(());
                }

                // Proceed with purge
                let response = if table_output {
                    with_spinner_result(
                        format!("Purging {target}..."),
                        "DLQ purged",
                        client.dlq_purge(true, *start_sequence, *end_sequence),
                    )
                    .await?
                } else {
                    client
                        .dlq_purge(true, *start_sequence, *end_sequence)
                        .await?
                };

                CommandOutput::single(response, format_dlq_purge_table).display(&format)?;
            }
        }
        Ok(())
    }
}

use sinex_primitives::rpc::dlq::{
    DlqListResponse, DlqMessagePeek, DlqPeekRequest, DlqPeekResponse,
};
use sinex_primitives::rpc::sources::{SourceMaterialDetail, SourcesShowRequest};

const DEFAULT_DLQ_PREVIEW_CHARS: usize = 200;
const TRIAGE_DLQ_PREVIEW_CHARS: usize = 1200;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DlqTriageReport {
    total_messages: u64,
    pressure_level: sinex_primitives::RuntimePressureLevel,
    first_seq: u64,
    last_seq: u64,
    inspected_tail: usize,
    groups: Vec<DlqTriageGroup>,
    recommended_next: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DlqTriageGroup {
    reason_bucket: String,
    original_subject: Option<String>,
    count: usize,
    first_sequence: u64,
    last_sequence: u64,
    sample_previews: Vec<String>,
    material_ids: Vec<String>,
    material_statuses: Vec<DlqTriageMaterialStatus>,
    inspect_command: String,
    purge_command: String,
    caveat: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DlqTriageMaterialStatus {
    material_id: String,
    lookup_status: String,
    source_identifier: Option<String>,
    material_status: Option<String>,
    failure_reason: Option<String>,
    total_bytes: Option<i64>,
    has_blob: Option<bool>,
    event_count: Option<i64>,
    start_time: Option<String>,
    end_time: Option<String>,
}

pub(crate) async fn build_dlq_triage_report(
    client: &GatewayClient,
    tail: usize,
    all_retained: bool,
) -> Result<DlqTriageReport> {
    let stats = client.dlq_list().await?;
    let inspected_tail = dlq_inspected_tail(tail, all_retained, &stats);
    let request = DlqPeekRequest {
        limit: inspected_tail,
        payload_preview_chars: TRIAGE_DLQ_PREVIEW_CHARS,
        start_sequence: tail_start_sequence(inspected_tail, stats.first_seq, stats.last_seq),
    };
    let peek = client.dlq_peek_request(request).await?;
    let mut report = dlq_triage_report(stats, peek, inspected_tail);
    enrich_dlq_triage_material_statuses(client, &mut report).await;
    Ok(report)
}

fn dlq_inspected_tail(tail: usize, all_retained: bool, stats: &DlqListResponse) -> usize {
    if all_retained {
        usize::try_from(stats.pending_sequence_span).unwrap_or(usize::MAX)
    } else {
        tail
    }
}

pub(crate) fn dlq_cleanup_plan(report: DlqTriageReport) -> DlqCleanupPlanView {
    let items = report
        .groups
        .iter()
        .map(dlq_cleanup_plan_item)
        .collect::<Vec<_>>();
    let candidate_count = items
        .iter()
        .filter(|item| is_cleanup_candidate(item))
        .count();
    let blocked_count = items.len().saturating_sub(candidate_count);
    let purge_candidate_messages = items
        .iter()
        .filter(|item| item.decision == "purge_candidate")
        .map(|item| item.count)
        .sum();
    let requeue_candidate_messages = items
        .iter()
        .filter(|item| item.decision == "requeue_candidate")
        .map(|item| item.count)
        .sum();
    let coalesced_actions = dlq_cleanup_coalesced_actions(&items);
    let recommended_next = if report.total_messages == 0 {
        "DLQ is empty; no cleanup needed".to_string()
    } else if candidate_count == 0 {
        "No sampled ranges are cleanup candidates; inspect blockers before mutating DLQ".to_string()
    } else if blocked_count == 0 {
        "All sampled groups have bounded cleanup actions; run purge/requeue commands intentionally"
            .to_string()
    } else {
        "Run only candidate cleanup actions; inspect blocked groups before wider mutation"
            .to_string()
    };

    DlqCleanupPlanView {
        schema_version: DLQ_CLEANUP_PLAN_SCHEMA_VERSION.to_string(),
        total_messages: report.total_messages,
        pressure_level: report.pressure_level,
        retained_sequence_span: format!("{}..{}", report.first_seq, report.last_seq),
        inspected_tail: report.inspected_tail,
        candidate_count,
        blocked_count,
        purge_candidate_messages,
        requeue_candidate_messages,
        coalesced_actions,
        items,
        recommended_next,
    }
}

fn is_cleanup_candidate(item: &DlqCleanupPlanItemView) -> bool {
    matches!(
        item.decision.as_str(),
        "purge_candidate" | "requeue_candidate"
    )
}

fn dlq_cleanup_coalesced_actions(items: &[DlqCleanupPlanItemView]) -> Vec<DlqCleanupActionView> {
    let mut ranges = items
        .iter()
        .filter(|item| is_cleanup_candidate(item))
        .filter_map(|item| {
            let (start, end) = parse_sequence_range(&item.sequence_range)?;
            Some((start, end, item))
        })
        .collect::<Vec<_>>();
    ranges.sort_by_key(|(start, end, _)| (*start, *end));

    let mut actions: Vec<DlqCleanupActionView> = Vec::new();
    for (start, end, item) in ranges {
        if let Some(last) = actions.last_mut()
            && let Some((last_start, last_end)) = parse_sequence_range(&last.sequence_range)
            && start <= last_end.saturating_add(1)
            && last.action == cleanup_action_for_decision(&item.decision)
        {
            let new_end = last_end.max(end);
            last.sequence_range = format!("{last_start}..{new_end}");
            last.message_count += item.count;
            last.group_count += 1;
            if !last.reason_buckets.contains(&item.reason_bucket) {
                last.reason_buckets.push(item.reason_bucket.clone());
                last.reason_buckets.sort();
            }
            let command = cleanup_command_for_decision(&item.decision, last_start, new_end);
            last.command = command.clone();
            if item.decision == "purge_candidate" {
                last.purge_command = Some(command);
            } else if item.decision == "requeue_candidate" {
                last.requeue_command = Some(command);
            }
            continue;
        }

        let command = cleanup_command_for_decision(&item.decision, start, end);
        actions.push(DlqCleanupActionView {
            action: cleanup_action_for_decision(&item.decision).to_string(),
            sequence_range: format!("{start}..{end}"),
            message_count: item.count,
            group_count: 1,
            reason_buckets: vec![item.reason_bucket.clone()],
            purge_command: (item.decision == "purge_candidate").then(|| command.clone()),
            requeue_command: (item.decision == "requeue_candidate").then(|| command.clone()),
            command,
        });
    }
    actions
}

fn cleanup_action_for_decision(decision: &str) -> &'static str {
    match decision {
        "purge_candidate" => "purge",
        "requeue_candidate" => "requeue",
        _ => "inspect",
    }
}

fn cleanup_command_for_decision(decision: &str, start: u64, end: u64) -> String {
    match decision {
        "requeue_candidate" => {
            format!("sinexctl ops dlq requeue --start-sequence {start} --end-sequence {end}")
        }
        _ => {
            format!(
                "sinexctl ops dlq purge --start-sequence {start} --end-sequence {end} --confirm"
            )
        }
    }
}

fn parse_sequence_range(range: &str) -> Option<(u64, u64)> {
    let (start, end) = range.split_once("..")?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

fn dlq_cleanup_plan_item(group: &DlqTriageGroup) -> DlqCleanupPlanItemView {
    let span_len = group
        .last_sequence
        .saturating_sub(group.first_sequence)
        .saturating_add(1) as usize;
    let mut blockers = Vec::new();
    if span_len != group.count {
        blockers.push(format!(
            "sampled group is non-contiguous: {} messages across {} retained sequences",
            group.count, span_len
        ));
    }
    let requeue_candidate = dlq_cleanup_is_requeue_candidate(&group.reason_bucket);
    let completed_duplicate_blob_upsert = dlq_cleanup_is_completed_duplicate_blob_upsert(group);
    if !requeue_candidate
        && dlq_cleanup_requires_material_evidence(&group.reason_bucket)
        && !completed_duplicate_blob_upsert
    {
        if group.material_ids.is_empty() {
            blockers.push("no material IDs were extracted from sampled messages".to_string());
        }
        if group.material_statuses.len() != group.material_ids.len() {
            blockers.push(format!(
                "material lookup coverage mismatch: {} status rows for {} material IDs",
                group.material_statuses.len(),
                group.material_ids.len()
            ));
        }
        for status in &group.material_statuses {
            if status.lookup_status != "found" {
                blockers.push(format!(
                    "material {} lookup status is {}",
                    status.material_id, status.lookup_status
                ));
                continue;
            }
            if status.material_status.as_deref() != Some("failed") {
                blockers.push(format!(
                    "material {} status is {}",
                    status.material_id,
                    status.material_status.as_deref().unwrap_or("unknown")
                ));
            }
            if status.failure_reason.is_none() {
                blockers.push(format!(
                    "material {} has no recorded failure reason",
                    status.material_id
                ));
            }
        }
    }

    let decision = if requeue_candidate && blockers.is_empty() {
        "requeue_candidate"
    } else if blockers.is_empty() {
        "purge_candidate"
    } else {
        "inspect_only"
    }
    .to_string();
    let purge_command = (decision == "purge_candidate").then(|| {
        format!(
            "sinexctl ops dlq purge --start-sequence {} --end-sequence {} --confirm",
            group.first_sequence, group.last_sequence
        )
    });
    let requeue_command = (decision == "requeue_candidate").then(|| {
        format!(
            "sinexctl ops dlq requeue --start-sequence {} --end-sequence {}",
            group.first_sequence, group.last_sequence
        )
    });
    let evidence = dlq_cleanup_evidence(group);

    DlqCleanupPlanItemView {
        decision,
        reason_bucket: group.reason_bucket.clone(),
        count: group.count,
        sequence_range: format!("{}..{}", group.first_sequence, group.last_sequence),
        purge_command,
        requeue_command,
        inspect_command: group.inspect_command.clone(),
        evidence,
        blockers,
    }
}

fn dlq_cleanup_requires_material_evidence(reason_bucket: &str) -> bool {
    !matches!(reason_bucket, "occurrence_duplicate.equivalence_key_exists")
}

fn dlq_cleanup_is_requeue_candidate(reason_bucket: &str) -> bool {
    reason_bucket
        .starts_with("error_payload.persistence_error_database_error_persisting_batch_timed_out")
}

fn dlq_cleanup_is_completed_duplicate_blob_upsert(group: &DlqTriageGroup) -> bool {
    group.reason_bucket == "error_payload.material_persist_failed"
        && !group.material_statuses.is_empty()
        && group
            .material_statuses
            .iter()
            .all(|status| status.lookup_status == "found")
        && group
            .material_statuses
            .iter()
            .all(|status| status.material_status.as_deref() == Some("completed"))
        && group
            .material_statuses
            .iter()
            .all(|status| status.has_blob == Some(true))
        && group
            .material_statuses
            .iter()
            .all(|status| status.event_count.unwrap_or_default() > 0)
        && group
            .material_statuses
            .iter()
            .all(|status| status.failure_reason.is_none())
        && group
            .material_ids
            .iter()
            .all(|material_id| dlq_group_preview_mentions_duplicate_blob(group, material_id))
}

fn dlq_group_preview_mentions_duplicate_blob(group: &DlqTriageGroup, material_id: &str) -> bool {
    group.sample_previews.iter().any(|preview| {
        preview.contains(material_id)
            && preview.contains("Failed to insert blob metadata")
            && preview.contains("duplicate key value violates unique constraint")
            && preview.contains("uk_blobs_annex_backend_content_hash")
    })
}

fn dlq_cleanup_evidence(group: &DlqTriageGroup) -> Vec<String> {
    let mut evidence = vec![
        format!("reason_bucket={}", group.reason_bucket),
        format!("message_count={}", group.count),
    ];
    if !dlq_cleanup_requires_material_evidence(&group.reason_bucket) {
        evidence.push("reason_contract=duplicate_occurrence_suppression".to_string());
        evidence.push("material_lookup=not_required_for_this_reason".to_string());
    }
    if dlq_cleanup_is_completed_duplicate_blob_upsert(group) {
        evidence.push("reason_contract=completed_duplicate_blob_upsert".to_string());
        evidence.push(
            "material_lookup=completed_blob_backed_material_with_duplicate_blob_error".to_string(),
        );
    }
    if !group.material_statuses.is_empty() {
        let found = group
            .material_statuses
            .iter()
            .filter(|status| status.lookup_status == "found")
            .count();
        let failed = group
            .material_statuses
            .iter()
            .filter(|status| status.material_status.as_deref() == Some("failed"))
            .count();
        let with_blob = group
            .material_statuses
            .iter()
            .filter(|status| status.has_blob == Some(true))
            .count();
        evidence.push(format!(
            "materials: found={found}/{} failed={failed}/{} blob_backed={with_blob}/{}",
            group.material_statuses.len(),
            group.material_statuses.len(),
            group.material_statuses.len()
        ));
        let sources = group
            .material_statuses
            .iter()
            .filter_map(|status| status.source_identifier.as_deref())
            .take(3)
            .collect::<Vec<_>>();
        if !sources.is_empty() {
            evidence.push(format!("sample_sources={}", sources.join(", ")));
        }
    }
    evidence
}

fn dlq_triage_report(
    stats: DlqListResponse,
    peek: DlqPeekResponse,
    inspected_tail: usize,
) -> DlqTriageReport {
    let mut groups = Vec::new();
    for group in &peek.groups {
        let mut messages = peek
            .messages
            .iter()
            .filter(|message| {
                message.sequence >= group.first_sequence
                    && message.sequence <= group.last_sequence
                    && message.original_subject == group.original_subject
                    && dlq_triage_reason_bucket(&message.payload_preview) == group.reason_bucket
            })
            .collect::<Vec<_>>();
        messages.sort_by_key(|message| message.sequence);
        for run in contiguous_message_runs(&messages) {
            let Some(first) = run.first() else {
                continue;
            };
            let Some(last) = run.last() else {
                continue;
            };
            let first_sequence = first.sequence;
            let last_sequence = last.sequence;
            let count = run.len();
            let material_ids = material_ids_from_messages(&run);
            let sample_previews = run
                .iter()
                .map(|message| message.payload_preview.clone())
                .take(3)
                .collect();
            let span_len = last_sequence
                .saturating_sub(first_sequence)
                .saturating_add(1) as usize;
            let purge_command = if span_len == count {
                format!(
                    "sinexctl ops dlq purge --start-sequence {first_sequence} --end-sequence {last_sequence} --confirm"
                )
            } else {
                "range purge unsafe for this non-contiguous bucket; inspect samples first"
                    .to_string()
            };
            groups.push(DlqTriageGroup {
                reason_bucket: group.reason_bucket.clone(),
                original_subject: group.original_subject.clone(),
                count,
                first_sequence,
                last_sequence,
                sample_previews,
                material_ids,
                material_statuses: Vec::new(),
                inspect_command: format!(
                    "sinexctl ops dlq peek --start-sequence {first_sequence} -n {span_len}"
                ),
                purge_command,
                caveat: dlq_triage_caveat(&group.reason_bucket).to_string(),
            });
        }
    }

    let recommended_next = if stats.total_messages == 0 {
        "DLQ is empty; no action needed".to_string()
    } else if groups.is_empty() {
        "No sampled groups; run sinexctl ops dlq peek".to_string()
    } else {
        "Inspect group commands before requeue or purge; use purge only for fixed historical residue"
            .to_string()
    };

    DlqTriageReport {
        total_messages: stats.total_messages,
        pressure_level: stats.pressure_level,
        first_seq: stats.first_seq,
        last_seq: stats.last_seq,
        inspected_tail,
        groups,
        recommended_next,
    }
}

fn contiguous_message_runs<'a>(messages: &[&'a DlqMessagePeek]) -> Vec<Vec<&'a DlqMessagePeek>> {
    let mut runs = Vec::new();
    let mut current = Vec::new();
    let mut previous_sequence = None;
    for message in messages {
        let contiguous = previous_sequence
            .map(|previous| message.sequence == previous + 1)
            .unwrap_or(true);
        if !contiguous && !current.is_empty() {
            runs.push(std::mem::take(&mut current));
        }
        current.push(*message);
        previous_sequence = Some(message.sequence);
    }
    if !current.is_empty() {
        runs.push(current);
    }
    runs
}

fn material_ids_from_messages(messages: &[&DlqMessagePeek]) -> Vec<String> {
    let mut ids = Vec::new();
    for message in messages {
        for id in material_ids_from_preview(&message.payload_preview) {
            if !ids.iter().any(|existing| existing == &id) {
                ids.push(id);
            }
        }
    }
    ids
}

fn material_ids_from_preview(preview: &str) -> Vec<String> {
    let marker = "\"material_id\":\"";
    let mut ids = Vec::new();
    let mut rest = preview;
    while let Some(start) = rest.find(marker) {
        let value_start = start + marker.len();
        let value = &rest[value_start..];
        let Some(end) = value.find('"') else {
            break;
        };
        let candidate = &value[..end];
        if is_uuid_like(candidate) {
            ids.push(candidate.to_string());
        }
        rest = &value[end..];
    }
    ids
}

fn is_uuid_like(candidate: &str) -> bool {
    candidate.len() == 36
        && candidate.chars().enumerate().all(|(idx, ch)| {
            matches!(idx, 8 | 13 | 18 | 23) == (ch == '-') && (ch == '-' || ch.is_ascii_hexdigit())
        })
}

fn dlq_triage_reason_bucket(preview: &str) -> String {
    if preview.contains("equivalence_key") && preview.contains("already exists") {
        "occurrence_duplicate.equivalence_key_exists".to_string()
    } else if let Some(error_code) = preview_error_code(preview) {
        format!("error_payload.{error_code}")
    } else if preview.contains("\"error\"") {
        "error_payload.unparsed".to_string()
    } else if preview.contains("[payload contains dangerous Unicode characters]") {
        "unsafe_unicode_preview".to_string()
    } else if preview.is_empty() {
        "empty_preview".to_string()
    } else {
        "unclassified_preview".to_string()
    }
}

fn preview_error_code(preview: &str) -> Option<String> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(preview)
        && let Some(error) = value.get("error").and_then(|error| error.as_str())
    {
        return Some(sanitize_reason_token(error));
    }

    preview_leading_error_string(preview).map(|error| sanitize_reason_token(&error))
}

fn preview_leading_error_string(preview: &str) -> Option<String> {
    let marker = "\"error\":\"";
    let start = preview.find(marker)? + marker.len();
    let rest = &preview[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn sanitize_reason_token(reason: &str) -> String {
    let mut token = String::new();
    for ch in reason.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            token.push(ch.to_ascii_lowercase());
        } else if !token.ends_with('_') {
            token.push('_');
        }
    }
    token.trim_matches('_').to_string()
}

fn dlq_triage_caveat(reason_bucket: &str) -> &'static str {
    match reason_bucket {
        "error_payload.slice_arrival_timeout" => {
            "Usually source-material assembly residue; verify newer materials for the same source complete before purge."
        }
        "error_payload.material_assembly_corruption_detected" => {
            "Assembly invariant failure; purge only after the assembler fix is deployed and the tail stops growing."
        }
        "occurrence_duplicate.equivalence_key_exists" => {
            "Duplicate occurrence suppression; safe to purge only after confirming the source is no longer emitting new duplicates."
        }
        _ => "Inspect samples before deciding between source repair, requeue, or purge.",
    }
}

async fn enrich_dlq_triage_material_statuses(client: &GatewayClient, report: &mut DlqTriageReport) {
    for group in &mut report.groups {
        let mut statuses = Vec::new();
        for material_id in &group.material_ids {
            let request = SourcesShowRequest {
                material_id: material_id.clone(),
            };
            let status = match client.sources_show(request).await {
                Ok(response) => dlq_triage_material_status(&response.material),
                Err(error) => DlqTriageMaterialStatus {
                    material_id: material_id.clone(),
                    lookup_status: "lookup_failed".to_string(),
                    source_identifier: None,
                    material_status: None,
                    failure_reason: Some(error.to_string()),
                    total_bytes: None,
                    has_blob: None,
                    event_count: None,
                    start_time: None,
                    end_time: None,
                },
            };
            statuses.push(status);
        }
        group.material_statuses = statuses;
    }
}

fn dlq_triage_material_status(material: &SourceMaterialDetail) -> DlqTriageMaterialStatus {
    DlqTriageMaterialStatus {
        material_id: material.id.clone(),
        lookup_status: "found".to_string(),
        source_identifier: Some(material.source_identifier.clone()),
        material_status: Some(material.status.to_string()),
        failure_reason: material_failure_reason(material),
        total_bytes: material.total_bytes,
        has_blob: Some(material.optional_blob_id.is_some()),
        event_count: material.event_count,
        start_time: material.start_time.clone(),
        end_time: material.end_time.clone(),
    }
}

fn material_failure_reason(material: &SourceMaterialDetail) -> Option<String> {
    material
        .metadata
        .get("failure_reason")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            material
                .metadata
                .get("error")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        })
}

async fn build_dlq_peek_request(
    client: &GatewayClient,
    limit: usize,
    start_sequence: Option<u64>,
    tail: bool,
    payload_preview_chars: usize,
) -> Result<DlqPeekRequest> {
    if tail && start_sequence.is_some() {
        return Err(color_eyre::eyre::eyre!(
            "Use either --tail or --start-sequence, not both"
        ));
    }

    let resolved_start_sequence = if tail {
        let stats = client.dlq_list().await?;
        tail_start_sequence(limit, stats.first_seq, stats.last_seq)
    } else {
        start_sequence
    };

    Ok(DlqPeekRequest {
        limit,
        payload_preview_chars,
        start_sequence: resolved_start_sequence,
    })
}

fn tail_start_sequence(limit: usize, first_seq: u64, last_seq: u64) -> Option<u64> {
    if limit == 0 || first_seq == 0 || last_seq == 0 {
        return None;
    }
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    let tail_start = last_seq.saturating_sub(limit.saturating_sub(1));
    Some(tail_start.max(first_seq))
}

fn validate_requeue_selector(
    event_id: Option<&str>,
    start_sequence: Option<u64>,
    end_sequence: Option<u64>,
    all: bool,
) -> Result<()> {
    let selector_count = usize::from(event_id.is_some())
        + usize::from(start_sequence.is_some() || end_sequence.is_some())
        + usize::from(all);
    if selector_count != 1 {
        return Err(color_eyre::eyre::eyre!(
            "Use exactly one requeue selector: --event-id, --start-sequence/--end-sequence, or --all"
        ));
    }
    match (start_sequence, end_sequence) {
        (Some(start), Some(end)) if start == 0 || end == 0 => Err(color_eyre::eyre::eyre!(
            "--start-sequence and --end-sequence must be positive"
        )),
        (Some(start), Some(end)) if start > end => Err(color_eyre::eyre::eyre!(
            "--start-sequence must be <= --end-sequence"
        )),
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => Err(color_eyre::eyre::eyre!(
            "Use both --start-sequence and --end-sequence for sequence-range requeue"
        )),
    }
}

fn validate_purge_selector(start_sequence: Option<u64>, end_sequence: Option<u64>) -> Result<()> {
    match (start_sequence, end_sequence) {
        (Some(start), Some(end)) if start == 0 || end == 0 => Err(color_eyre::eyre::eyre!(
            "--start-sequence and --end-sequence must be positive"
        )),
        (Some(start), Some(end)) if start > end => Err(color_eyre::eyre::eyre!(
            "--start-sequence must be <= --end-sequence"
        )),
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => Err(color_eyre::eyre::eyre!(
            "Use both --start-sequence and --end-sequence, or neither to purge all"
        )),
    }
}

fn purge_target_label(
    start_sequence: Option<u64>,
    end_sequence: Option<u64>,
    total_messages: u64,
) -> String {
    match (start_sequence, end_sequence) {
        (Some(start), Some(end)) if start == end => format!("sequence {start}"),
        (Some(start), Some(end)) => format!("sequence range {start}..{end}"),
        _ => format!("{total_messages} messages"),
    }
}

fn format_dlq_purge_table(response: &sinex_primitives::rpc::dlq::DlqPurgeResponse) -> String {
    format!(
        "{}: {} messages purged (operation {})",
        response.status, response.purged_count, response.operation_id
    )
}

/// Format DLQ statistics as table
fn format_dlq_stats_table(stats: &DlqListResponse) -> String {
    let mut output = String::new();
    output.push_str("DLQ Statistics:\n");
    output.push_str(&format!("  Total messages: {}\n", stats.total_messages));
    output.push_str(&format!(
        "  Total bytes: {}\n",
        format_bytes(stats.total_bytes)
    ));
    output.push_str(&format!("  First sequence: {}\n", stats.first_seq));
    output.push_str(&format!("  Last sequence: {}\n", stats.last_seq));
    output.push_str(&format!("  Pressure: {}\n", stats.pressure_level));
    output.push_str(&format!(
        "  Runtime action: {}\n",
        stats.resource_pressure.runtime_action
    ));
    output.push_str(&format!(
        "  Retry batch size: {}\n",
        stats.resource_pressure.retry_batch_size
    ));
    output.push_str(&format!(
        "  Pending sequence span: {}\n",
        stats.pending_sequence_span
    ));
    output.push_str(&format!(
        "  Recommended action: {}\n",
        stats.recommended_action
    ));
    output.push_str(&format!("  Action reason: {}\n", stats.action_reason));
    output
}

fn format_dlq_triage_table(report: &DlqTriageReport) -> String {
    let mut output = String::new();
    output.push_str("DLQ Triage:\n");
    output.push_str(&format!("  Total messages: {}\n", report.total_messages));
    output.push_str(&format!("  Pressure: {}\n", report.pressure_level));
    output.push_str(&format!(
        "  Retained sequence span: {}..{}\n",
        report.first_seq, report.last_seq
    ));
    output.push_str(&format!("  Inspected tail: {}\n", report.inspected_tail));
    output.push_str(&format!("  Next: {}\n", report.recommended_next));

    if report.groups.is_empty() {
        output.push_str("\nNo sampled DLQ groups.\n");
        return output;
    }

    output.push_str("\nGroups:\n");
    output.push_str(&format!("{}\n", "─".repeat(80)));
    for group in &report.groups {
        output.push_str(&format!(
            "{} message(s), seq {}..{}\n",
            group.count, group.first_sequence, group.last_sequence
        ));
        output.push_str(&format!("  Reason: {}\n", group.reason_bucket));
        output.push_str(&format!(
            "  Original subject: {}\n",
            group.original_subject.as_deref().unwrap_or("(unknown)")
        ));
        if !group.material_ids.is_empty() {
            output.push_str(&format!(
                "  Material IDs: {}\n",
                group.material_ids.join(", ")
            ));
        }
        if !group.material_statuses.is_empty() {
            output.push_str("  Material status:\n");
            for status in &group.material_statuses {
                output.push_str(&format!("    {}\n", format_triage_material_status(status)));
            }
        }
        output.push_str(&format!("  Inspect: {}\n", group.inspect_command));
        output.push_str(&format!("  Purge if historical: {}\n", group.purge_command));
        output.push_str(&format!("  Caveat: {}\n", group.caveat));
        output.push_str(&format!("{}\n", "─".repeat(80)));
    }
    output
}

fn format_dlq_cleanup_plan_table(plan: &DlqCleanupPlanView) -> String {
    let mut output = String::new();
    output.push_str("DLQ Cleanup Plan:\n");
    output.push_str(&format!("  Total messages: {}\n", plan.total_messages));
    output.push_str(&format!("  Pressure: {}\n", plan.pressure_level));
    output.push_str(&format!(
        "  Retained sequence span: {}\n",
        plan.retained_sequence_span
    ));
    output.push_str(&format!("  Inspected tail: {}\n", plan.inspected_tail));
    output.push_str(&format!("  Candidate groups: {}\n", plan.candidate_count));
    output.push_str(&format!("  Blocked groups: {}\n", plan.blocked_count));
    output.push_str(&format!(
        "  Purge candidate messages: {}\n",
        plan.purge_candidate_messages
    ));
    output.push_str(&format!(
        "  Requeue candidate messages: {}\n",
        plan.requeue_candidate_messages
    ));
    output.push_str(&format!("  Next: {}\n", plan.recommended_next));

    if plan.items.is_empty() {
        output.push_str("\nNo sampled DLQ groups.\n");
        return output;
    }

    if !plan.coalesced_actions.is_empty() {
        output.push_str("\nCoalesced cleanup actions:\n");
        output.push_str(&format!("{}\n", "─".repeat(80)));
        for action in &plan.coalesced_actions {
            output.push_str(&format!(
                "{} message(s), {} group(s), seq {}\n",
                action.message_count, action.group_count, action.sequence_range
            ));
            output.push_str(&format!(
                "  Reasons: {}\n",
                action.reason_buckets.join(", ")
            ));
            output.push_str(&format!("  Action: {}\n", action.action));
            output.push_str(&format!("  Command: {}\n", action.command));
            output.push_str(&format!("{}\n", "─".repeat(80)));
        }
    }

    output.push_str("\nItems:\n");
    output.push_str(&format!("{}\n", "─".repeat(80)));
    for item in &plan.items {
        output.push_str(&format!(
            "{}: {} message(s), seq {}\n",
            item.decision, item.count, item.sequence_range
        ));
        output.push_str(&format!("  Reason: {}\n", item.reason_bucket));
        output.push_str(&format!("  Inspect: {}\n", item.inspect_command));
        if let Some(command) = &item.purge_command {
            output.push_str(&format!("  Purge candidate: {command}\n"));
        }
        if let Some(command) = &item.requeue_command {
            output.push_str(&format!("  Requeue candidate: {command}\n"));
        }
        if !item.blockers.is_empty() {
            output.push_str("  Blockers:\n");
            for blocker in &item.blockers {
                output.push_str(&format!("    - {blocker}\n"));
            }
        }
        if !item.evidence.is_empty() {
            output.push_str("  Evidence:\n");
            for evidence in &item.evidence {
                output.push_str(&format!("    - {evidence}\n"));
            }
        }
        output.push_str(&format!("{}\n", "─".repeat(80)));
    }
    output
}

fn format_triage_material_status(status: &DlqTriageMaterialStatus) -> String {
    if status.lookup_status != "found" {
        return format!(
            "{} lookup={}: {}",
            status.material_id,
            status.lookup_status,
            status.failure_reason.as_deref().unwrap_or("unknown error")
        );
    }

    let source = status.source_identifier.as_deref().unwrap_or("-");
    let material_status = status.material_status.as_deref().unwrap_or("-");
    let bytes = status
        .total_bytes
        .map_or_else(|| "-".to_string(), |bytes| bytes.to_string());
    let blob = status
        .has_blob
        .map_or_else(|| "-".to_string(), |has_blob| has_blob.to_string());
    let events = status
        .event_count
        .map_or_else(|| "-".to_string(), |count| count.to_string());
    let failure = status.failure_reason.as_deref().unwrap_or("-");

    format!(
        "{} status={} source={} bytes={} blob={} events={} failure={}",
        status.material_id, material_status, source, bytes, blob, events, failure
    )
}

/// Format DLQ peek response as table
fn format_dlq_peek_table(response: &DlqPeekResponse) -> String {
    if response.messages.is_empty() {
        return "No messages in DLQ.".to_string();
    }

    let mut output = String::new();
    if !response.groups.is_empty() {
        output.push_str("DLQ Groups:\n");
        output.push_str(&format!("{}\n", "─".repeat(80)));
        for group in &response.groups {
            output.push_str(&format!(
                "  {} message(s), seq {}..{}\n",
                group.count, group.first_sequence, group.last_sequence
            ));
            output.push_str(&format!("    Reason: {}\n", group.reason_bucket));
            let subject = group.original_subject.as_deref().unwrap_or("(unknown)");
            output.push_str(&format!("    Original subject: {subject}\n"));
            if let Some(sample) = group.sample_previews.first() {
                output.push_str(&format!("    Sample: {sample}\n"));
            }
        }
        output.push('\n');
    }

    output.push_str(&format_dlq_messages_table(&response.messages));
    output
}

/// Format DLQ messages as table
fn format_dlq_messages_table(messages: &[DlqMessagePeek]) -> String {
    let mut output = String::new();
    output.push_str("DLQ Messages:\n");
    output.push_str(&format!("{}\n", "─".repeat(80)));
    for (i, msg) in messages.iter().enumerate() {
        output.push_str(&format!(
            "\nMessage #{} (seq: {}, retries: {})\n",
            i + 1,
            msg.sequence,
            msg.retry_count
        ));
        output.push_str(&format!("  Subject: {}\n", msg.subject));
        if let Some(ref orig) = msg.original_subject {
            output.push_str(&format!("  Original subject: {orig}\n"));
        }
        output.push_str(&format!("  Preview: {}\n", msg.payload_preview));
        if msg.payload_redacted {
            output.push_str("  Privacy: redacted\n");
        }
        if !msg.privacy_caveats.is_empty() {
            let caveats = msg
                .privacy_caveats
                .iter()
                .map(|caveat| {
                    caveat.ref_.as_ref().map_or_else(
                        || caveat.id.clone(),
                        |ref_| format!("{} [{}]", caveat.id, ref_.id),
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&format!("  Privacy caveats: {caveats}\n"));
        }
        if i < messages.len() - 1 {
            output.push_str(&format!("{}\n", "─".repeat(80)));
        }
    }
    output
}

#[cfg(test)]
#[path = "dlq_test.rs"]
mod tests;
