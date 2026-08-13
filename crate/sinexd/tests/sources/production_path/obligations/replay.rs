//! Replay obligations for the production-path harness.
//!
//! The generic obligation below remains a cheap parser determinism check. The
//! `ManifestAndSourceRemoval` proof is deliberately separate because it needs
//! the real event engine, source host, registry, and CAS. It is a route-level
//! proof for `fs`, not a claim that every source family has the same material
//! fidelity.

use crate::AdapterKind;
use futures::StreamExt;
use sinex_primitives::environment::environment;
use sinex_primitives::{ControlSubject, MaterialManifestV1, Uuid};
use sinexd::runtime::stream::ReplayMaterialOccurrence;
use sinexd::runtime::{
    Checkpoint, ContentStoreConfig, ContentStoreManager, MaterialReplayContext, ReplayScopeFilters,
    ResolvedReplayMaterial, ScanArgs, SourceScanAck, SourceScanCommand, SourceScanProgress,
    TimeHorizon,
};
use sinexd::sources::dispatch::default_parser_dispatch;
use xtask::sandbox::prelude::*;

const MANIFEST_REPLAY_SOURCE_ID: &str = "fs";
const MANIFEST_REPLAY_EVENT_TYPE: &str = "file.created";

/// Run the replay obligation for a source.
///
/// # Errors
///
/// Returns an error if either dispatch run fails or the event types diverge.
pub async fn run(
    source_id: &str,
    _adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
) -> Result<(), String> {
    let dispatch = default_parser_dispatch();

    // First run — simulates original ingestion.
    let material_id_1 = Uuid::now_v7();
    let outcome_1 = dispatch(source_id, fixture_data, Some(material_id_1))
        .map_err(|e| format!("replay first dispatch error for '{source_id}': {e}"))?;

    // Second run — simulates replay with new material id.
    let material_id_2 = Uuid::now_v7();
    let outcome_2 = dispatch(source_id, fixture_data, Some(material_id_2))
        .map_err(|e| format!("replay second dispatch error for '{source_id}': {e}"))?;

    // Material IDs must differ (replay uses new IDs).
    assert_ne!(
        material_id_1, material_id_2,
        "BUG: material IDs must differ between replay runs"
    );

    // Event types must match — parser is deterministic.
    let types_1: Vec<&str> = outcome_1
        .events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    let types_2: Vec<&str> = outcome_2
        .events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    if types_1 != types_2 {
        return Err(format!(
            "replay for '{source_id}': event types differ between runs. \
             run1={types_1:?} run2={types_2:?}"
        ));
    }

    // Verify expected event types appear in both runs.
    for &expected in expected_event_types {
        if !types_1.contains(&expected) {
            return Err(format!(
                "replay for '{source_id}': expected event type '{expected}' \
                 missing from replay output. Got: {types_1:?}"
            ));
        }
    }

    Ok(())
}

#[derive(Debug)]
struct MaterialWitness {
    id: Uuid,
    material_kind: String,
    source_identifier: String,
    metadata: serde_json::Value,
    total_bytes: i64,
}

type EventWitness = (
    Uuid,
    i64,
    Option<i64>,
    Option<i64>,
    Option<Vec<u8>>,
    serde_json::Value,
);

async fn find_completed_material(
    pool: &sqlx::PgPool,
    logical_source_identifier: &str,
) -> Result<Option<MaterialWitness>, sqlx::Error> {
    sqlx::query_as::<_, (Uuid, String, String, serde_json::Value, i64)>(
        r#"
        SELECT id,
               material_kind,
               source_identifier,
               metadata,
               COALESCE(total_bytes, -1)::bigint
        FROM raw.source_material_registry
        WHERE status = 'completed'
          AND metadata->>'logical_source_identifier' = $1
        ORDER BY staged_at DESC
        LIMIT 1
        "#,
    )
    .bind(logical_source_identifier)
    .fetch_optional(pool)
    .await
    .map(|row| {
        row.map(
            |(id, material_kind, source_identifier, metadata, total_bytes)| MaterialWitness {
                id,
                material_kind,
                source_identifier,
                metadata,
                total_bytes,
            },
        )
    })
}

async fn find_material_events(
    pool: &sqlx::PgPool,
    material_id: Uuid,
) -> Result<Vec<EventWitness>, sqlx::Error> {
    sqlx::query_as::<_, EventWitness>(
        r#"
        SELECT id,
               COALESCE(anchor_byte, -1)::bigint,
               offset_start,
               offset_end,
               anchor_payload_hash,
               payload
        FROM core.events
        WHERE source_material_id = $1
          AND event_type = $2
        ORDER BY ts_persisted, id
        "#,
    )
    .bind(material_id)
    .bind(MANIFEST_REPLAY_EVENT_TYPE)
    .fetch_all(pool)
    .await
}

/// Run the route-level `ManifestAndSourceRemoval` obligation.
///
/// This starts the registered `fs` source factory through the real source-host
/// runtime and lets the real material assembler persist the manifest. Replay
/// is then dispatched through the source runtime's historical scan command,
/// which loads the manifest-authoritative CAS object and its exact occurrence
/// range. The original path is changed and removed before that command.
#[sinex_test(timeout = 180)]
async fn manifest_and_source_removal_obligation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let cas_dir = tempfile::tempdir()?;
    let source_dir = tempfile::tempdir()?;
    let source_path = source_dir.path().join("production-path-proof.txt");
    let source_path_string = source_path.to_string_lossy().into_owned();
    let payload = b"production path manifest proof: exact CAS bytes";

    // The source host and event engine must resolve the same CAS root. EnvGuard
    // also prevents this production-path test from leaking its test CAS into
    // neighboring tests.
    let mut env = EnvGuard::new();
    env.set("SINEX_CONTENT_STORE_PATH", cas_dir.path());

    let event_engine_work_dir = tempfile::tempdir()?;
    let mut event_engine = start_test_event_engine_with_config(
        TestEventEngineConfig {
            nats: ctx.nats_handle()?.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(event_engine_work_dir.path().to_path_buf()),
            namespace: Some(ctx.pipeline_namespace().prefix().to_string()),
            consumer_fetch_max_messages: 32,
            consumer_fetch_timeout_ms: 50,
            database_pool_size: 4,
            reject_initial_replay: false,
        },
        Some(&ctx),
    )
    .await?;

    let source_work_dir = tempfile::tempdir()?;
    let runtime_config = serde_json::json!({
        "watch_paths": [source_dir.path()],
        "max_capture_bytes": 1024,
    });
    let mut source = start_test_source(
        TestSourceDriverConfig {
            source_id: MANIFEST_REPLAY_SOURCE_ID.to_string(),
            nats: ctx.nats_handle()?.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(source_work_dir.path().to_path_buf()),
            namespace: Some(ctx.pipeline_namespace().prefix().to_string()),
            runtime_config: Some(runtime_config.to_string()),
            service_name: Some("production-path-manifest-replay".to_string()),
        },
        Some(&ctx),
    )
    .await?;

    // Initial capture goes through FileContentDropAdapter -> AcquisitionManager
    // -> material assembler -> registry/CAS, rather than registering a fake row.
    tokio::fs::write(&source_path, payload).await?;
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool().clone();
            let source_path_string = source_path_string.clone();
            async move {
                Ok::<bool, sqlx::Error>(
                    find_completed_material(&pool, &source_path_string)
                        .await?
                        .is_some(),
                )
            }
        },
        Timeouts::LONG,
    )
    .await?;

    let material = find_completed_material(ctx.pool(), &source_path_string)
        .await?
        .expect("wait established a completed source material");
    assert_eq!(material.total_bytes, payload.len() as i64);
    assert_eq!(material.metadata["path"], source_path_string);
    assert_eq!(
        material.metadata["event_kind"], "Created",
        "the replay occurrence must come from the real file-drop record metadata"
    );

    // The registry's metadata points to the canonical manifest object. Decode,
    // validate, and re-encode it before any replay is attempted.
    let manifest_reference = material.metadata["material_manifest"]["content_key"]
        .as_str()
        .ok_or_else(|| eyre!("completed fs material has no manifest CAS reference"))?;
    assert_eq!(
        material.metadata["material_manifest"]["manifest_type"],
        sinex_primitives::MATERIAL_MANIFEST_V1
    );
    let cas_root = camino::Utf8PathBuf::from_path_buf(cas_dir.path().to_path_buf())
        .map_err(|path| eyre!("test CAS path is not UTF-8: {}", path.display()))?;
    let content_store = ContentStoreManager::new(
        ContentStoreConfig {
            root_path: cas_root,
            ..Default::default()
        },
        ctx.pool().clone(),
        None,
    )?;
    let manifest_bytes = content_store
        .retrieve_cas_object(manifest_reference)
        .await?;
    let manifest =
        match MaterialManifestV1::decode(&manifest_bytes).map_err(|error| eyre!(error))? {
            sinex_primitives::DecodedMaterialManifest::V1(manifest) => manifest,
            decoded => return Err(eyre!("expected MaterialManifestV1, got {decoded:?}")),
        };
    manifest.validate().map_err(|error| eyre!(error))?;
    assert_eq!(manifest.canonical_bytes()?, manifest_bytes);
    assert_eq!(manifest.source_material_id, material.id);
    assert_eq!(manifest.bytes.encoded_size, payload.len() as u64);
    assert_eq!(
        manifest.bytes.encoded.value_hex,
        blake3::hash(payload).to_hex().to_string()
    );

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool().clone();
            let material_id = material.id;
            async move {
                Ok::<bool, sqlx::Error>(!find_material_events(&pool, material_id).await?.is_empty())
            }
        },
        Timeouts::LONG,
    )
    .await?;
    let original_events = find_material_events(ctx.pool(), material.id).await?;
    assert_eq!(original_events.len(), 1);
    let original_event = &original_events[0];
    assert_eq!(original_event.1, 0);
    assert_eq!(original_event.2, Some(0));
    assert_eq!(original_event.3, Some(payload.len() as i64));
    assert_eq!(
        original_event.4.as_deref(),
        Some(blake3::hash(payload).as_bytes().as_slice())
    );

    // Anti-vacuity mutation: replay must not pass because the old path still
    // happens to contain equivalent bytes. The path is changed, then removed.
    tokio::fs::write(
        &source_path,
        b"mutated path bytes that replay must ignore completely",
    )
    .await?;
    tokio::fs::remove_file(&source_path).await?;
    assert!(
        !source_path.exists(),
        "the original source path must be gone"
    );

    let operation_id = Uuid::now_v7();
    let replay = MaterialReplayContext {
        operation_id,
        materials: vec![ResolvedReplayMaterial {
            source_material_id: material.id,
            material_kind: material.material_kind.clone(),
            source_identifier: material.source_identifier.clone(),
            material_metadata: material.metadata.clone(),
            material_start_time: None,
            material_end_time: None,
        }],
        occurrences: vec![ReplayMaterialOccurrence {
            source_material_id: material.id,
            anchor_byte: original_event.1,
            offset_start: original_event.2,
            offset_end: original_event.3,
            record_metadata: material.metadata.clone(),
        }],
        replay_scope: ReplayScopeFilters {
            material_ids: Some(vec![material.id]),
            event_types: Some(vec![MANIFEST_REPLAY_EVENT_TYPE.to_string()]),
        },
    };
    let mut args = ScanArgs::default();
    args.replay = Some(replay);
    let command = SourceScanCommand {
        operation_id,
        from: Checkpoint::None,
        until: TimeHorizon::Historical {
            end_time: Timestamp::now(),
        },
        args,
    };

    let nats = ctx.nats_client();
    let progress_subject =
        environment().nats_subject(&ControlSubject::replay_progress(operation_id));
    let mut progress = nats.subscribe(progress_subject).await?;
    let scan_subject =
        environment().nats_subject(&ControlSubject::source_scan(MANIFEST_REPLAY_SOURCE_ID));
    let ack_message = nats
        .request(scan_subject, serde_json::to_vec(&command)?.into())
        .await
        .map_err(|error| eyre!("source scan request failed: {error}"))?;
    let ack: SourceScanAck = serde_json::from_slice(&ack_message.payload)?;
    assert!(
        ack.accepted,
        "real source runtime rejected replay: {:?}",
        ack.error
    );

    let final_progress: SourceScanProgress =
        tokio::time::timeout(Duration::from_secs(Timeouts::LONG), async {
            loop {
                let message = progress
                    .next()
                    .await
                    .ok_or_else(|| eyre!("source replay progress subscription closed"))?;
                let update: SourceScanProgress = serde_json::from_slice(&message.payload)?;
                if update.operation_id != operation_id {
                    continue;
                }
                if update.error.is_some() || update.final_report.is_some() {
                    return Ok::<SourceScanProgress, color_eyre::Report>(update);
                }
            }
        })
        .await
        .map_err(|_| eyre!("timed out waiting for source replay progress"))??;
    assert!(
        final_progress.error.is_none(),
        "source replay failed: {final_progress:?}"
    );
    let report = final_progress
        .final_report
        .ok_or_else(|| eyre!("source replay ended without a final report"))?;
    assert_eq!(report.events_processed, 1);
    assert_eq!(final_progress.events_emitted, 1);

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool().clone();
            let material_id = material.id;
            async move {
                Ok::<bool, sqlx::Error>(find_material_events(&pool, material_id).await?.len() == 2)
            }
        },
        Timeouts::LONG,
    )
    .await?;
    let replayed_events = find_material_events(ctx.pool(), material.id).await?;
    assert_eq!(replayed_events.len(), 2);
    let replayed_event = &replayed_events[1];
    assert_ne!(
        replayed_event.0, original_event.0,
        "replay must mint a fresh event id"
    );
    assert_eq!(
        replayed_event.1..replayed_event.3.unwrap(),
        original_event.1..original_event.3.unwrap(),
        "replay must preserve occurrence coordinates"
    );
    assert_eq!(replayed_event.2, original_event.2);
    assert_eq!(replayed_event.3, original_event.3);
    assert_eq!(replayed_event.4, original_event.4);
    assert_eq!(replayed_event.5["path"], source_path_string);
    assert_ne!(
        replayed_event.4.as_deref(),
        Some(
            blake3::hash(b"mutated path bytes that replay must ignore completely")
                .as_bytes()
                .as_slice()
        ),
        "replay must not hash bytes from the mutated or removed source path"
    );

    source.stop().await?;
    event_engine.stop().await?;
    Ok(())
}
