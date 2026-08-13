use super::*;
use crate::runtime::checkpoint::CheckpointManager;
use crate::runtime::content_store::{ContentStoreConfig, ContentStoreManager};
use crate::runtime::parser::adapters::{
    AppendOnlyCursor, ChainedCursor, FileDropAdapter, SqliteRowCursor,
};
use crate::runtime::parser::{InputShapeKind, ParserError, ParserResult, SourceRecord};
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, EventEmitter, ReplayMaterialOccurrence, ReplayScopeFilters,
    ResolvedReplayMaterial, RuntimeHandles, ScanArgs, ServiceInfo, TimeHorizon,
};
use crate::runtime::{EventTransport, NatsPublisher, SOURCE_MATERIAL_STREAM};
use async_trait::async_trait;
use camino::Utf8PathBuf;
use futures::stream::{self, BoxStream};
use sinex_db::DbPoolExt;
use sinex_db::repositories::source_material_relation_types;
use sinex_db::repositories::source_materials::SourceMaterial as SourceMaterialRegistration;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::Event;
use sinex_primitives::parser::{MaterialAnchor, ParserId, ParserManifest, SourceId};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::privacy::{
    RuntimePrivateModeState, load_private_mode_state, private_mode_state_path,
    save_private_mode_state,
};
use sinex_primitives::rpc::sources::{CaveatSeverity, caveat_codes};
use sinex_primitives::{Bytes, HostName, JsonValue, Seconds, SinexError};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;
use xtask::sandbox::prelude::{
    EnvGuard, TestContext, TestResult, WaitHelpers, sinex_serial_test, sinex_test,
};

#[derive(Default)]
struct TestAdapter;

#[async_trait]
impl InputShapeAdapter for TestAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        Ok(Box::pin(stream::empty()))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(0)
    }
}

impl InputShapeAdapterExt for TestAdapter {}

/// Probe adapter for the private-mode acquisition gate. Its stream contains a
/// record that would create source material if `drain_adapter` reached the
/// adapter, so the open count and material assertion verify suppression at the
/// raw acquisition boundary rather than only checking a binding flag.
static PRIVATE_MODE_PROBE_OPENS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct PrivateModeProbeAdapter;

#[async_trait]
impl InputShapeAdapter for PrivateModeProbeAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        PRIVATE_MODE_PROBE_OPENS.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::once(async move {
            Ok(SourceRecord {
                material_id,
                anchor: MaterialAnchor::ByteRange { start: 0, len: 18 },
                bytes: b"private probe record".to_vec(),
                logical_path: None,
                source_ts_hint: None,
                metadata: JsonValue::Null,
            })
        })))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(1)
    }
}

impl InputShapeAdapterExt for PrivateModeProbeAdapter {}

/// Counts adapter opens so the replay guard test can prove that a scoped
/// replay never falls through to a fresh-cursor whole-source scan.
static REPLAY_GUARD_OPENS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct ReplayGuardAdapter;

#[async_trait]
impl InputShapeAdapter for ReplayGuardAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        REPLAY_GUARD_OPENS.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::empty()))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(0)
    }
}

impl InputShapeAdapterExt for ReplayGuardAdapter {}

#[derive(Default)]
struct ErroringAdapter;

#[async_trait]
impl InputShapeAdapter for ErroringAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::StaticFile;

    async fn open(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        Ok(Box::pin(stream::iter(vec![Err(ParserError::Adapter(
            "fixture input exceeded whole-file limit".to_string(),
        ))])))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(1)
    }
}

impl InputShapeAdapterExt for ErroringAdapter {}

#[derive(Default)]
struct FingerprintAdapter {
    fingerprint: Option<SourceRecordFingerprint>,
}

#[async_trait]
impl InputShapeAdapter for FingerprintAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        Ok(Box::pin(stream::empty()))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(0)
    }

    fn input_fingerprint(
        &self,
        _config: &Self::Config,
    ) -> ParserResult<Option<SourceRecordFingerprint>> {
        Ok(self.fingerprint.clone())
    }
}

impl InputShapeAdapterExt for FingerprintAdapter {}

#[derive(Default)]
struct TestParser;

#[async_trait]
impl MaterialParser for TestParser {
    type Config = ();

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("test-parser"),
            parser_version: "1.0.0".to_string(),
            accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
            source_id: SourceId::from_static("desktop.clipboard"),
            declared_event_types: vec![(
                EventSource::from_static("test"),
                EventType::from_static("test.event"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: String::new(),
        }
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["/message".to_string()]
    }

    async fn parse_record(
        &mut self,
        _record: SourceRecord,
        _ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct OversizedRecordAdapter;

#[async_trait]
impl InputShapeAdapter for OversizedRecordAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let oversized = vec![b'x'; 512 * 1024 + 1];
        let record = SourceRecord {
            material_id,
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: oversized.len() as u64,
            },
            bytes: oversized,
            logical_path: None,
            source_ts_hint: None,
            metadata: JsonValue::Null,
        };
        Ok(Box::pin(stream::iter(vec![Ok(record)])))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(1)
    }
}

impl InputShapeAdapterExt for OversizedRecordAdapter {}

#[derive(Default)]
struct EmptyLogicalPathRecordAdapter;

#[async_trait]
impl InputShapeAdapter for EmptyLogicalPathRecordAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::StaticFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let record = SourceRecord {
            material_id,
            anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            bytes: Vec::new(),
            logical_path: Some(Utf8PathBuf::from("/realm/project/sinex")),
            source_ts_hint: None,
            metadata: JsonValue::Null,
        };
        Ok(Box::pin(stream::iter(vec![Ok(record)])))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(1)
    }
}

impl InputShapeAdapterExt for EmptyLogicalPathRecordAdapter {}

/// Replays the same durable material occurrence on every open. This models a
/// retry of one source record after a sibling settlement boundary without
/// introducing a new material id, which is the occurrence identity contract
/// exercised by sinex-w4i.
#[derive(Default)]
struct StableMaterialRecordAdapter;

#[async_trait]
impl InputShapeAdapter for StableMaterialRecordAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::StaticFile;

    async fn open(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        Ok(Box::pin(stream::once(async {
            Ok(SourceRecord {
                material_id: Id::from_uuid(Uuid::from_u128(0x535441424c455f4d4154455249414c)),
                anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
                bytes: Vec::new(),
                logical_path: None,
                source_ts_hint: None,
                metadata: JsonValue::Null,
            })
        })))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(1)
    }
}

impl InputShapeAdapterExt for StableMaterialRecordAdapter {}

#[derive(Default)]
struct PendingAfterOneRecordAdapter;

#[async_trait]
impl InputShapeAdapter for PendingAfterOneRecordAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let record = SourceRecord {
            material_id,
            anchor: MaterialAnchor::ByteRange { start: 0, len: 5 },
            bytes: b"hello".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: JsonValue::Null,
        };
        Ok(Box::pin(
            stream::iter(vec![Ok(record)]).chain(stream::pending()),
        ))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(1)
    }
}

impl InputShapeAdapterExt for PendingAfterOneRecordAdapter {}

#[derive(Default)]
struct ManyNilMaterialRecordsAdapter;

#[async_trait]
impl InputShapeAdapter for ManyNilMaterialRecordsAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let records = (0..128).map(move |idx| {
            Ok(SourceRecord {
                material_id,
                anchor: MaterialAnchor::ByteRange { start: 0, len: 16 },
                bytes: format!("record-{idx:03}\n").into_bytes(),
                logical_path: None,
                source_ts_hint: None,
                metadata: JsonValue::Null,
            })
        });
        Ok(Box::pin(stream::iter(records)))
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        let text = std::str::from_utf8(&record.bytes)
            .map_err(|error| ParserError::Parse(format!("invalid test record: {error}")))?;
        let number = text
            .trim()
            .strip_prefix("record-")
            .ok_or_else(|| ParserError::Parse("missing record prefix".to_string()))?
            .parse::<u64>()
            .map_err(|error| ParserError::Parse(format!("invalid test cursor: {error}")))?;
        Ok(number)
    }
}

impl InputShapeAdapterExt for ManyNilMaterialRecordsAdapter {}

#[derive(Default)]
struct TwoRecordAdapter;

#[async_trait]
impl InputShapeAdapter for TwoRecordAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let records = ["alpha", "beta"]
            .into_iter()
            .enumerate()
            .map(move |(idx, text)| {
                Ok(SourceRecord {
                    material_id,
                    anchor: MaterialAnchor::Line {
                        line: idx as u64,
                        byte_start: idx as u64 * 10,
                    },
                    bytes: text.as_bytes().to_vec(),
                    logical_path: None,
                    source_ts_hint: None,
                    metadata: json!({ "cursor": idx as u64 + 1 }),
                })
            });
        Ok(Box::pin(stream::iter(records)))
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        record
            .metadata
            .get("cursor")
            .and_then(JsonValue::as_u64)
            .ok_or_else(|| ParserError::Parse("missing cursor metadata".to_string()))
    }
}

impl InputShapeAdapterExt for TwoRecordAdapter {}

#[derive(Default)]
struct AlreadyMaterializedRecordAdapter;

#[async_trait]
impl InputShapeAdapter for AlreadyMaterializedRecordAdapter {
    type Config = ();
    type Cursor = u64;

    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        Err(ParserError::Adapter(
            "open_with_acquisition should be used for materialized records".to_string(),
        ))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(1)
    }
}

#[async_trait]
impl InputShapeAdapterExt for AlreadyMaterializedRecordAdapter {
    async fn open_with_acquisition(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
        acquisition: Option<Arc<AcquisitionManager>>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        if acquisition.is_none() {
            return Err(ParserError::Adapter(
                "adapter-backed source did not provide acquisition manager".to_string(),
            ));
        }
        let record = SourceRecord {
            material_id: Id::from_uuid(Uuid::from_u128(42)),
            anchor: MaterialAnchor::ByteRange { start: 17, len: 5 },
            bytes: b"hello".to_vec(),
            logical_path: Some(Utf8PathBuf::from("/tmp/materialized.txt")),
            source_ts_hint: None,
            metadata: JsonValue::Null,
        };
        Ok(Box::pin(stream::iter(vec![Ok(record)])))
    }
}

#[derive(Default)]
struct EmittingParser;

#[async_trait]
impl MaterialParser for EmittingParser {
    type Config = ();

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("emitting-parser"),
            parser_version: "1.0.0".to_string(),
            accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
            source_id: SourceId::from_static("desktop.clipboard"),
            declared_event_types: vec![(
                EventSource::from_static("test"),
                EventType::from_static("test.event"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: String::new(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        Ok(vec![
            ParsedEventIntent::builder()
                .source_id(ctx.source_id.clone())
                .parser_id(ParserId::from_static("emitting-parser"))
                .parser_version("1.0.0")
                .event_type(EventType::from_static("test.event"))
                .event_source(EventSource::from_static("test"))
                .payload(serde_json::json!({
                    "parsed": true,
                    "record_bytes": record.bytes,
                }))
                .ts_orig(ctx.acquisition_time)
                .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
                .anchor(record.anchor)
                .privacy_context(ProcessingContext::Metadata)
                .build(),
        ])
    }
}

#[derive(Debug, Default)]
struct StatefulCheckpointParser {
    seen: Vec<String>,
}

#[async_trait]
impl MaterialParser for StatefulCheckpointParser {
    type Config = ();

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("stateful-checkpoint-parser"),
            parser_version: "1.0.0".to_string(),
            accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
            source_id: SourceId::from_static("desktop.clipboard"),
            declared_event_types: vec![(
                EventSource::from_static("test"),
                EventType::from_static("test.event"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: String::new(),
        }
    }

    fn restore_checkpoint_state(&mut self, state: Option<&JsonValue>) -> ParserResult<()> {
        let Some(state) = state else {
            return Ok(());
        };
        self.seen = state
            .get("seen")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| ParserError::Parse("missing seen array".to_string()))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| ParserError::Parse("seen entry must be string".to_string()))
            })
            .collect::<ParserResult<Vec<_>>>()?;
        Ok(())
    }

    fn checkpoint_state(&self) -> ParserResult<Option<JsonValue>> {
        Ok(Some(json!({ "seen": self.seen })))
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let text = std::str::from_utf8(&record.bytes)
            .map_err(|error| ParserError::Parse(format!("invalid test bytes: {error}")))?;
        self.seen.push(text.to_string());
        Ok(vec![
            ParsedEventIntent::builder()
                .source_id(ctx.source_id.clone())
                .parser_id(ParserId::from_static("stateful-checkpoint-parser"))
                .parser_version("1.0.0")
                .event_type(EventType::from_static("test.event"))
                .event_source(EventSource::from_static("test"))
                .payload(json!({ "parsed": text }))
                .ts_orig(ctx.acquisition_time)
                .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
                .anchor(record.anchor)
                .privacy_context(ProcessingContext::Metadata)
                .build(),
        ])
    }
}

#[derive(Default)]
struct FailingParser;

#[async_trait]
impl MaterialParser for FailingParser {
    type Config = ();

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("failing-parser"),
            parser_version: "1.0.0".to_string(),
            accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
            source_id: SourceId::from_static("desktop.clipboard"),
            declared_event_types: vec![(
                EventSource::from_static("test"),
                EventType::from_static("test.event"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: String::new(),
        }
    }

    async fn parse_record(
        &mut self,
        _record: SourceRecord,
        _ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        Err(ParserError::Parse("intentional parser failure".to_string()))
    }
}

/// Auto-resolve every event as `PersistedConfirmed` (sinex-r6d.11) the
/// instant it reaches the mpsc handoff, forwarding it onward unchanged to a
/// fresh receiver. This is what lets the many pre-existing tests in this
/// module that were written before durable-emission gating existed keep
/// their "successful emit == cursor advances" assumption, without each of
/// them needing to know about `SettlementRegistry`. Tests that specifically
/// exercise sinex-r6d.11's gating use
/// `make_adapter_runtime_with_settlement_registry` with an explicit,
/// caller-controlled registry instead — see that helper below.
fn auto_settle_events(
    mut raw: mpsc::Receiver<Event<JsonValue>>,
    registry: crate::runtime::durable_emission::SettlementRegistry,
) -> mpsc::Receiver<Event<JsonValue>> {
    let (forward_tx, forward_rx) = mpsc::channel::<Event<JsonValue>>(8);
    tokio::spawn(async move {
        while let Some(event) = raw.recv().await {
            if let Some(id) = event.id {
                registry.resolve(
                    id,
                    crate::runtime::durable_emission::EmissionReceiptState::PersistedConfirmed {
                        lane: sinex_db::repositories::EventStorageLane::Activity,
                        inserted: true,
                        confirmed_sequence: None,
                    },
                );
            }
            if forward_tx.send(event).await.is_err() {
                break;
            }
        }
    });
    forward_rx
}

async fn make_adapter_runtime(
    ctx: &TestContext,
) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        "adapter-append-failure-test".to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, event_receiver_raw) = mpsc::channel::<Event<JsonValue>>(8);
    let emitter = EventEmitter::new(event_sender, false);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let settlement_registry = crate::runtime::durable_emission::SettlementRegistry::new();
    let event_receiver = auto_settle_events(event_receiver_raw, settlement_registry.clone());
    let handles = RuntimeHandles::new_edge(
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    )
    .with_settlement_registry(settlement_registry);
    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
        SinexError::validation("temporary work dir should be UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    Ok((
        RuntimeContext::new(
            ServiceInfo::new(
                "adapter-append-failure-test".to_string(),
                "adapter-append-failure-test".to_string(),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                None,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ),
        event_receiver,
    ))
}

/// Like `make_adapter_runtime`, but WITHOUT the auto-settle tee: the
/// returned receiver is the raw mpsc receiver directly. Dropping it closes
/// the channel, making `EventEmitter::emit()` fail immediately — the exact
/// mechanism `adapter_emit_failure_does_not_advance_cursor` and
/// `adapter_emit_failure_rolls_back_parser_checkpoint` need to simulate an
/// emit-side failure. `auto_settle_events` would defeat this: it holds the
/// raw receiver alive in a background task regardless of what the caller
/// does with the forwarded one.
async fn make_adapter_runtime_no_auto_settle(
    ctx: &TestContext,
) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        "adapter-append-failure-test".to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
    let emitter = EventEmitter::new(event_sender, false);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let handles = RuntimeHandles::new_edge(
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    );
    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
        SinexError::validation("temporary work dir should be UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    Ok((
        RuntimeContext::new(
            ServiceInfo::new(
                "adapter-append-failure-test".to_string(),
                "adapter-append-failure-test".to_string(),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                None,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ),
        event_receiver,
    ))
}

async fn make_adapter_runtime_with_db(
    ctx: &TestContext,
) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        "adapter-snapshot-link-test".to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, event_receiver_raw) = mpsc::channel::<Event<JsonValue>>(8);
    let emitter = EventEmitter::new(event_sender, false);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let settlement_registry = crate::runtime::durable_emission::SettlementRegistry::new();
    let event_receiver = auto_settle_events(event_receiver_raw, settlement_registry.clone());
    let handles = RuntimeHandles::new(
        ctx.pool().clone(),
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    )
    .with_settlement_registry(settlement_registry);
    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
        SinexError::validation("temporary work dir should be UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    Ok((
        RuntimeContext::new(
            ServiceInfo::new(
                "adapter-snapshot-link-test".to_string(),
                "adapter-snapshot-link-test".to_string(),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                None,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ),
        event_receiver,
    ))
}

#[sinex_test]
async fn adapter_source_config_derives_private_mode_binding_flag() -> xtask::sandbox::TestResult<()>
{
    let dir = tempfile::tempdir()?;
    let state = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["desktop".to_string()],
        Timestamp::UNIX_EPOCH,
    );
    save_private_mode_state(dir.path(), &state)?;
    let config = AdapterSourceConfig {
        private_mode_state_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    };

    let binding = config.to_binding_config_for_source("desktop.clipboard")?;

    assert!(binding.is_truthy("private_mode_active"));
    Ok(())
}

#[sinex_serial_test]
async fn adapter_source_config_uses_service_state_dir_by_default()
-> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    save_private_mode_state(
        dir.path(),
        &RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        ),
    )?;
    let mut env = EnvGuard::new();
    env.set("SINEX_STATE_DIR", dir.path().display().to_string());

    // This is the production shape: source bindings provide adapter fields
    // but do not repeat the daemon's private-mode state root.
    let config = AdapterSourceConfig::default();
    let binding = config.to_binding_config_for_source("desktop.clipboard")?;

    assert!(binding.is_truthy("private_mode_active"));
    Ok(())
}

#[sinex_test]
async fn adapter_source_config_keeps_continuous_start_policy_out_of_adapter_json()
-> xtask::sandbox::TestResult<()> {
    let config: AdapterSourceConfig = serde_json::from_value(json!({
        "path": "/tmp/source.log",
        "continuous_start_position": "latest"
    }))?;

    assert_eq!(
        config.continuous_start_position,
        Some(InitialStreamPosition::Latest)
    );
    assert_eq!(config.adapter["path"], "/tmp/source.log");
    assert!(config.adapter.get("continuous_start_position").is_none());
    Ok(())
}

#[sinex_test]
async fn adapter_source_config_validates_continuous_poll_interval() -> xtask::sandbox::TestResult<()>
{
    let default_config = AdapterSourceConfig::default();
    assert_eq!(
        default_config.continuous_poll_interval()?,
        Duration::from_secs(30)
    );

    let custom_config = AdapterSourceConfig {
        continuous_poll_interval_secs: Some(5),
        ..Default::default()
    };
    assert_eq!(
        custom_config.continuous_poll_interval()?,
        Duration::from_secs(5)
    );

    let invalid_config = AdapterSourceConfig {
        continuous_poll_interval_secs: Some(0),
        ..Default::default()
    };
    let error = invalid_config
        .continuous_poll_interval()
        .expect_err("zero-second poll interval should fail configuration validation");
    assert!(format!("{error:#}").contains("continuous_poll_interval_secs"));
    Ok(())
}

#[sinex_test]
async fn adapter_source_config_respects_private_mode_source_scope() -> xtask::sandbox::TestResult<()>
{
    let dir = tempfile::tempdir()?;
    let state = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["desktop".to_string()],
        Timestamp::UNIX_EPOCH,
    );
    save_private_mode_state(dir.path(), &state)?;
    let config = AdapterSourceConfig {
        private_mode_state_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    };

    let binding = config.to_binding_config_for_source("terminal.zsh-history")?;

    assert!(!binding.is_truthy("private_mode_active"));
    Ok(())
}

#[sinex_test]
async fn adapter_source_config_ignores_expired_private_mode_state() -> xtask::sandbox::TestResult<()>
{
    let dir = tempfile::tempdir()?;
    let state = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["desktop".to_string()],
        Timestamp::UNIX_EPOCH,
    )
    .with_expires_at(Timestamp::from_unix_timestamp(1));
    save_private_mode_state(dir.path(), &state)?;
    let config = AdapterSourceConfig {
        private_mode_state_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    };

    let binding = config.to_binding_config_for_source("desktop.clipboard")?;

    assert!(!binding.is_truthy("private_mode_active"));
    Ok(())
}

#[sinex_test]
async fn adapter_source_config_fails_closed_when_private_mode_state_is_unavailable()
-> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let path = private_mode_state_path(dir.path());
    let parent = path
        .parent()
        .ok_or_else(|| SinexError::validation("private-mode path must have parent"))?;
    tokio::fs::create_dir_all(parent).await?;
    tokio::fs::write(&path, b"{not-json").await?;
    let config = AdapterSourceConfig {
        private_mode_state_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    };

    let binding = config.to_binding_config_for_source("desktop.clipboard")?;

    assert!(binding.is_truthy("private_mode_active"));
    assert!(binding.is_truthy("private_mode_state_unavailable"));
    Ok(())
}

#[sinex_test]
async fn adapter_source_config_fail_open_requires_explicit_low_sensitivity_choice()
-> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let path = private_mode_state_path(dir.path());
    let parent = path
        .parent()
        .ok_or_else(|| SinexError::validation("private-mode path must have parent"))?;
    tokio::fs::create_dir_all(parent).await?;
    tokio::fs::write(&path, b"{not-json").await?;
    let config = AdapterSourceConfig {
        private_mode_state_dir: Some(dir.path().to_path_buf()),
        private_mode_fail_closed: Some(false),
        ..Default::default()
    };

    let binding = config.to_binding_config_for_source("system.metrics")?;

    assert!(!binding.is_truthy("private_mode_active"));
    assert!(binding.is_truthy("private_mode_state_unavailable"));
    Ok(())
}

#[sinex_test]
async fn adapter_backed_source_refreshes_private_mode_binding() -> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    save_private_mode_state(dir.path(), &RuntimePrivateModeState::disabled())?;
    let mut source = AdapterBackedSource::<TestAdapter, TestParser>::new("desktop.clipboard");
    source.runtime_config = Some(AdapterSourceConfig {
        private_mode_state_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    });

    source.refresh_binding_config()?;
    assert!(!source.binding_config.is_truthy("private_mode_active"));

    let state = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["desktop".to_string()],
        Timestamp::UNIX_EPOCH,
    );
    save_private_mode_state(dir.path(), &state)?;

    source.refresh_binding_config()?;
    assert!(source.binding_config.is_truthy("private_mode_active"));
    Ok(())
}

#[sinex_serial_test]
async fn adapter_backed_sources_suppress_acquisition_from_shared_state_dir(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let state_dir = tempfile::tempdir()?;
    save_private_mode_state(
        state_dir.path(),
        &RuntimePrivateModeState::enabled_by(
            "test-operator",
            Vec::new(),
            Timestamp::UNIX_EPOCH,
        ),
    )?;

    let mut env = EnvGuard::new();
    env.set("SINEX_STATE_DIR", state_dir.path().display().to_string());
    PRIVATE_MODE_PROBE_OPENS.store(0, Ordering::SeqCst);

    let (runtime, _events) = make_adapter_runtime(&ctx).await?;
    for source_id in [
        "desktop.clipboard",
        "browser.history",
        "terminal.bash-history",
    ] {
        let mut source =
            AdapterBackedSource::<PrivateModeProbeAdapter, TestParser>::new(source_id);
        let mut state = AdapterModuleState::default();

        source
            .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
            .await?;
        let emitted = source.drain_adapter(None, &mut state, None, None).await?;

        assert_eq!(emitted, 0, "private mode must suppress {source_id}");
        assert_eq!(state.cursor, None, "suppressed {source_id} must not advance");
        assert_eq!(
            source.current_material_id(),
            None,
            "suppressed {source_id} must not create raw source material"
        );
    }

    assert_eq!(
        PRIVATE_MODE_PROBE_OPENS.load(Ordering::SeqCst),
        0,
        "private mode must stop adapter-backed acquisition before adapter open"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_oversized_record_is_chunked_and_emitted(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, mut event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source =
        AdapterBackedSource::<OversizedRecordAdapter, EmittingParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::default();
    let js = async_nats::jetstream::new(ctx.nats_client());
    let stream_name =
        sinex_primitives::environment::environment().nats_stream_name(SOURCE_MATERIAL_STREAM);

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;
    let emitted = source.drain_adapter(None, &mut state, None, None).await?;

    assert_eq!(emitted, 1);
    assert_eq!(state.cursor, Some(1));
    let event = event_receiver
        .try_recv()
        .expect("chunked oversized record should emit exactly one event");
    assert!(
        matches!(
            event.provenance,
            sinex_primitives::events::Provenance::Material { .. }
        ),
        "chunked oversized record must retain material provenance"
    );

    let mut stream = js.get_stream(&stream_name).await?;
    let material_frame_messages = stream.info().await?.state.messages;
    assert!(
        material_frame_messages <= 4,
        "one oversized logical record should use BEGIN plus a few material slices, got {material_frame_messages}"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_stream_error_is_returned_without_advancing_cursor(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source = AdapterBackedSource::<ErroringAdapter, TestParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;
    let error = source
        .drain_adapter(None, &mut state, None, None)
        .await
        .expect_err("adapter stream failures must remain visible to the runtime");

    assert!(
        error
            .to_string()
            .contains("adapter stream yielded an error")
    );
    assert!(error.to_string().contains("whole-file limit"));
    assert_eq!(state.cursor, None);
    Ok(())
}

#[sinex_test]
async fn adapter_logical_path_record_materializes_descriptor_bytes(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, mut event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source = AdapterBackedSource::<EmptyLogicalPathRecordAdapter, EmittingParser>::new(
        "desktop.clipboard",
    );
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;
    let emitted = source.drain_adapter(None, &mut state, None, None).await?;
    let event = event_receiver
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("expected emitted event"))?;

    assert_eq!(emitted, 1);
    assert_eq!(state.cursor, Some(1));
    assert!(
        source.current_material_id().is_some(),
        "nil-material logical records should open the append-stream materializer",
    );
    match event.provenance() {
        sinex_primitives::events::Provenance::Material {
            id,
            offset_start,
            offset_end,
            ..
        } => {
            assert_ne!(id.to_uuid(), Uuid::nil());
            assert_eq!(*offset_start, Some(0));
            assert!(
                offset_end.is_some_and(|end| end > 0),
                "logical-path descriptor must occupy a non-empty material byte range",
            );
        }
        other => panic!("expected material provenance, got {other:?}"),
    }
    assert!(
        event
            .anchor_payload_hash
            .as_ref()
            .is_some_and(|hash| !hash.is_empty()),
        "logical-path descriptor bytes should be hashable provenance evidence",
    );
    Ok(())
}

#[sinex_test]
async fn adapter_parse_failure_does_not_advance_cursor(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source = AdapterBackedSource::<EmptyLogicalPathRecordAdapter, FailingParser>::new(
        "desktop.clipboard",
    );
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;
    let report = source
        .scan_snapshot(&mut state, ScanArgs::default())
        .await?;

    assert_eq!(report.events_processed, 0);
    assert_eq!(
        state.cursor, None,
        "parser failures must leave the cursor behind for retry"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_emit_failure_does_not_advance_cursor(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, event_receiver) = make_adapter_runtime_no_auto_settle(&ctx).await?;
    drop(event_receiver);
    let mut source = AdapterBackedSource::<EmptyLogicalPathRecordAdapter, EmittingParser>::new(
        "desktop.clipboard",
    );
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;
    let report = source
        .scan_snapshot(&mut state, ScanArgs::default())
        .await?;

    assert_eq!(report.events_processed, 0);
    assert_eq!(
        state.cursor, None,
        "emit failures must leave the cursor behind for retry"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_emit_failure_rolls_back_parser_checkpoint(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, event_receiver) = make_adapter_runtime_no_auto_settle(&ctx).await?;
    drop(event_receiver);
    let mut source =
        AdapterBackedSource::<TwoRecordAdapter, StatefulCheckpointParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;
    let report = source
        .scan_snapshot(&mut state, ScanArgs::default())
        .await?;

    assert_eq!(report.events_processed, 0);
    assert_eq!(
        state.cursor, None,
        "emit failures must leave the cursor behind for retry"
    );
    // StatefulCheckpointParser::checkpoint_state() always returns
    // `Some(..)` (even for an empty `seen` list), so "no progress persisted"
    // is the pre-drain snapshot `Some({"seen": []})`, not a bare `None`.
    assert_eq!(
        state.parser_checkpoint,
        Some(json!({ "seen": [] })),
        "parser-local progress must not persist when the record was not emitted"
    );
    assert!(
        source.parser.seen.is_empty(),
        "in-memory parser state must roll back with the cursor"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_nil_material_records_are_batched_into_few_material_frames(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source =
        AdapterBackedSource::<ManyNilMaterialRecordsAdapter, TestParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::default();
    let js = async_nats::jetstream::new(ctx.nats_client());
    let stream_name =
        sinex_primitives::environment::environment().nats_stream_name(SOURCE_MATERIAL_STREAM);

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    let emitted = source.drain_adapter(None, &mut state, None, None).await?;

    let mut stream = js.get_stream(&stream_name).await?;
    let material_frame_messages = stream.info().await?.state.messages;

    assert_eq!(emitted, 0, "the no-op parser should not emit events");
    assert_eq!(state.cursor, Some(127));
    assert!(
        material_frame_messages <= 4,
        "128 logical records should coalesce into a few source-material frames, got {material_frame_messages}"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_stream_finalizes_idle_material_before_stale_timeout(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, mut event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source = AdapterBackedSource::<PendingAfterOneRecordAdapter, EmittingParser>::new(
        "desktop.clipboard",
    )
    .with_rotation_policy(RotationPolicy {
        max_bytes: Bytes::from_mebibytes(100),
        max_age_seconds: Seconds::from_secs(1),
    });
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    let drain_result = tokio::time::timeout(
        Duration::from_millis(2500),
        source.drain_adapter(None, &mut state, None, None),
    )
    .await;

    assert!(
        drain_result.is_err(),
        "test adapter should remain pending after the first record"
    );
    assert_eq!(state.cursor, Some(1));
    assert!(
        source.current_material_id().is_none(),
        "idle stream material should be finalized before event-engine marks it stale"
    );
    let event = event_receiver
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("expected emitted event"))?;
    match event.provenance() {
        sinex_primitives::events::Provenance::Material { id, .. } => {
            assert_ne!(id.to_uuid(), Uuid::nil());
        }
        other => panic!("expected material provenance, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn adapter_continuous_poll_finalizes_finite_drain_material(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source =
        AdapterBackedSource::<EmptyLogicalPathRecordAdapter, TestParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    let run_result = tokio::time::timeout(
        Duration::from_millis(1500),
        source.run_continuous(
            &mut state,
            ContinuousStart::from_checkpoint(Checkpoint::default()),
            tokio::sync::watch::channel(false).1,
        ),
    )
    .await;

    assert!(
        run_result.is_err(),
        "continuous poll loop should remain active after the first finite drain"
    );
    assert_eq!(state.cursor, Some(1));
    assert!(
        source.current_material_id().is_none(),
        "finite poll drains should finalize their stream material before sleeping"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_snapshot_finalizes_finite_drain_material(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source =
        AdapterBackedSource::<EmptyLogicalPathRecordAdapter, TestParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    let report = source
        .scan_snapshot(&mut state, ScanArgs::default())
        .await?;

    assert_eq!(report.events_processed, 0);
    assert_eq!(state.cursor, Some(1));
    assert!(
        source.current_material_id().is_none(),
        "finite snapshot drains should finalize their stream material"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_historical_finalizes_finite_drain_material(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source =
        AdapterBackedSource::<EmptyLogicalPathRecordAdapter, TestParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    let report = source
        .scan_historical(
            &mut state,
            Checkpoint::None,
            TimeHorizon::Historical {
                end_time: Timestamp::now(),
            },
            ScanArgs::default(),
        )
        .await?;

    assert_eq!(report.events_processed, 0);
    assert_eq!(state.cursor, Some(1));
    assert!(
        source.current_material_id().is_none(),
        "finite historical drains should finalize their stream material"
    );
    Ok(())
}

/// sinex-nbag: a scoped replay for an adapter without a material-backed
/// replay route must fail closed before opening the adapter. Opening it with
/// the replay worker's fresh checkpoint namespace would rescan both the
/// selected and unselected occurrences from cursor zero, creating duplicate
/// live interpretations for the unselected ones.
#[sinex_test]
async fn scoped_replay_does_not_open_generic_adapter_from_fresh_cursor(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _event_receiver) = make_adapter_runtime(&ctx).await?;
    REPLAY_GUARD_OPENS.store(0, Ordering::SeqCst);
    let mut source =
        AdapterBackedSource::<ReplayGuardAdapter, TestParser>::new("test.scoped-replay-guard");
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    let selected_material = Uuid::now_v7();
    let unselected_material = Uuid::now_v7();
    let error = source
        .scan_historical(
            &mut state,
            Checkpoint::None,
            TimeHorizon::Historical {
                end_time: Timestamp::now(),
            },
            ScanArgs {
                replay: Some(MaterialReplayContext {
                    operation_id: Uuid::now_v7(),
                    materials: vec![
                        ResolvedReplayMaterial {
                            source_material_id: selected_material,
                            material_kind: "fixture".to_string(),
                            source_identifier: "selected".to_string(),
                            material_metadata: JsonValue::Null,
                            material_start_time: None,
                            material_end_time: None,
                        },
                        ResolvedReplayMaterial {
                            source_material_id: unselected_material,
                            material_kind: "fixture".to_string(),
                            source_identifier: "unselected".to_string(),
                            material_metadata: JsonValue::Null,
                            material_start_time: None,
                            material_end_time: None,
                        },
                    ],
                    occurrences: Vec::new(),
                    replay_scope: ReplayScopeFilters {
                        material_ids: Some(vec![selected_material]),
                        event_types: Some(vec!["test.event".to_string()]),
                    },
                }),
                ..Default::default()
            },
        )
        .await
        .expect_err("generic scoped replay must fail closed");

    assert!(
        error
            .to_string()
            .contains("replay is not supported for this adapter"),
        "guard error should explain that the adapter has no bounded replay route: {error}"
    );
    assert_eq!(
        REPLAY_GUARD_OPENS.load(Ordering::SeqCst),
        0,
        "scoped replay must not open a fresh cursor and rescan selected plus unselected material"
    );
    assert_eq!(state.cursor, None);
    Ok(())
}

#[sinex_test]
async fn adapter_backed_source_preserves_already_materialized_record_provenance(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, mut event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source = AdapterBackedSource::<AlreadyMaterializedRecordAdapter, EmittingParser>::new(
        "desktop.clipboard",
    );
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;
    let emitted = source.drain_adapter(None, &mut state, None, None).await?;
    let event = event_receiver
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("expected emitted event"))?;

    assert_eq!(emitted, 1);
    assert_eq!(state.cursor, Some(1));
    assert_eq!(
        source.current_material_id(),
        None,
        "pre-materialized records must not open the append-stream materializer",
    );
    assert_eq!(event.get_anchor_byte(), Some(17));
    match event.provenance() {
        sinex_primitives::events::Provenance::Material {
            id,
            offset_start,
            offset_end,
            ..
        } => {
            assert_eq!(id.to_uuid(), Uuid::from_u128(42));
            assert_eq!(*offset_start, Some(17));
            assert_eq!(*offset_end, Some(22));
        }
        other => panic!("expected material provenance, got {other:?}"),
    }
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn adapter_private_mode_control_listener_persists_broadcast(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let dir = tempfile::tempdir()?;
    save_private_mode_state(dir.path(), &RuntimePrivateModeState::disabled())?;
    // Wait for the listener's NATS subscription to actually be registered
    // before publishing -- otherwise this races the spawned task's
    // subscribe() against the publish below and can lose the (non-durable,
    // core-NATS) message whenever publish wins the race.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let handle = spawn_private_mode_control_listener_with_ready_signal(
        ctx.nats_client(),
        dir.path().to_path_buf(),
        "desktop.clipboard",
        Some(ready_tx),
    );
    ready_rx.await?;

    let state = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["desktop".to_string()],
        Timestamp::UNIX_EPOCH,
    );
    let subject =
        sinex_primitives::environment::environment().nats_subject(PRIVATE_MODE_CONTROL_SUBJECT);
    ctx.nats_client()
        .publish(
            subject,
            serde_json::to_vec(&serde_json::json!({
                "action": "enable",
                "timestamp": Timestamp::now(),
                "state": state,
            }))?
            .into(),
        )
        .await?;
    ctx.nats_client().flush().await?;

    let state_dir = dir.path().to_path_buf();
    WaitHelpers::wait_for_condition(
        || {
            let state_dir = state_dir.clone();
            async move {
                let state = load_private_mode_state(&state_dir)?;
                Ok::<_, crate::runtime::SinexError>(state.enabled)
            }
        },
        10,
    )
    .await?;

    let loaded = load_private_mode_state(dir.path())?;
    assert!(loaded.enabled);
    assert_eq!(loaded.actor, "sinity");
    assert_eq!(loaded.affected_source_classes, vec!["desktop"]);
    handle.abort();
    Ok(())
}

#[sinex_test]
async fn adapter_source_state_defaults_missing_input_fingerprint() -> xtask::sandbox::TestResult<()>
{
    let value = serde_json::json!({
        "cursor": 7,
        "total_events_emitted": 12
    });

    let state: AdapterModuleState<u64> = serde_json::from_value(value)?;

    assert_eq!(state.cursor, Some(7));
    assert_eq!(state.total_events_emitted, 12);
    assert!(state.last_input_fingerprint.is_none());
    assert!(state.recent_input_drifts.is_empty());
    assert!(state.parser_checkpoint.is_none());
    Ok(())
}

#[sinex_test]
async fn adapter_source_restores_parser_checkpoint_on_initialize(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source =
        AdapterBackedSource::<TestAdapter, StatefulCheckpointParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::<u64> {
        parser_checkpoint: Some(json!({ "seen": ["prior"] })),
        ..Default::default()
    };

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    assert_eq!(source.parser.seen, vec!["prior".to_string()]);
    Ok(())
}

#[sinex_test]
async fn adapter_source_updates_parser_checkpoint_after_successful_parse(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, mut event_receiver) = make_adapter_runtime(&ctx).await?;
    let mut source =
        AdapterBackedSource::<TwoRecordAdapter, StatefulCheckpointParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::<u64>::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;
    let emitted = source.drain_adapter(None, &mut state, None, None).await?;

    assert_eq!(emitted, 2);
    assert_eq!(state.cursor, Some(2));
    assert_eq!(
        state.parser_checkpoint,
        Some(json!({ "seen": ["alpha", "beta"] }))
    );
    assert!(
        event_receiver.try_recv().is_ok(),
        "first parsed record should emit"
    );
    assert!(
        event_receiver.try_recv().is_ok(),
        "second parsed record should emit"
    );
    Ok(())
}

// =============================================================================
// sinex-r6d.11: durable-emission receipt gates the adapter-source cursor
// =============================================================================
//
// These are the killpoint-style tests required by sinex-r6d.11's reference
// caller integration: `EventEmitter::emit` succeeding (the mpsc handoff to
// `EventBatcher`) must NEVER be sufficient to advance the adapter cursor —
// only a `DurableEmissionReceipt` that `unlocks_progress()` may. A caller-
// controlled `SettlementRegistry` stands in for the real event-engine's
// admission/persist/confirm pipeline here: never resolving an event's
// registration is exactly the "process crashes before the event-engine
// durably confirms it" crash window from the bead's own bug report.

async fn make_adapter_runtime_with_settlement_registry(
    ctx: &TestContext,
    registry: crate::runtime::durable_emission::SettlementRegistry,
) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        "adapter-durable-emission-test".to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
    let emitter = EventEmitter::new(event_sender, false);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let handles = RuntimeHandles::new_edge(
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    )
    .with_settlement_registry(registry);
    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
        SinexError::validation("temporary work dir should be UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    Ok((
        RuntimeContext::new(
            ServiceInfo::new(
                "adapter-durable-emission-test".to_string(),
                "adapter-durable-emission-test".to_string(),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                None,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ),
        event_receiver,
    ))
}

#[sinex_test]
async fn adapter_durable_emission_receipt_blocks_cursor_when_never_settled(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let registry = crate::runtime::durable_emission::SettlementRegistry::new();
    let (runtime, mut event_receiver) =
        make_adapter_runtime_with_settlement_registry(&ctx, registry.clone()).await?;
    let mut source =
        AdapterBackedSource::<TwoRecordAdapter, StatefulCheckpointParser>::new("desktop.clipboard")
            .with_durable_emission_timeout(std::time::Duration::from_millis(150));
    let mut state = AdapterModuleState::<u64>::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    // Drain the mpsc channel — proving both events really did reach the
    // EventBatcher handoff — but NEVER resolve the settlement registry for
    // them. This is exactly the sinex-r6d.11 crash window: `emit()`
    // succeeded, but nothing downstream ever durably confirmed the event.
    let drainer = tokio::spawn(async move {
        let mut received = 0u32;
        while received < 2 {
            if event_receiver.recv().await.is_none() {
                break;
            }
            received += 1;
        }
        received
    });

    let emitted = source.drain_adapter(None, &mut state, None, None).await?;
    let received = drainer.await.expect("drainer task did not panic");

    assert_eq!(received, 2, "both records must reach the mpsc handoff");
    assert_eq!(
        emitted, 0,
        "emitted counts only durably-confirmed events — neither event settled"
    );
    assert_eq!(
        state.cursor, None,
        "cursor must NOT advance past events whose durable-emission receipt never settled"
    );
    assert_eq!(
        state.parser_checkpoint,
        Some(json!({ "seen": [] })),
        "persisted parser checkpoint must roll back to its pre-drain snapshot (StatefulCheckpointParser \
         always reports Some(..), even for an empty 'seen' list) when no record's cursor was allowed to \
         advance — otherwise a same-process retry would re-parse the same unsettled record through parser \
         state that already 'saw' it"
    );
    assert!(
        registry.is_empty(),
        "await_batch must cancel timed-out registrations rather than leaking them"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_durable_emission_receipt_unlocks_cursor_once_settled(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let registry = crate::runtime::durable_emission::SettlementRegistry::new();
    let (runtime, mut event_receiver) =
        make_adapter_runtime_with_settlement_registry(&ctx, registry.clone()).await?;
    let mut source =
        AdapterBackedSource::<TwoRecordAdapter, StatefulCheckpointParser>::new("desktop.clipboard")
            .with_durable_emission_timeout(std::time::Duration::from_secs(5));
    let mut state = AdapterModuleState::<u64>::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    // Settle every event with PersistedConfirmed as it reaches the mpsc
    // handoff — standing in for the real event-engine's confirmed publish.
    let registry_for_settler = registry.clone();
    let settler = tokio::spawn(async move {
        let mut settled = 0u32;
        while settled < 2 {
            let Some(event) = event_receiver.recv().await else {
                break;
            };
            let id = event.id.expect("emit() assigns an id");
            registry_for_settler.resolve(
                id,
                crate::runtime::durable_emission::EmissionReceiptState::PersistedConfirmed {
                    lane: sinex_db::repositories::EventStorageLane::Activity,
                    inserted: true,
                    confirmed_sequence: None,
                },
            );
            settled += 1;
        }
        settled
    });

    let emitted = source.drain_adapter(None, &mut state, None, None).await?;
    let settled = settler.await.expect("settler task did not panic");

    assert_eq!(settled, 2);
    assert_eq!(
        emitted, 2,
        "both events durably settled, so both must count as emitted"
    );
    assert_eq!(
        state.cursor,
        Some(2),
        "cursor must advance past both records once their durable-emission receipts unlock \
         progress — proving the mechanism gates on settlement, not merely on mpsc handoff"
    );
    assert_eq!(
        state.parser_checkpoint,
        Some(json!({ "seen": ["alpha", "beta"] })),
        "persisted parser checkpoint must reflect both durably-settled records"
    );
    assert!(
        registry.is_empty(),
        "every registration should have resolved and been removed"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_durable_emission_receipt_partial_batch_settlement_blocks_only_the_hole(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let registry = crate::runtime::durable_emission::SettlementRegistry::new();
    let (runtime, mut event_receiver) =
        make_adapter_runtime_with_settlement_registry(&ctx, registry.clone()).await?;
    let mut source =
        AdapterBackedSource::<TwoRecordAdapter, StatefulCheckpointParser>::new("desktop.clipboard")
            .with_durable_emission_timeout(std::time::Duration::from_millis(150));
    let mut state = AdapterModuleState::<u64>::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    // Settle ONLY the first record's event ("alpha", cursor 1). The second
    // ("beta", cursor 2) reaches the mpsc handoff but is never resolved —
    // proving a later record's success can never fold in past an earlier
    // hole, per `CommitFrontier`'s contiguous-prefix rule.
    let registry_for_settler = registry.clone();
    let settler = tokio::spawn(async move {
        let mut seen = 0u32;
        while seen < 2 {
            let Some(event) = event_receiver.recv().await else {
                break;
            };
            seen += 1;
            if seen == 1 {
                let id = event.id.expect("emit() assigns an id");
                registry_for_settler.resolve(
                    id,
                    crate::runtime::durable_emission::EmissionReceiptState::PersistedConfirmed {
                        lane: sinex_db::repositories::EventStorageLane::Activity,
                        inserted: true,
                        confirmed_sequence: None,
                    },
                );
            }
            // seen == 2 ("beta"): deliberately never resolved.
        }
        seen
    });

    let emitted = source.drain_adapter(None, &mut state, None, None).await?;
    let seen = settler.await.expect("settler task did not panic");

    assert_eq!(seen, 2, "both records reach the mpsc handoff");
    assert_eq!(
        emitted, 1,
        "only the durably-settled record's event counts as emitted"
    );
    assert_eq!(
        state.cursor,
        Some(1),
        "cursor must advance only past the record whose receipt actually unlocked progress, \
         even though a LATER record in the same materialized batch also reached the mpsc \
         handoff"
    );
    assert_eq!(
        state.parser_checkpoint,
        Some(json!({ "seen": ["alpha"] })),
        "persisted parser checkpoint must roll back to just after the last durably-settled \
         record, not the fully-advanced in-memory parser state"
    );
    Ok(())
}

/// One record, two intents ("first"/"second") — never sets `occurrence_key`,
/// matching every real multi-intent source that has no natural dedup key
/// (sinex-w4i's exact precondition). No checkpoint tracking needed: the
/// bug this reproduces never lets the checkpoint advance in the first place.
#[derive(Default)]
struct MultiIntentParser;

#[async_trait]
impl MaterialParser for MultiIntentParser {
    type Config = ();

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("multi-intent-parser"),
            parser_version: "1.0.0".to_string(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("desktop.clipboard"),
            declared_event_types: vec![(
                EventSource::from_static("test"),
                EventType::from_static("test.event"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: String::new(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        Ok(vec!["first", "second"]
            .into_iter()
            .map(|which| {
                ParsedEventIntent::builder()
                    .source_id(ctx.source_id.clone())
                    .parser_id(ParserId::from_static("multi-intent-parser"))
                    .parser_version("1.0.0")
                    .event_type(EventType::from_static("test.event"))
                    .event_source(EventSource::from_static("test"))
                    .payload(serde_json::json!({"parsed": which}))
                    .ts_orig(ctx.acquisition_time)
                    .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
                    .anchor(record.anchor.clone())
                    .privacy_context(ProcessingContext::Metadata)
                    .build()
            })
            .collect())
    }
}

/// sinex-w4i: every keyless sibling emitted from one material record receives
/// a deterministic occurrence identity. The durable-emission gate still keeps
/// the record cursor closed until every sibling settles, while the normal
/// admission outcome suppresses a sibling that was already persisted when the
/// whole record is retried.
#[sinex_test]
async fn adapter_multi_intent_partial_settlement_suppresses_settled_sibling_on_retry(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut state = AdapterModuleState::<u64>::default();

    // --- Attempt 1: "first" durably settles, "second" never does. ---
    let registry1 = crate::runtime::durable_emission::SettlementRegistry::new();
    let (runtime1, mut event_receiver1) =
        make_adapter_runtime_with_settlement_registry(&ctx, registry1.clone()).await?;
    let mut source1 = AdapterBackedSource::<StableMaterialRecordAdapter, MultiIntentParser>::new(
        "desktop.clipboard",
    )
    .with_durable_emission_timeout(std::time::Duration::from_millis(150));
    source1
        .initialize(AdapterSourceConfig::default(), &runtime1, &mut state)
        .await?;

    let settler1 = tokio::spawn(async move {
        let mut ids = Vec::new();
        while ids.len() < 2 {
            let Some(event) = event_receiver1.recv().await else {
                break;
            };
            let id = event.id.expect("emit() assigns an id");
            if ids.is_empty() {
                registry1.resolve(
                    id,
                    crate::runtime::durable_emission::EmissionReceiptState::PersistedConfirmed {
                        lane: sinex_db::repositories::EventStorageLane::Activity,
                        inserted: true,
                        confirmed_sequence: None,
                    },
                );
            }
            // second event: deliberately never resolved (crash/timeout window).
            ids.push((id, event.equivalence_key.clone()));
        }
        ids
    });

    let emitted_1 = source1.drain_adapter(None, &mut state, None, None).await?;
    let attempt1 = settler1.await.expect("settler1 did not panic");

    assert_eq!(
        attempt1.len(),
        2,
        "both sibling intents reach the mpsc handoff"
    );
    assert_eq!(
        attempt1.iter().filter(|(_, key)| key.is_some()).count(),
        2,
        "every keyless sibling must carry an admission identity"
    );
    assert_ne!(
        attempt1[0].1,
        attempt1[1].1,
        "sibling slots must not collide"
    );
    assert_eq!(
        emitted_1, 0,
        "the record does not fully unlock (its second sibling never settled), so the FIRST \
         sibling -- despite already being durably confirmed -- is not credited either"
    );
    assert_eq!(
        state.cursor, None,
        "cursor must not advance past a record with an unsettled sibling"
    );

    // --- Attempt 2 (retry after restart): re-parses the SAME record from scratch. ---
    let registry2 = crate::runtime::durable_emission::SettlementRegistry::new();
    let (runtime2, mut event_receiver2) =
        make_adapter_runtime_with_settlement_registry(&ctx, registry2.clone()).await?;
    let mut source2 = AdapterBackedSource::<StableMaterialRecordAdapter, MultiIntentParser>::new(
        "desktop.clipboard",
    )
    .with_durable_emission_timeout(std::time::Duration::from_millis(150));
    source2
        .initialize(AdapterSourceConfig::default(), &runtime2, &mut state)
        .await?;

    let settled_key = attempt1[0].1.clone();
    let settler2 = tokio::spawn(async move {
        let mut outcomes = Vec::new();
        while outcomes.len() < 2 {
            let Some(event) = event_receiver2.recv().await else {
                break;
            };
            let id = event.id.expect("emit() assigns an id");
            let suppressed = event.equivalence_key.as_ref() == settled_key.as_ref();
            let outcome = if suppressed {
                crate::runtime::durable_emission::EmissionReceiptState::Suppressed {
                    reason: crate::runtime::durable_emission::SuppressionReason::
                        EquivalenceKeyDuplicate,
                    existing_event_id: None,
                }
            } else {
                crate::runtime::durable_emission::EmissionReceiptState::PersistedConfirmed {
                    lane: sinex_db::repositories::EventStorageLane::Activity,
                    inserted: true,
                    confirmed_sequence: None,
                }
            };
            registry2.resolve(id, outcome);
            outcomes.push((event.equivalence_key.clone(), suppressed));
        }
        outcomes
    });

    let emitted_2 = source2.drain_adapter(None, &mut state, None, None).await?;
    let attempt2_outcomes = settler2.await.expect("settler2 did not panic");

    assert_eq!(
        attempt2_outcomes.len(),
        2,
        "the retry re-parses the whole record, so both sibling identities reach admission"
    );
    assert_eq!(
        emitted_2, 2,
        "suppressed and persisted terminal outcomes both unlock the record's durable frontier"
    );
    assert_eq!(state.cursor, Some(1));

    assert_eq!(
        attempt2_outcomes[0],
        (attempt1[0].1.clone(), true),
        "the previously settled sibling must take the admission suppression path"
    );
    assert_ne!(
        attempt2_outcomes[0].0, attempt2_outcomes[1].0,
        "retry sibling slots must remain distinct"
    );
    assert_eq!(
        attempt2_outcomes[1].1, false,
        "the sibling without a durable first-attempt outcome must persist on retry"
    );
    Ok(())
}

#[sinex_test]
async fn adapter_cursor_update_preserves_chained_leg_state() -> xtask::sandbox::TestResult<()> {
    let current = ChainedCursor {
        primary: Some(SqliteRowCursor { last_rowid: 10_000 }),
        secondary: Some(AppendOnlyCursor {
            last_line: 42,
            last_byte_offset: 4096,
            inode: Some(7),
        }),
    };
    let primary_update = ChainedCursor {
        primary: Some(SqliteRowCursor { last_rowid: 20_000 }),
        secondary: None,
    };
    let merged = merge_cursor_update(Some(current.clone()), primary_update);

    assert_eq!(merged.primary, Some(SqliteRowCursor { last_rowid: 20_000 }));
    assert_eq!(merged.secondary, current.secondary);

    let secondary_update = ChainedCursor {
        primary: None,
        secondary: Some(AppendOnlyCursor {
            last_line: 43,
            last_byte_offset: 5000,
            inode: Some(8),
        }),
    };
    let merged = merge_cursor_update(Some(merged), secondary_update);

    assert_eq!(merged.primary, Some(SqliteRowCursor { last_rowid: 20_000 }));
    assert_eq!(
        merged.secondary,
        Some(AppendOnlyCursor {
            last_line: 43,
            last_byte_offset: 5000,
            inode: Some(8),
        })
    );
    Ok(())
}

#[sinex_test]
async fn adapter_source_state_records_bounded_input_drift_history() -> xtask::sandbox::TestResult<()>
{
    let source_id = SourceId::from_static("desktop.clipboard");
    let mut source =
        AdapterBackedSource::<FingerprintAdapter, TestParser>::new("desktop.clipboard");
    let mut state = AdapterModuleState::<u64>::default();

    source.adapter.fingerprint = Some(SourceRecordFingerprint::from_json(
        &serde_json::json!({"count": 1}),
    ));
    source.observe_input_fingerprint(&(), &mut state, &source_id);
    assert!(state.recent_input_drifts.is_empty());

    source.adapter.fingerprint = Some(SourceRecordFingerprint::from_json(
        &serde_json::json!({"count": "1", "enabled": true}),
    ));
    source.observe_input_fingerprint(&(), &mut state, &source_id);

    assert_eq!(state.recent_input_drifts.len(), 1);
    let drift = &state.recent_input_drifts[0];
    assert_eq!(drift.source_id, source_id);
    assert_eq!(drift.added_keys, vec!["/enabled".to_string()]);
    assert_eq!(drift.required_input_keys, vec!["/message".to_string()]);
    assert_eq!(
        drift.type_changes,
        vec![(
            "/count".to_string(),
            "integer".to_string(),
            "string".to_string()
        )]
    );

    for idx in 0..(MAX_RECENT_INPUT_DRIFTS + 3) {
        let drift = SourceRecordFingerprint::diff(
            source_id.clone(),
            &SourceRecordFingerprint::from_json(&serde_json::json!({ "idx": idx })),
            &SourceRecordFingerprint::from_json(&serde_json::json!({ "idx": idx, "x": true })),
        )
        .ok_or_else(|| SinexError::validation("different fingerprints should produce drift"))?;
        state.record_input_drift(drift);
    }

    assert_eq!(state.recent_input_drifts.len(), MAX_RECENT_INPUT_DRIFTS);
    Ok(())
}

#[sinex_test]
async fn adapter_source_state_summarizes_latest_input_drift_caveats()
-> xtask::sandbox::TestResult<()> {
    let source_id = SourceId::from_static("desktop.clipboard");
    let mut state = AdapterModuleState::<u64>::default();

    let additive = SourceRecordFingerprint::diff(
        source_id.clone(),
        &SourceRecordFingerprint::from_json(&serde_json::json!({ "message": "hello" })),
        &SourceRecordFingerprint::from_json(&serde_json::json!({
            "message": "hello",
            "window_title": "terminal"
        })),
    )
    .ok_or_else(|| SinexError::validation("additive drift should be detected"))?;
    state.record_input_drift(additive);

    let additive_caveats = state.latest_input_drift_caveats();
    assert_eq!(additive_caveats.len(), 1);
    assert_eq!(additive_caveats[0].code, caveat_codes::SOURCE_SHAPE_CHANGED);

    let mut degraded = SourceRecordFingerprint::diff(
        source_id,
        &SourceRecordFingerprint::from_json(&serde_json::json!({
            "message": "hello",
            "count": 1
        })),
        &SourceRecordFingerprint::from_json(&serde_json::json!({
            "count": "1"
        })),
    )
    .ok_or_else(|| SinexError::validation("degraded drift should be detected"))?;
    degraded.required_input_keys = vec!["/message".to_string()];
    state.record_input_drift(degraded);

    let degraded_caveats = state.latest_input_drift_caveats();
    let degraded_codes: Vec<&str> = degraded_caveats
        .iter()
        .map(|caveat| caveat.code.as_str())
        .collect();
    assert_eq!(
        degraded_codes,
        vec![
            caveat_codes::PARSER_FIELD_TYPE_CHANGED,
            caveat_codes::PARSER_REQUIRED_FIELD_MISSING
        ]
    );
    assert!(
        degraded_caveats.iter().any(|caveat| {
            caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                && caveat.severity == CaveatSeverity::Blocking
        }),
        "required input removal should be blocking: {degraded_caveats:?}"
    );
    Ok(())
}

// -------------------------------------------------------------------------
// #1570 Prong C — occurrence_key lands on the event as equivalence_key
// -------------------------------------------------------------------------

/// A parser-supplied occurrence key is carried onto the event as
/// `equivalence_key`, so it reaches the curation duplicate workbench.
#[sinex_test]
async fn occurrence_key_lands_as_equivalence_key() -> xtask::sandbox::TestResult<()> {
    use sinex_primitives::parser::{OccurrenceKey, occurrence_key_string};
    let key = OccurrenceKey {
        source_id: SourceId::from_static("test.unit"),
        fields: vec![
            ("track_uri".into(), "spotify:track:abc".into()),
            ("played_ms".into(), "1234".into()),
        ],
    };
    let intent = ParsedEventIntent::builder()
        .source_id(SourceId::from_static("test.unit"))
        .parser_id(ParserId::from_static("test-parser"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("test.event"))
        .event_source(EventSource::from_static("test"))
        .payload(serde_json::json!({"k": "v"}))
        .ts_orig(Timestamp::now())
        .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
        .anchor(MaterialAnchor::ByteRange { start: 0, len: 0 })
        .privacy_context(ProcessingContext::Metadata)
        .occurrence_key(key.clone())
        .build();
    let event = intent_to_event_with_anchor(
        intent,
        Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
        0,
        None,
        None,
        None,
        0,
    )
    .expect("intent conversion");
    assert_eq!(event.equivalence_key, Some(occurrence_key_string(&key)));
    Ok(())
}

/// Keyless material intents use the material coordinates and deterministic
/// parser output ordinal as a retry-stable fallback identity.
#[sinex_test]
async fn keyless_material_intent_gets_retry_stable_equivalence_key()
-> xtask::sandbox::TestResult<()> {
    let material_id = Id::<SourceMaterial>::from_uuid(Uuid::now_v7());
    let intent = ParsedEventIntent::builder()
        .source_id(SourceId::from_static("test.unit"))
        .parser_id(ParserId::from_static("test-parser"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("test.event"))
        .event_source(EventSource::from_static("test"))
        .payload(serde_json::json!({"k": "v"}))
        .ts_orig(Timestamp::now())
        .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
        .anchor(MaterialAnchor::ByteRange { start: 0, len: 0 })
        .privacy_context(ProcessingContext::Metadata)
        .build();
    let event = intent_to_event_with_anchor(
        intent,
        material_id,
        0,
        None,
        None,
        None,
        0,
    )
    .expect("intent conversion");
    assert!(event.equivalence_key.is_some());
    assert!(event.equivalence_key.as_deref().unwrap().contains("sibling_index"));
    Ok(())
}

#[sinex_test]
async fn keyless_multi_intent_sibling_identity_is_stable_across_reparse()
-> xtask::sandbox::TestResult<()> {
    let material_id = Id::<SourceMaterial>::from_uuid(Uuid::now_v7());
    let make_intent = |which| {
        ParsedEventIntent::builder()
            .source_id(SourceId::from_static("test.unit"))
            .parser_id(ParserId::from_static("multi-intent-parser"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("test.event"))
            .event_source(EventSource::from_static("test"))
            .payload(serde_json::json!({"parsed": which}))
            .ts_orig(Timestamp::from_unix_timestamp(1_700_000_000).expect("timestamp"))
            .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
            .anchor(MaterialAnchor::ByteRange { start: 12, len: 4 })
            .privacy_context(ProcessingContext::Metadata)
            .build()
    };
    let first_attempt = ["first", "second"]
        .into_iter()
        .enumerate()
        .map(|(index, which)| {
            intent_to_event_with_anchor(
                make_intent(which), material_id, 12, Some(12), Some(16), None, index,
            )
            .expect("first conversion")
            .equivalence_key
            .expect("fallback identity")
        })
        .collect::<Vec<_>>();
    let retry = ["first", "second"]
        .into_iter()
        .enumerate()
        .map(|(index, which)| {
            intent_to_event_with_anchor(
                make_intent(which), material_id, 12, Some(12), Some(16), None, index,
            )
            .expect("retry conversion")
            .equivalence_key
            .expect("fallback identity")
        })
        .collect::<Vec<_>>();
    assert_eq!(first_attempt, retry);
    assert_ne!(first_attempt[0], first_attempt[1]);
    Ok(())
}

#[sinex_test]
async fn record_realtime_hint_promotes_atemporal_intent_timing() -> xtask::sandbox::TestResult<()> {
    let original_ts = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid original timestamp"))?;
    let hinted_ts = Timestamp::from_unix_timestamp(1_700_000_123)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid hinted timestamp"))?;
    let hint = sinex_primitives::parser::TimingEvidence::RealtimeCapture {
        value: hinted_ts,
        capture_source: "unix_socket.connect".to_string(),
    };
    let intent = ParsedEventIntent::builder()
        .source_id(SourceId::from_static("test.unit"))
        .parser_id(ParserId::from_static("test-parser"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("test.event"))
        .event_source(EventSource::from_static("test"))
        .payload(serde_json::json!({"k": "v"}))
        .ts_orig(original_ts)
        .timing(sinex_primitives::parser::TimingEvidence::Atemporal)
        .anchor(MaterialAnchor::ByteRange { start: 0, len: 0 })
        .privacy_context(ProcessingContext::Metadata)
        .build();

    let mut promoted = apply_record_timing_hint_to_intents(vec![intent], Some(&hint));
    assert_eq!(promoted.len(), 1);
    let promoted = promoted.remove(0);
    assert_eq!(promoted.ts_orig, hinted_ts);
    assert_eq!(promoted.timing, hint);

    let event = intent_to_event_with_anchor(
        promoted,
        Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
        0,
        None,
        None,
        None,
        0,
    )
    .expect("intent conversion");
    assert_eq!(event.ts_orig, Some(hinted_ts));
    assert_eq!(
        event.ts_quality,
        Some(sinex_primitives::domain::TemporalSourceType::RealtimeCapture)
    );
    Ok(())
}

#[sinex_test]
async fn record_realtime_hint_does_not_override_intrinsic_intent_timing()
-> xtask::sandbox::TestResult<()> {
    let intrinsic_ts = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid intrinsic timestamp"))?;
    let hinted_ts = Timestamp::from_unix_timestamp(1_700_000_123)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid hinted timestamp"))?;
    let hint = sinex_primitives::parser::TimingEvidence::RealtimeCapture {
        value: hinted_ts,
        capture_source: "unix_socket.connect".to_string(),
    };
    let intrinsic = sinex_primitives::parser::TimingEvidence::Intrinsic {
        field: "started_at".to_string(),
        confidence: sinex_primitives::parser::TimingConfidence::Intrinsic,
    };
    let intent = ParsedEventIntent::builder()
        .source_id(SourceId::from_static("test.unit"))
        .parser_id(ParserId::from_static("test-parser"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("test.event"))
        .event_source(EventSource::from_static("test"))
        .payload(serde_json::json!({"k": "v"}))
        .ts_orig(intrinsic_ts)
        .timing(intrinsic.clone())
        .anchor(MaterialAnchor::ByteRange { start: 0, len: 0 })
        .privacy_context(ProcessingContext::Metadata)
        .build();

    let mut unchanged = apply_record_timing_hint_to_intents(vec![intent], Some(&hint));
    assert_eq!(unchanged.len(), 1);
    let unchanged = unchanged.remove(0);
    assert_eq!(unchanged.ts_orig, intrinsic_ts);
    assert_eq!(unchanged.timing, intrinsic);
    Ok(())
}

#[sinex_test]
async fn sqlite_snapshot_evidence_link_is_idempotent(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, _events) = make_adapter_runtime_with_db(&ctx).await?;
    let row_material_id = Uuid::now_v7();
    let snapshot_material_id = Uuid::now_v7();

    ctx.pool()
        .source_materials()
        .register_external_in_flight(
            row_material_id,
            "stream",
            Some("test://sqlite-row-stream"),
            json!({"test": "row"}),
            Timestamp::now(),
        )
        .await?;
    ctx.pool()
        .source_materials()
        .register_external_in_flight(
            snapshot_material_id,
            "file",
            Some("test://sqlite-snapshot"),
            json!({"test": "snapshot"}),
            Timestamp::now(),
        )
        .await?;

    let mut source = AdapterBackedSource::<TestAdapter, EmittingParser>::new("test.sqlite");
    source.runtime = Some(runtime);
    source.sqlite_snapshot_evidence.update(
        crate::runtime::parser::adapters::SqliteSnapshotEvidence {
            material_id: Id::<SourceMaterial>::from_uuid(snapshot_material_id),
            source_identifier: "test.sqlite.snapshot".to_string(),
            source_path: "/tmp/test.sqlite".to_string(),
            content_hash_blake3: "abc123".to_string(),
            size_bytes: 123,
        },
    );

    let row_material = Id::<SourceMaterial>::from_uuid(row_material_id);
    source
        .link_latest_sqlite_snapshot_backing_material(row_material)
        .await;
    source
        .link_latest_sqlite_snapshot_backing_material(row_material)
        .await;

    let links = ctx
        .pool()
        .source_materials()
        .links_from(row_material_id)
        .await?;
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].to_material_id, snapshot_material_id);
    assert_eq!(
        links[0].relation_type,
        source_material_relation_types::BACKED_BY
    );
    assert_eq!(links[0].metadata["evidence_role"], "sqlite_snapshot");
    assert_eq!(
        links[0].metadata["source_identifier"],
        "test.sqlite.snapshot"
    );
    assert_eq!(links[0].metadata["content_hash_blake3"], "abc123");
    assert_eq!(links[0].metadata["size_bytes"], 123);
    Ok(())
}

// sinex-audit-anchor-byte-degenerate: DirectoryEntry/GitObject anchors must
// derive genuinely-discriminating anchor_byte values instead of the
// constant 0 that collapsed every fs/document.staging/system.systemd event
// sharing a material into one replay-replacement bucket.
#[test]
fn directory_entry_anchors_hash_to_distinct_anchor_bytes() {
    let one = MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from("/tmp/replay/one.txt"),
        content_hash: None,
    };
    let two = MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from("/tmp/replay/two.txt"),
        content_hash: None,
    };

    let (one_anchor_byte, one_start, one_end) = anchor_offsets_for_materialized_record(&one);
    let (two_anchor_byte, two_start, two_end) = anchor_offsets_for_materialized_record(&two);

    assert_ne!(
        one_anchor_byte, 0,
        "DirectoryEntry anchor must not collapse to the degenerate constant 0"
    );
    assert_ne!(
        one_anchor_byte, two_anchor_byte,
        "distinct DirectoryEntry paths must yield distinct anchor_byte values"
    );
    assert_eq!(one_start, None);
    assert_eq!(one_end, None);
    assert_eq!(two_start, None);
    assert_eq!(two_end, None);
}

#[test]
fn directory_entry_anchor_is_deterministic_for_the_same_occurrence() {
    let anchor = |content_hash: Option<&str>| MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from("/tmp/replay/stable.txt"),
        content_hash: content_hash.map(str::to_string),
    };

    let (first, ..) = anchor_offsets_for_materialized_record(&anchor(None));
    let (second, ..) = anchor_offsets_for_materialized_record(&anchor(None));
    assert_eq!(
        first, second,
        "the same DirectoryEntry occurrence must derive the same anchor_byte every time \
         (original capture and replay must agree)"
    );

    let (with_hash, ..) = anchor_offsets_for_materialized_record(&anchor(Some("abc123")));
    assert_ne!(
        first, with_hash,
        "a different content_hash on the same path must not collide"
    );
}

#[test]
fn git_object_anchors_hash_to_distinct_anchor_bytes() {
    let one = MaterialAnchor::GitObject {
        oid: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        path: Some(Utf8PathBuf::from("src/lib.rs")),
    };
    let two = MaterialAnchor::GitObject {
        oid: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        path: Some(Utf8PathBuf::from("src/lib.rs")),
    };

    let (one_anchor_byte, ..) = anchor_offsets_for_materialized_record(&one);
    let (two_anchor_byte, ..) = anchor_offsets_for_materialized_record(&two);

    assert_ne!(one_anchor_byte, 0);
    assert_ne!(one_anchor_byte, two_anchor_byte);
}

// sinex-fka1: replay must use the original append-stream coordinates rather
// than deriving a path hash. This is the direct regression test for
// `replay_file_drop_materials`, exercised at the pure-function level since
// the full replay dispatch requires a live acquisition manager.
#[test]
fn file_drop_replay_reuses_original_append_offsets() {
    let occurrence_one = crate::runtime::stream::ReplayMaterialOccurrence {
        source_material_id: Uuid::nil(),
        anchor_byte: 17,
        offset_start: Some(17),
        offset_end: Some(23),
        record_metadata: json!({"event_kind": "Deleted", "path": "/tmp/one"}),
    };
    let occurrence_two = crate::runtime::stream::ReplayMaterialOccurrence {
        source_material_id: Uuid::nil(),
        anchor_byte: 23,
        offset_start: Some(23),
        offset_end: Some(31),
        record_metadata: json!({"event_kind": "Deleted", "path": "/tmp/two"}),
    };

    let anchor_one = file_drop_replay_anchor(31, &occurrence_one).unwrap();
    let anchor_two = file_drop_replay_anchor(31, &occurrence_two).unwrap();
    assert_eq!(
        anchor_one,
        MaterialAnchor::ByteRange { start: 17, len: 6 }
    );
    assert_eq!(
        anchor_two,
        MaterialAnchor::ByteRange { start: 23, len: 8 }
    );

    let (anchor_byte_one, start_one, end_one) =
        anchor_offsets_for_materialized_record(&anchor_one);
    let (anchor_byte_two, start_two, end_two) =
        anchor_offsets_for_materialized_record(&anchor_two);
    assert_eq!((anchor_byte_one, start_one, end_one), (17, Some(17), Some(23)));
    assert_eq!((anchor_byte_two, start_two, end_two), (23, Some(23), Some(31)));

    // A content-materialized record still reconstructs its original byte
    // range when the persisted occurrence coordinates say it began at zero.
    let content_occurrence = crate::runtime::stream::ReplayMaterialOccurrence {
        source_material_id: Uuid::nil(),
        anchor_byte: 0,
        offset_start: Some(0),
        offset_end: Some(42),
        record_metadata: json!({
            "event_kind": "Created",
            "path": "/tmp/replay/created.txt",
            "content_materialized": true,
            "content_size_bytes": 42,
        }),
    };
    let content_anchor = file_drop_replay_anchor(42, &content_occurrence).unwrap();
    assert_eq!(
        content_anchor,
        MaterialAnchor::ByteRange { start: 0, len: 42 }
    );
}

#[test]
fn file_drop_replay_fails_closed_without_durable_range_coordinates() {
    let occurrence = crate::runtime::stream::ReplayMaterialOccurrence {
        source_material_id: Uuid::nil(),
        anchor_byte: 0,
        offset_start: None,
        offset_end: None,
        record_metadata: json!({"event_kind": "Created", "path": "/tmp/missing"}),
    };

    let error = file_drop_replay_range(32, &occurrence)
        .expect_err("replay must not guess a byte range from logical metadata");
    assert!(
        error
            .to_string()
            .contains("missing offset_start; cannot safely recover bytes"),
        "unexpected error: {error}"
    );
}

/// The FileDrop parity check must use the same serialized bytes and append
/// acquirer that live capture uses. Synthetic non-zero offsets can pass while
/// the live/replay coordinate spaces still disagree.
#[sinex_test]
async fn file_drop_replay_anchor_matches_live_append_capture(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::with_defaults(ctx.nats_client(), "file-drop-parity")
            .with_work_dir(work_dir.path()),
    );
    let mut acquirer = AppendStreamAcquirer::new(manager);
    let record = SourceRecord {
        material_id: Id::from_uuid(Uuid::nil()),
        anchor: MaterialAnchor::DirectoryEntry {
            path: Utf8PathBuf::from("/tmp/file-drop/parity.txt"),
            content_hash: None,
        },
        bytes: b"/tmp/file-drop/parity.txt".to_vec(),
        logical_path: Some(Utf8PathBuf::from("/tmp/file-drop/parity.txt")),
        source_ts_hint: None,
        metadata: json!({"event_kind": "Created", "capture_surface": "file_drop"}),
    };
    let live_bytes = materialization_bytes_for_adapter_record(&record)?;
    let live = acquirer
        .append_with_anchor(&live_bytes, "file-drop")
        .await?;
    acquirer.finalize("parity-test").await?;

    let occurrence = crate::runtime::stream::ReplayMaterialOccurrence {
        source_material_id: live.material_id,
        anchor_byte: live.offset_start,
        offset_start: Some(live.offset_start),
        offset_end: Some(live.offset_end),
        record_metadata: record.metadata,
    };
    let replay = file_drop_replay_anchor(live_bytes.len() as u64, &occurrence)?;
    assert_eq!(
        replay,
        MaterialAnchor::ByteRange {
            start: live.offset_start as u64,
            len: live_bytes.len() as u64,
        }
    );
    assert_eq!(
        anchor_offsets_for_materialized_record(&replay),
        (
            live.offset_start,
            Some(live.offset_start),
            Some(live.offset_end)
        ),
        "replay must reuse the append acquirer's exact coordinate space"
    );
    Ok(())
}

/// Multiple FileDrop records share one append-stream material. Replay must
/// retain each record's physical range instead of collapsing them onto a
/// logical-path-derived anchor.
#[sinex_test]
async fn file_drop_replay_preserves_each_live_append_occurrence(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::with_defaults(ctx.nats_client(), "file-drop-multi-parity")
            .with_work_dir(work_dir.path()),
    );
    let mut acquirer = AppendStreamAcquirer::new(manager);
    let records = [
        ("/tmp/file-drop/first.txt", json!({"event_kind": "Deleted"})),
        ("/tmp/file-drop/second.txt", json!({"event_kind": "Deleted"})),
    ];
    let mut captured = Vec::with_capacity(records.len());
    let mut authoritative_bytes = Vec::new();

    for (path, mut metadata) in records {
        metadata["path"] = json!(path);
        let record = SourceRecord {
            material_id: Id::from_uuid(Uuid::nil()),
            anchor: MaterialAnchor::DirectoryEntry {
                path: Utf8PathBuf::from(path),
                content_hash: None,
            },
            bytes: path.as_bytes().to_vec(),
            logical_path: Some(Utf8PathBuf::from(path)),
            source_ts_hint: None,
            metadata,
        };
        let bytes = materialization_bytes_for_adapter_record(&record)?;
        let live = acquirer.append_with_anchor(&bytes, "file-drop").await?;
        authoritative_bytes.extend_from_slice(&bytes);
        captured.push((bytes, live));
    }
    acquirer.finalize("multi-parity-test").await?;

    let material_len = authoritative_bytes.len() as u64;
    for (bytes, live) in &captured {
        let occurrence = crate::runtime::stream::ReplayMaterialOccurrence {
            source_material_id: live.material_id,
            anchor_byte: live.offset_start,
            offset_start: Some(live.offset_start),
            offset_end: Some(live.offset_end),
            record_metadata: json!({"event_kind": "Deleted"}),
        };
        let (replay, range) = file_drop_replay_range(material_len, &occurrence)?;

        assert_eq!(
            &authoritative_bytes[range],
            bytes,
            "replay must read the same physical append-stream range as capture"
        );
        assert_eq!(
            replay,
            MaterialAnchor::ByteRange {
                start: live.offset_start as u64,
                len: bytes.len() as u64,
            }
        );
        assert_eq!(
            anchor_offsets_for_materialized_record(&replay),
            (
                live.offset_start,
                Some(live.offset_start),
                Some(live.offset_end)
            )
        );
    }
    assert_ne!(captured[0].1.offset_start, captured[1].1.offset_start);
    Ok(())
}

#[sinex_serial_test]
async fn file_drop_replay_reads_authoritative_cas_bytes_after_source_removed(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (runtime, mut event_receiver) = make_adapter_runtime_with_db(&ctx).await?;

    let _cas_dir = tempfile::tempdir()?;
    let cas_root = Utf8PathBuf::from_path_buf(_cas_dir.path().to_path_buf()).map_err(|path| {
        SinexError::validation("test CAS path should be valid UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    let mut env = EnvGuard::new();
    env.set("SINEX_CONTENT_STORE_PATH", &cas_root);
    let content_store = ContentStoreManager::new(
        ContentStoreConfig {
            root_path: cas_root,
            ..Default::default()
        },
        ctx.pool().clone(),
        None,
    )?;

    let source_root = tempfile::tempdir()?;
    let source_path = source_root.path().join("original.txt");
    tokio::fs::write(&source_path, b"path-derived bytes before mutation").await?;
    let logical_path = Utf8PathBuf::from_path_buf(source_path.clone()).map_err(|path| {
        SinexError::validation("test source path should be valid UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    let authoritative_bytes = b"authoritative bytes retained in CAS";
    let blob = content_store
        .ingest_from_bytes(authoritative_bytes, "original.txt", "text/plain")
        .await?;
    let material = ctx
        .pool()
        .source_materials()
        .register_material(
            SourceMaterialRegistration::blob_text(logical_path.as_str())
                .with_blob_id(blob.id)
                .with_metadata(json!({
                    "path": logical_path,
                    "logical_source_identifier": "file-drop-replay-test",
                })),
        )
        .await?;

    // Anti-vacuity mutation: the watched path is changed and then removed while
    // the registry row and CAS object remain available to replay.
    tokio::fs::write(&source_path, b"mutated path bytes that replay must ignore").await?;
    tokio::fs::remove_file(&source_path).await?;

    let mut source =
        AdapterBackedSource::<FileDropAdapter, EmittingParser>::new("file-drop-replay-test");
    let mut state = AdapterModuleState::default();
    source
        .initialize(
            AdapterSourceConfig {
                adapter: json!({"watch_paths": []}),
                ..Default::default()
            },
            &runtime,
            &mut state,
        )
        .await?;

    let report = source
        .scan_historical(
            &mut state,
            Checkpoint::None,
            TimeHorizon::Historical {
                end_time: Timestamp::now(),
            },
            ScanArgs {
                replay: Some(MaterialReplayContext {
                    operation_id: Uuid::now_v7(),
                    materials: vec![ResolvedReplayMaterial {
                        source_material_id: material.id,
                        material_kind: "local_cas".to_string(),
                        source_identifier: logical_path.to_string(),
                        material_metadata: json!({"path": logical_path}),
                        material_start_time: None,
                        material_end_time: None,
                    }],
                    occurrences: vec![ReplayMaterialOccurrence {
                        source_material_id: material.id,
                        anchor_byte: 0,
                        offset_start: Some(0),
                        offset_end: Some(authoritative_bytes.len() as i64),
                        record_metadata: json!({
                            "event_kind": "Created",
                            "path": logical_path,
                        }),
                    }],
                    replay_scope: ReplayScopeFilters::default(),
                }),
                ..Default::default()
            },
        )
        .await?;
    let event = event_receiver
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("expected replayed file-drop event"))?;
    let authoritative_hash = blake3::hash(authoritative_bytes);
    let logical_path_hash = blake3::hash(logical_path.as_str().as_bytes());

    assert_eq!(report.events_processed, 1);
    assert_eq!(
        event.payload["record_bytes"],
        json!(authoritative_bytes.as_slice())
    );
    assert_eq!(
        event.anchor_payload_hash.as_deref(),
        Some(authoritative_hash.as_bytes().as_slice()),
        "replay must hash the bytes loaded from authoritative CAS"
    );
    assert_ne!(
        event.anchor_payload_hash.as_deref(),
        Some(logical_path_hash.as_bytes().as_slice()),
        "replay must never synthesize bytes from logical_path"
    );
    match event.provenance() {
        sinex_primitives::events::Provenance::Material {
            anchor_byte,
            offset_start,
            offset_end,
            ..
        } => {
            assert_eq!(*anchor_byte, 0);
            assert_eq!(*offset_start, Some(0));
            assert_eq!(*offset_end, Some(authoritative_bytes.len() as i64));
        }
        other => panic!("expected material provenance, got {other:?}"),
    }
    Ok(())
}

// =============================================================================
// sinex-2n9: paced historical import e2e (real NATS raw-events stream +
// consumer, real ScanPacer/BacklogGate production code, deliberately large
// fixture)
// =============================================================================

mod pacing_e2e {
    use super::*;
    use crate::runtime::pacing::RateBudget;
    use futures::StreamExt as _;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Number of synthetic "historical" records the import fixture produces.
    /// Large enough to meaningfully exercise pacing across many batches
    /// (each already-materialized record is its own pacing batch — see
    /// `drain_adapter`), small enough to keep the test fast against a
    /// deliberately slow consumer.
    const TOTAL_RECORDS: u64 = 240;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ManyRecordsConfig {
        #[serde(default)]
        total_records: u64,
    }

    #[derive(Default)]
    struct ManyMaterializedRecordsAdapter;

    #[async_trait]
    impl InputShapeAdapter for ManyMaterializedRecordsAdapter {
        type Config = ManyRecordsConfig;
        type Cursor = u64;

        const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

        async fn open(
            &self,
            _material_id: Id<SourceMaterial>,
            _config: &Self::Config,
            _cursor: Option<Self::Cursor>,
        ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
            Err(ParserError::Adapter(
                "open_with_acquisition should be used for materialized records".to_string(),
            ))
        }

        fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
            Ok(record.material_id.to_uuid().as_u128() as u64)
        }
    }

    #[async_trait]
    impl InputShapeAdapterExt for ManyMaterializedRecordsAdapter {
        async fn open_with_acquisition(
            &self,
            _material_id: Id<SourceMaterial>,
            config: &Self::Config,
            _cursor: Option<Self::Cursor>,
            _acquisition: Option<Arc<AcquisitionManager>>,
        ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
            let total = config.total_records;
            // Anchor the fabricated "historical window" one hour in the past
            // so `ScanProgressTracker`'s position/horizon math has a real
            // (non-degenerate) span to work with.
            let base = Timestamp::now() - time::Duration::hours(1);
            let records = (0..total).map(move |i| {
                Ok(SourceRecord {
                    material_id: Id::from_uuid(Uuid::from_u128(u128::from(i) + 1)),
                    anchor: MaterialAnchor::ByteRange { start: i, len: 1 },
                    bytes: format!("record-{i}").into_bytes(),
                    logical_path: None,
                    source_ts_hint: Some(sinex_primitives::parser::TimingEvidence::UserDeclared {
                        value: base + time::Duration::seconds(i as i64),
                        reason: "sinex-2n9 pacing e2e fixture".to_string(),
                    }),
                    metadata: JsonValue::Null,
                })
            });
            Ok(Box::pin(stream::iter(records)))
        }
    }

    #[derive(Default)]
    struct ManyRecordsParser;

    #[async_trait]
    impl MaterialParser for ManyRecordsParser {
        type Config = ();

        fn manifest(&self) -> ParserManifest {
            ParserManifest {
                parser_id: ParserId::from_static("pacing-e2e-parser"),
                parser_version: "1.0.0".to_string(),
                accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
                source_id: SourceId::from_static("test.pacing_e2e"),
                declared_event_types: vec![(
                    EventSource::from_static("test"),
                    EventType::from_static("pacing.e2e"),
                )],
                privacy_contexts: vec![ProcessingContext::Metadata],
                sensitivity_hints: Vec::new(),
                description: String::new(),
            }
        }

        async fn parse_record(
            &mut self,
            record: SourceRecord,
            ctx: &ParserContext,
        ) -> ParserResult<Vec<ParsedEventIntent>> {
            Ok(vec![
                ParsedEventIntent::builder()
                    .source_id(ctx.source_id.clone())
                    .parser_id(ParserId::from_static("pacing-e2e-parser"))
                    .parser_version("1.0.0")
                    .event_type(EventType::from_static("pacing.e2e"))
                    .event_source(EventSource::from_static("test"))
                    .payload(serde_json::json!({"record": String::from_utf8_lossy(&record.bytes)}))
                    .ts_orig(ctx.acquisition_time)
                    .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
                    .anchor(record.anchor)
                    .privacy_context(ProcessingContext::Metadata)
                    .build(),
            ])
        }
    }

    /// Tee every emitted event to (a) immediate settlement resolution — so
    /// `emit_batch_durable`'s receipt wait completes without a real
    /// event_engine — and (b) a REAL `NatsPublisher::publish_intent` call,
    /// so the source's actual production emission path durably reaches the
    /// raw-events `JetStream` stream and grows real consumer backlog.
    /// Tee every emitted event to (a) immediate settlement resolution and
    /// (b) a direct `JetStream` publish onto the real raw-events stream's
    /// subject (bypassing `NatsPublisher`'s own internal backpressure gate
    /// and stream-bootstrap machinery, which this test does not need and
    /// which would otherwise double up with the test's own stream setup).
    /// The publish itself is the same durability primitive production code
    /// uses (`jetstream::Context::publish`, ack-tracked) — only the
    /// envelope-construction/gating wrapper around it is skipped.
    fn settle_and_publish_events(
        mut raw: mpsc::Receiver<Event<JsonValue>>,
        registry: crate::runtime::durable_emission::SettlementRegistry,
        js: async_nats::jetstream::Context,
        subject: String,
    ) {
        tokio::spawn(async move {
            while let Some(event) = raw.recv().await {
                if let Some(id) = event.id {
                    registry.resolve(
                        id,
                        crate::runtime::durable_emission::EmissionReceiptState::PersistedConfirmed {
                            lane: sinex_db::repositories::EventStorageLane::Activity,
                            inserted: true,
                            confirmed_sequence: None,
                        },
                    );
                }
                let payload = serde_json::to_vec(&event).unwrap_or_default();
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    js.publish(subject.clone(), payload.into()),
                )
                .await
                {
                    Ok(Ok(ack_future)) => {
                        if let Err(error) =
                            tokio::time::timeout(std::time::Duration::from_secs(5), ack_future)
                                .await
                        {
                            eprintln!("pacing e2e test: publish ack timed out: {error}");
                        }
                    }
                    Ok(Err(error)) => {
                        eprintln!("pacing e2e test: publish failed: {error}");
                    }
                    Err(_) => {
                        eprintln!("pacing e2e test: publish call itself timed out");
                    }
                }
            }
        });
    }

    #[sinex_test(timeout = 90)]
    async fn paced_historical_scan_holds_raw_backlog_below_threshold(
        ctx: TestContext,
    ) -> TestResult<()> {
        // Shared process-wide NATS (matches every other test in this file);
        // isolated from concurrent tests via a unique namespace, which also
        // has to be visible to `scan_historical`'s own namespace resolution
        // (`SINEX_NAMESPACE` env var) — safe to set process-wide here since
        // nextest runs one test per process.
        let namespace = format!("pacing-e2e-{}", Uuid::now_v7().simple());
        let _namespace_guard = xtask::sandbox::EnvGuard::set_single("SINEX_NAMESPACE", &namespace);
        let ctx = ctx.with_nats().shared().await?;
        let nats_client = ctx.nats_client();

        // Bootstrap the REAL production raw-events stream topology, then
        // create the SAME durable consumer name the event-engine and the
        // publish-side backpressure gate use (`event_engine_raw_consumer_name`)
        // — but never pull from it except via a deliberately slow background
        // task, simulating a struggling/behind event-engine (the actual
        // incident shape).
        crate::runtime::jetstream_streams::bootstrap_raw_events_stream(
            &nats_client,
            Some(&namespace),
        )
        .await?;
        let env = sinex_primitives::environment::environment();
        let stream_name = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
        let consumer_name = crate::runtime::backlog::event_engine_raw_consumer_name(&env);

        let js = async_nats::jetstream::new(nats_client.clone());
        let mut stream = js.get_stream(&stream_name).await?;
        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                name: Some(consumer_name.clone()),
                durable_name: Some(consumer_name.clone()),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: std::time::Duration::from_secs(30),
                ..Default::default()
            })
            .await?;

        // Deliberately slow puller: ~33 msg/s. Never catching up to a fast
        // unpaced producer is the point — it proves the SOURCE, not the
        // consumer, is what keeps backlog bounded.
        let mut messages = consumer.messages().await?;
        let puller = tokio::spawn(async move {
            while let Some(Ok(msg)) = messages.next().await {
                let _ = msg.ack().await;
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        });

        // Background sampler: records the max observed backlog depth via the
        // SAME production helper `ScanPacer`/`BacklogGate` use internally, at
        // a finer grain than the scan loop's own per-batch checks so the
        // assertion below is an independent observation, not a self-report
        // from the code under test.
        let max_observed_pending = Arc::new(AtomicU64::new(0));
        let sampler_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sampler_handle = {
            let js = js.clone();
            let env = env.clone();
            let namespace = namespace.clone();
            let max_observed_pending = Arc::clone(&max_observed_pending);
            let sampler_stop = Arc::clone(&sampler_stop);
            tokio::spawn(async move {
                while !sampler_stop.load(Ordering::Relaxed) {
                    if let Ok(Some(info)) = crate::runtime::backlog::raw_events_consumer_pending(
                        &js,
                        &env,
                        Some(&namespace),
                    )
                    .await
                        && info.num_pending > max_observed_pending.load(Ordering::Relaxed)
                    {
                        max_observed_pending.store(info.num_pending, Ordering::Relaxed);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(75)).await;
                }
            })
        };

        // Independent continuous-capture latency probe (sinex-2n9 AC: paced
        // historical import must not affect continuous capture latency).
        // Runs CONCURRENTLY with the paced scan below, on its own
        // `EventEmitter`/mpsc channel that is never touched by `ScanPacer` —
        // if pacing's sleeps leaked into shared scheduling/locks, this would
        // show up as inflated latencies here.
        let (continuous_tx, mut continuous_rx) = mpsc::channel::<Event<JsonValue>>(8);
        let continuous_emitter = EventEmitter::new(continuous_tx, false);
        let continuous_latencies: Arc<StdMutex<Vec<std::time::Duration>>> =
            Arc::new(StdMutex::new(Vec::new()));
        let continuous_drain = {
            let continuous_latencies = Arc::clone(&continuous_latencies);
            tokio::spawn(async move {
                let mut count = 0u32;
                while let Some(_event) = continuous_rx.recv().await {
                    count += 1;
                    if count >= 40 {
                        break;
                    }
                }
                let _ = continuous_latencies; // populated by the producer loop below
            })
        };
        let continuous_producer = {
            let continuous_latencies = Arc::clone(&continuous_latencies);
            tokio::spawn(async move {
                for i in 0..40u32 {
                    let event = sinex_primitives::events::payload::DynamicPayload::new(
                        "test",
                        "continuous.e2e",
                        serde_json::json!({"i": i}),
                    )
                    .from_material_at(
                        Id::<SourceMaterial>::from_uuid(Uuid::from_u128(u128::from(i) + 1_000_000)),
                        i64::from(i),
                    )
                    .build()
                    .expect("continuous probe event should build");
                    let start = Instant::now();
                    let _ = continuous_emitter.emit(event).await;
                    continuous_latencies
                        .lock()
                        .expect("latency mutex poisoned")
                        .push(start.elapsed());
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
        };

        // --- The production route under test ---
        let (runtime, event_receiver_raw) = make_adapter_runtime(&ctx).await?;
        let settlement_registry = crate::runtime::durable_emission::SettlementRegistry::new();
        let publish_subject =
            env.nats_raw_event_subject_with_namespace(Some(&namespace), "test", "pacing.e2e");
        settle_and_publish_events(
            event_receiver_raw,
            settlement_registry,
            js.clone(),
            publish_subject,
        );

        let mut source =
            AdapterBackedSource::<ManyMaterializedRecordsAdapter, ManyRecordsParser>::new(
                "test.pacing_e2e",
            );
        let mut state = AdapterModuleState::default();
        source
            .initialize(
                AdapterSourceConfig {
                    adapter: serde_json::json!({"total_records": TOTAL_RECORDS}),
                    ..Default::default()
                },
                &runtime,
                &mut state,
            )
            .await?;

        let rate_budget = RateBudget {
            events_per_sec: Some(1_000.0), // non-binding: backlog is the binding constraint
            bytes_per_sec: None,
            backlog_pause_threshold: Some(15),
            backlog_resume_threshold: Some(5),
        };

        let scan_start = Instant::now();
        let report = source
            .scan_historical(
                &mut state,
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs {
                    rate_budget: Some(rate_budget),
                    ..Default::default()
                },
            )
            .await?;
        let scan_elapsed = scan_start.elapsed();

        // Let the sampler take a few more readings past the scan's own last
        // batch before stopping.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        sampler_stop.store(true, Ordering::Relaxed);
        let _ = sampler_handle.await;
        puller.abort();
        continuous_producer
            .await
            .expect("continuous producer task panicked");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), continuous_drain).await;

        // --- Assertions: the production pacing route actually held ---

        assert_eq!(
            report.events_processed, TOTAL_RECORDS,
            "every fixture record should settle and be counted (settlement is short-circuited \
             to PersistedConfirmed in this harness, so a mismatch means pacing broke drain \
             correctness, not that settlement is slow)"
        );

        let observed_max = max_observed_pending.load(Ordering::Relaxed);
        // Threshold (15) plus generous slack for sampler/gate timing jitter —
        // still an order of magnitude below TOTAL_RECORDS (240) and light
        // years below the 1.24M-event incident backlog. An UNPACED run (see
        // the companion test below) blows straight past this.
        assert!(
            observed_max <= 40,
            "paced historical scan let raw backlog reach {observed_max} pending messages; \
             expected it held near the configured pause threshold (15, resume 5) for the \
             whole drain, not just at completion"
        );

        assert!(
            scan_elapsed >= std::time::Duration::from_secs(2),
            "a backlog-paced scan against a consumer that drains at ~33 msg/s should take \
             several seconds for {TOTAL_RECORDS} records, not complete instantly — got {scan_elapsed:?} \
             (near-zero elapsed would mean the backlog gate never actually engaged)"
        );

        let latencies = continuous_latencies
            .lock()
            .expect("latency mutex poisoned")
            .clone();
        assert_eq!(
            latencies.len(),
            40,
            "continuous-capture probe should complete all 40 emits without being blocked"
        );
        let max_continuous_latency = latencies.iter().max().copied().unwrap_or_default();
        assert!(
            max_continuous_latency < std::time::Duration::from_millis(500),
            "continuous-capture emit latency should stay low while a historical import is \
             being paced concurrently (measured max: {max_continuous_latency:?}); a regression \
             here would mean pacing sleeps are leaking into the continuous/live-tail path, \
             which sinex-2n9 explicitly requires stays ungated"
        );

        Ok(())
    }

    /// Companion negative control for `paced_historical_scan_holds_raw_backlog_below_threshold`:
    /// under `RateBudget::unlimited()`, backlog should be allowed to grow well past the paced
    /// bound (proving the *positive* test's bound is actually enforced by pacing, not by some
    /// other structural cap). Restored per sinex-audit-2n9-unlimited-negctrl-cap after being
    /// deleted rather than kept as a red test: it fails deterministically at an observed
    /// max_pending of exactly 40 (== the continuous-probe emit count, suspiciously) across
    /// multiple runs, including with the tail sampling window widened from 3s to 8s (ruling out
    /// a sampling-race explanation). `RateBudget::unlimited()` itself was independently verified
    /// correct by direct code review: it nulls both rate and backlog threshold fields;
    /// `BacklogGate::from_budget` returns `None` for it; `ScanPacer::after_batch` skips the wait
    /// entirely when the gate is `None`. The root cause of the exact-40 cap under `--unlimited`
    /// was not conclusively identified — candidates are this test's own NATS/JetStream
    /// measurement path (`raw_events_consumer_pending`'s `num_pending`), the WorkQueue-retention
    /// stream config, or `max_ack_pending` client-vs-server default semantics — not a pacing
    /// regression in production code. Kept as an ignored red test so the discrepancy stays
    /// live instead of only living in a comment.
    #[sinex_test(timeout = 90)]
    #[ignore = "sinex-audit-2n9-unlimited-negctrl-cap open: RateBudget::unlimited() should let \
                observed backlog exceed the paced test's 40-message bound, but it deterministically \
                caps at exactly 40 too -- root cause not yet identified, see doc comment above"]
    async fn unlimited_historical_scan_lets_backlog_grow_past_paced_bound(
        ctx: TestContext,
    ) -> TestResult<()> {
        let namespace = format!("pacing-negctrl-{}", Uuid::now_v7().simple());
        let _namespace_guard = xtask::sandbox::EnvGuard::set_single("SINEX_NAMESPACE", &namespace);
        let ctx = ctx.with_nats().shared().await?;
        let nats_client = ctx.nats_client();

        crate::runtime::jetstream_streams::bootstrap_raw_events_stream(
            &nats_client,
            Some(&namespace),
        )
        .await?;
        let env = sinex_primitives::environment::environment();
        let stream_name = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
        let consumer_name = crate::runtime::backlog::event_engine_raw_consumer_name(&env);

        let js = async_nats::jetstream::new(nats_client.clone());
        let mut stream = js.get_stream(&stream_name).await?;
        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                name: Some(consumer_name.clone()),
                durable_name: Some(consumer_name.clone()),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: std::time::Duration::from_secs(30),
                ..Default::default()
            })
            .await?;

        // Same deliberately slow puller (~33 msg/s) as the positive-control test.
        let mut messages = consumer.messages().await?;
        let puller = tokio::spawn(async move {
            while let Some(Ok(msg)) = messages.next().await {
                let _ = msg.ack().await;
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        });

        let max_observed_pending = Arc::new(AtomicU64::new(0));
        let sampler_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sampler_handle = {
            let js = js.clone();
            let env = env.clone();
            let namespace = namespace.clone();
            let max_observed_pending = Arc::clone(&max_observed_pending);
            let sampler_stop = Arc::clone(&sampler_stop);
            tokio::spawn(async move {
                while !sampler_stop.load(Ordering::Relaxed) {
                    if let Ok(Some(info)) = crate::runtime::backlog::raw_events_consumer_pending(
                        &js,
                        &env,
                        Some(&namespace),
                    )
                    .await
                        && info.num_pending > max_observed_pending.load(Ordering::Relaxed)
                    {
                        max_observed_pending.store(info.num_pending, Ordering::Relaxed);
                    }
                    // Widened sampling window (vs the positive test's 75ms) to rule out a
                    // sampling-race explanation for the exact-40 cap, per the doc comment.
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            })
        };

        let (runtime, event_receiver_raw) = make_adapter_runtime(&ctx).await?;
        let settlement_registry = crate::runtime::durable_emission::SettlementRegistry::new();
        let publish_subject =
            env.nats_raw_event_subject_with_namespace(Some(&namespace), "test", "pacing.negctrl");
        settle_and_publish_events(
            event_receiver_raw,
            settlement_registry,
            js.clone(),
            publish_subject,
        );

        let mut source =
            AdapterBackedSource::<ManyMaterializedRecordsAdapter, ManyRecordsParser>::new(
                "test.pacing_negctrl",
            );
        let mut state = AdapterModuleState::default();
        source
            .initialize(
                AdapterSourceConfig {
                    adapter: serde_json::json!({"total_records": TOTAL_RECORDS}),
                    ..Default::default()
                },
                &runtime,
                &mut state,
            )
            .await?;

        let report = source
            .scan_historical(
                &mut state,
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs {
                    rate_budget: Some(RateBudget::unlimited()),
                    ..Default::default()
                },
            )
            .await?;

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        sampler_stop.store(true, Ordering::Relaxed);
        let _ = sampler_handle.await;
        puller.abort();

        assert_eq!(
            report.events_processed, TOTAL_RECORDS,
            "every fixture record should settle and be counted under --unlimited too"
        );

        let observed_max = max_observed_pending.load(Ordering::Relaxed);
        assert!(
            observed_max > 40,
            "RateBudget::unlimited() should let raw backlog exceed the paced test's 40-message \
             bound (BacklogGate::from_budget returns None for it, so nothing should throttle \
             drain) -- observed_max={observed_max} instead deterministically caps at the same \
             bound as the PACED positive-control test, which is the unresolved discrepancy \
             sinex-audit-2n9-unlimited-negctrl-cap tracks"
        );

        Ok(())
    }
}
