use super::*;
use crate::runtime::checkpoint::CheckpointManager;
use crate::runtime::parser::adapters::{AppendOnlyCursor, ChainedCursor, SqliteRowCursor};
use crate::runtime::parser::{InputShapeKind, ParserError, ParserResult, SourceRecord};
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, EventEmitter, RuntimeHandles, ScanArgs, ServiceInfo, TimeHorizon,
};
use crate::runtime::{EventTransport, NatsPublisher, SOURCE_MATERIAL_STREAM};
use async_trait::async_trait;
use camino::Utf8PathBuf;
use futures::stream::{self, BoxStream};
use sinex_db::DbPoolExt;
use sinex_db::repositories::source_material_relation_types;
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
use std::collections::HashMap;
use tokio::sync::mpsc;
use xtask::sandbox::prelude::{TestContext, TestResult, WaitHelpers, sinex_test};

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
                .payload(serde_json::json!({"parsed": true}))
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
    let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
    let emitter = EventEmitter::new(event_sender, false);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let handles = RuntimeHandles::new(
        ctx.pool().clone(),
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
    let emitted = source.drain_adapter(None, &mut state, None).await?;

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
    let emitted = source.drain_adapter(None, &mut state, None).await?;
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
    let (runtime, event_receiver) = make_adapter_runtime(&ctx).await?;
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

    let emitted = source.drain_adapter(None, &mut state, None).await?;

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
        max_age_seconds: Seconds::from_secs(2),
    });
    let mut state = AdapterModuleState::default();

    source
        .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
        .await?;

    let drain_result = tokio::time::timeout(
        Duration::from_millis(1500),
        source.drain_adapter(None, &mut state, None),
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
    let mut source = AdapterBackedSource::<EmptyLogicalPathRecordAdapter, TestParser>::new(
        "desktop.clipboard",
    );
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
    let mut source = AdapterBackedSource::<EmptyLogicalPathRecordAdapter, TestParser>::new(
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
    let mut source = AdapterBackedSource::<EmptyLogicalPathRecordAdapter, TestParser>::new(
        "desktop.clipboard",
    );
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
    let emitted = source.drain_adapter(None, &mut state, None).await?;
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
    let handle = spawn_private_mode_control_listener(
        ctx.nats_client(),
        dir.path().to_path_buf(),
        "desktop.clipboard",
    );

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
    )
    .expect("intent conversion");
    assert_eq!(event.equivalence_key, Some(occurrence_key_string(&key)));
    Ok(())
}

/// Intents without an occurrence key leave `equivalence_key` unset (the
/// curation workbench simply has nothing to group on for that event).
#[sinex_test]
async fn absent_occurrence_key_leaves_equivalence_key_none() -> xtask::sandbox::TestResult<()> {
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
        Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
        0,
        None,
        None,
        None,
    )
    .expect("intent conversion");
    assert_eq!(event.equivalence_key, None);
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
