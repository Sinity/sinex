//! Adapter for D-Bus signal subscriptions.
//!
//! Subscribes to D-Bus signals matching configured match rules and yields one
//! [`SourceRecord`] per signal received. Cursor is `()` — D-Bus signals are
//! transient; there is no replay. The anchor is a
//! [`MaterialAnchor::StreamFrame`] with a monotonic frame counter.
//!
//! # Testability
//!
//! D-Bus requires a running session or system bus, which is often unavailable
//! in CI. The adapter is backed by a [`DbusBackend`] trait so tests can
//! inject a mock. The default impl uses `dbus_tokio`.
//!
//! # Feature gate
//!
//! The real `dbus` / `dbus_tokio` implementation is compiled only when the
//! `dbus-adapter` feature is enabled on `sinex-node-sdk`. In test builds the
//! mock backend is always available.

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// Config types
// =============================================================================

/// Which D-Bus bus to connect to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbusBus {
    Session,
    System,
}

/// Configuration for [`DbusStreamAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbusStreamConfig {
    /// Which bus to connect to.
    pub bus: DbusBus,

    /// D-Bus match rules (e.g.
    /// `"type='signal',interface='org.freedesktop.DBus.Properties'"`).
    pub match_rules: Vec<String>,
}

/// No cursor for [`DbusStreamAdapter`] — signals are transient / anchor-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbusStreamCursor;

// =============================================================================
// DbusMessage — what the backend yields
// =============================================================================

/// A decoded D-Bus signal message.
///
/// Backends yield this; the adapter converts it to a [`SourceRecord`].
#[derive(Debug, Clone)]
pub struct DbusMessage {
    /// The D-Bus interface (e.g. `"org.freedesktop.DBus.Properties"`).
    pub interface: String,
    /// The member (signal name, e.g. `"PropertiesChanged"`).
    pub member: String,
    /// The object path.
    pub path: String,
    /// The sender bus name.
    pub sender: Option<String>,
    /// The signal body serialized as JSON.
    pub body_json: serde_json::Value,
}

// =============================================================================
// DbusBackend trait — allows mock injection
// =============================================================================

/// Abstracts D-Bus connection so tests can inject a mock.
///
/// The default implementation (gated on `dbus-adapter` feature) uses
/// `dbus` + `dbus_tokio`. Tests use [`MockDbusBackend`].
pub trait DbusBackend: Send + 'static {
    /// Subscribe to the given match rules and yield messages.
    ///
    /// The returned stream must be driven to completion by the caller.
    fn subscribe(
        self: Box<Self>,
        bus: DbusBus,
        match_rules: Vec<String>,
    ) -> BoxStream<'static, ParserResult<DbusMessage>>;
}

// =============================================================================
// MockDbusBackend — for tests
// =============================================================================

/// A mock [`DbusBackend`] that yields a pre-configured sequence of messages.
pub struct MockDbusBackend {
    messages: Vec<DbusMessage>,
}

impl MockDbusBackend {
    pub fn new(messages: Vec<DbusMessage>) -> Self {
        Self { messages }
    }
}

impl DbusBackend for MockDbusBackend {
    fn subscribe(
        self: Box<Self>,
        _bus: DbusBus,
        _match_rules: Vec<String>,
    ) -> BoxStream<'static, ParserResult<DbusMessage>> {
        use futures::stream::{self, StreamExt};
        Box::pin(stream::iter(self.messages.into_iter().map(Ok)))
    }
}

// =============================================================================
// DbusStreamAdapter
// =============================================================================

/// Adapter for D-Bus signal subscriptions.
///
/// Subscribes to D-Bus signals via the configured match rules and emits one
/// [`SourceRecord`] per signal. To inject a mock bus in tests, use
/// [`DbusStreamAdapter::with_backend`].
pub struct DbusStreamAdapter {
    backend: Box<dyn DbusBackend + Send + Sync>,
}

impl DbusStreamAdapter {
    /// Create an adapter with a custom backend (useful for tests).
    pub fn with_backend(backend: impl DbusBackend + Send + Sync + 'static) -> Self {
        Self {
            backend: Box::new(backend),
        }
    }
}

#[async_trait]
impl InputShapeAdapter for DbusStreamAdapter {
    type Config = DbusStreamConfig;
    type Cursor = DbusStreamCursor;
    const KIND: InputShapeKind = InputShapeKind::DbusSubscription;

    async fn open(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        // `open` is called once; move the backend out of self.
        // This means DbusStreamAdapter can only be opened once.
        // That is acceptable for a live subscription adapter.
        Err(ParserError::Adapter(
            "DbusStreamAdapter::open requires the backend to be moved; use open_with_backend instead".into(),
        ))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(DbusStreamCursor)
    }
}

impl DbusStreamAdapter {
    /// Open the adapter and move the backend into the stream.
    ///
    /// Use this instead of `InputShapeAdapter::open` when the adapter was
    /// created with a backend.
    pub fn open_with_backend(
        backend: Box<dyn DbusBackend + Send + Sync>,
        material_id: Id<SourceMaterial>,
        config: &DbusStreamConfig,
    ) -> BoxStream<'static, ParserResult<SourceRecord>> {
        let bus = config.bus;
        let rules = config.match_rules.clone();
        let mut frame_index: u64 = 0;

        let message_stream = backend.subscribe(bus, rules);

        use futures::StreamExt;
        Box::pin(message_stream.map(move |msg_result| {
            let msg = msg_result?;
            let bytes = serde_json::to_vec(&msg.body_json)
                .map_err(|e| ParserError::Parse(format!("failed to serialize dbus body: {e}")))?;

            let anchor = MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index,
            };
            frame_index += 1;

            let metadata = serde_json::json!({
                "interface": msg.interface,
                "member": msg.member,
                "path": msg.path,
                "sender": msg.sender,
            });

            Ok(SourceRecord {
                material_id,
                anchor,
                bytes,
                logical_path: None,
                source_ts_hint: None,
                metadata,
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;
    use futures::StreamExt;

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    fn make_msg(interface: &str, member: &str) -> DbusMessage {
        DbusMessage {
            interface: interface.into(),
            member: member.into(),
            path: "/org/test".into(),
            sender: Some(":1.42".into()),
            body_json: serde_json::json!({ "key": "value" }),
        }
    }

    #[sinex_test]
    async fn test_mock_backend_yields_messages() -> xtask::sandbox::TestResult<()> {
        let msgs = vec![
            make_msg("org.test.Iface", "Signal1"),
            make_msg("org.test.Iface", "Signal2"),
        ];
        let config = DbusStreamConfig {
            bus: DbusBus::Session,
            match_rules: vec!["type='signal'".into()],
        };

        let stream = DbusStreamAdapter::open_with_backend(
            Box::new(MockDbusBackend::new(msgs)),
            dummy_material_id(),
            &config,
        );

        let records: Vec<_> = stream.collect().await;
        assert_eq!(records.len(), 2);
        assert!(records[0].is_ok());
        assert!(records[1].is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_anchor_is_stream_frame() -> xtask::sandbox::TestResult<()> {
        let msgs = vec![make_msg("org.test.Iface", "Sig")];
        let config = DbusStreamConfig {
            bus: DbusBus::System,
            match_rules: vec![],
        };

        let stream = DbusStreamAdapter::open_with_backend(
            Box::new(MockDbusBackend::new(msgs)),
            dummy_material_id(),
            &config,
        );

        let records: Vec<_> = stream.collect().await;
        let record = records[0].as_ref().unwrap();
        assert!(matches!(record.anchor, MaterialAnchor::StreamFrame { .. }));
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_frame_index_monotonic() -> xtask::sandbox::TestResult<()> {
        let msgs = vec![
            make_msg("org.a", "Sig1"),
            make_msg("org.b", "Sig2"),
            make_msg("org.c", "Sig3"),
        ];
        let config = DbusStreamConfig {
            bus: DbusBus::Session,
            match_rules: vec![],
        };

        let stream = DbusStreamAdapter::open_with_backend(
            Box::new(MockDbusBackend::new(msgs)),
            dummy_material_id(),
            &config,
        );

        let records: Vec<_> = stream.collect().await;
        let indices: Vec<u64> = records
            .iter()
            .map(|r| match &r.as_ref().unwrap().anchor {
                MaterialAnchor::StreamFrame { frame_index, .. } => *frame_index,
                _ => panic!("wrong anchor"),
            })
            .collect();

        for w in indices.windows(2) {
            assert!(w[0] < w[1]);
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_metadata_has_interface_and_member() -> xtask::sandbox::TestResult<()> {
        let msgs = vec![make_msg("org.example.Interface", "TestMember")];
        let config = DbusStreamConfig {
            bus: DbusBus::Session,
            match_rules: vec![],
        };

        let stream = DbusStreamAdapter::open_with_backend(
            Box::new(MockDbusBackend::new(msgs)),
            dummy_material_id(),
            &config,
        );

        let records: Vec<_> = stream.collect().await;
        let record = records[0].as_ref().unwrap();
        assert_eq!(record.metadata["interface"], "org.example.Interface");
        assert_eq!(record.metadata["member"], "TestMember");
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_cursor_after_always_unit() -> xtask::sandbox::TestResult<()> {
        let adapter = DbusStreamAdapter::with_backend(MockDbusBackend::new(vec![]));
        let record = SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::StreamFrame { material_offset: 0, frame_index: 0 },
            bytes: b"{}".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let cursor = adapter.cursor_after(&record).unwrap();
        assert_eq!(cursor, DbusStreamCursor);
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_empty_message_stream() -> xtask::sandbox::TestResult<()> {
        let config = DbusStreamConfig {
            bus: DbusBus::Session,
            match_rules: vec![],
        };
        let stream = DbusStreamAdapter::open_with_backend(
            Box::new(MockDbusBackend::new(vec![])),
            dummy_material_id(),
            &config,
        );
        let records: Vec<_> = stream.collect().await;
        assert!(records.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_kind_is_dbus_subscription() -> xtask::sandbox::TestResult<()> {
        assert_eq!(DbusStreamAdapter::KIND, InputShapeKind::DbusSubscription);
        Ok(())
    }
}
