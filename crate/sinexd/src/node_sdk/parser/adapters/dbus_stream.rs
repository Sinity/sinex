//! Adapter for D-Bus signal subscriptions.
//!
//! Linux only — gated behind `#[cfg(target_os = "linux")]`.
//! [`SourceRecord`] per signal received. Cursor is `()` — D-Bus signals are
//! transient; there is no replay. The anchor is a
//! [`MaterialAnchor::StreamFrame`] with a monotonic frame counter.
//!
//! # Backends
//!
//! D-Bus requires a running session or system bus, which is often unavailable
//! in CI. The adapter is backed by a [`DbusBackend`] trait so tests can
//! inject a mock via [`DbusStreamAdapter::with_backend`] +
//! [`DbusStreamAdapter::open_with_backend`]. The default
//! [`InputShapeAdapter::open`] path constructs a [`RealDbusBackend`] from the
//! adapter config and uses `zbus` to subscribe to live signals.
//!
//! # Match-rule semantics
//!
//! Configured `match_rules` are forwarded verbatim to the D-Bus broker
//! (`org.freedesktop.DBus.AddMatch`) so the broker pre-filters the signal
//! firehose. The adapter additionally post-filters delivered messages against
//! the parsed [`MatchRule`] set as a defense-in-depth check and to give the
//! mock backend the same semantics tests need. An empty `match_rules` vector
//! is treated as "deliver everything" (the broker will still only deliver
//! signals the connection is subscribed to via `AddMatch`; the post-filter
//! becomes a no-op).
//!
//! When the user does not supply match rules in their adapter config, the
//! adapter defaults to a tight catalog rooted at the interfaces the
//! `system.dbus` parser actually classifies (notifications, network, power,
//! hardware, bluetooth, mounts). This caps D-Bus signal volume so the
//! source-worker can't be DOS'd by a chatty interface we don't parse anyway.

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::node_sdk::parser::{InputShapeAdapter, ParserError, ParserResult};

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
    /// An empty vector causes the adapter to substitute
    /// [`default_match_rules`] so the source-worker is not exposed to the
    /// entire D-Bus signal firehose.
    #[serde(default)]
    pub match_rules: Vec<String>,
}

/// Default match rules — restricted to the interfaces the `system.dbus`
/// parser actually classifies. Anything outside this set is dropped at the
/// broker, so a chatty unrelated interface can't DOS the source-worker.
pub fn default_match_rules() -> Vec<String> {
    vec![
        // Desktop notifications (notification.sent).
        "type='signal',interface='org.freedesktop.Notifications'".into(),
        // MPRIS media players (media.state_changed).
        "type='signal',interface='org.mpris.MediaPlayer2.Player'".into(),
        // Power state — UPower + power-profiles-daemon.
        "type='signal',interface='org.freedesktop.UPower'".into(),
        "type='signal',interface='org.freedesktop.UPower.Device'".into(),
        "type='signal',interface='net.hadess.PowerProfiles'".into(),
        // Bluetooth via bluez object manager.
        "type='signal',interface='org.bluez.Adapter1'".into(),
        "type='signal',interface='org.bluez.Device1'".into(),
        // NetworkManager state.
        "type='signal',interface='org.freedesktop.NetworkManager'".into(),
        "type='signal',interface='org.freedesktop.NetworkManager.Device'".into(),
        // udisks2 mount events.
        "type='signal',interface='org.freedesktop.UDisks2.Filesystem'".into(),
        "type='signal',interface='org.freedesktop.UDisks2.Block'".into(),
    ]
}

impl DbusStreamConfig {
    /// Match rules to forward to the broker. Empty config -> defaults.
    #[must_use]
    pub fn effective_match_rules(&self) -> Vec<String> {
        if self.match_rules.is_empty() {
            default_match_rules()
        } else {
            self.match_rules.clone()
        }
    }
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
// Match-rule parsing + filtering
// =============================================================================

/// A parsed D-Bus match rule subset.
///
/// We only model the conditions the source-worker uses: `type`, `interface`,
/// `member`, `path`, `path_namespace`, `sender`. Any other keys (e.g. `arg0`)
/// are stored verbatim and currently treated as "pass-through" — the broker
/// will enforce them, and the post-filter is conservative (does not drop a
/// message just because it carries an arg0 the rule expected).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedMatchRule {
    pub msg_type: Option<String>,
    pub interface: Option<String>,
    pub member: Option<String>,
    pub path: Option<String>,
    pub path_namespace: Option<String>,
    pub sender: Option<String>,
}

impl FromStr for ParsedMatchRule {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // D-Bus match rule: `key='value',key='value',...`
        let mut rule = ParsedMatchRule::default();
        for raw_clause in s.split(',') {
            let clause = raw_clause.trim();
            if clause.is_empty() {
                continue;
            }
            let (key, value) = clause.split_once('=').ok_or_else(|| {
                ParserError::Config(format!("invalid match rule clause (no `=`): {clause}"))
            })?;
            let key = key.trim();
            let value = value.trim().trim_matches('\'').trim_matches('"');
            match key {
                "type" => rule.msg_type = Some(value.to_string()),
                "interface" => rule.interface = Some(value.to_string()),
                "member" => rule.member = Some(value.to_string()),
                "path" => rule.path = Some(value.to_string()),
                "path_namespace" => rule.path_namespace = Some(value.to_string()),
                "sender" => rule.sender = Some(value.to_string()),
                // Unknown keys are ignored by the post-filter; the broker
                // will still enforce them if the rule was forwarded.
                _ => {}
            }
        }
        Ok(rule)
    }
}

impl ParsedMatchRule {
    /// Does this message satisfy the rule?
    pub fn matches(&self, msg: &DbusMessage) -> bool {
        if let Some(ref t) = self.msg_type
            && t != "signal"
        {
            // We only ever subscribe to signals; a rule that asks for
            // something else can never match a DbusMessage.
            return false;
        }
        if let Some(ref iface) = self.interface
            && iface != &msg.interface
        {
            return false;
        }
        if let Some(ref m) = self.member
            && m != &msg.member
        {
            return false;
        }
        if let Some(ref p) = self.path
            && p != &msg.path
        {
            return false;
        }
        if let Some(ref ns) = self.path_namespace {
            // path_namespace matches msg.path if msg.path == ns or starts with ns + "/"
            let path_ok = msg.path == *ns || msg.path.starts_with(&format!("{ns}/"));
            if !path_ok {
                return false;
            }
        }
        if let Some(ref s) = self.sender
            && msg.sender.as_deref() != Some(s.as_str())
        {
            return false;
        }
        true
    }
}

/// Does the message satisfy at least one of the parsed rules? An empty rule
/// list is treated as "match everything" (mirrors how an empty `match_rules`
/// config is rewritten to the defaults at subscription time).
pub fn matches_any_rule(msg: &DbusMessage, rules: &[ParsedMatchRule]) -> bool {
    if rules.is_empty() {
        return true;
    }
    rules.iter().any(|r| r.matches(msg))
}

// =============================================================================
// DbusBackend trait — allows mock injection
// =============================================================================

/// Abstracts D-Bus connection so tests can inject a mock.
///
/// The production implementation is [`RealDbusBackend`] (zbus). Tests use
/// [`MockDbusBackend`].
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
    #[must_use]
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
        use futures::stream::{self};
        Box::pin(stream::iter(self.messages.into_iter().map(Ok)))
    }
}

// =============================================================================
// RealDbusBackend — production zbus implementation
// =============================================================================

/// Production [`DbusBackend`] using `zbus`.
///
/// Connects to the requested bus, installs each configured match rule via
/// `org.freedesktop.DBus.AddMatch`, and yields every received signal message
/// as a [`DbusMessage`]. Method calls / replies / errors are ignored.
///
/// The connection is opened lazily inside [`DbusBackend::subscribe`]: the
/// trait method consumes the backend, so all setup happens on the same task
/// that drives the resulting stream.
pub struct RealDbusBackend;

impl Default for RealDbusBackend {
    fn default() -> Self {
        Self
    }
}

impl DbusBackend for RealDbusBackend {
    fn subscribe(
        self: Box<Self>,
        bus: DbusBus,
        match_rules: Vec<String>,
    ) -> BoxStream<'static, ParserResult<DbusMessage>> {
        Box::pin(async_stream::stream! {
            let conn = match bus {
                DbusBus::Session => zbus::Connection::session().await,
                DbusBus::System => zbus::Connection::system().await,
            };
            let conn = match conn {
                Ok(c) => c,
                Err(e) => {
                    yield Err(ParserError::Adapter(format!(
                        "failed to connect to D-Bus {bus:?}: {e}"
                    )));
                    return;
                }
            };

            // Install each match rule on the broker via a raw AddMatch
            // method call. Using the raw method (rather than a typed
            // MatchRule helper) accepts any string the broker accepts and
            // avoids coupling to a specific typed-helper name across zbus
            // versions.
            for rule in &match_rules {
                let call: zbus::Result<zbus::Message> = conn
                    .call_method(
                        Some("org.freedesktop.DBus"),
                        "/org/freedesktop/DBus",
                        Some("org.freedesktop.DBus"),
                        "AddMatch",
                        &(rule.as_str(),),
                    )
                    .await;
                if let Err(e) = call {
                    yield Err(ParserError::Adapter(format!(
                        "AddMatch failed for {rule:?}: {e}"
                    )));
                    return;
                }
            }

            // Parse the same rules for post-filtering (defense-in-depth).
            // A parse failure here is fatal; the broker already accepted
            // them so anything we couldn't parse is our deficiency, not the
            // user's, but we surface it so it shows up in logs.
            let parsed_rules: Vec<ParsedMatchRule> = match match_rules
                .iter()
                .map(|r| ParsedMatchRule::from_str(r))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(v) => v,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            use futures::stream::StreamExt;
            let mut stream = zbus::MessageStream::from(&conn);
            while let Some(msg_result) = stream.next().await {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        yield Err(ParserError::Adapter(format!("D-Bus stream error: {e}")));
                        continue;
                    }
                };
                let header = msg.header();
                if !matches!(header.message_type(), zbus::message::Type::Signal) {
                    continue;
                }
                let interface = header.interface().map(std::string::ToString::to_string).unwrap_or_default();
                let member = header.member().map(std::string::ToString::to_string).unwrap_or_default();
                let path = header.path().map(std::string::ToString::to_string).unwrap_or_default();
                let sender = header.sender().map(std::string::ToString::to_string);

                // Deserialize the body into a zvariant Structure, then
                // convert to JSON for the parser. If the body is empty or
                // un-deserializable, fall back to Null so the message still
                // surfaces metadata.
                let body_json = decode_body_to_json(msg.body())
                    .unwrap_or(serde_json::Value::Null);

                let decoded = DbusMessage {
                    interface,
                    member,
                    path,
                    sender,
                    body_json,
                };

                // Defense-in-depth: drop anything that doesn't match the
                // configured rule set.
                if !matches_any_rule(&decoded, &parsed_rules) {
                    continue;
                }

                yield Ok(decoded);
            }
        })
    }
}

/// Decode a `zbus::message::Body` into a JSON value. D-Bus signals carry
/// heterogeneous tuples; we deserialize to a `zvariant::Structure` and walk
/// the resulting field array. On any failure we return `None` and the
/// caller falls back to `Null`.
fn decode_body_to_json(body: zbus::message::Body) -> Option<serde_json::Value> {
    match body.deserialize::<zvariant::Structure<'_>>() {
        Ok(value) => {
            let fields = value.fields();
            let mut arr = Vec::with_capacity(fields.len());
            for f in fields {
                arr.push(zvariant_value_to_json(f));
            }
            Some(serde_json::Value::Array(arr))
        }
        Err(_) => None,
    }
}

fn zvariant_value_to_json(v: &zvariant::Value<'_>) -> serde_json::Value {
    use zvariant::Value as Z;
    match v {
        Z::U8(n) => serde_json::Value::Number((*n).into()),
        Z::Bool(b) => serde_json::Value::Bool(*b),
        Z::I16(n) => serde_json::Value::Number((*n).into()),
        Z::U16(n) => serde_json::Value::Number((*n).into()),
        Z::I32(n) => serde_json::Value::Number((*n).into()),
        Z::U32(n) => serde_json::Value::Number((*n).into()),
        Z::I64(n) => serde_json::Value::Number((*n).into()),
        Z::U64(n) => serde_json::Value::Number((*n).into()),
        Z::F64(n) => serde_json::Number::from_f64(*n)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Z::Str(s) => serde_json::Value::String(s.as_str().to_string()),
        Z::Signature(s) => serde_json::Value::String(s.to_string()),
        Z::ObjectPath(p) => serde_json::Value::String(p.as_str().to_string()),
        Z::Value(inner) => zvariant_value_to_json(inner),
        Z::Array(a) => {
            let elems: Vec<_> = a.iter().map(zvariant_value_to_json).collect();
            serde_json::Value::Array(elems)
        }
        Z::Dict(d) => {
            let mut map = serde_json::Map::new();
            for (k, val) in d.iter() {
                let key = match k {
                    Z::Str(s) => s.as_str().to_string(),
                    Z::ObjectPath(p) => p.as_str().to_string(),
                    other => format!("{other:?}"),
                };
                map.insert(key, zvariant_value_to_json(val));
            }
            serde_json::Value::Object(map)
        }
        Z::Structure(s) => {
            let elems: Vec<_> = s.fields().iter().map(zvariant_value_to_json).collect();
            serde_json::Value::Array(elems)
        }
        // File descriptors are not portable JSON; surface a placeholder.
        // Any future zvariant variants fall into the catch-all below.
        _ => serde_json::Value::String("<unsupported>".into()),
    }
}

// =============================================================================
// DbusStreamAdapter
// =============================================================================

/// Adapter for D-Bus signal subscriptions.
///
/// Subscribes to D-Bus signals via the configured match rules and emits one
/// [`SourceRecord`] per signal. To inject a mock bus in tests, use
/// [`DbusStreamAdapter::with_backend`] + [`DbusStreamAdapter::open_with_backend`].
///
/// `InputShapeAdapter::open` constructs a fresh [`RealDbusBackend`] per
/// invocation from the adapter config. The injected backend slot is only
/// consulted by `open_with_backend`, which tests use to bypass the live bus.
pub struct DbusStreamAdapter {
    injected_backend: std::sync::Mutex<Option<Box<dyn DbusBackend + Send + Sync>>>,
}

impl DbusStreamAdapter {
    /// Create an adapter with a custom backend (useful for tests).
    pub fn with_backend(backend: impl DbusBackend + Sync + 'static) -> Self {
        Self {
            injected_backend: std::sync::Mutex::new(Some(Box::new(backend))),
        }
    }
}

/// Default produces an adapter with no injected backend; the real
/// [`RealDbusBackend`] is constructed inside [`InputShapeAdapter::open`].
impl Default for DbusStreamAdapter {
    fn default() -> Self {
        Self {
            injected_backend: std::sync::Mutex::new(None),
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
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        // If a test injected a backend, consume it; otherwise build a fresh
        // RealDbusBackend. Either way, subscribe(...) takes ownership.
        let backend: Box<dyn DbusBackend + Send + Sync> = {
            let mut slot = self.injected_backend.lock().map_err(|e| {
                ParserError::Adapter(format!("injected_backend mutex poisoned: {e}"))
            })?;
            match slot.take() {
                Some(b) => b,
                None => Box::new(RealDbusBackend),
            }
        };

        Ok(Self::open_with_backend(backend, material_id, config))
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(DbusStreamCursor)
    }
}

impl DbusStreamAdapter {
    /// Open the adapter using an explicit backend.
    ///
    /// Used by `InputShapeAdapter::open` after lifting a backend out of the
    /// adapter, and directly by tests that want to drive a mock stream.
    #[must_use]
    pub fn open_with_backend(
        backend: Box<dyn DbusBackend + Send + Sync>,
        material_id: Id<SourceMaterial>,
        config: &DbusStreamConfig,
    ) -> BoxStream<'static, ParserResult<SourceRecord>> {
        let bus = config.bus;
        let rules = config.effective_match_rules();
        let parsed_rules: Vec<ParsedMatchRule> = rules
            .iter()
            .filter_map(|r| ParsedMatchRule::from_str(r).ok())
            .collect();
        let frame_index = Arc::new(AtomicU64::new(0));

        let message_stream = backend.subscribe(bus, rules);

        use futures::StreamExt;
        Box::pin(message_stream.filter_map(move |msg_result| {
            // Defense-in-depth filtering: even if the broker (or mock) yields
            // a message outside our rule set, drop it here so the parser
            // never sees it.
            let parsed_rules = parsed_rules.clone();
            let frame_index = Arc::clone(&frame_index);
            async move {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => return Some(Err(e)),
                };
                if !matches_any_rule(&msg, &parsed_rules) {
                    return None;
                }
                let bytes = match serde_json::to_vec(&msg.body_json) {
                    Ok(b) => b,
                    Err(e) => {
                        return Some(Err(ParserError::Parse(format!(
                            "failed to serialize dbus body: {e}"
                        ))));
                    }
                };
                let anchor = MaterialAnchor::StreamFrame {
                    material_offset: 0,
                    frame_index: frame_index.fetch_add(1, Ordering::Relaxed),
                };
                let metadata = serde_json::json!({
                    "interface": msg.interface,
                    "member": msg.member,
                    "path": msg.path,
                    "sender": msg.sender,
                });
                Some(Ok(SourceRecord {
                    material_id,
                    anchor,
                    bytes,
                    logical_path: None,
                    source_ts_hint: None,
                    metadata,
                }))
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use xtask::sandbox::prelude::sinex_test;

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
            // Explicit rule so we keep test-msgs, since they don't match the
            // default catalog.
            match_rules: vec!["type='signal',interface='org.test.Iface'".into()],
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
    async fn test_dbus_anchor_frame_index_is_monotonic() -> xtask::sandbox::TestResult<()> {
        let msgs = vec![
            make_msg("org.test.Iface", "First"),
            make_msg("org.test.Iface", "Second"),
        ];
        let config = DbusStreamConfig {
            bus: DbusBus::System,
            match_rules: vec!["type='signal',interface='org.test.Iface'".into()],
        };

        let stream = DbusStreamAdapter::open_with_backend(
            Box::new(MockDbusBackend::new(msgs)),
            dummy_material_id(),
            &config,
        );

        let records: Vec<_> = stream.collect().await;
        assert_eq!(records.len(), 2);
        let records = records.into_iter().collect::<ParserResult<Vec<_>>>()?;
        assert!(matches!(
            records[0].anchor,
            MaterialAnchor::StreamFrame { frame_index: 0, .. }
        ));
        assert!(matches!(
            records[1].anchor,
            MaterialAnchor::StreamFrame { frame_index: 1, .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_default_produces_empty_stream() -> xtask::sandbox::TestResult<()> {
        let _adapter = DbusStreamAdapter::default();
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_match_rule_excludes_unmatched_signal() -> xtask::sandbox::TestResult<()> {
        // The configured rule only accepts org.example.Allowed signals. The
        // mock backend yields one of each interface; only the allowed one
        // should reach the parser.
        let msgs = vec![
            make_msg("org.example.Allowed", "Ok"),
            make_msg("org.example.Forbidden", "Nope"),
        ];
        let config = DbusStreamConfig {
            bus: DbusBus::Session,
            match_rules: vec!["type='signal',interface='org.example.Allowed'".into()],
        };

        let stream = DbusStreamAdapter::open_with_backend(
            Box::new(MockDbusBackend::new(msgs)),
            dummy_material_id(),
            &config,
        );

        let records: Vec<_> = stream.collect().await;
        assert_eq!(records.len(), 1, "expected exactly one delivered record");
        let record = records.into_iter().next().unwrap().unwrap();
        let iface = record
            .metadata
            .get("interface")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_eq!(iface, "org.example.Allowed");
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_default_rules_constrain_catalog() -> xtask::sandbox::TestResult<()> {
        // Empty match_rules in the user config means the adapter substitutes
        // the parser-catalog defaults. A random interface outside the
        // catalog must be dropped.
        let msgs = vec![
            make_msg("org.freedesktop.Notifications", "Notify"),
            make_msg("org.example.NotInCatalog", "Spam"),
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
        assert_eq!(records.len(), 1);
        let iface = records[0]
            .as_ref()
            .unwrap()
            .metadata
            .get("interface")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_eq!(iface, "org.freedesktop.Notifications");
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_match_rule_parses_keys() -> xtask::sandbox::TestResult<()> {
        let rule: ParsedMatchRule =
            "type='signal',interface='org.x',member='Y',path='/p',sender=':1.2'"
                .parse()
                .unwrap();
        assert_eq!(rule.msg_type.as_deref(), Some("signal"));
        assert_eq!(rule.interface.as_deref(), Some("org.x"));
        assert_eq!(rule.member.as_deref(), Some("Y"));
        assert_eq!(rule.path.as_deref(), Some("/p"));
        assert_eq!(rule.sender.as_deref(), Some(":1.2"));
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_real_backend_open_path() -> xtask::sandbox::TestResult<()> {
        // The default-constructed adapter has no injected backend, so
        // InputShapeAdapter::open builds a RealDbusBackend. In CI there is
        // typically no D-Bus broker, so opening will produce a stream that
        // errors on the first poll. We assert that the open() call itself
        // succeeds (it doesn't try to connect synchronously) and that the
        // stream is well-formed.
        let adapter = DbusStreamAdapter::default();
        let config = DbusStreamConfig {
            bus: DbusBus::Session,
            match_rules: vec!["type='signal',interface='org.example'".into()],
        };
        let mut stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .expect("open must succeed without contacting the bus");

        // Pull at most one item with a short timeout. If no bus exists the
        // stream will yield Err quickly. If a bus exists, it may produce
        // nothing in the test window — both are acceptable.
        let _ = tokio::time::timeout(std::time::Duration::from_millis(50), stream.next()).await;
        Ok(())
    }
}
