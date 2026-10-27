//! Keyboard and pointer input as the input logger (scribe-tap) wrote it: one
//! event per logged record, pointer motion included.
//!
//! The logger's records have no IDs. A record is identified by its logger
//! session, its times, what it reports and `n`, its number among
//! records of that session identical in all of those (a mouse writes many
//! equal motion records within one millisecond). The parser numbers records in
//! file order, so a re-read from the start numbers them the same way and is a
//! repeat.
use crate::{Class, KeyKind, ReplaceRule, TimeQuality, Timestamp, TsOrig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A key went down, or repeated while held.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct KeyPressed {
    /// The logger process that wrote the record, one per input device.
    pub session: String,
    /// When the logger wrote the record (milliseconds).
    pub logged_at: Timestamp,
    /// The kernel's time of the key event (microseconds): the keystroke's own
    /// timing. Older logs do not have it. The logger's time can trail it by
    /// seconds when the pipeline backs up.
    pub kernel_at: Option<Timestamp>,
    /// The key's name as the logger wrote it (`KEY_A`, or `KEY_106` when it
    /// had no name for the code).
    pub key: String,
    /// The kernel's key code; absent in older logs.
    pub code: Option<u16>,
    /// The kernel's value: 1 pressed, 2 repeated while held; absent in older
    /// logs, which logged presses only.
    pub value: Option<i32>,
    /// Whether the key changed the logger's typed-text buffer.
    pub changed: Option<bool>,
    /// The clipboard text the logger read when the key was a paste.
    pub clipboard: Option<String>,
    /// The focused window, as older logs recorded it.
    pub window: Option<String>,
    pub n: u32,
    /// Fields this parser does not know yet, kept by name.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A key went up.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct KeyReleased {
    pub session: String,
    pub logged_at: Timestamp,
    /// The kernel's time of the key event; see [`KeyPressed::kernel_at`].
    pub kernel_at: Option<Timestamp>,
    pub key: String,
    pub code: Option<u16>,
    pub value: Option<i32>,
    pub changed: Option<bool>,
    pub n: u32,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

const KEY_FIELDS: &[&str] = &["session", "logged_at", "kernel_at", "key", "value", "n"];

/// The kernel's time when the record has it, else the logger's.
fn key_time(kernel_at: Option<Timestamp>, logged_at: Timestamp) -> TsOrig {
    TsOrig::known(kernel_at.unwrap_or(logged_at), TimeQuality::Captured)
}

impl crate::EventPayload for KeyPressed {
    const VERSION: u64 = 2;
    const EVENT_TYPE: &'static str = "key.pressed";
    const KEY: KeyKind = KeyKind::Fields(KEY_FIELDS);
    const REPLACE: ReplaceRule = ReplaceRule::SuppressRepeat;
    const FIELD_HINTS: &'static [(&'static str, Class)] = &[
        ("/session", Class::Guarded),
        ("/logged_at", Class::Guarded),
        ("/kernel_at", Class::Guarded),
        ("/key", Class::Guarded),
        ("/code", Class::Guarded),
        ("/value", Class::Guarded),
        ("/changed", Class::Guarded),
        ("/clipboard", Class::Guarded),
        ("/window", Class::Guarded),
        ("/n", Class::Guarded),
        ("/extra", Class::Guarded),
    ];

    fn ts_orig(&self) -> TsOrig {
        key_time(self.kernel_at, self.logged_at)
    }

    fn declaration() -> crate::KindDeclaration {
        crate::KindDeclaration {
            kind: crate::KindRef {
                name: Self::EVENT_TYPE.into(),
                version: Self::VERSION,
            },
            declaring_producer: "sinex".into(),
            payload_schema: Some(Self::payload_schema()),
            normalization: crate::kinds::Normalization::Rust,
            key_rule: Some(crate::kinds::KeyRule::Fields {
                fields: KEY_FIELDS.iter().map(|f| (*f).into()).collect(),
            }),
            replace_rule: Some(Self::REPLACE),
            time_rule: Some(crate::kinds::TimeRule::FirstPresent {
                fields: vec!["kernel_at".into(), "logged_at".into()],
                quality: crate::TimeQuality::Captured,
            }),
            field_hints: Self::FIELD_HINTS
                .iter()
                .map(|(field, class)| crate::kinds::FieldHint {
                    field: (*field).into(),
                    class: *class,
                })
                .collect(),
            removed_rule: Some(crate::kinds::RemovedRule::Never),
            keep_known: vec![],
            owned_blobs: vec![],
            search: Self::SEARCH,
        }
    }

    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("schemas serialize")
    }
}

impl crate::EventPayload for KeyReleased {
    const VERSION: u64 = 2;
    const EVENT_TYPE: &'static str = "key.released";
    const KEY: KeyKind = KeyKind::Fields(KEY_FIELDS);
    const REPLACE: ReplaceRule = ReplaceRule::SuppressRepeat;
    const FIELD_HINTS: &'static [(&'static str, Class)] = &[
        ("/session", Class::Guarded),
        ("/logged_at", Class::Guarded),
        ("/kernel_at", Class::Guarded),
        ("/key", Class::Guarded),
        ("/code", Class::Guarded),
        ("/value", Class::Guarded),
        ("/changed", Class::Guarded),
        ("/n", Class::Guarded),
        ("/extra", Class::Guarded),
    ];

    fn ts_orig(&self) -> TsOrig {
        key_time(self.kernel_at, self.logged_at)
    }

    fn declaration() -> crate::KindDeclaration {
        crate::KindDeclaration {
            kind: crate::KindRef {
                name: Self::EVENT_TYPE.into(),
                version: Self::VERSION,
            },
            declaring_producer: "sinex".into(),
            payload_schema: Some(Self::payload_schema()),
            normalization: crate::kinds::Normalization::Rust,
            key_rule: Some(crate::kinds::KeyRule::Fields {
                fields: KEY_FIELDS.iter().map(|f| (*f).into()).collect(),
            }),
            replace_rule: Some(Self::REPLACE),
            time_rule: Some(crate::kinds::TimeRule::FirstPresent {
                fields: vec!["kernel_at".into(), "logged_at".into()],
                quality: crate::TimeQuality::Captured,
            }),
            field_hints: Self::FIELD_HINTS
                .iter()
                .map(|(field, class)| crate::kinds::FieldHint {
                    field: (*field).into(),
                    class: *class,
                })
                .collect(),
            removed_rule: Some(crate::kinds::RemovedRule::Never),
            keep_known: vec![],
            owned_blobs: vec![],
            search: Self::SEARCH,
        }
    }

    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("schemas serialize")
    }
}

/// A pointer button went down or up. The logger records no kernel time for
/// pointer records, so the time is the logger's.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "pointer.button",
    key = ["session", "logged_at", "button", "pressed", "n"],
    replace = repeat,
    time = logged_at,
    quality = captured,
    class = guarded
)]
pub struct PointerButton {
    pub session: String,
    pub logged_at: Timestamp,
    /// `BTN_LEFT`, `BTN_RIGHT`, `BTN_SIDE`…
    pub button: String,
    pub pressed: bool,
    /// The kernel's value as logged (1 down, 0 up).
    pub value: i64,
    pub n: u32,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// The pointer moved along one axis: a relative step (`REL_X`, `REL_Y`) or an
/// absolute position (`ABS_X`, a tablet's).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "pointer.moved",
    key = ["session", "logged_at", "axis", "value", "n"],
    replace = repeat,
    time = logged_at,
    quality = captured,
    class = guarded
)]
pub struct PointerMoved {
    pub session: String,
    pub logged_at: Timestamp,
    pub axis: String,
    /// The step (relative axes) or the position (absolute axes).
    pub value: i64,
    pub absolute: bool,
    pub n: u32,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A scroll wheel turned: `REL_WHEEL` and `REL_HWHEEL` in detents,
/// `REL_WHEEL_HI_RES` and `REL_HWHEEL_HI_RES` in 1/120 of one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "pointer.scrolled",
    key = ["session", "logged_at", "axis", "value", "n"],
    replace = repeat,
    time = logged_at,
    quality = captured,
    class = guarded
)]
pub struct PointerScrolled {
    pub session: String,
    pub logged_at: Timestamp,
    pub axis: String,
    pub value: i64,
    pub n: u32,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A record about the logger itself (`start`, `stop`, a `focus` change in
/// older logs, a typed-text `snapshot`), or one of a kind this parser has no
/// type for, with its fields as written.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "input.logger",
    key = ["session", "logged_at", "event", "n"],
    replace = repeat,
    time = logged_at,
    quality = captured,
    class = guarded
)]
pub struct InputLogger {
    pub session: String,
    pub logged_at: Timestamp,
    /// The record's `event` as written.
    pub event: String,
    pub n: u32,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub fields: Map<String, Value>,
}
