//! What applications expose to assistive technology (AT-SPI): focus moving
//! between controls, text changing in them, and the focused window's tree of
//! controls. Accessibility text is what was on screen without reading pixels.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Focus moved to a control.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "accessibility.focused", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct AccessibilityFocused {
    pub received_at: Timestamp,
    pub app: Option<String>,
    /// The control's role (`push button`, `entry`, `document web`…).
    pub role: Option<String>,
    pub name: Option<String>,
    /// The title of the window holding the control.
    pub window: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// Text in a control changed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "accessibility.text_changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct AccessibilityTextChanged {
    pub received_at: Timestamp,
    pub app: Option<String>,
    pub role: Option<String>,
    pub name: Option<String>,
    /// `insert` or `delete`.
    pub change_type: Option<String>,
    /// The inserted or deleted text.
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// The focused window's tree of controls, read whole. The tree itself stays
/// in the stored bytes; the event carries its size and its text: every
/// control's name and text, in tree order, each distinct line once.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "accessibility.tree", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct AccessibilityTree {
    pub received_at: Timestamp,
    pub app: Option<String>,
    pub window: Option<String>,
    pub controls: u64,
    /// Controls whose text the capture cut short.
    pub truncated: u64,
    pub text: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
