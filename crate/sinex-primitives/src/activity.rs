use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ActivitySourceKind {
    Unknown,
    Window,
    Browser,
    Terminal,
}

impl ActivitySourceKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Window => "window",
            Self::Browser => "browser",
            Self::Terminal => "terminal",
        }
    }
}

impl fmt::Display for ActivitySourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[must_use]
pub fn classify_trusted_activity_signal(
    source: &str,
    event_type: &str,
) -> Option<ActivitySourceKind> {
    match (source, event_type) {
        ("wm.hyprland", "window.focused") | ("activitywatch", "window.active") => {
            Some(ActivitySourceKind::Window)
        }
        ("activitywatch", "browser.tab.active") | ("webhistory", "page.visited") => {
            Some(ActivitySourceKind::Browser)
        }
        (source, "command.executed")
            if source.starts_with("shell.") || source == "shell.history" =>
        {
            Some(ActivitySourceKind::Terminal)
        }
        _ => None,
    }
}

#[must_use]
pub fn primary_activity_source(counts: &BTreeMap<ActivitySourceKind, u64>) -> ActivitySourceKind {
    counts
        .iter()
        .max_by(|(left_key, left_count), (right_key, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_key.as_str().cmp(left_key.as_str()))
        })
        .map_or(ActivitySourceKind::Unknown, |(key, _)| *key)
}
