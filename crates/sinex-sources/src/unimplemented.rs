//! Catalog sources Sinex has no parser for yet, with the canonical locations
//! where their provider keeps its data. Discovery checks those locations so
//! the dry run can say which unbuilt sources this host actually has: the
//! catalog is a floor, and this census makes it measurable. An entry is
//! removed when its source is built.
use crate::discovery::{DiscoveryContext, Location};
use serde::Serialize;
use std::path::PathBuf;

/// A catalog source without an implementation.
pub struct Unimplemented {
    pub name: &'static str,
    pub purpose: &'static str,
    /// Where the provider keeps the data the source would read.
    pub locations: &'static [Location],
}

const fn at(root: &'static str, path: &'static str) -> Location {
    Location { root, path }
}

/// Catalog sources from the source families page
/// whose provider has a canonical location on a host.
pub const UNIMPLEMENTED: &[Unimplemented] = &[
    Unimplemented {
        name: "terminal.nushell-history",
        purpose: "shell recall",
        locations: &[
            at("config", "nushell/history.sqlite3"),
            at("config", "nushell/history.txt"),
        ],
    },
    Unimplemented {
        name: "browser.history.firefox",
        purpose: "browsing history and bookmarks",
        locations: &[at("home", ".mozilla/firefox")],
    },
    Unimplemented {
        name: "browser.history.qutebrowser",
        purpose: "browsing history",
        locations: &[at("data", "qutebrowser/history.sqlite")],
    },
    Unimplemented {
        name: "desktop.window-manager.sway",
        purpose: "window and workspace state",
        locations: &[at("environment", "SWAYSOCK")],
    },
    Unimplemented {
        name: "desktop.window-manager.niri",
        purpose: "window and workspace state",
        locations: &[at("environment", "NIRI_SOCKET")],
    },
    Unimplemented {
        name: "notes.logseq",
        purpose: "notes and their revisions",
        locations: &[at("home", ".logseq")],
    },
    Unimplemented {
        name: "editor.vscode",
        purpose: "editor sessions and recent files",
        locations: &[at("config", "Code/User/globalStorage/state.vscdb")],
    },
    Unimplemented {
        name: "chat.telegram",
        purpose: "chat messages",
        locations: &[at("data", "TelegramDesktop")],
    },
    Unimplemented {
        name: "chat.signal",
        purpose: "chat messages",
        locations: &[at("config", "Signal/sql/db.sqlite")],
    },
    Unimplemented {
        name: "games.steam",
        purpose: "play sessions",
        locations: &[at("data", "Steam/logs")],
    },
    Unimplemented {
        name: "video.mpv",
        purpose: "what was watched and where playback stopped",
        locations: &[at("config", "mpv/watch_later")],
    },
    Unimplemented {
        name: "reading.zotero",
        purpose: "the reading library and its annotations",
        locations: &[at("home", "Zotero/zotero.sqlite")],
    },
];

/// An unbuilt source whose provider's data exists on this host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Applicable {
    pub name: &'static str,
    pub purpose: &'static str,
    pub found: Vec<PathBuf>,
}

/// The unbuilt sources whose canonical locations exist on this host. Only
/// metadata is read; nothing is opened.
pub fn census(context: &DiscoveryContext) -> Vec<Applicable> {
    UNIMPLEMENTED
        .iter()
        .filter_map(|source| {
            let found: Vec<PathBuf> = source
                .locations
                .iter()
                .flat_map(|location| crate::discovery::paths(location, context).unwrap_or_default())
                .filter(|path| std::fs::symlink_metadata(path).is_ok())
                .collect();
            (!found.is_empty()).then_some(Applicable {
                name: source.name,
                purpose: source.purpose,
                found,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
