//! Every source, one module per family, registered in one list.
pub mod activitywatch;
pub mod arbtt;
pub mod asciinema;
pub mod atuin;
pub mod calendar;
pub mod chromium;
/// The Wayland clipboard and primary selection.
pub mod clipboard;
/// Capture from the session bus, shared by the D-Bus sources.
pub mod dbus;
pub(crate) mod fields;
pub mod file_watch;
pub mod forecast;
pub mod goodreads;
pub mod google_takeout;
pub mod hyprland;
pub mod journald;
/// The media library: scans of library roots and library requests.
pub mod library;
/// The machine telemetry collector's SQLite databases.
pub mod machine_telemetry;
pub mod mail;
/// Live mail over IMAP, with IDLE and XOAUTH2.
pub mod mail_imap;
/// Screen stills and audio segments on the `Media` driver.
pub mod media_capture;
pub mod messenger;
/// Media players (MPRIS) on the session bus.
pub mod mpris;
pub mod notes;
/// Desktop notifications on the session bus.
pub mod notifications;
pub mod operator;
pub mod outlook;
/// The forward crawl of browser-history pages, opt-in per domain.
pub mod page_crawl;
pub mod raindrop;
pub mod reddit;
/// Commits in local repositories (`code.git`).
pub mod repository;
pub mod samsung_cloud;
pub mod samsung_health;
pub mod samsung_sms;
/// The session integration socket: Neovim and kitty integrations.
pub mod session_socket;
/// The Sinnix capture lake's lanes.
pub mod sinnix_lake;
pub mod sleep_as_android;
pub mod spotify;
pub mod takeout_chrome;
pub mod weechat;
pub mod wykop;
pub mod zsh;

use crate::binding::{Binding, InputFormat, Settings};
use crate::registry::{Source, SourceSpec};

/// Every input format and the sources that read it: the one list the
/// catalog, backfill, the binding runner, `sinex-session` and replay all
/// work from.
pub fn formats() -> &'static [&'static InputFormat] {
    &[
        &atuin::FORMAT,
        &zsh::FORMAT,
        &hyprland::FORMAT,
        &journald::FORMAT,
        &machine_telemetry::FORMAT,
        &file_watch::FORMAT,
        &activitywatch::FORMAT,
        &repository::FORMAT,
        &chromium::FORMAT,
        &takeout_chrome::FORMAT,
        &spotify::FORMAT,
        &raindrop::FORMAT,
        &notes::FORMAT,
        &mail::MBOX_FORMAT,
        &mail::MAILDIR_FORMAT,
        &mail_imap::FORMAT,
        &outlook::FORMAT,
        &outlook::CSV_FORMAT,
        &weechat::FORMAT,
        &calendar::FORMAT,
        &google_takeout::FORMAT,
        &goodreads::FORMAT,
        &messenger::FORMAT,
        &sleep_as_android::FORMAT,
        &wykop::FORMAT,
        &reddit::FORMAT,
        &samsung_health::FORMAT,
        &samsung_sms::FORMAT,
        &samsung_cloud::FORMAT,
        &arbtt::FORMAT,
        &asciinema::FORMAT,
        &operator::RAW_LOG_FORMAT,
        &operator::STEERING_FORMAT,
        &sinnix_lake::FORMAT,
        &notifications::FORMAT,
        &mpris::FORMAT,
        &clipboard::FORMAT,
        &session_socket::FORMAT,
        &media_capture::FORMAT,
        &library::FORMAT,
        &forecast::FORMAT,
        &page_crawl::FORMAT,
    ]
}

/// Every source's declaration, each once.
pub fn specs() -> Vec<&'static SourceSpec> {
    let mut out: Vec<&'static SourceSpec> = Vec::new();
    for format in formats() {
        for spec in (format.specs)() {
            if !out
                .iter()
                .any(|s| s.name == spec.name && s.parser == spec.parser)
            {
                out.push(spec);
            }
        }
    }
    out
}

/// The format named `name` (`--read NAME=PATH`).
pub fn format(name: &str) -> Option<&'static InputFormat> {
    formats().iter().copied().find(|k| k.name == name)
}

/// The format that reads the source `spec_name`.
///
/// Several formats can read one source name (the live clipboard and its lake
/// lane); this is the first registered, and a configuration names the format
/// to choose another.
pub fn format_of(spec_name: &str) -> Option<&'static InputFormat> {
    formats().iter().copied().find(|k| k.reads(spec_name))
}

/// The format reading the source `name` with `parser`. Neither is unique
/// alone: a name is shared by one source read two ways (the live clipboard
/// and its lake lane), a parser by sources of one format (mbox, Maildir and
/// Outlook mail).
fn format_of_source(name: &str, parser: &str) -> Option<&'static InputFormat> {
    formats().iter().copied().find(|k| {
        (k.specs)()
            .iter()
            .any(|s| s.name == name && s.parser == parser)
    })
}

/// The declaration of the source `spec_name`.
pub fn spec(spec_name: &str) -> Option<&'static SourceSpec> {
    specs().into_iter().find(|s| s.name == spec_name)
}

/// The parser of `binding`, built with the settings it was read with.
pub fn source_for(binding: &Binding) -> Result<Box<dyn Source>, String> {
    let format = format_of_source(&binding.source, &binding.parser).ok_or_else(|| {
        format!(
            "{} with {}: no registered source",
            binding.source, binding.parser
        )
    })?;
    (format.source)(binding)
}

/// The binding a material read by `parser` on `host` had, for a material
/// stored without one (written by an outside producer): the source whose
/// parser it is, with only the host known.
pub fn binding_for_parser(parser: &str, host: &str) -> Option<Binding> {
    let spec = specs().into_iter().find(|s| s.parser == parser)?;
    Some(Binding {
        name: String::new(),
        source: spec.name.to_owned(),
        parser: spec.parser.to_owned(),
        input: Default::default(),
        settings: Settings::new(host),
    })
}

#[cfg(test)]
mod tests;
