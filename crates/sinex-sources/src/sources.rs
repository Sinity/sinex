//! Every source, one module per family, registered in one list.
#[cfg(feature = "full")]
pub mod activitywatch;
#[cfg(feature = "full")]
pub mod arbtt;
#[cfg(feature = "full")]
pub mod asciinema;
#[cfg(feature = "full")]
pub mod atuin;
#[cfg(feature = "full")]
pub mod bash;
#[cfg(feature = "full")]
pub mod calendar;
#[cfg(feature = "full")]
pub mod chromium;
/// The Wayland clipboard and primary selection.
pub mod clipboard;
/// Capture from the session bus, shared by the D-Bus sources.
pub mod dbus;
pub(crate) mod fields;
pub mod file_watch;
#[cfg(feature = "full")]
pub mod fish;
#[cfg(feature = "full")]
pub mod forecast;
#[cfg(feature = "full")]
pub mod goodreads;
#[cfg(feature = "full")]
pub mod google_takeout;
pub mod hyprland;
/// The keyboard and pointer log (scribe-tap).
#[cfg(feature = "full")]
pub mod input_log;
pub mod journald;
/// kitty scrollback snapshots in the Sinnix lake.
pub mod kitty_scrollback;
/// The media library: scans of library roots and library requests.
#[cfg(feature = "full")]
pub mod library;
#[cfg(feature = "full")]
pub mod mail;
/// Live mail over IMAP, with IDLE and XOAUTH2.
#[cfg(feature = "full")]
pub mod mail_imap;
/// Screen stills and audio segments on the `Media` driver.
pub mod media_capture;
#[cfg(feature = "full")]
pub mod messenger;
/// Media players (MPRIS) on the session bus.
pub mod mpris;
#[cfg(feature = "full")]
pub mod notes;
/// Desktop notifications on the session bus.
pub mod notifications;
#[cfg(feature = "full")]
pub mod operator;
#[cfg(feature = "full")]
pub mod outlook;
/// The forward crawl of browser-history pages, opt-in per domain.
#[cfg(feature = "full")]
pub mod page_crawl;
#[cfg(feature = "full")]
pub mod raindrop;
#[cfg(feature = "full")]
pub mod reddit;
/// Commits in local repositories (`code.git`).
#[cfg(feature = "full")]
pub mod repository;
#[cfg(feature = "full")]
pub mod samsung_cloud;
#[cfg(feature = "full")]
pub mod samsung_health;
#[cfg(feature = "full")]
pub mod samsung_sms;
/// The session integration socket: Neovim and kitty integrations.
pub mod session_socket;
/// Numbering shell history records, shared by zsh, bash and fish.
#[cfg(feature = "full")]
pub(crate) mod shell_history;
/// The Sinnix capture lake's lanes.
#[cfg(feature = "full")]
pub mod sinnix_lake;
#[cfg(feature = "full")]
pub mod sleep_as_android;
#[cfg(feature = "full")]
pub mod spotify;
#[cfg(feature = "full")]
pub mod takeout_chrome;
/// Sinex's own state, sampled by the daemon.
pub mod telemetry;
pub(crate) mod terminal_text;
#[cfg(feature = "full")]
pub mod weechat;
#[cfg(feature = "full")]
pub mod wykop;
#[cfg(feature = "full")]
pub mod zsh;

use sinex_source_core::binding::{Binding, InputFormat, Settings};
use sinex_source_core::registry::{Source, SourceSpec};

/// Every input format and the sources that read it: the one list the
/// catalog, backfill, the binding runner, `sinex-session` and replay all
/// work from.
pub fn formats() -> &'static [&'static InputFormat] {
    &[
        #[cfg(feature = "full")]
        &atuin::FORMAT,
        #[cfg(feature = "full")]
        &zsh::FORMAT,
        #[cfg(feature = "full")]
        &bash::FORMAT,
        #[cfg(feature = "full")]
        &fish::FORMAT,
        &hyprland::FORMAT,
        &journald::FORMAT,
        &file_watch::FORMAT,
        #[cfg(feature = "full")]
        &activitywatch::FORMAT,
        #[cfg(feature = "full")]
        &repository::FORMAT,
        #[cfg(feature = "full")]
        &chromium::FORMAT,
        #[cfg(feature = "full")]
        &takeout_chrome::FORMAT,
        #[cfg(feature = "full")]
        &spotify::FORMAT,
        #[cfg(feature = "full")]
        &raindrop::FORMAT,
        #[cfg(feature = "full")]
        &notes::FORMAT,
        #[cfg(feature = "full")]
        &mail::MBOX_FORMAT,
        #[cfg(feature = "full")]
        &mail::MAILDIR_FORMAT,
        #[cfg(feature = "full")]
        &mail_imap::FORMAT,
        #[cfg(feature = "full")]
        &outlook::FORMAT,
        #[cfg(feature = "full")]
        &outlook::CSV_FORMAT,
        #[cfg(feature = "full")]
        &weechat::FORMAT,
        #[cfg(feature = "full")]
        &calendar::FORMAT,
        #[cfg(feature = "full")]
        &google_takeout::FORMAT,
        #[cfg(feature = "full")]
        &goodreads::FORMAT,
        #[cfg(feature = "full")]
        &messenger::FORMAT,
        #[cfg(feature = "full")]
        &sleep_as_android::FORMAT,
        #[cfg(feature = "full")]
        &wykop::FORMAT,
        #[cfg(feature = "full")]
        &reddit::FORMAT,
        #[cfg(feature = "full")]
        &samsung_health::FORMAT,
        #[cfg(feature = "full")]
        &samsung_sms::FORMAT,
        #[cfg(feature = "full")]
        &samsung_cloud::FORMAT,
        #[cfg(feature = "full")]
        &arbtt::FORMAT,
        #[cfg(feature = "full")]
        &asciinema::FORMAT,
        &kitty_scrollback::FORMAT,
        #[cfg(feature = "full")]
        &input_log::FORMAT,
        #[cfg(feature = "full")]
        &operator::RAW_LOG_FORMAT,
        #[cfg(feature = "full")]
        &operator::STEERING_FORMAT,
        #[cfg(feature = "full")]
        &sinnix_lake::FORMAT,
        &notifications::FORMAT,
        &mpris::FORMAT,
        &clipboard::FORMAT,
        &session_socket::FORMAT,
        &media_capture::FORMAT,
        #[cfg(feature = "full")]
        &library::FORMAT,
        #[cfg(feature = "full")]
        &forecast::FORMAT,
        #[cfg(feature = "full")]
        &page_crawl::FORMAT,
        &telemetry::FORMAT,
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

#[cfg(all(test, feature = "full"))]
mod tests;
