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
pub mod goodreads;
pub mod google_takeout;
pub mod hyprland;
pub mod journald;
/// The media library: scans of library roots and library requests.
pub mod library;
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
/// The session producer socket: Neovim and kitty integrations.
pub mod session_socket;
/// The Sinnix capture lake's envelope lanes (phase 1 of the port).
pub mod sinnix_lake;
pub mod sleep_as_android;
pub mod spotify;
pub mod takeout_chrome;
pub mod weechat;
pub mod wykop;
pub mod zsh;

use crate::binding::{Binding, InputKind, Settings};
use crate::registry::{Source, SourceSpec};

/// Every kind of input and the sources that read it: the one list the
/// catalog, backfill, the binding runner, `sinex-session` and replay all
/// work from.
pub fn input_kinds() -> &'static [&'static InputKind] {
    &[
        &atuin::KIND,
        &zsh::KIND,
        &hyprland::KIND,
        &journald::KIND,
        &file_watch::KIND,
        &activitywatch::KIND,
        &repository::KIND,
        &chromium::KIND,
        &takeout_chrome::KIND,
        &spotify::KIND,
        &raindrop::KIND,
        &notes::KIND,
        &mail::MBOX_KIND,
        &mail::MAILDIR_KIND,
        &mail_imap::KIND,
        &outlook::KIND,
        &outlook::CSV_KIND,
        &weechat::KIND,
        &calendar::KIND,
        &google_takeout::KIND,
        &goodreads::KIND,
        &messenger::KIND,
        &sleep_as_android::KIND,
        &wykop::KIND,
        &reddit::KIND,
        &samsung_health::KIND,
        &samsung_sms::KIND,
        &samsung_cloud::KIND,
        &arbtt::KIND,
        &asciinema::KIND,
        &operator::RAW_LOG_KIND,
        &operator::STEERING_KIND,
        &sinnix_lake::KIND,
        &notifications::KIND,
        &mpris::KIND,
        &clipboard::KIND,
        &session_socket::KIND,
        &media_capture::KIND,
        &library::KIND,
        &page_crawl::KIND,
    ]
}

/// Every source's declaration, each once.
pub fn specs() -> Vec<&'static SourceSpec> {
    let mut out: Vec<&'static SourceSpec> = Vec::new();
    for kind in input_kinds() {
        for spec in (kind.specs)() {
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

/// The kind named `name` (`--source NAME=PATH`).
pub fn input_kind(name: &str) -> Option<&'static InputKind> {
    input_kinds().iter().copied().find(|k| k.name == name)
}

/// The kind that reads the source `spec_name`.
///
/// Several kinds can read one source name (the live clipboard and its lake
/// lane); this is the first registered, and a configuration names the kind
/// to choose another.
pub fn input_kind_of(spec_name: &str) -> Option<&'static InputKind> {
    input_kinds().iter().copied().find(|k| k.reads(spec_name))
}

/// The kind reading the source `name` with `parser`. Neither is unique
/// alone: a name is shared by one source read two ways (the live clipboard
/// and its lake lane), a parser by sources of one format (mbox, Maildir and
/// Outlook mail).
pub fn input_kind_of_source(name: &str, parser: &str) -> Option<&'static InputKind> {
    input_kinds().iter().copied().find(|k| {
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
    let kind = input_kind_of_source(&binding.source, &binding.parser).ok_or_else(|| {
        format!(
            "{} with {}: no registered source",
            binding.source, binding.parser
        )
    })?;
    (kind.source)(binding)
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
