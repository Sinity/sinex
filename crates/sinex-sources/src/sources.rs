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
pub mod query;
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
    #[cfg(not(feature = "full"))]
    const FORMATS: &[&InputFormat] = &[
        &hyprland::FORMAT,
        &journald::FORMAT,
        &file_watch::FORMAT,
        &kitty_scrollback::FORMAT,
        &notifications::FORMAT,
        &mpris::FORMAT,
        &clipboard::FORMAT,
        &session_socket::FORMAT,
        &media_capture::FORMAT,
    ];
    // Order selects the default when several formats read the same source name.
    #[cfg(feature = "full")]
    const FORMATS: &[&InputFormat] = &[
        &atuin::FORMAT,
        &zsh::FORMAT,
        &bash::FORMAT,
        &fish::FORMAT,
        &hyprland::FORMAT,
        &journald::FORMAT,
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
        &kitty_scrollback::FORMAT,
        &input_log::FORMAT,
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
&query::FORMAT,
        &page_crawl::FORMAT,
    ];
    FORMATS
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
    let source = (format.source)(binding)?;
    let Some(class) = binding.settings.class else {
        return Ok(source);
    };
    let mut spec = source.spec().clone();
    spec.class = spec.class.max(class);
    Ok(Box::new(ConfiguredSource { source, spec }))
}

struct ConfiguredSource {
    source: Box<dyn Source>,
    spec: SourceSpec,
}

impl Source for ConfiguredSource {
    fn spec(&self) -> &SourceSpec {
        &self.spec
    }
    fn drainer<'a>(
        &'a self,
        mut input: sinex_source_core::DrainInput<'a>,
    ) -> Result<
        Box<
            dyn Iterator<
                    Item = Result<sinex_source_core::DrainOutput, sinex_source_core::RecordError>,
                > + Send
                + 'a,
        >,
        sinex_source_core::RecordError,
    > {
        input.class = input.class.max(self.spec.class);
        self.source.drainer(input)
    }
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
