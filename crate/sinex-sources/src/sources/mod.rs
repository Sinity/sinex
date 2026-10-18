//! Every source, one module per family, registered in one list.
pub mod activitywatch;
pub mod atuin;
pub mod chromium;
mod fields;
pub mod file_watch;
pub mod hyprland;
pub mod journald;
pub mod mail;
pub mod notes;
pub mod raindrop;
/// Commits in local repositories (`code.git`).
pub mod repository;
pub mod spotify;
pub mod takeout_chrome;
pub mod weechat;
pub mod zsh;

use crate::registry::{Source, SourceSpec};

/// Every source's declaration.
pub fn specs() -> Vec<&'static SourceSpec> {
    vec![
        &atuin::SPEC,
        &zsh::SPEC,
        &hyprland::SPEC,
        &journald::SPEC,
        &activitywatch::SPEC,
        &repository::SPEC,
        &file_watch::SPEC,
        &chromium::VISITS,
        &chromium::URLS,
        &takeout_chrome::SPEC,
        &spotify::SPEC,
        &raindrop::SPEC,
        &notes::SPEC,
        &mail::MBOX,
        &mail::MAILDIR,
        &weechat::SPEC,
    ]
}

/// The source whose parser read a stored material, as a replay re-reads it
/// with the current version of that parser. `host` is the material's input
/// host, for parsers that record it.
pub fn by_parser(parser: &str, host: &str) -> Option<Box<dyn Source>> {
    match parser {
        p if p == atuin::SPEC.parser => Some(Box::new(atuin::source())),
        p if p == zsh::SPEC.parser => Some(Box::new(zsh::source(host.to_owned()))),
        p if p == hyprland::SPEC.parser => Some(Box::new(hyprland::source())),
        p if p == journald::SPEC.parser => Some(Box::new(journald::source())),
        p if p == activitywatch::SPEC.parser => Some(Box::new(activitywatch::source())),
        p if p == repository::SPEC.parser => Some(Box::new(repository::source())),
        p if p == file_watch::SPEC.parser => Some(Box::new(file_watch::source())),
        p if p == chromium::VISITS.parser => Some(Box::new(chromium::visits())),
        p if p == chromium::URLS.parser => Some(Box::new(chromium::urls())),
        p if p == takeout_chrome::SPEC.parser => Some(Box::new(takeout_chrome::source())),
        p if p == spotify::SPEC.parser => Some(Box::new(spotify::source())),
        p if p == raindrop::SPEC.parser => Some(Box::new(raindrop::source())),
        p if p == notes::SPEC.parser => Some(Box::new(notes::source())),
        // mbox and Maildir share the parser; the spec named is the mbox one.
        p if p == mail::MBOX.parser => Some(Box::new(mail::mbox_source())),
        // WeeChat's zone is a setting of the capture, not in the stored log; a
        // replay reads with this system's zone.
        p if p == weechat::SPEC.parser => Some(Box::new(weechat::source(
            weechat::zone(None).unwrap_or(jiff::tz::TimeZone::UTC),
        ))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_parser_resolves_to_its_source() {
        for spec in specs() {
            let source = by_parser(spec.parser, "h").expect("registered parser resolves");
            assert_eq!(source.spec().parser, spec.parser);
        }
        assert!(by_parser("no-such-parser", "h").is_none());
    }
}
