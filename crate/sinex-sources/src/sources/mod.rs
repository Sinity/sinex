//! Every source, one module per family, registered in one list.
pub mod activitywatch;
pub mod arbtt;
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

use crate::registry::{Source, SourceSpec};

/// Every source's declaration.
pub fn specs() -> Vec<&'static SourceSpec> {
    let mut out = vec![
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
        &google_takeout::activity::MY_ACTIVITY,
        &google_takeout::activity::YOUTUBE,
        &google_takeout::keep::SPEC,
        &google_takeout::tasks::SPEC,
        &google_takeout::purchases::PLAY_STORE,
        &google_takeout::purchases::PURCHASES,
        &calendar::SPEC,
        &goodreads::SPEC,
        &messenger::SPEC,
        &sleep_as_android::SPEC,
        &wykop::SPEC,
        &reddit::SPEC,
        &samsung_health::SPEC,
        &samsung_sms::SPEC,
        &operator::RAW_LOG,
        &operator::STEERING[0],
        &operator::STEERING[1],
        &operator::STEERING[2],
        &library::SCAN,
        &library::REQUEST,
        &notifications::SPEC,
        &mpris::SPEC,
        &clipboard::SPEC,
        &session_socket::SPEC,
        &media_capture::SPEC,
        &page_crawl::SPEC,
        &mail_imap::SPEC,
    ];
    out.extend(sinnix_lake::LANES.iter().map(|l| &l.spec));
    out
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
        p if p == google_takeout::activity::MY_ACTIVITY.parser => {
            Some(Box::new(google_takeout::activity::my_activity()))
        }
        p if p == google_takeout::activity::YOUTUBE.parser => {
            Some(Box::new(google_takeout::activity::youtube()))
        }
        p if p == google_takeout::keep::SPEC.parser => {
            Some(Box::new(google_takeout::keep::source()))
        }
        p if p == google_takeout::tasks::SPEC.parser => {
            Some(Box::new(google_takeout::tasks::source()))
        }
        p if p == google_takeout::purchases::PLAY_STORE.parser => {
            Some(Box::new(google_takeout::purchases::play_store()))
        }
        p if p == google_takeout::purchases::PURCHASES.parser => {
            Some(Box::new(google_takeout::purchases::purchases()))
        }
        p if p == goodreads::SPEC.parser => Some(Box::new(goodreads::source())),
        p if p == messenger::SPEC.parser => Some(Box::new(messenger::source())),
        p if p == sleep_as_android::SPEC.parser => Some(Box::new(sleep_as_android::source())),
        p if p == wykop::SPEC.parser => Some(Box::new(wykop::source())),
        p if p == reddit::SPEC.parser => Some(Box::new(reddit::source())),
        p if p == samsung_health::SPEC.parser => Some(Box::new(samsung_health::source())),
        p if p == samsung_sms::SPEC.parser => Some(Box::new(samsung_sms::source())),
        p if p == operator::RAW_LOG.parser => Some(Box::new(operator::raw_log(
            weechat::zone(None).unwrap_or(jiff::tz::TimeZone::UTC),
        ))),
        p if p.starts_with("steering-") => {
            let table = p.trim_start_matches("steering-");
            ["commitments", "activities", "reviews"]
                .into_iter()
                .find(|t| *t == table)
                .and_then(operator::steering)
                .map(|s| Box::new(s) as Box<dyn Source>)
        }
        // Like WeeChat, floating times are read in this system's zone.
        p if p == calendar::SPEC.parser => Some(Box::new(calendar::source(
            weechat::zone(None).unwrap_or(jiff::tz::TimeZone::UTC),
        ))),
        p if p == library::SCAN.parser => Some(Box::new(library::scan_source())),
        p if p == library::REQUEST.parser => Some(Box::new(library::request_source())),
        p if p == notifications::SPEC.parser => Some(Box::new(notifications::source())),
        p if p == mpris::SPEC.parser => Some(Box::new(mpris::source())),
        p if p == clipboard::SPEC.parser => Some(Box::new(clipboard::source())),
        p if p == session_socket::SPEC.parser => Some(Box::new(session_socket::source())),
        p if p == media_capture::SPEC.parser => Some(Box::new(media_capture::source())),
        p if p == page_crawl::SPEC.parser => Some(Box::new(page_crawl::source())),
        p => sinnix_lake::LANES
            .iter()
            .find(|l| l.spec.parser == p)
            .map(|l| Box::new(sinnix_lake::source(l)) as Box<dyn Source>),
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
