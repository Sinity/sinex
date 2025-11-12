//! Shared sample identifiers so tests don't reinvent ad-hoc strings.
//! Keeping these centralized avoids "repo-test" proliferation and
//! makes it obvious when two fixtures are meant to collide.
use sinex_core::{EventSource, EventType};

pub const SOURCE_FIXTURE_REPO_PRIMARY: &str = "fixture.source.repo.primary";
pub const SOURCE_FIXTURE_REPO_SECONDARY: &str = "fixture.source.repo.secondary";
pub const EVENT_TYPE_FIXTURE_QUERY_SAFETY: &str = "fixture.event.query_safety";

pub const EVENT_SOURCE_REPO_PRIMARY: EventSource =
    EventSource::from_static(SOURCE_FIXTURE_REPO_PRIMARY);
pub const EVENT_SOURCE_REPO_SECONDARY: EventSource =
    EventSource::from_static(SOURCE_FIXTURE_REPO_SECONDARY);
pub const EVENT_TYPE_QUERY_SAFETY: EventType =
    EventType::from_static(EVENT_TYPE_FIXTURE_QUERY_SAFETY);
