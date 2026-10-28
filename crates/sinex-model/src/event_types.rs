//! Bundled payload validators. One Rust type per bundled event type; `ALL`
//! is the single list the schema registry, NixOS options and documentation are
//! generated from.
use crate::EventPayload;

pub mod account;
pub mod browser;
pub mod capture;
pub mod code;
pub mod communication;
pub mod derived;
pub mod desktop;
pub mod documents;
pub mod environment;
pub mod finance;
pub mod forecast;
pub mod fused;
pub mod gmail;
pub mod health;
pub mod imported_text;
pub mod input;
pub mod library;
pub mod machine;
pub mod media;
pub mod operator;
pub mod peripheral;
pub mod query;
pub mod records;
pub mod router;
pub mod session;
pub mod shell;
pub mod social;
pub mod system;
pub mod telemetry;
pub mod work;

pub use derived::{FigureValue, HealthFigure, MetricFigure, StateInterval};
pub use fused::CommandFused;
use serde_json::Value;
pub use shell::{CommandExecuted, CommandImported, ShellSession};

/// A registered event type, as the schema registry stores it.
#[derive(Clone, Debug)]
pub struct RegisteredEventType {
    pub event_type: &'static str,
    /// Declared natural key; `build` applies the typed payload's per-record choice.
    pub key: crate::KeyKind,
    pub replace: crate::ReplaceRule,
    /// See [`crate::EventPayload::KEEP_KNOWN`].
    pub keep_known: &'static [&'static str],
    pub declaration: fn() -> crate::KindDeclaration,
    pub json_schema: fn() -> Value,
    /// Build an event of this type from a JSON payload, as arriving over the
    /// NATS ingress: the payload is decoded into the Rust type first, so the
    /// type's time and removal rules apply exactly as for in-process sources.
    pub build: Build,
}

/// See [`RegisteredEventType::build`].
pub type Build = fn(
    &Value,
    &str,
    crate::Origin,
    crate::ReadingOrder,
    [u8; 32],
    crate::Class,
) -> Result<crate::Event, crate::BuildError>;

const fn info<P: EventPayload>() -> RegisteredEventType {
    RegisteredEventType {
        event_type: P::EVENT_TYPE,
        key: P::KEY,
        replace: P::REPLACE,
        keep_known: P::KEEP_KNOWN,
        declaration: P::declaration,
        json_schema: P::payload_schema,
        build: build_from_json::<P>,
    }
}

fn build_from_json<P: EventPayload>(
    payload: &Value,
    source: &str,
    origin: crate::Origin,
    reading_order: crate::ReadingOrder,
    record_hash: [u8; 32],
    class: crate::Class,
) -> Result<crate::Event, crate::BuildError> {
    use serde::de::IntoDeserializer;
    let typed: P =
        serde_path_to_error::deserialize(payload.clone().into_deserializer()).map_err(|error| {
            let mut pointer = error
                .path()
                .iter()
                .map(|s| s.to_string().replace('~', "~0").replace('/', "~1"))
                .collect::<Vec<_>>()
                .join("/");
            let mut keyword = "decode";
            let reason = error.inner().to_string();
            if pointer.is_empty()
                && let Some(field) = reason
                    .strip_prefix("missing field `")
                    .and_then(|s| s.strip_suffix('`'))
                && P::payload_schema()
                    .get("properties")
                    .and_then(Value::as_object)
                    .is_some_and(|fields| fields.contains_key(field))
            {
                pointer = field.replace('~', "~0").replace('/', "~1");
                keyword = "required";
            }
            crate::BuildError::Payload(P::EVENT_TYPE.into(), format!("{keyword} at /{pointer}"))
        })?;
    crate::Event::build(&typed, source, origin, reading_order, record_hash, class)
}

/// Bundled metadata is immutable for this executable. Admission compares a
/// saved declaration with these values once per kind in its batch.
pub fn declarations() -> &'static [crate::KindDeclaration] {
    static DECLARATIONS: std::sync::OnceLock<Vec<crate::KindDeclaration>> =
        std::sync::OnceLock::new();
    DECLARATIONS.get_or_init(|| ALL.iter().map(|kind| (kind.declaration)()).collect())
}

/// Every bundled payload validator. Outside kinds are stored declarations.
pub const ALL: &[RegisteredEventType] = &[
    info::<query::Mark>(),
    info::<query::Saved>(),
    info::<documents::DocumentCaptured>(),
    info::<library::FilenameHints>(),
    info::<imported_text::TranscriptCue>(),
    info::<imported_text::OcrText>(),
    info::<capture::Gap>(),
    info::<forecast::Issued>(),
    info::<forecast::Resolved>(),
    info::<CommandExecuted>(),
    info::<CommandImported>(),
    info::<finance::TransactionPosted>(),
    info::<finance::JournalDirective>(),
    info::<finance::JournalImported>(),
    // Desktop
    info::<desktop::WindowFocused>(),
    info::<desktop::WindowOpened>(),
    info::<desktop::WindowClosed>(),
    info::<desktop::WindowMoved>(),
    info::<desktop::WindowTitleChanged>(),
    info::<desktop::WorkspaceSwitched>(),
    info::<desktop::MonitorFocused>(),
    info::<desktop::DesktopOther>(),
    // Session
    info::<session::NotificationShown>(),
    info::<session::NotificationClosed>(),
    info::<session::NotificationAction>(),
    info::<session::ClipboardCopied>(),
    info::<session::PlayerChanged>(),
    info::<session::EditorReport>(),
    info::<session::TerminalReport>(),
    info::<session::MediaSegment>(),
    info::<session::FrameCaptured>(),
    info::<session::ScreenText>(),
    info::<session::PageFetched>(),
    info::<desktop::WindowSpan>(),
    info::<desktop::PresenceSpan>(),
    info::<desktop::PresenceInterval>(),
    info::<desktop::BrowserTabActive>(),
    info::<desktop::ActivityWatchOther>(),
    info::<desktop::WindowSampled>(),
    // Machine
    info::<machine::JournalEntry>(),
    info::<machine::LakeObservation>(),
    info::<machine::FileChanged>(),
    info::<machine::TerminalSession>(),
    info::<machine::TerminalOutput>(),
    info::<machine::ScrollbackCaptured>(),
    info::<machine::ScrollbackWindow>(),
    info::<desktop::DomainUsage>(),
    // Keyboard and pointer
    info::<input::KeyPressed>(),
    info::<input::KeyReleased>(),
    info::<input::PointerButton>(),
    info::<input::PointerMoved>(),
    info::<input::PointerScrolled>(),
    info::<input::InputLogger>(),
    // Accessibility, audio, devices, network and the room
    info::<peripheral::DisplaySampled>(),
    info::<peripheral::BatterySampled>(),
    info::<peripheral::BluetoothAudioSampled>(),
    info::<environment::AirSampled>(),
    info::<system::UnitStarted>(),
    info::<system::UnitStopped>(),
    info::<system::UnitReloaded>(),
    info::<system::BootStarted>(),
    info::<system::SleepStarted>(),
    info::<system::SleepEnded>(),
    info::<system::ShutdownStarted>(),
    info::<system::DeviceChanged>(),
    // Machine records and the router
    info::<records::GenerationActivated>(),
    info::<records::BackupChecked>(),
    info::<records::BackupDrilled>(),
    info::<records::TaskStoreDrilled>(),
    info::<records::SandboxAuditFinding>(),
    info::<records::UsageAssessed>(),
    info::<records::UsageCensusTaken>(),
    info::<records::ConfigDriftChecked>(),
    info::<records::ExperimentStarted>(),
    info::<records::ExperimentRun>(),
    info::<router::RouterLogLine>(),
    info::<router::DhcpLeaseSampled>(),
    info::<router::DeviceTrafficSampled>(),
    info::<router::WifiClientSampled>(),
    // Machine telemetry
    info::<telemetry::ResourceSampled>(),
    info::<telemetry::ProcessKilled>(),
    info::<telemetry::HardwareState>(),
    // Code and the development tools observed
    info::<code::GitCommit>(),
    info::<code::GitRefUpdated>(),
    info::<code::GithubPullRequest>(),
    info::<code::GithubReview>(),
    info::<code::GithubCheck>(),
    info::<code::GithubWorkflowRun>(),
    info::<code::GithubIssue>(),
    info::<work::AgentctlEvent>(),
    // Derived
    info::<CommandFused>(),
    info::<fused::FocusFused>(),
    info::<fused::VisitFused>(),
    info::<fused::PlayFused>(),
    info::<fused::ClipboardFused>(),
    info::<fused::NotificationFused>(),
    info::<fused::MailFused>(),
    info::<fused::PostFused>(),
    info::<fused::ConversationFused>(),
    info::<ShellSession>(),
    info::<StateInterval>(),
    info::<MetricFigure>(),
    info::<HealthFigure>(),
    // Browsing, listening, reading, notes, communication
    info::<browser::BrowserVisit>(),
    info::<browser::BrowserUrl>(),
    info::<browser::BrowserHistoryEntry>(),
    info::<browser::LinkFollowed>(),
    info::<browser::ReadingStackChanged>(),
    info::<media::MusicPlayed>(),
    info::<media::BookmarkSaved>(),
    info::<media::NoteDocument>(),
    info::<communication::ChatMessage>(),
    info::<communication::MailMessage>(),
    info::<communication::CallLogged>(),
    info::<gmail::ProviderMessage>(),
    info::<gmail::MailChanged>(),
    info::<gmail::HistoryGap>(),
    info::<gmail::ProviderMissing>(),
    // Account exports
    info::<account::ActionRecorded>(),
    info::<account::VideoWatched>(),
    info::<account::VideoSearched>(),
    info::<account::NoteObserved>(),
    info::<account::TaskRevised>(),
    info::<account::PurchaseObserved>(),
    info::<account::CalendarEvent>(),
    info::<account::CalendarInstance>(),
    info::<account::AccountRow>(),
    info::<account::LocationSample>(),
    info::<communication::ConversationMessage>(),
    info::<media::BookStatus>(),
    info::<health::HealthRecord>(),
    info::<social::PostPublished>(),
    info::<social::VoteCast>(),
    info::<social::NotificationReceived>(),
    info::<social::AccountSetting>(),
    info::<operator::LogEntry>(),
    info::<operator::SteeringRow>(),
    // Library
    info::<library::FileCopy>(),
    info::<library::Item>(),
    info::<library::Rendition>(),
    info::<library::Preview>(),
    info::<library::Assignment>(),
    info::<library::Collection>(),
    info::<library::Entry>(),
    info::<library::Rating>(),
    info::<library::Comparison>(),
    info::<library::View>(),
    info::<library::Dismissal>(),
];

/// The registered type named `event_type`, for code that holds a type name
/// read from the database and needs the static one.
pub fn find(event_type: &str) -> Option<&'static RegisteredEventType> {
    ALL.iter().find(|k| k.event_type == event_type)
}

#[cfg(test)]
mod tests;
