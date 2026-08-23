use schemars::JsonSchema;
use serde::Serialize;

use crate::source_contracts::ResourceProfile;

/// How the source's checkpoint state is shaped.
///
/// Determines what the runtime checkpoint adapter must support and what
/// idempotency/replay strategy the source uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointFamily {
    /// Append-only stream (filesystem watcher, journald tail, browser export).
    AppendStream,
    /// Mutable backing store snapshotted with row-level occurrence anchors.
    MutableSnapshot {
        backing_store_kind: &'static str,
        occurrence_anchor: &'static str,
    },
    /// Journal/log API consumed by sequential cursor (e.g. systemd journal).
    Journal,
    /// Source has no native cursor; runtime polls and diffs.
    Polling,
    /// Internal in-memory state observed at intervals.
    LiveObservation,
}

/// Declared replayability of a runtime binding's source data.
///
/// This class describes the source-level recovery substrate, not whether a
/// particular delivery transport can redeliver a message. See
/// [`SourceRecoveryPolicy`] for the authority and loss decision that make the
/// class operational.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceReplayabilityClass {
    RetainedMaterial,
    AppendStream,
    MutableSnapshot,
    JournalCursor,
    ExternalProducer,
    LiveObservation,
    DerivedInternal,
}

/// The authority a binding may use to recover after a checkpoint or process
/// failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatchUpAuthority {
    SourceMaterialRegistry,
    BackingStore,
    JournalRetention,
    ExternalProducer,
    ParentEvents,
    None,
}

/// Explicit disposition for data a recovery attempt cannot reconstruct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcceptedLossPolicy {
    NoSilentLoss,
    BoundedByAuthority {
        boundary: &'static str,
    },
    Accepted {
        rationale: &'static str,
        record: &'static str,
    },
}

/// Recovery contract for one deployed source binding.
///
/// Each binding must declare all three dimensions. This is intentionally
/// separate from [`TransportSemantics`]: delivery replay does not establish
/// authority to re-read source data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SourceRecoveryPolicy {
    pub replayability_class: SourceReplayabilityClass,
    pub catch_up_authority: CatchUpAuthority,
    pub accepted_loss_policy: AcceptedLossPolicy,
}

impl SourceRecoveryPolicy {
    pub const RETAINED_MATERIAL: Self = Self {
        replayability_class: SourceReplayabilityClass::RetainedMaterial,
        catch_up_authority: CatchUpAuthority::SourceMaterialRegistry,
        accepted_loss_policy: AcceptedLossPolicy::NoSilentLoss,
    };

    pub const APPEND_STREAM: Self = Self {
        replayability_class: SourceReplayabilityClass::AppendStream,
        catch_up_authority: CatchUpAuthority::BackingStore,
        accepted_loss_policy: AcceptedLossPolicy::NoSilentLoss,
    };

    pub const MUTABLE_SNAPSHOT: Self = Self {
        replayability_class: SourceReplayabilityClass::MutableSnapshot,
        catch_up_authority: CatchUpAuthority::BackingStore,
        accepted_loss_policy: AcceptedLossPolicy::NoSilentLoss,
    };

    pub const JOURNAL_CURSOR: Self = Self {
        replayability_class: SourceReplayabilityClass::JournalCursor,
        catch_up_authority: CatchUpAuthority::JournalRetention,
        accepted_loss_policy: AcceptedLossPolicy::BoundedByAuthority {
            boundary: "upstream journal retention",
        },
    };

    pub const EXTERNAL_PRODUCER: Self = Self {
        replayability_class: SourceReplayabilityClass::ExternalProducer,
        catch_up_authority: CatchUpAuthority::ExternalProducer,
        accepted_loss_policy: AcceptedLossPolicy::NoSilentLoss,
    };

    pub const DERIVED_INTERNAL: Self = Self {
        replayability_class: SourceReplayabilityClass::DerivedInternal,
        catch_up_authority: CatchUpAuthority::ParentEvents,
        accepted_loss_policy: AcceptedLossPolicy::NoSilentLoss,
    };

    #[must_use]
    pub const fn live_observation(rationale: &'static str, record: &'static str) -> Self {
        Self {
            replayability_class: SourceReplayabilityClass::LiveObservation,
            catch_up_authority: CatchUpAuthority::None,
            accepted_loss_policy: AcceptedLossPolicy::Accepted { rationale, record },
        }
    }

    #[must_use]
    pub const fn is_replayable(self) -> bool {
        !matches!(
            self.replayability_class,
            SourceReplayabilityClass::LiveObservation
        )
    }

    /// Verify that the declared class, authority, and loss decision form a
    /// meaningful recovery contract.
    pub fn validate(self) -> Result<(), &'static str> {
        let class_authority_matches = matches!(
            (self.replayability_class, self.catch_up_authority),
            (
                SourceReplayabilityClass::RetainedMaterial,
                CatchUpAuthority::SourceMaterialRegistry
            ) | (
                SourceReplayabilityClass::AppendStream | SourceReplayabilityClass::MutableSnapshot,
                CatchUpAuthority::BackingStore
            ) | (
                SourceReplayabilityClass::JournalCursor,
                CatchUpAuthority::JournalRetention
            ) | (
                SourceReplayabilityClass::ExternalProducer,
                CatchUpAuthority::ExternalProducer
            ) | (
                SourceReplayabilityClass::LiveObservation,
                CatchUpAuthority::None
            ) | (
                SourceReplayabilityClass::DerivedInternal,
                CatchUpAuthority::ParentEvents
            )
        );
        if !class_authority_matches {
            return Err("replayability class and catch-up authority are incompatible");
        }

        match self.accepted_loss_policy {
            AcceptedLossPolicy::NoSilentLoss => Ok(()),
            AcceptedLossPolicy::BoundedByAuthority { boundary } if boundary.is_empty() => {
                Err("bounded-loss policy requires a non-empty authority boundary")
            }
            AcceptedLossPolicy::BoundedByAuthority { .. } => Ok(()),
            AcceptedLossPolicy::Accepted { rationale, record }
                if rationale.is_empty() || record.is_empty() =>
            {
                Err("accepted-loss policy requires a rationale and durable record")
            }
            AcceptedLossPolicy::Accepted { .. } => Ok(()),
        }
    }
}

/// Whether wiping Sinex's own copy of this source's data would lose
/// information that exists nowhere else (sinex-sn6s).
///
/// Sinex's dataset is currently treated as safely wipeable-and-reimportable
/// (dev speed over durability) precisely because every live source has *some*
/// other durable copy: the upstream app/service's own store, or Sinex's own
/// replay pipeline re-deriving the event from a reconstructable parent. That
/// assumption holds only as long as every deployed [`SourceRuntimeBinding`]
/// says so explicitly. The day an ephemeral-upstream-only source comes online
/// (IMAP email, a comms bridge, anything whose only durable copy is Sinex's
/// own row) without declaring `NotReconstructable`, the "safe to wipe"
/// premise silently breaks. Startup validation refuses to enable a deployed
/// binding that omits this declaration — see
/// `sinexd::sources::bindings::validate_bindings`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceCriticality {
    /// A canonical copy of this source's data exists outside Sinex (the
    /// upstream app/service keeps its own durable store), or Sinex's copy is
    /// itself rebuildable by replay from reconstructable parent events.
    /// Wiping Sinex's own copy loses nothing durable.
    Reconstructable,
    /// No canonical copy exists outside Sinex beyond a bounded retention
    /// window (e.g. a capped rolling journal export). Sinex's own copy is
    /// the only durable record of history past that window.
    NotReconstructable,
}

/// Privacy classification of the source's payloads.
///
/// The schema-apply engine reconciles a CHECK constraint on the
/// `privacy_tier` column of `raw.source_material_registry` when that
/// column exists. See issue #1236; the spec is forward-declared and
/// skipped at apply time when the column is absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, sinex_macros::DbCheck)]
#[serde(rename_all = "snake_case")]
#[db_check(
    schema = "raw",
    table = "source_material_registry",
    column = "privacy_tier",
    version = 1
)]
pub enum PrivacyTier {
    Public,
    Sensitive,
    Secret,
}

/// Time horizons the source serves on the *normal* runtime plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Horizon {
    Continuous,
    Historical,
}

/// How the source is invoked at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeShape {
    Continuous,
    OnDemand,
    Scheduled,
}

/// Raw material lifecycle declared by a source binding.
///
/// This is package-mode metadata for completeness, operations, and operator
/// views. It is not a disclosure engine and does not authorize hidden
/// redaction, deletion, or censorship.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterialLifecyclePolicy {
    /// Raw material is retained according to the source contract retention.
    RetainRaw,
    /// Raw bytes are staged for parsing and may be discarded after admission.
    EphemeralRaw,
    /// Raw bytes are not persisted; events/artifacts carry refs and caveats.
    DerivedOnly,
    /// Material is held for operator inspection or policy before admission.
    QuarantineUntilReviewed,
    /// External material remains outside Sinex; Sinex stores refs/outcomes.
    ExternalReferenceOnly,
}

impl MaterialLifecyclePolicy {
    #[must_use]
    pub const fn default_for(profile: ResourceProfile) -> Self {
        match profile {
            ResourceProfile::LiveWatcher | ResourceProfile::EmbeddedEmitter => Self::EphemeralRaw,
            ResourceProfile::EventStreamConsumer => Self::DerivedOnly,
            ResourceProfile::BoundedFile
            | ResourceProfile::BoundedStream
            | ResourceProfile::DirectoryScan
            | ResourceProfile::Oneshot => Self::RetainRaw,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    Direct,
    LocalQueue,
    CoreNats,
    JetStream,
    Kv,
    Filesystem,
    ExternalApi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverySemantics {
    SameProcess,
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnceNotClaimed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingSemantics {
    MaterialOrder,
    CursorOrder,
    BestEffort,
    Unordered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TransportSemantics {
    pub transport: TransportKind,
    pub delivery: DeliverySemantics,
    pub ordering: OrderingSemantics,
    /// Whether this *transport* can replay/redeliver. It is not source
    /// recovery authority; consult [`SourceRecoveryPolicy`] for that contract.
    pub replayable: bool,
    pub dlq: bool,
    pub backpressure: bool,
}

impl TransportSemantics {
    pub const DIRECT_APPEND_STREAM: Self = Self {
        transport: TransportKind::Direct,
        delivery: DeliverySemantics::SameProcess,
        ordering: OrderingSemantics::CursorOrder,
        replayable: true,
        dlq: false,
        backpressure: false,
    };

    pub const LOCAL_LIVE_QUEUE: Self = Self {
        transport: TransportKind::LocalQueue,
        delivery: DeliverySemantics::AtMostOnce,
        ordering: OrderingSemantics::BestEffort,
        replayable: false,
        dlq: true,
        backpressure: true,
    };

    pub const JETSTREAM_DURABLE: Self = Self {
        transport: TransportKind::JetStream,
        delivery: DeliverySemantics::AtLeastOnce,
        ordering: OrderingSemantics::CursorOrder,
        replayable: true,
        dlq: true,
        backpressure: true,
    };

    pub const EXTERNAL_API_CURSOR: Self = Self {
        transport: TransportKind::ExternalApi,
        delivery: DeliverySemantics::AtMostOnce,
        ordering: OrderingSemantics::CursorOrder,
        replayable: true,
        dlq: true,
        backpressure: true,
    };

    #[must_use]
    pub const fn default_for(
        runner: RunnerPack,
        checkpoint: CheckpointFamily,
        runtime: RuntimeShape,
    ) -> Self {
        let transport = match runner {
            RunnerPack::Staged | RunnerPack::InProcess => TransportKind::Direct,
            RunnerPack::Live => TransportKind::LocalQueue,
            RunnerPack::External => TransportKind::JetStream,
            RunnerPack::SinexdSource => match runtime {
                RuntimeShape::Continuous => TransportKind::LocalQueue,
                RuntimeShape::OnDemand | RuntimeShape::Scheduled => TransportKind::Direct,
            },
        };
        let ordering = match checkpoint {
            CheckpointFamily::AppendStream | CheckpointFamily::Journal => {
                OrderingSemantics::CursorOrder
            }
            CheckpointFamily::MutableSnapshot { .. } => OrderingSemantics::MaterialOrder,
            CheckpointFamily::Polling | CheckpointFamily::LiveObservation => {
                OrderingSemantics::BestEffort
            }
        };
        let delivery = match transport {
            TransportKind::Direct => DeliverySemantics::SameProcess,
            TransportKind::JetStream => DeliverySemantics::AtLeastOnce,
            TransportKind::LocalQueue
            | TransportKind::CoreNats
            | TransportKind::Kv
            | TransportKind::Filesystem
            | TransportKind::ExternalApi => DeliverySemantics::AtMostOnce,
        };
        Self {
            transport,
            delivery,
            ordering,
            replayable: !matches!(checkpoint, CheckpointFamily::LiveObservation),
            dlq: matches!(
                transport,
                TransportKind::JetStream | TransportKind::LocalQueue
            ),
            backpressure: !matches!(transport, TransportKind::Direct),
        }
    }
}

/// Retention policy for events emitted by this source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetentionPolicy {
    Forever,
    Days { days: u32 },
    Tiered { hot_days: u32, warm_days: u32 },
}

/// How the source identifies real-world occurrences.
///
/// Adjacently tagged (`content = "key"`) so the `Uuid5From` newtype variant
/// serializes — internal tagging cannot serialize a newtype variant holding a
/// primitive. The catalog export (#1727) is the first JSON consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", content = "key", rename_all = "snake_case")]
pub enum OccurrenceIdentity {
    Uuid5From(&'static str),
    Natural,
    Anchor,
}

/// Which runner hosts a source binding at deployment time.
///
/// Replaces the former free-form `runner_pack`/`implementation_mode` strings
/// (26×`sinexd-source`, 3×`live`, plus `parser:staged`/`external:*`/`in_process:*`
/// variants on automata and embedded emitters). The *domain* a source belongs to
/// already lives in [`SourceContract::namespace`]; this enum carries only the
/// runner kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerPack {
    /// Hosted in-process by `sinexd` as a source binding (the common case).
    SinexdSource,
    /// Live capture binding (e.g. D-Bus signal listeners).
    Live,
    /// Staged-export parser fed from operator-staged material.
    Staged,
    /// External producer that publishes events over NATS (e.g. polylogue).
    External,
    /// Emitted from within a sinex binary / automaton, not a hosted source.
    InProcess,
}
/// What resource a source reads, de-conflated from the data-category labels the
/// former `access_policy` string mixed in (e.g. `target_home_read:.zsh_history`
/// vs `personal_social_data`). This carries only the *locator* axis; data
/// classification belongs to [`SourceContract::privacy_tier`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum AccessScope {
    /// No direct external access (internal/derived).
    Internal,
    /// Reads operator-staged export material (GDPR/Takeout dumps).
    StagedExport,
    /// Reads a path under the target user's home directory.
    TargetHome { path: &'static str },
    /// Reads a path under the realm data lake.
    TargetData { path: &'static str },
    /// Bridges a target runtime surface (window manager, clipboard).
    RuntimeBridge { surface: &'static str },
    /// Reads the systemd journal.
    SystemdJournal,
    /// Reads kernel uevents (udev monitor).
    KernelUevents,
    /// Listens on the D-Bus session bus.
    SessionBus,
    /// Listens on the D-Bus system bus.
    SystemBus,
    /// Reads operator-configured watch roots.
    ConfiguredRoots,
    /// Reads a local library root.
    LibraryRoot,
}
