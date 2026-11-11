//! The SQL each field of each table lowers to, matched on the field
//! identities `sinex_model::query::tables` declares.
//!
//! Templates name the table's table alias `{t}` and, for the archive, the
//! archive row `{a}`; the compiler substitutes a distinct alias per nesting
//! level, so an inner query of a time join never shadows the outer one.
use sinex_model::query::{
    ArchiveField, EventField, FailureField, FieldId, GapField, MaterialField, OperationField,
    RunField, SourceField, Table,
};

/// The `FROM` clause of a table, with `{t}` (and `{a}`) for its aliases.
pub(crate) fn from(table: Table) -> &'static str {
    match table {
        Table::Events | Table::Drift => "core.visible_events {t}",
        Table::Gaps => super::source_views::GAPS,
        Table::Archive => {
            "core.visible_events {t} JOIN audit.archive_changes {a} ON {a}.event_id = {t}.id"
        }
        Table::Materials => {
            "raw.materials {t} LEFT JOIN raw.inputs {t}_i ON {t}_i.id = {t}.input_id \
             LEFT JOIN raw.revisions {t}_v ON {t}_v.id = {t}.revision_id"
        }
        Table::Runs => "core.runs {t} LEFT JOIN core.manifests {t}_m ON {t}_m.id = {t}.manifest_id",
        // Registered sources, and every other source that has had events.
        Table::Sources => {
            "(SELECT r.name, r.shapes, r.version, r.event_types, r.purpose, r.expectation, \
                r.silent_after, r.every, r.expires_after FROM registry.sources r \
              UNION ALL \
              SELECT c.source, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM core.source_counts c \
              WHERE NOT EXISTS (SELECT 1 FROM registry.sources r WHERE r.name = c.source)) \
             {t}(name, shapes, version, event_types, purpose, expectation, silent_after, every, \
               expires_after)"
        }
        Table::Operations => "core.operations {t}",
        Table::Failures => {
            "raw.failures {t} LEFT JOIN raw.materials {t}_m ON {t}_m.id = {t}.material_id \
             LEFT JOIN raw.inputs {t}_i ON {t}_i.id = {t}_m.input_id"
        }
    }
}

/// A row's own tie-breaking key, so equal sort values never split differently
/// between pages.
pub(crate) fn tiebreak(table: Table) -> &'static str {
    match table {
        Table::Sources => "{t}.name",
        Table::Gaps => "{t}.source || '/' || {t}.run::text",
        _ => "{t}.id",
    }
}

/// A column of the source's counts: `{t}` is the source row.
fn source_count(column: &str) -> String {
    format!("(SELECT sc.{column} FROM core.source_counts sc WHERE sc.source = {{t}}.name)")
}

/// The SQL of a declared field. The match is exhaustive, so a field the model
/// declares without SQL here fails the build; a payload field an alias
/// promotes has its SQL in [`alias_field`].
pub(crate) fn field(id: FieldId) -> Option<String> {
    let event = |field: EventField| -> &'static str {
        match field {
            EventField::Id => "{t}.id",
            EventField::EventType => "{t}.event_type",
            EventField::KindVersion => "{t}.kind_version",
            EventField::Validation => "{t}.validation",
            EventField::Source => "{t}.source",
            EventField::Host => "(SELECT r.host FROM core.runs r WHERE r.id = {t}.run_id)",
            EventField::When => "{t}.ts_orig",
            EventField::TimeQuality => "{t}.ts_quality",
            EventField::TimeSuspect => {
                "(CASE WHEN {t}.ts_orig > {t}.ts_coided + interval '5 minutes' THEN 'future' \
                  WHEN {t}.ts_orig = 'epoch'::timestamptz THEN 'epoch zero' END)"
            }
            EventField::Seen => "{t}.ts_coided",
            EventField::Written => "{t}.ts_persisted",
            EventField::Origin => {
                "(CASE WHEN {t}.material_id IS NULL THEN 'derived' ELSE 'material' END)"
            }
            EventField::Material => "{t}.material_id",
            EventField::Offset => "{t}.anchor",
            EventField::Output => "{t}.output_index::bigint",
            EventField::Parents => "{t}.parents",
            EventField::Producer => "(SELECT r.name FROM core.runs r WHERE r.id = {t}.run_id)",
            EventField::ProducerVersion => {
                "(SELECT m.version FROM core.runs r JOIN core.manifests m ON m.id = r.manifest_id \
                  WHERE r.id = {t}.run_id)"
            }
            EventField::Run => "{t}.run_id",
            EventField::Text => {
                "(SELECT s.text FROM core.visible_search_text s WHERE s.event_id = {t}.id)"
            }
            EventField::Key => "{t}.equivalence_key",
            EventField::Class => "{t}.class",
            EventField::State => "{t}.record_state",
            EventField::CommitSeq => "{t}.commit_seq",
            EventField::Payload => "{t}.payload",
        }
    };
    let sql: &str = match id {
        FieldId::Event(field) => event(field),
        FieldId::Archive(field) => match field {
            ArchiveField::Archived => "{a}.recorded_at",
            ArchiveField::Reason => "{a}.reason",
            ArchiveField::Operation => "{a}.operation_id",
            ArchiveField::ReplacedBy => "{a}.replaced_by",
            ArchiveField::ArchivedSeq => "{a}.commit_seq",
        },
        FieldId::Gap(field) => match field {
            GapField::Source => "{t}.source",
            GapField::Run => "{t}.run",
            GapField::Start => "{t}.start",
            GapField::End => "{t}.end",
            GapField::Reason => "{t}.reason",
        },
        FieldId::Material(field) => match field {
            MaterialField::Id => "{t}.id",
            MaterialField::Binding => "{t}_i.binding",
            MaterialField::Parser => "{t}.parser",
            MaterialField::ParserVersion => "{t}.parser_version::bigint",
            MaterialField::Hash => "{t}.blob",
            MaterialField::Size => "{t}.length",
            MaterialField::Stored => "{t}_v.started_at",
            MaterialField::OriginPath => "{t}_i.locator",
            MaterialField::Host => "{t}_i.host",
            MaterialField::Part => "{t}.part_index::bigint",
            MaterialField::PreviousPart => "{t}.previous_part_id",
            MaterialField::Events => {
                "(SELECT count(*) FROM core.visible_events me WHERE me.material_id = {t}.id \
              AND NOT EXISTS (SELECT 1 FROM audit.archived ma WHERE ma.event_id = me.id))"
            }
            MaterialField::First => {
                "(SELECT min(me.ts_orig) FROM core.visible_events me WHERE me.material_id = {t}.id \
              AND NOT EXISTS (SELECT 1 FROM audit.archived ma WHERE ma.event_id = me.id))"
            }
            MaterialField::Last => {
                "(SELECT max(me.ts_orig) FROM core.visible_events me WHERE me.material_id = {t}.id \
              AND NOT EXISTS (SELECT 1 FROM audit.archived ma WHERE ma.event_id = me.id))"
            }
        },
        FieldId::Run(field) => match field {
            RunField::Id => "{t}.id",
            RunField::Producer => "{t}.name",
            RunField::ProducerVersion => "{t}_m.version",
            RunField::Host => "{t}.host",
            RunField::Started => "{t}.started_at",
            RunField::Ended => "{t}.ended_at",
            RunField::Status => {
                "(CASE WHEN {t}.ended_at IS NULL THEN 'running' ELSE 'finished' END)"
            }
            RunField::Events => "{t}.events",
            RunField::Failed => "(SELECT count(*) FROM raw.failures rf WHERE rf.run_id = {t}.id)",
        },
        FieldId::Operation(field) => match field {
            OperationField::Id => "{t}.id",
            OperationField::Kind => "{t}.kind",
            OperationField::Actor => "{t}.actor",
            OperationField::Scope => "{t}.scope::text",
            OperationField::Started => "{t}.at",
        },
        FieldId::Failure(field) => match field {
            FailureField::Id => "{t}.id",
            FailureField::When => "{t}.failed_at",
            FailureField::Source => {
                "(SELECT b.source FROM raw.bindings b WHERE b.binding={t}_i.binding)"
            }
            FailureField::Material => "{t}.material_id",
            FailureField::Offset => "{t}.anchor",
            FailureField::Reason => "{t}.reason",
        },
        FieldId::Source(field) => {
            return Some(match field {
                SourceField::Continuity => super::source_views::continuity(),
                SourceField::Gaps => super::source_views::gaps(),
                SourceField::Drift => super::source_views::drift(),
                SourceField::RecordFailures => super::source_views::record_failures(),
                SourceField::Binding | SourceField::Source => "{t}.name".to_string(),
                SourceField::Shapes => "array_to_string({t}.shapes, ',')".to_string(),
                SourceField::Expectation => "{t}.expectation".to_string(),
                SourceField::SilentAfter => {
                    "extract(epoch FROM {t}.silent_after)::bigint".to_string()
                }
                SourceField::Every => "extract(epoch FROM {t}.every)::bigint".to_string(),
                SourceField::ExpiresAfter => {
                    "extract(epoch FROM {t}.expires_after)::bigint".to_string()
                }
                SourceField::Version => "{t}.version::bigint".to_string(),
                SourceField::EventTypes => "array_to_string({t}.event_types, ',')".to_string(),
                SourceField::Purpose => "{t}.purpose".to_string(),
                SourceField::Events => format!("coalesce({}, 0)", source_count("events")),
                SourceField::First => source_count("first_event"),
                SourceField::LastEvent => source_count("last_event"),
                SourceField::LastSeen => source_count("last_seen"),
                SourceField::Failed24h => "(SELECT count(*) FROM raw.failures sf \
                  JOIN raw.materials sm ON sm.id = sf.material_id \
                  JOIN raw.inputs si ON si.id = sm.input_id \
                  JOIN raw.bindings sb ON sb.binding = si.binding \
                  WHERE sb.source = {t}.name AND sf.failed_at > now() - interval '24 hours')"
                    .to_string(),
            });
        }
        FieldId::Promoted => return None,
    };
    Some(sql.to_string())
}

/// A payload value read as text, a number or a time, `None` when it is absent
/// or of another type (three-valued: unknown, never coerced).
fn payload_text(path: &str) -> String {
    format!("({{t}}.payload #>> '{{{path}}}')")
}

fn payload_number(path: &str) -> String {
    format!(
        "(CASE WHEN jsonb_typeof({{t}}.payload #> '{{{path}}}') = 'number' THEN ({{t}}.payload #>> '{{{path}}}')::numeric END)"
    )
}

fn payload_time(path: &str) -> String {
    format!("core.payload_time({{t}}.payload #>> '{{{path}}}')")
}

fn payload_bool(path: &str) -> String {
    format!(
        "(CASE WHEN jsonb_typeof({{t}}.payload #> '{{{path}}}') = 'boolean' THEN ({{t}}.payload #>> '{{{path}}}')::boolean END)"
    )
}

/// A promoted field of an alias.
pub(crate) fn alias_field(alias: &str, name: &str) -> Option<String> {
    Some(match (alias, name) {
        ("telemetry", "component" | "name") => payload_text(name),
        ("shell-sessions", "host" | "day") => payload_text(name),
        ("shell-sessions", "commands" | "failed") => payload_number(name),
        ("shell-sessions", "start") => payload_time("started_at"),
        ("shell-sessions", "observed_until") => payload_time("ended_at"),
        ("shell-sessions", "duration") => format!(
            "extract(epoch from ({} - {}))", payload_time("ended_at"), payload_time("started_at")
        ),
        ("shell-sessions", "cwds") => "(CASE WHEN jsonb_typeof({t}.payload -> 'cwds') = 'array' THEN ARRAY(SELECT jsonb_array_elements_text({t}.payload -> 'cwds')) END)".to_string(),
        ("commands", "command") => payload_text("command"),
        ("commands", "cwd") => payload_text("cwd"),
        ("commands", "exit_code") => payload_number("exit_code"),
        ("commands", "duration") => format!("({} / 1e9)", payload_number("duration_ns")),
        ("commands", "start") => payload_time("started_at"),
        // A command without a recorded duration (a history file, or a shell
        // that did not report one) is placed at its start instant; an unknown
        // end must not read as a command still running.
        ("commands", "end") => format!(
            "coalesce({start} + make_interval(secs => {} / 1e9), {start})",
            payload_number("duration_ns"),
            start = payload_time("started_at"),
        ),
        ("commands", "session") => payload_text("session"),
        // A fused command was observed by its members' sources; a command
        // record no fused command joins, by its own source.
        ("commands", "observed_by") => {
            "coalesce((SELECT string_agg(m ->> 'source', ',' ORDER BY n) FROM jsonb_array_elements(\
              CASE WHEN jsonb_typeof({t}.payload -> 'members') = 'array' THEN {t}.payload -> 'members' END) WITH ORDINALITY AS x(m, n)), {t}.source)"
                .to_string()
        }
        ("duplicates", "family" | "left" | "right" | "left_source" | "right_source" | "strategy" | "decided_by") => {
            payload_text(name)
        }
        ("duplicates", "candidates") => payload_number(name),
        ("duplicates", "distance") => format!("({} / 1e9)", payload_number("distance_ns")),
        ("duplicates", "chosen") => payload_bool(name),
        ("duplicates", "decision") => payload_text("state"),
        ("focus", "app") => format!("coalesce({}, {})", payload_text("attributes,class"), payload_text("app")),
        ("focus", "title") => format!("coalesce({}, {})", payload_text("attributes,title"), payload_text("title")),
        ("visits", "url") => format!("coalesce({}, {})", payload_text("url"), payload_text("to_url")),
        ("visits", "title") => format!("coalesce({}, {})", payload_text("title"), payload_text("to_title")),
        ("plays", "title") => format!(
            "coalesce({}, {}, {})",
            payload_text("track_name"),
            payload_text("episode_name"),
            payload_text("title")
        ),
        ("plays", "artist") => format!(
            "coalesce({}, {}, {})",
            payload_text("artist_name"),
            payload_text("show_name"),
            payload_text("artists,0")
        ),
        ("plays", "album") => format!("coalesce({}, {})", payload_text("album_name"), payload_text("album")),
        ("plays", "player") => payload_text("player"),
        ("plays", "start") => "{t}.ts_orig".to_string(),
        // A player's report ends its track's length later; a stream of
        // unknown length reports a length no track reaches.
        ("plays", "end") => format!(
            "coalesce({}, CASE WHEN {length} BETWEEN 0 AND 9223372036854775 \
              THEN {{t}}.ts_orig + make_interval(secs => {length} / 1e6) END)",
            payload_time("ended_at"),
            length = payload_number("length_us"),
        ),
        ("focus", "start") => payload_time("started_at"),
        ("focus", "end") => payload_time("ended_at"),
        ("focus" | "plays", "duration") => format!(
            "extract(epoch from ({} - {}))",
            alias_field(alias, "end")?,
            alias_field(alias, "start")?
        ),
        ("items", "item" | "label" | "revision" | "content" | "media_type") => payload_text(name),
        ("media", "path" | "media_type" | "content") => payload_text(name),
        ("media", "size" | "width" | "height") => payload_number(name),
        ("media", "duration") => format!("({} / 1e3)", payload_number("duration_ms")),
        ("media", "stored") => payload_bool(name),
        ("curation", "item") => payload_text("target,item"),
        ("curation", "name") => payload_text("key"),
        ("curation", "value_origin") => payload_text("origin"),
        ("curation", "kind" | "value" | "author" | "request") => payload_text(name),
        ("figures", "metric" | "grain" | "day" | "dimension" | "dimension_value") => {
            payload_text(name)
        }
        ("figures", "value" | "numerator" | "denominator" | "rows") => payload_number(name),
        ("figures", "start") => payload_time("period_start"),
        ("figures", "end") => payload_time("period_end"),
        ("intervals" | "now", "start") => payload_time("started_at"),
        ("intervals" | "now", "end") => payload_time("ended_at"),
        ("intervals" | "now", "duration") => format!(
            "extract(epoch from ({} - {}))",
            payload_time("ended_at"),
            payload_time("started_at")
        ),
        ("intervals" | "now", "state") => payload_text("state_kind"),
        ("intervals" | "now", "subject") => payload_text("label"),
        ("intervals" | "now", "open") => {
            "(coalesce(jsonb_typeof({t}.payload -> 'ended_at'), 'null') = 'null' AND coalesce(jsonb_typeof({t}.payload -> 'fenced_at'), 'null') = 'null')".to_string()
        }
        _ => return None,
    })
}

/// The span of a row, for `during` and `containing`: `(start, end)`. An end
/// that is unknown is an open span. The boolean marks an included upper bound.
pub(crate) fn span(table: Table, alias: Option<&str>) -> Option<(String, String, bool)> {
    match (table, alias) {
        (Table::Events, Some(a @ ("intervals" | "now"))) => Some((
            alias_field(a,"start")?,
            "coalesce(core.payload_time({t}.payload ->> 'ended_at'), core.payload_time({t}.payload ->> 'fenced_at'))".into(),
            false,
        )),
        (Table::Events, Some(a)) => Some((
            alias_field(a, "start")?,
            alias_field(
                a,
                if a == "shell-sessions" {
                    "observed_until"
                } else {
                    "end"
                },
            )?,
            a == "shell-sessions",
        )),
        (Table::Runs, None) => Some(("{t}.started_at".into(), "{t}.ended_at".into(), false)),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
