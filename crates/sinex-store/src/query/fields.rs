//! The SQL each field of each table lowers to, keyed by the field names in
//! `sinex_model::query::tables`. A test checks that every field there has an
//! entry here.
//!
//! Templates name the table's table alias `{t}` and, for the archive, the
//! archive row `{a}`; the compiler substitutes a distinct alias per nesting
//! level, so an inner query of a time join never shadows the outer one.
use sinex_model::query::Table;

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
            "(SELECT r.name, r.driver, r.version, r.event_types, r.purpose FROM registry.sources r \
              UNION ALL \
              SELECT c.source, NULL, NULL, NULL, NULL FROM core.source_counts c \
              WHERE NOT EXISTS (SELECT 1 FROM registry.sources r WHERE r.name = c.source)) \
             {t}(name, driver, version, event_types, purpose)"
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

pub(crate) fn field(table: Table, name: &str) -> Option<String> {
    let event = |name: &str| -> Option<&'static str> {
        Some(match name {
            "id" => "{t}.id",
            "event_type" => "{t}.event_type",
            "kind_version" => "{t}.kind_version",
            "validation" => "{t}.validation",
            "source" => "{t}.source",
            "host" => "(SELECT r.host FROM core.runs r WHERE r.id = {t}.run_id)",
            "when" => "{t}.ts_orig",
            "time_quality" => "{t}.ts_quality",
            "time_suspect" => {
                "(CASE WHEN {t}.ts_orig > {t}.ts_coided + interval '5 minutes' THEN 'future' \
                  WHEN {t}.ts_orig = 'epoch'::timestamptz THEN 'epoch zero' END)"
            }
            "seen" => "{t}.ts_coided",
            "written" => "{t}.ts_persisted",
            "origin" => "(CASE WHEN {t}.material_id IS NULL THEN 'derived' ELSE 'material' END)",
            "material" => "{t}.material_id",
            "offset" => "{t}.anchor",
            "output" => "{t}.output_index::bigint",
            "parents" => "{t}.parents",
            "producer" => "(SELECT r.name FROM core.runs r WHERE r.id = {t}.run_id)",
            "producer_version" => {
                "(SELECT m.version FROM core.runs r JOIN core.manifests m ON m.id = r.manifest_id \
                  WHERE r.id = {t}.run_id)"
            }
            "run" => "{t}.run_id",
            "text" => "(SELECT s.text FROM core.visible_search_text s WHERE s.event_id = {t}.id)",
            "key" => "{t}.equivalence_key",
            "class" => "{t}.class",
            "state" => "{t}.record_state",
            "commit_seq" => "{t}.commit_seq",
            "payload" => "{t}.payload",
            _ => return None,
        })
    };
    let sql: &str = match table {
        Table::Events | Table::Drift => event(name)?,
        Table::Gaps => match name {
            "source" => "{t}.source",
            "run" => "{t}.run",
            "start" => "{t}.start",
            "end" => "{t}.end",
            "reason" => "{t}.reason",
            _ => return None,
        },
        Table::Archive => match name {
            "archived" => "{a}.recorded_at",
            "reason" => "{a}.reason",
            "operation" => "{a}.operation_id",
            "replaced_by" => "{a}.replaced_by",
            "archived_seq" => "{a}.commit_seq",
            _ => event(name)?,
        },
        Table::Materials => match name {
            "id" => "{t}.id",
            "binding" => "{t}_i.binding",
            "parser" => "{t}.parser",
            "parser_version" => "{t}.parser_version::bigint",
            "hash" => "{t}.blob",
            "size" => "{t}.length",
            "stored" => "{t}_v.started_at",
            "origin_path" => "{t}_i.locator",
            "host" => "{t}_i.host",
            "part" => "{t}.part_index::bigint",
            "previous_part" => "{t}.previous_part_id",
            "events" => {
                "(SELECT count(*) FROM core.events me WHERE me.material_id = {t}.id \
                  AND NOT EXISTS (SELECT 1 FROM audit.archived ma WHERE ma.event_id = me.id))"
            }
            "first" => {
                "(SELECT min(me.ts_orig) FROM core.events me WHERE me.material_id = {t}.id \
                  AND NOT EXISTS (SELECT 1 FROM audit.archived ma WHERE ma.event_id = me.id))"
            }
            "last" => {
                "(SELECT max(me.ts_orig) FROM core.events me WHERE me.material_id = {t}.id \
                  AND NOT EXISTS (SELECT 1 FROM audit.archived ma WHERE ma.event_id = me.id))"
            }
            _ => return None,
        },
        Table::Runs => match name {
            "id" => "{t}.id",
            "producer" => "{t}.name",
            "producer_version" => "{t}_m.version",
            "host" => "{t}.host",
            "started" => "{t}.started_at",
            "ended" => "{t}.ended_at",
            "status" => "(CASE WHEN {t}.ended_at IS NULL THEN 'running' ELSE 'finished' END)",
            "events" => "{t}.events",
            "failed" => "(SELECT count(*) FROM raw.failures rf WHERE rf.run_id = {t}.id)",
            _ => return None,
        },
        Table::Sources => {
            return Some(match name {
                "continuity" | "gaps" | "drift" | "record_failures" => {
                    super::source_views::field(name)?
                }
                "binding" | "source" => "{t}.name".to_string(),
                "driver" => "{t}.driver".to_string(),
                "version" => "{t}.version::bigint".to_string(),
                "event_types" => "array_to_string({t}.event_types, ',')".to_string(),
                "purpose" => "{t}.purpose".to_string(),
                "events" => format!("coalesce({}, 0)", source_count("events")),
                "first" => source_count("first_event"),
                "last_event" => source_count("last_event"),
                "last_seen" => source_count("last_seen"),
                "failed_24h" => "(SELECT count(*) FROM raw.failures sf \
                      JOIN raw.materials sm ON sm.id = sf.material_id \
                      JOIN raw.inputs si ON si.id = sm.input_id \
                      JOIN raw.bindings sb ON sb.binding = si.binding \
                      WHERE sb.source = {t}.name AND sf.failed_at > now() - interval '24 hours')"
                    .to_string(),
                _ => return None,
            });
        }
        Table::Operations => match name {
            "id" => "{t}.id",
            "kind" => "{t}.kind",
            "actor" => "{t}.actor",
            "scope" => "{t}.scope::text",
            "started" => "{t}.at",
            _ => return None,
        },
        Table::Failures => match name {
            "id" => "{t}.id",
            "when" => "{t}.failed_at",
            "source" => "(SELECT b.source FROM raw.bindings b WHERE b.binding={t}_i.binding)",
            "material" => "{t}.material_id",
            "offset" => "{t}.anchor",
            "reason" => "{t}.reason",
            _ => return None,
        },
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
        ("commands", "end") => format!(
            "({} + make_interval(secs => {} / 1e9))",
            payload_time("started_at"),
            payload_number("duration_ns")
        ),
        ("commands", "session") => payload_text("session"),
        ("commands" | "focus" | "visits" | "plays", "observed_by") => {
            "(SELECT string_agg(x, ',') FROM jsonb_array_elements_text(\
              CASE WHEN jsonb_typeof({t}.payload -> 'observed_by') = 'array' THEN {t}.payload -> 'observed_by' END) x)"
                .to_string()
        }
        ("focus", "app" | "title") | ("visits", "url" | "title") => payload_text(name),
        ("plays", "title" | "artist" | "album" | "player") => payload_text(name),
        ("focus" | "plays", "start") => payload_time("started_at"),
        ("focus" | "plays", "end") => payload_time("ended_at"),
        ("focus" | "plays", "duration") => format!(
            "extract(epoch from ({} - {}))",
            payload_time("ended_at"),
            payload_time("started_at")
        ),
        ("items", "item" | "label" | "revision" | "content" | "media_type") => payload_text(name),
        ("media", "path" | "media_type" | "content") => payload_text(name),
        ("media", "size" | "width" | "height") => payload_number(name),
        ("media", "duration") => format!("({} / 1e3)", payload_number("duration_ms")),
        ("media", "stored") => payload_bool(name),
        ("assignments", "item") => payload_text("target,item"),
        ("assignments", "field" | "value" | "level") => payload_text(name),
        ("figures", "metric" | "grain" | "day" | "dimension" | "dimension_value") => {
            payload_text(name)
        }
        ("figures", "value" | "numerator" | "denominator" | "rows") => payload_number(name),
        ("figures", "start") => payload_time("period_start"),
        ("figures", "end") => payload_time("period_end"),
        ("intervals" | "now", "start") => payload_time("started_at"),
        ("intervals" | "now", "end") => payload_time("ended_at"),
        ("intervals" | "now", "duration") => format!(
            "extract(epoch from (coalesce({}, now()) - {}))",
            payload_time("ended_at"),
            payload_time("started_at")
        ),
        ("intervals" | "now", "state") => payload_text("state_kind"),
        ("intervals" | "now", "subject") => payload_text("label"),
        ("intervals" | "now", "open") => {
            "(coalesce(jsonb_typeof({t}.payload -> 'ended_at'), 'null') = 'null')".to_string()
        }
        _ => return None,
    })
}

/// The span of a row, for `during` and `containing`: `(start, end)`. An end
/// that is unknown is an open span. The boolean marks an included upper bound.
pub(crate) fn span(table: Table, alias: Option<&str>) -> Option<(String, String, bool)> {
    match (table, alias) {
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
