//! The SQL each field of each unit lowers to (spec `query.md` §3), keyed by
//! the field names in `sinex_model::query::units`. A test checks that every
//! field there has an entry here.
//!
//! Templates name the unit's table alias `{t}` and, for the archive, the
//! archive row `{a}`; the compiler substitutes a distinct alias per nesting
//! level, so an inner query of a time join never shadows the outer one.
use sinex_model::query::Unit;

/// The `FROM` clause of a unit, with `{t}` (and `{a}`) for its aliases.
pub(crate) fn from(unit: Unit) -> &'static str {
    match unit {
        Unit::Events => "core.visible_events {t}",
        Unit::Archive => {
            "core.visible_events {t} JOIN audit.archive_changes {a} ON {a}.event_id = {t}.id"
        }
        Unit::Materials => {
            "raw.materials {t} LEFT JOIN raw.inputs {t}_i ON {t}_i.id = {t}.input_id \
             LEFT JOIN raw.revisions {t}_v ON {t}_v.id = {t}.revision_id"
        }
        Unit::Runs => "core.runs {t} LEFT JOIN core.manifests {t}_m ON {t}_m.id = {t}.manifest_id",
        // Registered sources, and every source that has events, found by a
        // loose index scan over (source, ts_orig) rather than a full scan.
        Unit::Sources => {
            "(WITH RECURSIVE seen(name) AS ( \
                SELECT min(source) FROM core.events \
                UNION ALL \
                SELECT (SELECT min(source) FROM core.events WHERE source > seen.name) FROM seen WHERE seen.name IS NOT NULL) \
              SELECT r.name, r.driver, r.version, r.event_types, r.consumer FROM registry.sources r \
              UNION ALL \
              SELECT seen.name, NULL, NULL, NULL, NULL FROM seen \
              WHERE seen.name IS NOT NULL AND NOT EXISTS (SELECT 1 FROM registry.sources r WHERE r.name = seen.name)) \
             {t}(name, driver, version, event_types, consumer)"
        }
        Unit::Operations => "core.operations {t}",
        Unit::Failures => {
            "raw.failures {t} LEFT JOIN raw.materials {t}_m ON {t}_m.id = {t}.material_id \
             LEFT JOIN raw.inputs {t}_i ON {t}_i.id = {t}_m.input_id"
        }
    }
}

/// A row's own tie-breaking key, so equal sort values never split differently
/// between pages.
pub(crate) fn tiebreak(unit: Unit) -> &'static str {
    match unit {
        Unit::Sources => "{t}.name",
        _ => "{t}.id",
    }
}

/// Live events by a source: `{t}` is the source row.
const SOURCE_LIVE: &str = "FROM core.events se WHERE se.source = {t}.name \
     AND NOT EXISTS (SELECT 1 FROM audit.archived sa WHERE sa.event_id = se.id)";

pub(crate) fn field(unit: Unit, name: &str) -> Option<String> {
    let event = |name: &str| -> Option<&'static str> {
        Some(match name {
            "id" => "{t}.id",
            "kind" => "{t}.event_type",
            "source" => "{t}.source",
            "host" => "(SELECT r.host FROM core.runs r WHERE r.id = {t}.run_id)",
            "when" => "{t}.ts_orig",
            "time_quality" => "{t}.ts_quality",
            "seen" => "{t}.ts_coided",
            "written" => "{t}.ts_persisted",
            "origin" => "(CASE WHEN {t}.material_id IS NULL THEN 'derived' ELSE 'material' END)",
            "material" => "{t}.material_id",
            "anchor" => "{t}.anchor",
            "ordinal" => "{t}.ordinal::bigint",
            "parents" => "{t}.parents",
            "producer" => "(SELECT r.name FROM core.runs r WHERE r.id = {t}.run_id)",
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
    let sql: &str = match unit {
        Unit::Events => event(name)?,
        Unit::Archive => match name {
            "archived" => "{a}.archived_at",
            "reason" => "{a}.reason",
            "operation" => "{a}.operation_id",
            "replaced_by" => "{a}.replaced_by",
            "archived_seq" => "{a}.archived_seq",
            "known_from" => "{a}.commit_seq",
            "baseline" => "{a}.baseline",
            _ => event(name)?,
        },
        Unit::Materials => match name {
            "id" => "{t}.id",
            "binding" => "{t}_i.binding",
            "parser" => "{t}.parser",
            "parser_version" => "{t}.parser_version::bigint",
            "hash" => "{t}.blob",
            "size" => "{t}.durable_bytes",
            "staged" => "{t}_v.started_at",
            "origin_path" => "{t}_i.locator",
            "host" => "{t}_i.host",
            "part" => "{t}.part_index::bigint",
            "continues" => "{t}.previous_part_id",
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
        Unit::Runs => match name {
            "id" => "{t}.id",
            "producer" => "{t}.name",
            "producer_version" => "{t}_m.version",
            "host" => "{t}.host",
            "started" => "{t}.started_at",
            "ended" => "{t}.ended_at",
            "status" => "(CASE WHEN {t}.ended_at IS NULL THEN 'running' ELSE 'finished' END)",
            "events" => "(SELECT count(*) FROM core.events re WHERE re.run_id = {t}.id)",
            "failed" => "(SELECT count(*) FROM raw.failures rf WHERE rf.run_id = {t}.id)",
            _ => return None,
        },
        Unit::Sources => {
            return Some(match name {
                "binding" | "source" => "{t}.name".to_string(),
                "driver" => "{t}.driver".to_string(),
                "version" => "{t}.version::bigint".to_string(),
                "kinds" => "array_to_string({t}.event_types, ',')".to_string(),
                "consumer" => "{t}.consumer".to_string(),
                "events" => format!("(SELECT count(*) {SOURCE_LIVE})"),
                "first" => format!("(SELECT min(se.ts_orig) {SOURCE_LIVE})"),
                "last" | "last_event" => format!("(SELECT max(se.ts_orig) {SOURCE_LIVE})"),
                "last_seen" => format!("(SELECT max(se.ts_coided) {SOURCE_LIVE})"),
                "failed_24h" => "(SELECT count(*) FROM raw.failures sf \
                      JOIN raw.materials sm ON sm.id = sf.material_id \
                      JOIN raw.inputs si ON si.id = sm.input_id \
                      WHERE si.binding = {t}.name AND sf.failed_at > now() - interval '24 hours')"
                    .to_string(),
                _ => return None,
            });
        }
        Unit::Operations => match name {
            "id" => "{t}.id",
            "kind" => "{t}.kind",
            "actor" => "{t}.actor",
            "scope" => "{t}.scope::text",
            "started" => "{t}.at",
            _ => return None,
        },
        Unit::Failures => match name {
            "id" => "{t}.id",
            "when" => "{t}.failed_at",
            "stage" => "'parse'::text",
            "source" => "{t}_i.binding",
            "material" => "{t}.material_id",
            "anchor" => "{t}.anchor",
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
    format!(
        "(CASE WHEN ({{t}}.payload #>> '{{{path}}}') ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}T' THEN ({{t}}.payload #>> '{{{path}}}')::timestamptz END)"
    )
}

fn payload_bool(path: &str) -> String {
    format!(
        "(CASE WHEN jsonb_typeof({{t}}.payload #> '{{{path}}}') = 'boolean' THEN ({{t}}.payload #>> '{{{path}}}')::boolean END)"
    )
}

/// A promoted field of an alias.
pub(crate) fn alias_field(alias: &str, name: &str) -> Option<String> {
    Some(match (alias, name) {
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
        ("commands", "observed_by") => {
            "(SELECT string_agg(x, ',') FROM jsonb_array_elements_text(\
              CASE WHEN jsonb_typeof({t}.payload -> 'observed_by') = 'array' THEN {t}.payload -> 'observed_by' END) x)"
                .to_string()
        }
        ("items", "item" | "label" | "content" | "media_type") => payload_text(name),
        ("items", "revision") => payload_number(name),
        ("media", "path" | "media_type" | "content") => payload_text(name),
        ("media", "size" | "width" | "height") => payload_number(name),
        ("media", "duration") => format!("({} / 1e3)", payload_number("duration_ms")),
        ("media", "stored") => payload_bool(name),
        ("assignments", "item") => payload_text("target,item"),
        ("assignments", "field" | "value" | "level") => payload_text(name),
        (_, "start" | "end") => payload_time(name),
        (_, "duration" | "active") => payload_number(name),
        (_, "open") => payload_bool(name),
        (
            "sessions" | "intervals" | "now" | "days" | "pages",
            "project" | "depth" | "state" | "subject" | "day" | "top_sources" | "url" | "title"
            | "domain",
        ) => payload_text(name),
        _ => return None,
    })
}

/// The span of a row, for `during` and `containing`: `(start, end)`. An end
/// that is unknown is an open span. The boolean marks an included upper bound.
pub(crate) fn span(unit: Unit, alias: Option<&str>) -> Option<(String, String, bool)> {
    match (unit, alias) {
        (Unit::Events, Some(a)) => Some((
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
        (Unit::Runs, None) => Some(("{t}.started_at".into(), "{t}.ended_at".into(), false)),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
