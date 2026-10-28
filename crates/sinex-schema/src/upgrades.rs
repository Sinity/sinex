//! Forward upgrades preserve every row. The declarations carry the same
//! changes, so fresh and upgraded databases have the same schema.

pub(crate) const STEPS: &[fn() -> Vec<String>] = &[
    source_counts,
    whole_material_excision,
    coverage_runs,
    marks_and_saved_questions,
    blob_change_tracking,
    library_scans,
];

/// Landing order determines each step's schema version.
pub(crate) fn upgrades() -> Vec<Vec<String>> {
    STEPS.iter().map(|step| step()).collect()
}

fn source_counts() -> Vec<String> {
    vec![
        crate::tables::SOURCE_COUNTS.into(),
        "ALTER TABLE core.runs ADD COLUMN events bigint NOT NULL DEFAULT 0".into(),
        "CREATE TEMPORARY TABLE stored_counts ON COMMIT DROP AS
         SELECT e.source, e.run_id, count(*) AS events,
           count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)) AS live,
           min(e.ts_orig) AS first_event, max(e.ts_orig) AS last_event, max(e.ts_coided) AS last_seen
         FROM core.events e GROUP BY e.source, e.run_id"
            .into(),
        "INSERT INTO core.source_counts (source, events, first_event, last_event, last_seen)
         SELECT source, sum(live), min(first_event), max(last_event), max(last_seen)
         FROM stored_counts GROUP BY source"
            .into(),
        "UPDATE core.runs r SET events = c.events
         FROM (SELECT run_id, sum(events) AS events FROM stored_counts GROUP BY run_id) c
         WHERE r.id = c.run_id"
            .into(),
        "GRANT SELECT ON core.source_counts TO sinex_reader, sinex_writer".into(),
        "GRANT INSERT, UPDATE, DELETE ON core.source_counts TO sinex_writer".into(),
    ]
}

fn whole_material_excision() -> Vec<String> {
    vec![
        crate::tables::EXCISED_MATERIALS.into(),
        crate::tables::EXCISED_MATERIALS_BLOB.into(),
        "GRANT SELECT ON core.excised_materials TO sinex_reader, sinex_writer".into(),
        "GRANT INSERT, UPDATE, DELETE ON core.excised_materials TO sinex_writer".into(),
    ]
}

fn coverage_runs() -> Vec<String> {
    vec![
        "DROP INDEX IF EXISTS core.events_coverage".into(),
        crate::tables::COVERAGE_RUNS.into(),
    ]
}

fn marks_and_saved_questions() -> Vec<String> {
    register_kinds(&["operator.mark", "query.saved"], false)
}

fn blob_change_tracking() -> Vec<String> {
    crate::tables::blob_changes()
}

/// Register payload schemas and views together. Replacing a kind also updates its registry row.
pub(crate) fn register_kinds(names: &[&str], replace: bool) -> Vec<String> {
    let mut statements = Vec::new();
    for name in names {
        assert!(
            sinex_model::event_types::ALL
                .iter()
                .any(|kind| kind.event_type == *name),
            "unknown event type {name}"
        );
    }
    for kind in sinex_model::event_types::ALL
        .iter()
        .filter(|k| names.contains(&k.event_type))
    {
        if replace {
            statements.push(format!(
                "DROP VIEW IF EXISTS event_types.{}",
                crate::quote_ident(&kind.event_type.replace('.', "_"))
            ));
        }
        statements.push(crate::kind_view(kind));
        statements.push(format!(
            "GRANT SELECT ON event_types.{} TO sinex_reader, sinex_writer",
            crate::quote_ident(&kind.event_type.replace('.', "_"))
        ));
    }
    for (name, schema, key_kind, fields, rule) in crate::registry_rows()
        .into_iter()
        .filter(|(name, ..)| names.contains(&name.as_str()))
    {
        let literal = |s: &str| format!("'{}'", s.replace('\'', "''"));
        let fields = fields
            .iter()
            .map(|s| literal(s))
            .collect::<Vec<_>>()
            .join(",");
        let conflict = if replace {
            " ON CONFLICT (name) DO UPDATE SET payload_schema=EXCLUDED.payload_schema, key_kind=EXCLUDED.key_kind, key_fields=EXCLUDED.key_fields, replace_rule=EXCLUDED.replace_rule"
        } else {
            ""
        };
        statements.push(format!("INSERT INTO registry.event_types (name,payload_schema,key_kind,key_fields,replace_rule) VALUES ({},{}::jsonb,{},ARRAY[{fields}]::text[],{}){conflict}",literal(&name),literal(&schema.to_string()),literal(key_kind),literal(rule)));
    }
    statements
}

/// Request-created identifiers need an explicit decision about their references.
fn library_scans() -> Vec<String> {
    let mut statements = vec![
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM core.events WHERE event_type LIKE 'library.%') THEN
         RAISE EXCEPTION 'library records use request-created item IDs; decide how their references should follow deterministic item IDs before upgrading';
         END IF; END $$".into(),
    ];
    let names: Vec<_> = sinex_model::event_types::ALL
        .iter()
        .filter(|kind| kind.event_type.starts_with("library."))
        .map(|kind| kind.event_type)
        .collect();
    statements.extend(register_kinds(&names, true));
    statements
}
