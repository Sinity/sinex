//! Schema descriptions generate fresh DDL and the structural part of upgrades.
use crate::{Checks, event_types, tables};
use sinex_model::KindDeclaration;
use std::collections::BTreeMap;

pub(super) mod version_nine;

pub(crate) struct View {
    pub name: String,
    pub columns: Vec<String>,
    pub statement: String,
}

pub(crate) struct Description {
    pub tables: Vec<String>,
    pub after_tables: Vec<String>,
    pub visibility: Vec<String>,
    pub views: Vec<View>,
    pub kinds: Vec<KindDeclaration>,
    previous_producer_views: Vec<String>,
}

impl Description {
    pub(crate) fn current() -> Self {
        Self {
            tables: tables::tables(&Checks::from_model()),
            after_tables: tables::after_tables(),
            visibility: crate::visibility(),
            views: event_types::ALL.iter().map(crate::kind_view).collect(),
            kinds: event_types::declarations().to_vec(),
            previous_producer_views: vec![],
        }
    }

    pub(crate) fn before_output_ordinals() -> Self {
        let mut description = Self::current();
        for statement in &mut description.tables {
            *statement = statement
                .replace("output_index bigint", "output_index smallint")
                .replace(
                    "ord bigint PRIMARY KEY CHECK (ord >= 0)",
                    "ord integer PRIMARY KEY",
                )
                .replace(
                    "output_index smallint NOT NULL CHECK (output_index >= 0)",
                    "output_index smallint NOT NULL",
                );
            if statement.starts_with("CREATE TABLE core.live_locations (")
                || statement.starts_with("CREATE TABLE core.hidden_records (")
            {
                *statement = statement.replace(
                    "  CONSTRAINT output_index_in_range CHECK (output_index >= 0),\n",
                    "",
                );
            }
        }
        description
    }

    /// Type and range changes come from the same declarations as fresh tables.
    pub(crate) fn output_ordinal_changes() -> Vec<String> {
        let before = Self::before_output_ordinals();
        let after = Self::current();
        let mut changes = Vec::new();
        for (name, column) in [
            ("core.events", "output_index"),
            ("core.live_locations", "output_index"),
            ("core.hidden_records", "output_index"),
            ("core.admission_stage", "output_index"),
            ("core.admission_stage", "ord"),
        ] {
            let declared = after
                .tables
                .iter()
                .filter_map(|sql| Table::read(sql))
                .find(|table| table.name == name)
                .expect("ordinal table is declared");
            let prior = before
                .tables
                .iter()
                .filter_map(|sql| Table::read(sql))
                .find(|table| table.name == name)
                .expect("prior ordinal table is declared");
            let definition = &declared.columns[column];
            let data_type = definition
                .split_whitespace()
                .next()
                .expect("ordinal SQL type");
            changes.push(format!(
                "ALTER TABLE {name} ALTER COLUMN {column} TYPE {data_type}"
            ));
            if let Some((_, check)) = definition.split_once(" CHECK ") {
                changes.push(format!("ALTER TABLE {name} ADD CHECK {check}"));
            }
            for constraint in &declared.constraints {
                if !prior.constraints.contains(constraint) {
                    changes.push(format!("ALTER TABLE {name} ADD {constraint}"));
                }
            }
        }
        changes
    }

    pub(crate) fn before_foundations() -> Self {
        let mut description = Self::before_output_ordinals();
        description.tables.retain(|statement| {
            !statement.starts_with("CREATE TABLE core.blob_segments (")
                && !statement.starts_with("CREATE TABLE core.blob_extents (")
                && !statement.starts_with("CREATE TABLE core.producer_orders (")
                && !statement.starts_with("CREATE INDEX events_feed ")
                && !statement.starts_with("CREATE INDEX archive_changes_feed ")
                && !statement.starts_with("CREATE INDEX tombstones_feed ")
                && !statement.starts_with("CREATE INDEX tombstones_original_position ")
        });
        for statement in &mut description.tables {
            if statement.starts_with("CREATE TABLE core.ingress_frames (") {
                *statement = statement
                    .replace(",\n  family text", "")
                    .replace(",\n  write_kind_prefixes text[]", "");
            }
            if statement.starts_with("CREATE TABLE core.published (") {
                *statement = statement
                    .replace(",\n  stream_instance text NOT NULL DEFAULT ''", "")
                    .replace(
                        "PRIMARY KEY (event_id, stream_seq, stream_instance)",
                        "PRIMARY KEY (event_id, stream_seq)",
                    );
            }
            if statement.starts_with("CREATE TABLE core.event_tombstones (") {
                *statement = statement
                    .replace(",\n  commit_seq bigint", "")
                    .replace(",\n  original_commit_seq bigint", "")
                    .replace(",\n  class text", "");
            }
            *statement = statement.replace(
                "VALUES ('commit_seq', 0), ('excised', 0)",
                "VALUES ('commit_seq', 0)",
            );
            if statement.starts_with("CREATE TABLE registry.derivations (")
                || statement.starts_with("CREATE TABLE derivation.comparisons (")
            {
                *statement = statement
                    .replace("version bigint NOT NULL", "version integer NOT NULL")
                    .replace(",\n  context jsonb NOT NULL DEFAULT '{}'::jsonb", "")
                    .replace(",\n  build text NOT NULL DEFAULT ''", "");
            }
        }
        description
    }

    pub(crate) fn before_segments() -> Self {
        let mut description = Self::before_output_ordinals();
        description.tables.retain(|statement| {
            !statement.starts_with("CREATE TABLE core.blob_segments (")
                && !statement.starts_with("CREATE TABLE core.blob_extents (")
                && !statement.starts_with("CREATE TABLE core.producer_orders (")
                && !statement.starts_with("CREATE INDEX events_feed ")
                && !statement.starts_with("CREATE INDEX archive_changes_feed ")
                && !statement.starts_with("CREATE INDEX tombstones_feed ")
                && !statement.starts_with("CREATE INDEX tombstones_original_position ")
        });
        for statement in &mut description.tables {
            if statement.starts_with("CREATE TABLE core.ingress_frames (") {
                *statement = statement
                    .replace(",\n  family text", "")
                    .replace(",\n  write_kind_prefixes text[]", "");
            }
            if statement.starts_with("CREATE TABLE core.published (") {
                *statement = statement
                    .replace(",\n  stream_instance text NOT NULL DEFAULT ''", "")
                    .replace(
                        "PRIMARY KEY (event_id, stream_seq, stream_instance)",
                        "PRIMARY KEY (event_id, stream_seq)",
                    );
            }
            if statement.starts_with("CREATE TABLE core.event_tombstones (") {
                *statement = statement
                    .replace(",\n  commit_seq bigint", "")
                    .replace(",\n  original_commit_seq bigint", "")
                    .replace(",\n  class text", "");
            }
        }
        description
    }

    /// Producer metadata is additive; its version columns widen without changing values.
    pub(crate) fn producer_metadata_changes() -> Vec<String> {
        let after = Self::before_segments();
        let mut before = Self::before_foundations();
        let mut changes = Vec::new();
        for name in ["registry.derivations", "derivation.comparisons"] {
            let declared = after
                .tables
                .iter()
                .filter_map(|statement| Table::read(statement))
                .find(|table| table.name == name)
                .expect("producer table is declared");
            let definition = declared
                .columns
                .get("version")
                .expect("producer version is declared");
            let data_type = definition
                .split_whitespace()
                .next()
                .expect("version has a SQL type");
            let qualified = name
                .split('.')
                .map(crate::quote_ident)
                .collect::<Vec<_>>()
                .join(".");
            changes.push(format!(
                "ALTER TABLE {qualified} ALTER COLUMN version TYPE {data_type}"
            ));
            for statement in &mut before.tables {
                if statement.starts_with(&format!("CREATE TABLE {name} (")) {
                    *statement = statement
                        .replace("version integer NOT NULL", &format!("version {definition}"));
                }
            }
        }
        // The named data step fills the new counter from preserved rows.
        for statement in &mut before.tables {
            if statement.starts_with("INSERT INTO core.counters") {
                if let Some(initializer) = after
                    .tables
                    .iter()
                    .find(|current| current.starts_with("INSERT INTO core.counters"))
                {
                    statement.clone_from(initializer);
                }
            }
        }
        changes.extend(after.changes_from(&before, &[]));
        changes
    }

    pub(crate) fn before_kind_declarations() -> Self {
        Self {
            tables: version_nine::tables(&version_nine::checks()),
            after_tables: version_nine::after_tables(),
            visibility: vec![],
            views: vec![],
            kinds: vec![],
            previous_producer_views: version_nine::MOVED_VIEWS
                .iter()
                .map(|name| (*name).into())
                .collect(),
        }
    }

    pub(crate) async fn apply(
        &self,
        connection: &mut sqlx::PgConnection,
        owners: &crate::kinds::Owners,
    ) -> Result<(), crate::Error> {
        crate::kinds::install(connection, owners, &self.kinds).await?;
        crate::apply_views(connection, self).await
    }

    pub(crate) fn statements(&self) -> Vec<String> {
        let mut statements = self.tables.clone();
        statements.extend(self.after_tables.clone());
        statements.extend(self.visibility.clone());
        statements.extend(self.views.iter().map(|view| view.statement.clone()));
        statements
    }

    /// Table renames that preserve rows for a data transformation happen first.
    /// Every other structural change is derived from the two descriptions.
    pub(crate) fn changes_from(&self, before: &Self, renamed: &[&str]) -> Vec<String> {
        let before_tables: BTreeMap<_, _> = before
            .tables
            .iter()
            .filter_map(|statement| Table::read(statement))
            .filter(|table| !renamed.contains(&table.name.as_str()))
            .map(|table| (table.name.clone(), table))
            .collect();
        let mut changes: Vec<String> = before
            .previous_producer_views
            .iter()
            .map(|name| {
                format!(
                    "DROP VIEW IF EXISTS event_types.{}",
                    crate::quote_ident(name)
                )
            })
            .collect();
        for statement in &self.tables {
            if let Some(table) = Table::read(statement) {
                if let Some(prior) = before_tables.get(&table.name) {
                    for (name, definition) in &table.columns {
                        match prior.columns.get(name) {
                            Some(previous) => assert_eq!(
                                previous, definition,
                                "column changes need a declared transformation: {}.{name}",
                                table.name
                            ),
                            None => changes.push(format!(
                                "ALTER TABLE {} ADD COLUMN {name} {definition}",
                                table.name
                            )),
                        }
                    }
                    assert!(
                        prior
                            .columns
                            .keys()
                            .all(|name| table.columns.contains_key(name)),
                        "column removals need a declared transformation: {}",
                        table.name
                    );
                    assert_eq!(
                        prior.constraints, table.constraints,
                        "constraint changes need a declared transformation: {}",
                        table.name
                    );
                } else {
                    changes.push(statement.clone());
                }
            } else if !before.tables.contains(statement) {
                changes.push(statement.clone());
            }
        }
        for statement in &self.after_tables {
            if !before.after_tables.contains(statement) {
                changes.push(statement.clone());
            }
        }
        changes
    }
}

/// The table language is our own CREATE TABLE declarations. Quoted strings
/// and nested expressions are kept intact when separating its columns.
struct Table {
    name: String,
    columns: BTreeMap<String, String>,
    constraints: Vec<String>,
}

impl Table {
    fn read(statement: &str) -> Option<Self> {
        let statement = statement
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let rest = statement
            .strip_prefix("CREATE TABLE ")
            .or_else(|| statement.strip_prefix("CREATE UNLOGGED TABLE "))?;
        let (name, body) = rest.split_once('(').expect("table has columns");
        let body = body.trim().strip_suffix(')').expect("table closes columns");
        let mut table = Self {
            name: name.trim().into(),
            columns: BTreeMap::new(),
            constraints: vec![],
        };
        for entry in entries(body) {
            let (name, definition) = entry
                .split_once(char::is_whitespace)
                .expect("column has a type");
            if ["PRIMARY", "CONSTRAINT", "UNIQUE", "CHECK", "FOREIGN"].contains(&name) {
                table
                    .constraints
                    .push(entry.split_whitespace().collect::<Vec<_>>().join(" "));
            } else {
                table.columns.insert(
                    name.into(),
                    definition.split_whitespace().collect::<Vec<_>>().join(" "),
                );
            }
        }
        Some(table)
    }
}

fn entries(body: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    let mut quote = None;
    let mut bytes = body.bytes().enumerate().peekable();
    while let Some((at, byte)) = bytes.next() {
        if let Some(quoted) = quote {
            if byte == quoted {
                if bytes.peek().is_some_and(|(_, next)| *next == quoted) {
                    bytes.next();
                } else {
                    quote = None;
                }
            }
        } else {
            match byte {
                b'\'' | b'"' => quote = Some(byte),
                b'(' | b'[' => depth += 1,
                b')' | b']' => depth -= 1,
                b',' if depth == 0 => {
                    entries.push(body[start..at].trim());
                    start = at + 1;
                }
                _ => {}
            }
        }
    }
    assert_eq!(depth, 0, "balanced table declaration");
    assert_eq!(quote, None, "closed table quote");
    entries.push(body[start..].trim());
    entries
}

#[cfg(test)]
mod tests;
