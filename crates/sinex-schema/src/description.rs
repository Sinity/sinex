//! The schema description: the statements that create the current schema in
//! an empty database, what keeping an existing one current reinstalls, and
//! the structural part of an upgrade from an earlier description.
use crate::{Checks, event_types, tables};
use sinex_model::KindDeclaration;
use std::collections::BTreeMap;

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
}

impl Description {
    pub(crate) fn current() -> Self {
        Self {
            tables: tables::tables(&Checks::from_model()),
            after_tables: tables::after_tables(),
            visibility: crate::visibility(),
            views: event_types::ALL.iter().map(crate::kind_view).collect(),
            kinds: event_types::declarations().to_vec(),
        }
    }

    /// Install the bundled kinds and recreate the views and grants: what
    /// creation, an upgrade and every start run last.
    pub(crate) async fn apply(
        &self,
        connection: &mut sqlx::PgConnection,
        kind_prefixes: &crate::kinds::KindPrefixes,
    ) -> Result<(), crate::Error> {
        crate::kinds::install(connection, kind_prefixes, &self.kinds).await?;
        crate::apply_views(connection, self).await
    }

    pub(crate) fn statements(&self) -> Vec<String> {
        let mut statements = self.tables.clone();
        statements.extend(self.after_tables.clone());
        statements.extend(self.visibility.clone());
        statements.extend(self.views.iter().map(|view| view.statement.clone()));
        statements
    }

    /// The structural statements that take a database declared by `before`
    /// to this description: new tables and other new statements as declared,
    /// and the columns added to an existing table in one `ALTER TABLE`, so
    /// their constraints are checked in one scan of its rows. A changed or
    /// removed column or constraint is a data transformation, which an
    /// upgrade step declares itself; asking for one here is a defect.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the next structural upgrade step calls it")
    )]
    pub(crate) fn changes_from(&self, before: &Self) -> Vec<String> {
        let before_tables: BTreeMap<_, _> = before
            .tables
            .iter()
            .filter_map(|statement| Table::read(statement))
            .map(|table| (table.name.clone(), table))
            .collect();
        let mut changes = Vec::new();
        for statement in &self.tables {
            match Table::read(statement) {
                Some(table) => match before_tables.get(&table.name) {
                    Some(prior) => {
                        let mut added = Vec::new();
                        for (name, definition) in &table.columns {
                            match prior.columns.get(name) {
                                Some(previous) => assert_eq!(
                                    previous, definition,
                                    "column changes need a declared transformation: {}.{name}",
                                    table.name
                                ),
                                None => added.push(format!("ADD COLUMN {name} {definition}")),
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
                        if !added.is_empty() {
                            changes.push(format!(
                                "ALTER TABLE {} {}",
                                table.name,
                                added.join(", ")
                            ));
                        }
                    }
                    None => changes.push(statement.clone()),
                },
                None if !before.tables.contains(statement) => changes.push(statement.clone()),
                None => {}
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
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the next structural upgrade step calls it")
)]
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

/// The comma-separated entries of a table body, ignoring commas inside
/// quotes, parentheses and brackets.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the next structural upgrade step calls it")
)]
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
