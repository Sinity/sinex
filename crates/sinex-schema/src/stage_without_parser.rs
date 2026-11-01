//! A material staged before any parser recognises it has no parser: its
//! bytes are kept and a later replay assigns one. A parser and its version
//! are given together or not at all.

/// The parser columns of `raw.materials`.
pub(crate) const PARSER_COLUMNS: &str = "  parser text,\n  parser_version integer,";

/// The parser columns before materials could be stored without a parser.
const PREVIOUS_PARSER_COLUMNS: &str = "  parser text NOT NULL,\n  parser_version integer NOT NULL,";

/// The table constraint pairing the two columns.
pub(crate) const PARSER_WITH_VERSION: &str =
    "CONSTRAINT parser_with_version CHECK ((parser IS NULL) = (parser_version IS NULL))";

pub(crate) fn statements() -> Vec<String> {
    vec![format!(
        "ALTER TABLE raw.materials ALTER COLUMN parser DROP NOT NULL, ALTER COLUMN parser_version DROP NOT NULL, ADD {PARSER_WITH_VERSION}"
    )]
}

/// The statement as it was before this upgrade.
pub(crate) fn previous(statement: &mut String) {
    if statement.starts_with("CREATE TABLE raw.materials (") {
        *statement = statement
            .replace(PARSER_COLUMNS, PREVIOUS_PARSER_COLUMNS)
            .replace(&format!(",\n  {PARSER_WITH_VERSION}"), "");
    }
}
