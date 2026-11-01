//! Each source's declared expectation as rows: when it should have something,
//! so status can tell a quiet source from a dead one. The daemon rewrites
//! `registry.sources` from the build's declarations at every start.

/// One row per source name, written from the source declarations.
pub(crate) const SOURCES: &str = "CREATE TABLE registry.sources (
  name text PRIMARY KEY,
  -- Input shapes of the declarations that read it; empty when its records
  -- are written only on request.
  shapes text[] NOT NULL,
  version integer NOT NULL,
  event_types text[] NOT NULL,
  purpose text NOT NULL,
  expectation text NOT NULL CHECK (expectation IN ('continuous', 'cadence', 'export', 'on_demand')),
  -- Continuous sources: no new record for longer than this is suspicious.
  silent_after interval CHECK (silent_after > interval '0'),
  -- Sources read on a schedule: no read for longer than this is suspicious.
  every interval CHECK (every > interval '0'),
  -- How long the provider keeps its own records, where it expires them.
  expires_after interval CHECK (expires_after > interval '0'),
  CONSTRAINT continuous_declares_silence CHECK ((expectation = 'continuous') = (silent_after IS NOT NULL)),
  CONSTRAINT cadence_declares_period CHECK ((expectation = 'cadence') = (every IS NOT NULL))
)";

/// The table before sources declared expectations.
const PREVIOUS_SOURCES: &str = "CREATE TABLE registry.sources (
  name text PRIMARY KEY,
  driver text NOT NULL,
  version integer NOT NULL,
  event_types text[] NOT NULL,
  purpose text NOT NULL
)";

/// The table's rows are rewritten from the declarations when the daemon
/// starts, so the upgrade replaces it empty.
pub(crate) fn statements() -> Vec<String> {
    vec![
        "DROP TABLE registry.sources".into(),
        SOURCES.into(),
        crate::query_cursors::trigger("registry.sources"),
    ]
}

/// The statement as it was before this upgrade.
pub(crate) fn previous(statement: &mut String) {
    if statement == SOURCES {
        *statement = PREVIOUS_SOURCES.into();
    }
}
