//! The library's shown-value projection: for each target and tag or
//! property, the value the library shows, its level and state, and the
//! records supporting it. The writer keeps it equal to the precedence over
//! the live curation records in every transaction that changes them
//! (admission, replay, archive, restore); it is rebuildable from the events.
//!
//! The upgrade that adds it refuses a database holding library records: the
//! curation record replaced the per-kind curation events, and the copy,
//! item, rendition and view kinds changed version. No deployed database
//! holds library records; one that does needs a decision on carrying them
//! before upgrading, never a silent misreading.

pub(crate) const SHOWN: &str = "CREATE TABLE core.library_shown (
  item text NOT NULL,
  target text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('tag', 'property')),
  key text NOT NULL,
  state text NOT NULL,
  level text NOT NULL,
  value jsonb,
  supporters jsonb NOT NULL,
  PRIMARY KEY (target, kind, key)
)";

pub(crate) const SHOWN_ITEM: &str = "CREATE INDEX library_shown_item ON core.library_shown (item)";

pub(crate) const REFUSE_LIBRARY_RECORDS: &str = "DO $$ BEGIN IF EXISTS (SELECT 1 FROM core.events WHERE event_type LIKE 'library.%') THEN RAISE EXCEPTION 'library records predate the curation record and the changed copy, item, rendition and view kinds; decide how to carry them before upgrading'; END IF; END $$";

pub(crate) fn tables() -> Vec<String> {
    vec![SHOWN.into(), SHOWN_ITEM.into()]
}

pub(crate) fn before(statement: &str) -> bool {
    !statement.contains("core.library_shown")
}

pub(crate) fn statements() -> Vec<String> {
    let mut out = vec![REFUSE_LIBRARY_RECORDS.to_owned()];
    out.extend(tables());
    out.push("GRANT SELECT,INSERT,UPDATE,DELETE ON core.library_shown TO sinex_writer".into());
    out.push("REVOKE ALL ON core.library_shown FROM sinex_reader,PUBLIC".into());
    out
}
