//! The library's shown-value projection: for each target and tag or
//! property, the value the library shows, its level and state, and the
//! records supporting it. The writer keeps it equal to the precedence over
//! the live curation records in every transaction that changes them
//! (admission, replay, archive, restore); it is rebuildable from the events.

pub(crate) const SHOWN: &str = "CREATE TABLE core.library_shown (
  item text NOT NULL,
  target text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('tag', 'property')),
  key text NOT NULL,
  state text NOT NULL,
  level text NOT NULL,
  value jsonb,
  supporting_records jsonb NOT NULL,
  PRIMARY KEY (target, kind, key)
)";

pub(crate) const SHOWN_ITEM: &str = "CREATE INDEX library_shown_item ON core.library_shown (item)";

pub(crate) fn tables() -> Vec<String> {
    vec![SHOWN.into(), SHOWN_ITEM.into()]
}
