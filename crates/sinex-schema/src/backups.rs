//! Durable extent-prefix pins belong to the existing operation owner.
pub(crate) const HOLDS: &str = "CREATE TABLE core.backup_holds (
  operation_id uuid PRIMARY KEY REFERENCES core.operations(id),
  commit_position bigint NOT NULL CHECK (commit_position >= 0),
  blob_changes bigint NOT NULL CHECK (blob_changes >= 0),
  schema_version integer NOT NULL CHECK (schema_version > 0),
  released_operation uuid REFERENCES core.operations(id),
  released_at timestamptz,
  CHECK ((released_operation IS NULL) = (released_at IS NULL))
)";
pub(crate) const SEGMENTS: &str = "CREATE TABLE core.backup_segments (
  hold_id uuid NOT NULL REFERENCES core.backup_holds(operation_id),
  segment text NOT NULL,
  committed_end bigint NOT NULL CHECK (committed_end >= 0),
  PRIMARY KEY (hold_id,segment)
)";
pub(crate) const EXTENTS: &str = "CREATE TABLE core.backup_extents (
  hold_id uuid NOT NULL REFERENCES core.backup_holds(operation_id),
  hash text NOT NULL,
  descriptor bytea NOT NULL,
  PRIMARY KEY (hold_id,hash)
)";
pub(crate) const COVERS: &str = "CREATE FUNCTION core.backup_covers(segment_name text, byte_offset bigint)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS (SELECT 1 FROM core.backup_segments s
    JOIN core.backup_holds h ON h.operation_id=s.hold_id
    WHERE h.released_at IS NULL AND s.segment=segment_name AND byte_offset < s.committed_end)
$$";
pub(crate) fn tables() -> Vec<String> {
    [HOLDS, SEGMENTS, EXTENTS].into_iter().map(String::from).collect()
}
pub(crate) fn hooks() -> Vec<String> {
    vec![COVERS.into(), "REVOKE ALL ON FUNCTION core.backup_covers(text,bigint) FROM PUBLIC".into(),
      "GRANT EXECUTE ON FUNCTION core.backup_covers(text,bigint) TO sinex_writer".into()]
}
pub(crate) fn statements() -> Vec<String> {
    let mut out=tables(); out.extend(hooks()); out
}
pub(crate) fn before(statement: &str) -> bool {
    !statement.contains("core.backup_")
}
