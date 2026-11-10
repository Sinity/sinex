//! Bytes stored as content-defined chunks: the chunk names of each such blob,
//! in order. Each chunk's extent is an ordinary `core.blob_extents` row, and a
//! chunk several blobs share is stored once.
pub(crate) const TABLE: &str = "CREATE TABLE core.blob_chunks (
  blob text NOT NULL CHECK (blob ~ '^blake3:[0-9a-f]{64}$'),
  ordinal integer NOT NULL CHECK (ordinal >= 0),
  chunk text NOT NULL CHECK (chunk ~ '^blake3:[0-9a-f]{64}$'),
  PRIMARY KEY (blob, ordinal)
)";
pub(crate) const BY_CHUNK: &str = "CREATE INDEX blob_chunks_chunk ON core.blob_chunks (chunk)";
/// The read authority of a blob: its one extent, or its chunks' extents in
/// order. NULL while any of them is not held.
pub(crate) const DESCRIPTORS: &str = "CREATE FUNCTION core.blob_descriptors(name text)
RETURNS bytea[] LANGUAGE sql STABLE AS $$
  SELECT COALESCE(
    (SELECT ARRAY[x.descriptor] FROM core.blob_extents x WHERE x.hash = name AND NOT x.deleting),
    (SELECT array_agg(x.descriptor ORDER BY c.ordinal) FROM core.blob_chunks c
       JOIN core.blob_extents x ON x.hash = c.chunk AND NOT x.deleting
     WHERE c.blob = name
     HAVING count(*) > 0 AND count(*) = (SELECT count(*) FROM core.blob_chunks a WHERE a.blob = name)))
$$";
pub(crate) fn tables() -> Vec<String> {
    [TABLE, BY_CHUNK].into_iter().map(String::from).collect()
}
pub(crate) fn hooks() -> Vec<String> {
    vec![
        DESCRIPTORS.into(),
        "REVOKE ALL ON FUNCTION core.blob_descriptors(text) FROM PUBLIC".into(),
        "GRANT EXECUTE ON FUNCTION core.blob_descriptors(text) TO sinex_writer".into(),
    ]
}
pub(crate) fn statements() -> Vec<String> {
    let mut out = tables();
    out.extend(hooks());
    out
}
pub(crate) fn before(statement: &str) -> bool {
    !statement.contains("core.blob_chunks") && !statement.contains("core.blob_descriptors")
}
