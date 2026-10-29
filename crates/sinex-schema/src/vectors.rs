//! Rebuildable pgvector projection. Original dense values remain in immutable
//! text.embedding payloads; nullable projected values express backend limits.
pub(crate) const SPACES:&str="CREATE TABLE core.embedding_spaces (
  space text PRIMARY KEY,
  provider text NOT NULL,
  model text NOT NULL,
  revision text NOT NULL,
  settings_hash text NOT NULL,
  dimensions bigint NOT NULL CHECK (dimensions >= 0)
)";
pub(crate) const VECTORS:&str="CREATE TABLE core.embedding_vectors (
  embedding_id uuid PRIMARY KEY,
  target_id uuid,
  space text,
  dimensions bigint,
  projected vector,
  coverage text NOT NULL CHECK (coverage IN ('indexed', 'exact', 'fallback', 'unavailable')),
  reason text
)";
pub(crate) const LOOKUP:&str="CREATE INDEX embedding_target_space ON core.embedding_vectors (target_id,space)";
pub(crate) const VISIBLE:&str="CREATE VIEW core.visible_embedding_vectors AS SELECT v.embedding_id,v.target_id,v.space,v.dimensions,v.projected,v.coverage,v.reason
FROM core.embedding_vectors v JOIN core.events e ON e.id=v.embedding_id
WHERE core.class_visible(e.class)";
pub(crate) fn tables()->Vec<String>{vec![SPACES.into(),VECTORS.into(),LOOKUP.into()]}
pub(crate) fn statements()->Vec<String>{
    let mut out=tables();out.extend([
        "GRANT SELECT,INSERT,UPDATE,DELETE ON core.embedding_spaces,core.embedding_vectors TO sinex_writer".into(),
        "REVOKE ALL ON core.embedding_spaces,core.embedding_vectors FROM sinex_reader".into(),
    ]);out
}
