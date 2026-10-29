//! One grant-independent output projection, atomically activated per policy.
pub(crate) const STATE:&str="CREATE TABLE privacy.policy_state (
  id boolean PRIMARY KEY CHECK (id),
  desired_epoch bigint NOT NULL CHECK (desired_epoch >= 0),
  active_epoch bigint NOT NULL CHECK (active_epoch >= 0)
)";
pub(crate) const INIT:&str="INSERT INTO privacy.policy_state(id,desired_epoch,active_epoch) VALUES(true,0,0)";
pub(crate) const BUILDS:&str="CREATE TABLE privacy.output_builds (\n  epoch bigint PRIMARY KEY,\n  after_seq bigint NOT NULL\n)";
pub(crate) const ORIGINALS:&str="CREATE TABLE privacy.original_events (
  event_id uuid PRIMARY KEY,
  baseline_class text NOT NULL,
  basis_epoch bigint,
  equivalence_key text
)";
pub(crate) const OUTPUTS:&str="CREATE TABLE privacy.event_outputs (
  event_id uuid PRIMARY KEY,
  epoch bigint NOT NULL,
  payload jsonb NOT NULL,
  class text NOT NULL,
  equivalence_key text,
  changed boolean NOT NULL,
  state text NOT NULL CHECK (state IN ('complete','unavailable_original','unavailable_dependency','pending_dependency'))
)";
pub(crate) const GUARD:&str=r#"CREATE OR REPLACE FUNCTION privacy.require_active_epoch() RETURNS bigint LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,privacy AS $$
DECLARE desired bigint; active bigint;
BEGIN
 SELECT desired_epoch,active_epoch INTO STRICT desired,active FROM privacy.policy_state WHERE id;
 IF desired<>active THEN RAISE EXCEPTION 'privacy_projection_stale' USING ERRCODE='55000'; END IF;
 RETURN active;
END $$"#;
pub(crate) fn tables()->Vec<String>{vec![STATE.into(),INIT.into(),BUILDS.into(),ORIGINALS.into(),OUTPUTS.into()]}
pub(crate) fn hooks()->Vec<String>{vec![
 r#"CREATE OR REPLACE FUNCTION privacy.invalidate_outputs() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE privacy.policy_state SET desired_epoch=desired_epoch+1 WHERE id; RETURN NULL; END $$"#.into(),
 "CREATE TRIGGER rules_policy_epoch AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON privacy.rules FOR EACH STATEMENT EXECUTE FUNCTION privacy.invalidate_outputs()".into(),
 "CREATE TRIGGER floors_policy_epoch AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON privacy.source_classes FOR EACH STATEMENT EXECUTE FUNCTION privacy.invalidate_outputs()".into(),
 GUARD.into(),
 "REVOKE ALL ON FUNCTION privacy.require_active_epoch() FROM PUBLIC".into(),
 "REVOKE ALL ON FUNCTION privacy.invalidate_outputs() FROM PUBLIC, sinex_reader".into(),
 "GRANT EXECUTE ON FUNCTION privacy.require_active_epoch() TO sinex_reader, sinex_writer".into(),
]}
pub(crate) fn statements()->Vec<String>{let mut out=tables();out.extend(hooks());out.extend([
 "GRANT SELECT,INSERT,UPDATE,DELETE ON privacy.policy_state,privacy.output_builds,privacy.original_events,privacy.event_outputs TO sinex_writer".into(),
 "REVOKE ALL ON privacy.policy_state,privacy.output_builds,privacy.original_events,privacy.event_outputs FROM sinex_reader".into(),
]);out}
pub(crate) fn before(statement:&str)->bool{!statement.contains("privacy.output_builds")&&!statement.contains("privacy.policy_state")&&!statement.contains("privacy.original_events")&&!statement.contains("privacy.event_outputs")&&!statement.contains("privacy.invalidate_outputs")&&!statement.contains("privacy.require_active_epoch")}

/// The boundary owner composes these views with its privilege declarations.
pub(crate) const VISIBLE_EVENTS:&str=r#"CREATE VIEW core.visible_events AS SELECT
 e.id,e.event_type,e.source,e.run_id,e.material_id,e.anchor,e.output_index,e.parents,
 e.ts_orig,e.ts_quality,e.ts_coided,e.ts_persisted,e.commit_seq,
 CASE WHEN o.event_id IS NULL THEN jsonb_build_object('unavailable','privacy_projection_pending')
      WHEN o.state<>'complete' THEN jsonb_build_object('unavailable',o.state)
      WHEN core.class_visible(o.class) THEN o.payload ELSE jsonb_build_object('withheld',o.class) END AS payload,
 CASE WHEN o.state='complete' AND core.class_visible(o.class) THEN o.equivalence_key END AS equivalence_key,
 e.reading_order,e.record_hash,e.record_state,coalesce(o.class,e.class) AS class,e.kind_version,e.validation
 FROM core.events e LEFT JOIN privacy.event_outputs o ON o.event_id=e.id AND o.epoch=privacy.require_active_epoch()
 WHERE privacy.require_active_epoch()>=0"#;
pub(crate) const VISIBLE_SEARCH:&str=r#"CREATE VIEW core.visible_search_text AS SELECT s.event_id,s.text,s.tsv,s.trigram_indexed
 FROM core.search_text s JOIN privacy.event_outputs o ON o.event_id=s.event_id
 WHERE o.epoch=privacy.require_active_epoch() AND o.state='complete' AND core.class_visible(o.class)"#;
pub(crate) const VISIBLE_VECTORS:&str=r#"CREATE VIEW core.visible_embedding_vectors AS SELECT v.embedding_id,v.target_id,v.space,v.dimensions,v.projected,v.coverage,v.reason
 FROM core.embedding_vectors v JOIN privacy.event_outputs o ON o.event_id=v.embedding_id
 JOIN privacy.event_outputs target ON target.event_id=v.target_id
 WHERE o.epoch=privacy.require_active_epoch() AND target.epoch=o.epoch
 AND o.state='complete' AND target.state='complete' AND core.class_visible(o.class) AND core.class_visible(target.class)
 AND NOT o.changed AND NOT target.changed AND NOT EXISTS (
 WITH RECURSIVE ancestors(id) AS (
 SELECT parent FROM core.events e CROSS JOIN LATERAL unnest(e.parents) parent WHERE e.id IN(v.embedding_id,v.target_id)
 UNION SELECT parent FROM ancestors a JOIN core.events e ON e.id=a.id CROSS JOIN LATERAL unnest(e.parents) parent)
 SELECT 1 FROM ancestors a LEFT JOIN privacy.event_outputs p ON p.event_id=a.id
 WHERE p.event_id IS NULL OR p.epoch<>o.epoch OR p.state<>'complete' OR p.changed OR NOT core.class_visible(p.class))"#;
