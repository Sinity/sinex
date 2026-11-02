-- The objects the two emptied upgrade steps created, with synthetic rows.
CREATE TABLE privacy.policy_state (id boolean PRIMARY KEY CHECK (id), desired_epoch bigint NOT NULL, active_epoch bigint NOT NULL);
INSERT INTO privacy.policy_state VALUES (true, 1, 1);
CREATE TABLE privacy.output_builds (epoch bigint PRIMARY KEY, after_seq bigint NOT NULL, after_id uuid);
INSERT INTO privacy.output_builds VALUES (1, 0, NULL);
CREATE TABLE privacy.derivation_epochs (derivation text PRIMARY KEY, epoch bigint NOT NULL, cursor jsonb NOT NULL);
INSERT INTO privacy.derivation_epochs VALUES ('test.derivation', 1, '{}');
CREATE TABLE privacy.derivation_contracts (derivation text NOT NULL, version bigint NOT NULL, build text NOT NULL, requirements jsonb NOT NULL, PRIMARY KEY (derivation, version, build));
INSERT INTO privacy.derivation_contracts VALUES ('test.derivation', 1, '', '[]');
CREATE TABLE privacy.original_events (event_id uuid PRIMARY KEY, baseline_class text NOT NULL, recoverable boolean NOT NULL, basis_epoch bigint, equivalence_key text);
CREATE TABLE privacy.event_outputs (event_id uuid PRIMARY KEY, epoch bigint NOT NULL, payload jsonb NOT NULL, class text NOT NULL, equivalence_key text, changed boolean NOT NULL, trace jsonb NOT NULL, state text NOT NULL);
INSERT INTO privacy.original_events SELECT id, class, true, 1, NULL FROM core.events;
INSERT INTO privacy.event_outputs SELECT id, 1, payload, class, NULL, false, '[]', 'complete' FROM core.events;
INSERT INTO core.counters (name, value) VALUES ('privacy_outputs', 1);
CREATE FUNCTION privacy.invalidate_outputs() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE privacy.policy_state SET desired_epoch = desired_epoch + 1 WHERE id; RETURN NULL; END $$;
CREATE TRIGGER rules_policy_epoch AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON privacy.rules FOR EACH STATEMENT EXECUTE FUNCTION privacy.invalidate_outputs();
CREATE TRIGGER floors_policy_epoch AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON privacy.source_classes FOR EACH STATEMENT EXECUTE FUNCTION privacy.invalidate_outputs();
CREATE FUNCTION privacy.require_active_epoch() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT active_epoch FROM privacy.policy_state WHERE id $$;
CREATE OR REPLACE VIEW core.visible_events AS SELECT
 e.id,e.event_type,e.source,e.run_id,e.material_id,e.anchor,e.output_index,e.parents,
 e.ts_orig,e.ts_quality,e.ts_coided,e.ts_persisted,e.commit_seq,
 CASE WHEN o.event_id IS NULL THEN jsonb_build_object('unavailable','privacy_projection_pending')
      WHEN core.class_visible(o.class) THEN o.payload ELSE jsonb_build_object('withheld',o.class) END AS payload,
 CASE WHEN core.class_visible(o.class) THEN o.equivalence_key END AS equivalence_key,
 e.reading_order,e.record_hash,e.record_state,coalesce(o.class,e.class) AS class,e.kind_version,e.validation,e.producer_family,e.producer_identity,e.source_version,e.source_fingerprint
 FROM core.events e LEFT JOIN privacy.event_outputs o ON o.event_id=e.id AND o.epoch=privacy.require_active_epoch()
 WHERE privacy.require_active_epoch()>=0;
CREATE OR REPLACE VIEW core.visible_search_text AS SELECT s.event_id,s.text,s.tsv,s.trigram_indexed
 FROM core.search_text s JOIN privacy.event_outputs o ON o.event_id=s.event_id
 WHERE o.epoch=privacy.require_active_epoch() AND core.class_visible(o.class);
CREATE VIEW core.derivation_events AS SELECT e.id, o.payload, o.class
 FROM core.events e JOIN privacy.event_outputs o ON o.event_id=e.id
 WHERE o.epoch=privacy.require_active_epoch();
CREATE TABLE derivation.model_policies (policy_event uuid PRIMARY KEY, policy_hash bytea NOT NULL, authorized_at timestamptz NOT NULL);
INSERT INTO derivation.model_policies VALUES (uuidv7(), decode(repeat('02',32),'hex'), now());
CREATE TABLE derivation.model_calls (entry_id uuid PRIMARY KEY, policy_event uuid NOT NULL, state text NOT NULL);
INSERT INTO derivation.model_calls SELECT uuidv7(), policy_event, 'reserved' FROM derivation.model_policies;
