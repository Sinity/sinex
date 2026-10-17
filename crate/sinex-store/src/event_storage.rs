//! SQL generated only from fixed Rust declarations, applied before sealing the catalog receipt.
pub(crate) const FULL_TEXT_UNAVAILABLE_INDEX: &str =
    "CREATE INDEX search_text_unavailable ON core.search_text(generation,id) WHERE tsv IS NULL";
pub(crate) fn declarations() -> Vec<String> {
    let mut statements = vec![r#"CREATE FUNCTION sinex_schemas.event_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE material record;
BEGIN
 IF TG_WHEN='AFTER' THEN
  IF TG_OP='INSERT' AND NEW.source_material_id IS NOT NULL THEN UPDATE raw.source_material_registry SET parsed_event_count=parsed_event_count+1 WHERE id=NEW.source_material_id; END IF;
  IF TG_OP='DELETE' AND OLD.source_material_id IS NOT NULL THEN UPDATE raw.source_material_registry SET parsed_event_count=parsed_event_count-1 WHERE id=OLD.source_material_id; END IF;
  RETURN NULL;
 END IF;
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'event rows are immutable' USING ERRCODE='23514'; END IF;
 IF TG_OP='DELETE' THEN
  IF nullif(current_setting('sinex.purge_operation',true),'') IS NULL THEN
   RAISE EXCEPTION 'event deletion requires a purge operation' USING ERRCODE='23514';
  END IF;
  RETURN OLD;
 END IF;
 IF NEW.source_material_id IS NOT NULL THEN
  SELECT m.total_bytes,m.durable_bytes,m.status,b.status AS blob_status INTO material
   FROM raw.source_material_registry m LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE m.id=NEW.source_material_id;
  IF NOT FOUND OR NEW.anchor_byte IS NULL OR NEW.anchor_byte<0 OR NEW.anchor_byte>=material.durable_bytes
   OR (material.status<>'capturing' AND material.blob_status IS DISTINCT FROM 'present')
   OR ((NEW.offset_start IS NOT NULL OR NEW.offset_end IS NOT NULL) AND
      (NEW.offset_start IS NULL OR NEW.offset_end IS NULL OR NEW.offset_start<0 OR NEW.offset_end<NEW.offset_start OR NEW.offset_end>material.durable_bytes)) THEN
   RAISE EXCEPTION 'event range is outside durable material' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$"#.to_owned()];
    statements.extend([
        "CREATE FUNCTION sinex_schemas.timescale_preloaded() RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$ SELECT EXISTS (SELECT 1 FROM pg_catalog.unnest(pg_catalog.string_to_array(pg_catalog.current_setting('shared_preload_libraries'),',')) AS library WHERE pg_catalog.btrim(library)='timescaledb') $$".to_owned(),
        "REVOKE ALL ON FUNCTION sinex_schemas.timescale_preloaded() FROM PUBLIC".to_owned(),
        "GRANT EXECUTE ON FUNCTION sinex_schemas.timescale_preloaded() TO sinex_writer,sinex_reader".to_owned(),
    ]);
    for schema in ["core", "reflection"] {
        let table = [schema, ".events"].concat();
        for template in [
            "SELECT create_hypertable('$TABLE',by_range('id',INTERVAL '1 day'),create_default_indexes=>false)",
            "CREATE INDEX events_orig ON $TABLE (ts_orig)",
            "CREATE INDEX events_source_orig ON $TABLE (source,ts_orig)",
            "CREATE INDEX events_type_orig ON $TABLE (event_type,ts_orig)",
            "CREATE INDEX events_material_anchor ON $TABLE (source_material_id,anchor_byte)",
            "CREATE INDEX events_producer_orig ON $TABLE (producer_version_id,ts_orig) WHERE producer_version_id IS NOT NULL",
            "CREATE INDEX events_parents ON $TABLE USING gin (source_event_ids)",
            "CREATE TRIGGER event_guard BEFORE INSERT OR UPDATE OR DELETE ON $TABLE FOR EACH ROW EXECUTE FUNCTION sinex_schemas.event_guard()",
            "CREATE TRIGGER event_count AFTER INSERT OR DELETE ON $TABLE FOR EACH ROW EXECUTE FUNCTION sinex_schemas.event_guard()",
            "ALTER TABLE $TABLE SET (timescaledb.compress,timescaledb.compress_segmentby='source,event_type',timescaledb.compress_orderby='ts_orig,id',timescaledb.compress_index='minmax(ts_orig),bloom(source_material_id,anchor_byte),bloom(equivalence_key)')",
            "SELECT enable_chunk_skipping('$TABLE','ts_orig')",
            "SELECT add_compression_policy('$TABLE',INTERVAL '7 days')",
        ] {
            statements.push(template.replace("$TABLE", &table));
        }
    }
    statements.push(r#"CREATE FUNCTION sinex_schemas.operation_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF ROW(NEW.id,NEW.operation_type,NEW.actor,NEW.action_principal,NEW.action_id,NEW.request_format,NEW.request_ref,NEW.scope) IS DISTINCT FROM ROW(OLD.id,OLD.operation_type,OLD.actor,OLD.action_principal,OLD.action_id,OLD.request_format,OLD.request_ref,OLD.scope) THEN
  RAISE EXCEPTION 'operation identity and admitted request are immutable' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$"#.into());
    statements.push("CREATE TRIGGER operation_guard BEFORE UPDATE ON core.operations_log FOR EACH ROW EXECUTE FUNCTION sinex_schemas.operation_guard()".into());
    statements.push("CREATE INDEX search_text_command ON core.search_text USING gin(command_normalized gin_trgm_ops)".into());
    statements.push(
        "CREATE INDEX search_text_cwd ON core.search_text USING gin(cwd_normalized gin_trgm_ops)"
            .into(),
    );
    statements.push("CREATE INDEX search_text_tsv ON core.search_text USING gin(tsv)".into());
    statements.push(FULL_TEXT_UNAVAILABLE_INDEX.into());
    statements.push(
        "CREATE INDEX search_text_time ON core.search_text(generation,ts_orig,ts_orig_tiebreak,id)"
            .into(),
    );
    statements.push("CREATE INDEX event_project_scope ON core.event_project(generation,project,ts_orig,event_id)".into());
    statements.push("CREATE VIEW core.live_events AS SELECT e.* FROM core.events e WHERE e.record_state='present' AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.id=e.id)".into());
    statements
}
