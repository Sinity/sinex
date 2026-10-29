//! Availability of mutable query relations, changed atomically with their rows.
pub(crate) const INIT:&str="INSERT INTO core.counters (name,value) VALUES ('query_metadata',0)";
pub(crate) const RELATIONS:&[&str]=&["core.runs","core.manifests","registry.sources","raw.materials","raw.inputs","raw.revisions","raw.failures","core.operations","core.source_counts"];
pub(crate) fn triggers()->Vec<String> {
    let mut statements=vec!["CREATE FUNCTION core.advance_query_metadata() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      PERFORM value FROM core.counters WHERE name='commit_seq' FOR UPDATE;
      UPDATE core.counters SET value=value+1 WHERE name='query_metadata';
      RETURN NULL;
    END $$".into()];
    statements.extend(RELATIONS.iter().map(|table|format!("CREATE TRIGGER query_metadata BEFORE INSERT OR UPDATE OR DELETE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION core.advance_query_metadata()")));
    statements
}
pub(crate) fn before(statement:&str)->bool {
    !statement.starts_with("CREATE FUNCTION core.advance_query_metadata(") && !statement.starts_with("CREATE TRIGGER query_metadata ")
}
