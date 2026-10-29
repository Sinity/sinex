//! Registered native source versions and owner-assigned transport provenance.
pub(crate) const TABLE:&str="CREATE TABLE registry.source_declarations (
  name text NOT NULL,
  version bigint NOT NULL CHECK (version >= 0),
  family text NOT NULL,
  fingerprint text NOT NULL CHECK (fingerprint <> ''),
  definition jsonb NOT NULL CHECK (jsonb_typeof(definition)='object'),
  PRIMARY KEY (name,version)
)";
pub(crate) const EVENT_COLUMNS:&[(&str,&str)]=&[
 ("producer_family","text"),("producer_identity","text"),("producer_host","text"),
 ("producer_frame","bigint"),("source_version","bigint CHECK (source_version >= 0)"),("source_fingerprint","text"),
];
pub(crate) const FRAME_COLUMNS:&[(&str,&str)]=&[
 ("source_name","text"),("source_version","bigint CHECK (source_version >= 0)"),("source_fingerprint","text"),("producer_host","text"),
];
pub(crate) const FRAME_FK:&str="ALTER TABLE core.events ADD CONSTRAINT events_producer_frame FOREIGN KEY(producer_frame) REFERENCES core.ingress_frames(number) DEFERRABLE INITIALLY DEFERRED";
pub(crate) fn hooks()->Vec<String>{vec![
 "CREATE FUNCTION registry.keep_source_declaration() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'source declarations are immutable; register a new version' USING ERRCODE='23514'; END IF; RETURN NEW; END $$".into(),
 "CREATE TRIGGER source_declarations_immutable BEFORE UPDATE OR DELETE ON registry.source_declarations FOR EACH ROW EXECUTE FUNCTION registry.keep_source_declaration()".into(),
 FRAME_FK.into(),
]}
pub(crate) fn statements()->Vec<String>{
 let mut out=vec![TABLE.into()];
 for (name,ty) in EVENT_COLUMNS{out.push(format!("ALTER TABLE core.events ADD COLUMN {name} {ty}"));}
 for (name,ty) in FRAME_COLUMNS{out.push(format!("ALTER TABLE core.ingress_frames ADD COLUMN {name} {ty}"));}
 out.extend(hooks());out
}
pub(crate) fn fresh(statement:&mut String){
 let columns=if statement.starts_with("CREATE TABLE core.events ("){EVENT_COLUMNS}else if statement.starts_with("CREATE TABLE core.ingress_frames ("){FRAME_COLUMNS}else{return};
 let end=statement.rfind("\n)").expect("declared table closes on its own line");
 let extra=columns.iter().map(|(name,ty)|format!(",\n  {name} {ty}")).collect::<String>();
 statement.insert_str(end,&extra);
}
pub(crate) fn before(statement:&str)->bool{!statement.contains("registry.source_declarations")&&!statement.contains("registry.keep_source_declaration")&&!statement.contains("events_producer_frame")}
pub(crate) fn previous(statement:&mut String){
 if statement.starts_with("CREATE TABLE core.events (")||statement.starts_with("CREATE TABLE core.ingress_frames ("){
  for (name,ty) in EVENT_COLUMNS.iter().chain(FRAME_COLUMNS){*statement=statement.replace(&format!(",\n  {name} {ty}"),"");}
 }
}
