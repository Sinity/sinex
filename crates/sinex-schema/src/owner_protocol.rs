//! Private signing material for owner-issued producer capabilities.
pub(crate) const TABLE: &str = "CREATE TABLE core.owner_protocol_key (
  id boolean PRIMARY KEY CHECK (id),
  secret bytea NOT NULL CHECK (length(secret)=32)
)";
pub(crate) const INIT: &str = "INSERT INTO core.owner_protocol_key(id,secret) VALUES(true,decode(replace(gen_random_uuid()::text || gen_random_uuid()::text,'-',''),'hex'))";
pub(crate) fn statements() -> Vec<String> {
    vec![TABLE.into(), INIT.into()]
}
pub(crate) fn before(statement: &str) -> bool {
    !statement.contains("core.owner_protocol_key")
}
