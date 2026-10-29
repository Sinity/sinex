//! Read-only SQL may inspect data, but cannot change its role or grant context.
use crate::Error;
use sqlx::PgConnection;

pub(crate) const HARDEN: &str = r#"DO $sql_reader$
BEGIN
  IF (SELECT rolsuper FROM pg_roles WHERE rolname=current_user) THEN
    REVOKE EXECUTE ON FUNCTION pg_catalog.set_config(text,text,boolean) FROM PUBLIC, sinex_reader;
    GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text,text,boolean) TO sinex_writer;
  ELSIF has_function_privilege('sinex_reader','pg_catalog.set_config(text,text,boolean)','EXECUTE') THEN
    RAISE EXCEPTION 'unsafe Sinex SQL reader: run prepare-database as the database superuser';
  END IF;
END $sql_reader$"#;

/// Check role-context immutability before schema preparation or upgrade.
pub(crate) async fn require_context(conn: &mut PgConnection) -> Result<(), Error> {
    let safe: bool = sqlx::query_scalar(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sinex_reader')
         THEN NOT has_function_privilege('sinex_reader','pg_catalog.set_config(text,text,boolean)','EXECUTE')
         ELSE false END",
    ).fetch_one(conn).await?;
    if safe { Ok(()) } else { Err(Error::UnsafeReadBoundary) }
}

/// Effective grants include inherited roles and column-level privileges.
pub async fn require(conn: &mut PgConnection) -> Result<(), Error> {
    require_context(conn).await?;
    let private_readable:bool=sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname IN ('sinex_schema','core','audit','raw','registry','derivation','privacy')
          AND c.relkind IN ('r','p','v','m','f')
          AND NOT (n.nspname='core' AND c.relname IN ('visible_events','visible_search_text','visible_embedding_vectors','counters','source_counts'))
          AND NOT (n.nspname='registry' AND c.relname IN ('sources','derivations','event_types','kind_prefixes','source_declarations'))
          AND has_any_column_privilege('sinex_reader',c.oid,'SELECT'))"
    ).fetch_one(conn).await?;
    if private_readable {Err(Error::UnsafeReadBoundary)} else {Ok(())}
}

#[cfg(test)]
mod tests;
