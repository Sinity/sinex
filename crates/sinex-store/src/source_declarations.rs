//! Immutable owner-installed native source versions.
use crate::{Error, Result};
use sinex_model::{SourceDeclaration, SourceRef};
use sqlx::{PgConnection,Connection};
use futures::TryStreamExt;

pub async fn register(conn: &mut PgConnection, declarations: &[SourceDeclaration]) -> Result<()> {
    let mut tx=conn.begin().await?;
    sqlx::query("LOCK TABLE registry.source_declarations IN SHARE ROW EXCLUSIVE MODE").execute(&mut *tx).await?;
    let mut expected=std::collections::BTreeMap::new();
    for declaration in declarations {
        for kind in &declaration.kinds {
            if expected.insert(kind.kind.clone(),kind.definition_hash()).is_some_and(|old|old!=kind.definition_hash()) {
                return Err(Error::Invalid("owner source artifacts disagree on one kind version".into()));
            }
        }
    }
    let refs=expected.keys().cloned().collect::<Vec<_>>();
    let registered=sinex_schema::kinds::load(&mut tx,&refs).await?;
    if registered.iter().any(|(kind,definition)|expected.get(kind)!=Some(&definition.definition_hash())) {
        return Err(Error::Invalid("owner source artifact conflicts with a registered kind version".into()));
    }
    let names=refs.iter().map(|kind|kind.name.clone()).collect::<Vec<_>>();
    {
        let mut rows=sqlx::query_scalar::<_,serde_json::Value>("SELECT definition FROM registry.source_declarations d WHERE EXISTS(SELECT 1 FROM jsonb_array_elements(d.definition->'kinds') k WHERE k->'kind'->>'name'=ANY($1))")
            .bind(names).fetch(&mut *tx);
        while let Some(value)=rows.try_next().await? {
            let saved:SourceDeclaration=serde_json::from_value(value).map_err(|error|Error::Invalid(format!("invalid owner source declaration: {error}")))?;
            if saved.kinds.iter().any(|kind|expected.get(&kind.kind).is_some_and(|hash|*hash!=kind.definition_hash())) {
                return Err(Error::Invalid("immutable owner artifacts disagree on one kind version".into()));
            }
        }
    }
    for declaration in declarations {
        let fingerprint = declaration.fingerprint();
        let definition = serde_json::to_value(declaration).expect("source declarations serialize");
        sqlx::query("INSERT INTO registry.source_declarations(name,version,family,fingerprint,definition) VALUES($1,$2,$3,$4,$5) ON CONFLICT(name,version) DO NOTHING")
            .bind(&declaration.source.name).bind(i64::from(declaration.source.version))
            .bind(&declaration.family).bind(&fingerprint).bind(&definition).execute(&mut *tx).await?;
        let known = load(&mut tx, &declaration.source).await?.ok_or_else(|| Error::Invalid("registered source disappeared".into()))?;
        if known.fingerprint() != fingerprint {
            return Err(Error::Invalid(format!("source {} version {} has a conflicting immutable declaration", declaration.source.name, declaration.source.version)));
        }
    }
    tx.commit().await?;
    Ok(())
}

pub async fn load(conn: &mut PgConnection, source: &SourceRef) -> Result<Option<SourceDeclaration>> {
    let row:Option<(String,serde_json::Value)> = sqlx::query_as("SELECT fingerprint,definition FROM registry.source_declarations WHERE name=$1 AND version=$2")
        .bind(&source.name).bind(i64::from(source.version)).fetch_optional(conn).await?;
    row.map(|(fingerprint,value)| {
        let declaration:SourceDeclaration=serde_json::from_value(value).map_err(|error|Error::Invalid(format!("invalid owner source declaration: {error}")))?;
        if &declaration.source != source || declaration.fingerprint()!=fingerprint {
            return Err(Error::Invalid("owner source declaration fingerprint differs".into()));
        }
        Ok(declaration)
    }).transpose()
}
