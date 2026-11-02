//! Immutable owner-installed native source versions.
use crate::{Error, Result};
use futures::TryStreamExt;
use sinex_model::{KindDeclaration, KindRef, SourceDeclaration, SourceRef};
use sqlx::{Connection, PgConnection};

pub async fn register(conn: &mut PgConnection, declarations: &[SourceDeclaration]) -> Result<()> {
    let mut tx = conn.begin().await?;
    sqlx::query("LOCK TABLE registry.source_declarations IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    // Declarations usually repeat the same bundled kind definitions, so each
    // distinct definition is hashed once; an equal definition has an equal hash.
    let mut expected: std::collections::BTreeMap<KindRef, (&KindDeclaration, String)> =
        std::collections::BTreeMap::new();
    for declaration in declarations {
        for kind in &declaration.kinds {
            match expected.get(&kind.kind) {
                Some((known, _)) if *known == kind => {}
                Some((_, hash)) => {
                    if *hash != kind.definition_hash() {
                        return Err(Error::Invalid(
                            "owner source artifacts disagree on one kind version".into(),
                        ));
                    }
                }
                None => {
                    expected.insert(kind.kind.clone(), (kind, kind.definition_hash()));
                }
            }
        }
    }
    let refs = expected.keys().cloned().collect::<Vec<_>>();
    let registered = sinex_schema::kinds::load(&mut tx, &refs).await?;
    if registered.iter().any(|(kind, definition)| {
        expected.get(kind).is_none_or(|(known, hash)| {
            **known != *definition && *hash != definition.definition_hash()
        })
    }) {
        return Err(Error::Invalid(
            "owner source artifact conflicts with a registered kind version".into(),
        ));
    }
    let names = refs
        .iter()
        .map(|kind| kind.name.clone())
        .collect::<Vec<_>>();
    {
        // Saved declarations share kind definitions too; PostgreSQL returns
        // each distinct saved definition once.
        let mut rows=sqlx::query_scalar::<_,serde_json::Value>("SELECT DISTINCT k FROM registry.source_declarations d CROSS JOIN LATERAL jsonb_array_elements(d.definition->'kinds') k WHERE k->'kind'->>'name'=ANY($1)")
            .bind(names).fetch(&mut *tx);
        while let Some(value) = rows.try_next().await? {
            let saved: KindDeclaration = serde_json::from_value(value).map_err(|error| {
                Error::Invalid(format!("invalid owner source declaration: {error}"))
            })?;
            if expected
                .get(&saved.kind)
                .is_some_and(|(known, hash)| **known != saved && *hash != saved.definition_hash())
            {
                return Err(Error::Invalid(
                    "immutable owner artifacts disagree on one kind version".into(),
                ));
            }
        }
    }
    for declaration in declarations {
        let fingerprint = declaration.fingerprint();
        let definition = serde_json::to_value(declaration).expect("source declarations serialize");
        sqlx::query("INSERT INTO registry.source_declarations(name,version,family,fingerprint,definition) VALUES($1,$2,$3,$4,$5) ON CONFLICT(name,version) DO NOTHING")
            .bind(&declaration.source.name).bind(i64::from(declaration.source.version))
            .bind(&declaration.family).bind(&fingerprint).bind(&definition).execute(&mut *tx).await?;
        // The stored row must be exactly this declaration: the same
        // fingerprint and the same definition.
        let same: Option<bool> = sqlx::query_scalar("SELECT fingerprint=$3 AND definition=$4 FROM registry.source_declarations WHERE name=$1 AND version=$2")
            .bind(&declaration.source.name).bind(i64::from(declaration.source.version))
            .bind(&fingerprint).bind(&definition).fetch_optional(&mut *tx).await?;
        match same {
            None => return Err(Error::Invalid("registered source disappeared".into())),
            Some(false) => {
                return Err(Error::Invalid(format!(
                    "source {} version {} has a conflicting immutable declaration",
                    declaration.source.name, declaration.source.version
                )));
            }
            Some(true) => {}
        }
    }
    tx.commit().await?;
    Ok(())
}

pub async fn load(
    conn: &mut PgConnection,
    source: &SourceRef,
) -> Result<Option<SourceDeclaration>> {
    let row:Option<(String,serde_json::Value)> = sqlx::query_as("SELECT fingerprint,definition FROM registry.source_declarations WHERE name=$1 AND version=$2")
        .bind(&source.name).bind(i64::from(source.version)).fetch_optional(conn).await?;
    row.map(|(fingerprint, value)| {
        let declaration: SourceDeclaration = serde_json::from_value(value).map_err(|error| {
            Error::Invalid(format!("invalid owner source declaration: {error}"))
        })?;
        if &declaration.source != source || declaration.fingerprint() != fingerprint {
            return Err(Error::Invalid(
                "owner source declaration fingerprint differs".into(),
            ));
        }
        Ok(declaration)
    })
    .transpose()
}
