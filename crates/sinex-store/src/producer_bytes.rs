//! Producer byte custody. Reservation and final acknowledgment share the
//! existing blob-row deletion lock; an acknowledgment never borrows another
//! producer's pending owner.
use crate::{Error, Result};
use sinex_model::producer_bytes::{Material, MaterialFrame, Publication};
use sqlx::PgConnection;

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

pub async fn load(
    conn: &mut PgConnection,
    producer: &str,
    frame: &str,
) -> Result<Option<(Publication, bool)>> {
    let row: Option<(serde_json::Value,bool)> = sqlx::query_as("SELECT declaration,installed FROM raw.pending_producer_bytes WHERE producer=$1 AND frame=$2")
        .bind(producer).bind(frame).fetch_optional(&mut *conn).await?;
    let Some((value, installed)) = row else {
        return Ok(None);
    };
    let declaration: Publication = serde_json::from_value(value)
        .map_err(|_| invalid("invalid pending producer declaration"))?;
    declaration.validate().map_err(Error::Invalid)?;
    if declaration.frame != frame {
        return Err(invalid("pending producer frame differs"));
    }
    let rows: Vec<(String,String,i64)> = sqlx::query_as("SELECT p.role,p.blob,b.size FROM raw.pending_producer_blobs p JOIN core.blobs b ON b.hash=p.blob WHERE p.producer=$1 AND p.frame=$2 ORDER BY p.role")
        .bind(producer).bind(frame).fetch_all(conn).await?;
    let mut expected: Vec<_> = declaration
        .roles()
        .map(|(role, b)| (role.name().to_owned(), b.blob.clone(), b.length as i64))
        .collect();
    expected.sort();
    if rows != expected {
        return Err(invalid(
            "pending producer references differ from its declaration",
        ));
    }
    Ok(Some((declaration, installed)))
}

/// Caller owns an explicit transaction and source guard. Commit before install.
pub async fn reserve(
    conn: &mut PgConnection,
    producer: &str,
    declaration: &Publication,
) -> Result<()> {
    sinex_schema::require_current(conn).await?;
    declaration.validate().map_err(Error::Invalid)?;
    let value = serde_json::to_value(declaration)
        .map_err(|_| invalid("producer declaration cannot encode"))?;
    sqlx::query("INSERT INTO raw.pending_producer_bytes(producer,frame,declaration) VALUES($1,$2,$3) ON CONFLICT(producer,frame) DO NOTHING")
        .bind(producer).bind(&declaration.frame).bind(&value).execute(&mut *conn).await?;
    let actual: serde_json::Value = sqlx::query_scalar("SELECT declaration FROM raw.pending_producer_bytes WHERE producer=$1 AND frame=$2 FOR UPDATE")
        .bind(producer).bind(&declaration.frame).fetch_one(&mut *conn).await?;
    if actual != value {
        return Err(invalid(
            "producer publication already owns a different exact version",
        ));
    }
    let mut roles: Vec<_> = declaration.roles().collect();
    roles.sort_by(|a, b| a.1.blob.cmp(&b.1.blob));
    for (role, bytes) in roles {
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'reserved') ON CONFLICT(hash) DO NOTHING")
            .bind(&bytes.blob).bind(bytes.length as i64).execute(&mut *conn).await?;
        let size: i64 = sqlx::query_scalar("SELECT size FROM core.blobs WHERE hash=$1 FOR UPDATE")
            .bind(&bytes.blob)
            .fetch_one(&mut *conn)
            .await?;
        if size != bytes.length as i64 {
            return Err(invalid("producer blob reservation length conflicts"));
        }
        sqlx::query("INSERT INTO raw.pending_producer_blobs(producer,frame,role,blob) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
            .bind(producer).bind(&declaration.frame).bind(role.name()).bind(&bytes.blob).execute(&mut *conn).await?;
    }
    load(conn, producer, &declaration.frame).await?;
    Ok(())
}

/// Verification and installation happened outside Writer on receiver-owned bytes.
pub async fn installed(
    conn: &mut PgConnection,
    producer: &str,
    declaration: &Publication,
) -> Result<()> {
    let Some((actual, _)) = load(conn, producer, &declaration.frame).await? else {
        return Err(invalid("producer reservation is missing"));
    };
    if actual != *declaration {
        return Err(invalid("producer installation declaration differs"));
    }
    if let Some(material) = &declaration.material {
        sqlx::query("UPDATE core.blobs SET status='present' WHERE hash=$1 AND status IN ('reserved','deleting')")
            .bind(&material.bytes.blob).execute(&mut *conn).await?;
    }
    sqlx::query(
        "UPDATE raw.pending_producer_bytes SET installed=true WHERE producer=$1 AND frame=$2",
    )
    .bind(producer)
    .bind(&declaration.frame)
    .execute(conn)
    .await?;
    Ok(())
}

pub(crate) async fn validate(
    conn: &mut PgConnection,
    frame: &sinex_model::IngressFrame,
) -> Result<()> {
    if frame.refusal.is_some() {
        return Ok(());
    }
    let pending = load(conn, &frame.producer, &frame.frame).await?;
    if pending.as_ref().map(|(p, _)| p) != frame.publication.as_ref() {
        return Err(invalid(
            "producer admission lacks its archive-owned byte verification",
        ));
    }
    if let Some(progress) = &frame.material {
        let Some(material) = &progress.material else {
            return Err(invalid("producer material progress lacks material"));
        };
        let descriptor = MaterialFrame {
            id: material.id,
            input: progress.input.clone(),
            blob: material.blob.clone(),
            length: material.length,
            parser: material.parser.clone(),
            parser_version: material.parser_version,
            part_index: material.part_index,
            previous_part: material.previous_part,
            manifest: material.manifest.clone(),
        };
        if pending.as_ref().is_none_or(|(p, ready)| {
            !ready || p.material.as_ref() != Some(&Material::from_frame(&descriptor))
        }) {
            return Err(invalid(
                "producer material lacks its exact installed byte reservation",
            ));
        }
    } else if pending
        .as_ref()
        .is_some_and(|(p, ready)| !ready || p.material.is_some())
    {
        return Err(invalid(
            "producer publication does not match its final frame",
        ));
    }
    Ok(())
}

pub(crate) async fn complete(
    conn: &mut PgConnection,
    frame: &sinex_model::IngressFrame,
) -> Result<()> {
    if frame.refusal.is_some() {
        return Ok(());
    }
    validate(conn, frame).await?;
    if let Some(progress) = &frame.material {
        let material = progress
            .material
            .as_ref()
            .ok_or_else(|| invalid("producer final material is missing"))?;
        if crate::admission::material_matches(conn, &progress.binding, &progress.input, material)
            .await?
            != Some(true)
        {
            return Err(invalid(
                "producer publication has no exact ready material owner",
            ));
        }
    }
    sqlx::query(
        "UPDATE raw.pending_producer_bytes SET acknowledged=true WHERE producer=$1 AND frame=$2 AND installed",
    )
    .bind(&frame.producer)
    .bind(&frame.frame)
    .execute(conn)
    .await?;
    Ok(())
}

/// Serialize one publication's receiving/queued acknowledgment and private-spool
/// retirement outside Writer. The owned acquisition connection holds the session
/// form; cleanup takes the transaction form before unlinking its scoped files.
pub async fn lock(
    conn: &mut PgConnection,
    producer: &str,
    frame: &str,
    session: bool,
) -> Result<()> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinex producer byte custody 1\0");
    hash.update(producer.as_bytes());
    hash.update(&[0]);
    hash.update(frame.as_bytes());
    let key = i64::from_le_bytes(
        hash.finalize().as_bytes()[..8]
            .try_into()
            .expect("eight bytes"),
    );
    sqlx::query(if session {
        "SELECT pg_advisory_lock($1)"
    } else {
        "SELECT pg_advisory_xact_lock($1)"
    })
    .bind(key)
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn acknowledged(conn: &mut PgConnection, limit: i64) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as("SELECT producer,frame FROM raw.pending_producer_bytes WHERE acknowledged ORDER BY producer,frame LIMIT $1")
        .bind(limit).fetch_all(conn).await?)
}

/// Caller holds the publication lock and has removed its private spool. Blob
/// ownership remains locked until the projection and completed owner retire.
pub async fn retire(conn: &mut PgConnection, producer: &str, frame: &str) -> Result<()> {
    let terminal:bool=sqlx::query_scalar("SELECT coalesce((SELECT acknowledged FROM raw.pending_producer_bytes WHERE producer=$1 AND frame=$2 FOR UPDATE),false)")
        .bind(producer).bind(frame).fetch_one(&mut *conn).await?;
    if !terminal {
        return Err(invalid("producer private spool is not acknowledged"));
    }
    let references:Vec<String>=sqlx::query_scalar("SELECT DISTINCT blob FROM raw.pending_producer_blobs WHERE producer=$1 AND frame=$2 ORDER BY blob")
        .bind(producer).bind(frame).fetch_all(&mut *conn).await?;
    for blob in &references {
        sqlx::query("SELECT hash FROM core.blobs WHERE hash=$1 FOR UPDATE")
            .bind(blob)
            .execute(&mut *conn)
            .await?;
    }
    sqlx::query(
        "DELETE FROM raw.pending_producer_bytes WHERE producer=$1 AND frame=$2 AND acknowledged",
    )
    .bind(producer)
    .bind(frame)
    .execute(&mut *conn)
    .await?;
    for blob in references {
        if crate::excision::unreferenced(conn, &blob).await? {
            sqlx::query("UPDATE core.blobs SET status='deleting' WHERE hash=$1")
                .bind(blob)
                .execute(&mut *conn)
                .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
