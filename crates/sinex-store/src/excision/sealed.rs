//! Prepared sealed replacements contain ciphertext and wrapped capabilities only.
use super::{KeyEnvelope, Plan};
use crate::{Error, Result, sealed};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::MaterialId;
use sqlx::PgConnection;
use std::io::{BufRead, BufReader, Seek};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct KeyRotation {
    pub material: MaterialId,
    pub old: KeyEnvelope,
    pub replacement: KeyEnvelope,
}

#[derive(Clone)]
pub struct ReplayReplacement {
    pub material: MaterialId,
    pub old_blob: Option<String>,
    pub blob: String,
    /// Ciphertext evidence length, not original material length.
    pub length: u64,
}

#[derive(Clone)]
pub struct PreparedSealed {
    pub source: String,
    pub rotations: Vec<KeyRotation>,
    pub replay: Vec<ReplayReplacement>,
    /// Anonymous, operation-owned ciphertext stream. No decoded private values.
    pub changes: Arc<std::fs::File>,
}

#[derive(Serialize, Deserialize)]
pub enum CiphertextChange {
    Event {
        telemetry: bool,
        id: Uuid,
        material: Option<Uuid>,
        old: Value,
        replacement: Option<Value>,
    },
    Failure {
        id: i64,
        material: Uuid,
        anchor: i64,
        old: String,
        replacement: Option<String>,
    },
    Position {
        binding: String,
        cursor: Value,
        committed_seq: i64,
        old: Value,
        replacement: Value,
    },
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

/// Stage and revalidate before the Writer changes any material, event or key.
pub async fn stage(
    conn: &mut PgConnection,
    plan: &Plan,
    prepared: &PreparedSealed,
    check: &(dyn Fn() -> Result<()> + Send + Sync),
) -> Result<()> {
    check()?;
    if prepared.rotations.len() != plan.materials.iter().filter(|m| m.sealed).count()
        || prepared.rotations.iter().any(|r| {
            !plan
                .materials
                .iter()
                .any(|m| m.sealed && m.id == r.material)
        })
    {
        return Err(invalid(
            "sealed key rotations differ from selected materials",
        ));
    }
    sqlx::query(
        "CREATE TEMP TABLE excision_rekeys (
        kind text NOT NULL, row_id text NOT NULL, material_id uuid, anchor bigint,
        old_value jsonb NOT NULL, replacement jsonb, cursor jsonb, committed_seq bigint,
        PRIMARY KEY(kind,row_id)) ON COMMIT DROP",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "CREATE TEMP TABLE excision_rekey_keys (
        material_id uuid PRIMARY KEY, old_ref text NOT NULL, replacement_ref text NOT NULL,
        replacement_role text NOT NULL, old_role text NOT NULL) ON COMMIT DROP",
    )
    .execute(&mut *conn)
    .await?;
    for rotation in &prepared.rotations {
        check()?;
        let whole = plan
            .materials
            .iter()
            .find(|m| m.id == rotation.material)
            .expect("selected rotation")
            .whole;
        if whole != (rotation.replacement.role == sealed::SecretRole::RecordNamingOnly.as_str())
            || !whole && rotation.old.reference == rotation.replacement.reference
        {
            return Err(invalid(
                "sealed replacement must retire its old data capability",
            ));
        }
        if rotation.old.class != "sealed"
            || rotation.replacement.class != "sealed"
            || !matches!(
                rotation.replacement.role.as_str(),
                "data-and-record-naming" | "record-naming-only"
            )
        {
            return Err(invalid("invalid prepared sealed key role or class"));
        }
        let present: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM privacy.material_keys
            WHERE material_id=$1 AND key_ref=$2 AND wrapped_key=$3 AND class=$4 AND role=$5)",
        )
        .bind(rotation.material.0)
        .bind(&rotation.old.reference)
        .bind(&rotation.old.wrapped)
        .bind(&rotation.old.class)
        .bind(&rotation.old.role)
        .fetch_one(&mut *conn)
        .await?;
        if !present {
            return Err(invalid("sealed material key changed after preparation"));
        }
        sqlx::query("INSERT INTO excision_rekey_keys VALUES($1,$2,$3,$4,$5)")
            .bind(rotation.material.0)
            .bind(&rotation.old.reference)
            .bind(&rotation.replacement.reference)
            .bind(&rotation.replacement.role)
            .bind(&rotation.old.role)
            .execute(&mut *conn)
            .await?;
    }
    if prepared.replay.len() != prepared.rotations.len() {
        return Err(invalid("sealed replay replacements differ from key owners"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for replay in &prepared.replay {
        check()?;
        if !seen.insert(replay.material)
            || !prepared
                .rotations
                .iter()
                .any(|r| r.material == replay.material)
        {
            return Err(invalid("duplicate or unowned sealed replay replacement"));
        }
        let current: Option<String> =
            sqlx::query_scalar("SELECT erased_replay_blob FROM raw.materials WHERE id=$1")
                .bind(replay.material.0)
                .fetch_one(&mut *conn)
                .await?;
        if current != replay.old_blob
            || replay.blob.is_empty()
            || replay.length == 0
            || i64::try_from(replay.length).is_err()
        {
            return Err(invalid("sealed replay metadata changed after preparation"));
        }
    }
    let mut file = &*prepared.changes;
    file.rewind()
        .map_err(|_| invalid("prepared ciphertext stream cannot rewind"))?;
    for line in BufReader::new(file).lines() {
        check()?;
        let line = line.map_err(|_| invalid("prepared ciphertext stream cannot be read"))?;
        let change: CiphertextChange = serde_json::from_str(&line)
            .map_err(|_| invalid("invalid prepared ciphertext stream"))?;
        match change {
            CiphertextChange::Event {
                telemetry,
                id,
                material,
                old,
                replacement,
            } => {
                if !sealed::is_sealed(&old) {
                    return Err(invalid("prepared event is not ciphertext"));
                }
                if replacement.is_none() && (telemetry || !plan.events.contains(&id)) {
                    return Err(invalid(
                        "prepared event deletion is outside its selected closure",
                    ));
                }
                let rotation = prepared
                    .rotations
                    .iter()
                    .find(|r| Some(r.old.reference.as_str()) == sealed::key_ref(&old))
                    .ok_or_else(|| invalid("prepared event has no retiring key owner"))?;
                if replacement.as_ref().is_some_and(|value| {
                    sealed::key_ref(value) != Some(rotation.replacement.reference.as_str())
                        || rotation.replacement.role
                            != sealed::SecretRole::DataAndRecordNaming.as_str()
                }) {
                    return Err(invalid(
                        "replacement event uses a retired or naming-only capability",
                    ));
                }
                if let Some(value) = &replacement {
                    if !sealed::is_sealed(value) {
                        return Err(invalid("replacement event is not ciphertext"));
                    }
                }
                sqlx::query("INSERT INTO excision_rekeys(kind,row_id,material_id,old_value,replacement) VALUES($1,$2,$3,$4,$5)")
                    .bind(if telemetry {"telemetry.events"} else {"core.events"}).bind(id.to_string())
                    .bind(material).bind(old).bind(replacement).execute(&mut *conn).await?;
            }
            CiphertextChange::Failure {
                id,
                material,
                anchor,
                old,
                replacement,
            } => {
                let rotation = prepared
                    .rotations
                    .iter()
                    .find(|r| r.material.0 == material)
                    .ok_or_else(|| invalid("prepared failure has no retiring key owner"))?;
                let old_payload = sealed::sealed_text(&old)
                    .ok_or_else(|| invalid("prepared failure is not ciphertext"))?;
                if sealed::key_ref(&old_payload) != Some(rotation.old.reference.as_str())
                    || replacement.as_ref().is_some_and(|text| {
                        sealed::sealed_text(text).as_ref().and_then(sealed::key_ref)
                            != Some(rotation.replacement.reference.as_str())
                            || rotation.replacement.role
                                != sealed::SecretRole::DataAndRecordNaming.as_str()
                    })
                {
                    return Err(invalid(
                        "prepared failure uses a retired or naming-only capability",
                    ));
                }
                let selected = plan
                    .materials
                    .iter()
                    .any(|m| m.id.0 == material && (m.whole || m.anchors.contains(&anchor)));
                if selected != replacement.is_none()
                    || sealed::sealed_text(&old).is_none()
                    || replacement
                        .as_deref()
                        .is_some_and(|value| sealed::sealed_text(value).is_none())
                {
                    return Err(invalid("invalid prepared failure replacement or selection"));
                }
                sqlx::query("INSERT INTO excision_rekeys(kind,row_id,material_id,anchor,old_value,replacement) VALUES('failure',$1,$2,$3,$4,$5)")
                    .bind(id.to_string()).bind(material).bind(anchor).bind(Value::String(old))
                    .bind(replacement.map(Value::String)).execute(&mut *conn).await?;
            }
            CiphertextChange::Position {
                binding,
                cursor,
                committed_seq,
                old,
                replacement,
            } => {
                if !(old.is_null() || sealed::is_sealed(&old) || sealed::metadata::is_carry(&old))
                    || !(replacement.is_null()
                        || sealed::is_sealed(&replacement)
                        || sealed::metadata::is_carry(&replacement))
                {
                    return Err(invalid(
                        "prepared parser carry contains unprotected content",
                    ));
                }
                if let Some(old_ref) = sealed::key_ref(&old) {
                    if let Some(rotation) = prepared
                        .rotations
                        .iter()
                        .find(|r| r.old.reference == old_ref)
                    {
                        let matches = if rotation.replacement.role
                            == sealed::SecretRole::RecordNamingOnly.as_str()
                        {
                            replacement.is_null() || sealed::metadata::is_carry(&replacement)
                        } else {
                            sealed::key_ref(&replacement)
                                == Some(rotation.replacement.reference.as_str())
                        };
                        if !matches {
                            return Err(invalid(
                                "prepared carry uses a retired or naming-only capability",
                            ));
                        }
                    } else if replacement != old {
                        return Err(invalid(
                            "prepared carry changes an unowned content capability",
                        ));
                    }
                } else if sealed::metadata::is_carry(&old) {
                    if !(sealed::metadata::is_carry(&replacement)
                        || sealed::key_ref(&replacement).is_some_and(|key| {
                            prepared.rotations.iter().any(|r| {
                                r.replacement.reference == key
                                    && r.replacement.role
                                        == sealed::SecretRole::DataAndRecordNaming.as_str()
                            })
                        }))
                    {
                        return Err(invalid(
                            "class-owned carry replacement is unprotected or unowned",
                        ));
                    }
                } else if replacement != old {
                    return Err(invalid(
                        "prepared carry introduces an unowned content capability",
                    ));
                }
                sqlx::query("INSERT INTO excision_rekeys(kind,row_id,old_value,replacement,cursor,committed_seq) VALUES('position',$1,$2,$3,$4,$5)")
                    .bind(binding).bind(old).bind(replacement).bind(cursor).bind(committed_seq).execute(&mut *conn).await?;
            }
        }
    }
    let refs: Vec<String> = prepared
        .rotations
        .iter()
        .map(|r| r.old.reference.clone())
        .collect();
    for table in ["core.events", "telemetry.events"] {
        let changed: bool = sqlx::query_scalar(&format!("WITH current AS (
            SELECT id::text AS row_id,material_id,payload FROM {table} WHERE class='sealed' AND payload->>'key'=ANY($1))
            SELECT EXISTS(SELECT 1 FROM current c FULL JOIN
              (SELECT * FROM excision_rekeys WHERE kind=$2) s ON s.row_id=c.row_id
              WHERE c.row_id IS NULL OR s.row_id IS NULL OR c.payload<>s.old_value
                OR c.material_id IS DISTINCT FROM s.material_id)"))
            .bind(&refs).bind(table).fetch_one(&mut *conn).await?;
        if changed {
            return Err(invalid("sealed event dependents changed after preparation"));
        }
    }
    let owners: Vec<Uuid> = prepared.rotations.iter().map(|r| r.material.0).collect();
    let failures_changed: bool = sqlx::query_scalar("WITH current AS (
        SELECT id::text AS row_id,material_id,anchor,reason FROM raw.failures WHERE material_id=ANY($1))
        SELECT EXISTS(SELECT 1 FROM current c FULL JOIN
          (SELECT * FROM excision_rekeys WHERE kind='failure') s ON s.row_id=c.row_id
          WHERE c.row_id IS NULL OR s.row_id IS NULL OR to_jsonb(c.reason)<>s.old_value
            OR c.material_id IS DISTINCT FROM s.material_id OR c.anchor IS DISTINCT FROM s.anchor)")
        .bind(&owners).fetch_one(&mut *conn).await?;
    if failures_changed {
        return Err(invalid(
            "sealed failure dependents changed after preparation",
        ));
    }
    let candidates: Vec<Uuid> = plan.candidates.iter().map(|m| m.material.0).collect();
    let positions_changed: bool = sqlx::query_scalar("WITH bindings AS (
          SELECT binding FROM raw.bindings WHERE source=$1
          UNION SELECT i.binding FROM raw.inputs i JOIN raw.materials m ON m.input_id=i.id WHERE m.id=ANY($2)),
        current AS (SELECT p.* FROM raw.positions p JOIN bindings b USING(binding))
        SELECT EXISTS(SELECT 1 FROM current c FULL JOIN
          (SELECT * FROM excision_rekeys WHERE kind='position') s ON s.row_id=c.binding
          WHERE c.binding IS NULL OR s.row_id IS NULL OR c.carry<>s.old_value
            OR c.cursor<>s.cursor OR c.committed_seq<>s.committed_seq)")
        .bind(&prepared.source).bind(&candidates).fetch_one(&mut *conn).await?;
    if positions_changed {
        return Err(invalid("source positions changed after sealed preparation"));
    }
    Ok(())
}

/// Called only after ordinary excision selection is recorded, in its transaction.
pub async fn apply(
    conn: &mut PgConnection,
    prepared: &PreparedSealed,
    check: &(dyn Fn() -> Result<()> + Send + Sync),
) -> Result<usize> {
    for replay in &prepared.replay {
        check()?;
        sealed::metadata::register_blob(conn, &replay.blob, replay.length).await?;
        sqlx::query("UPDATE raw.materials SET erased_replay_blob=$2 WHERE id=$1")
            .bind(replay.material.0)
            .bind(&replay.blob)
            .execute(&mut *conn)
            .await?;
    }
    for rotation in &prepared.rotations {
        check()?;
        let rows = sqlx::query(
            "UPDATE privacy.material_keys SET key_ref=$2,wrapped_key=$3,class='sealed',role=$4
            WHERE material_id=$1 AND key_ref=$5 AND wrapped_key=$6 AND role=$7",
        )
        .bind(rotation.material.0)
        .bind(&rotation.replacement.reference)
        .bind(&rotation.replacement.wrapped)
        .bind(&rotation.replacement.role)
        .bind(&rotation.old.reference)
        .bind(&rotation.old.wrapped)
        .bind(&rotation.old.role)
        .execute(&mut *conn)
        .await?;
        if rows.rows_affected() != 1 {
            return Err(invalid("sealed key rotation lost its expected owner"));
        }
    }
    for table in ["core.events", "telemetry.events"] {
        check()?;
        sqlx::query(&format!(
            "UPDATE {table} e SET payload=s.replacement FROM excision_rekeys s
            WHERE s.kind=$1 AND s.row_id=e.id::text AND s.replacement IS NOT NULL"
        ))
        .bind(table)
        .execute(&mut *conn)
        .await?;
    }
    check()?;
    sqlx::query("DELETE FROM raw.failures f USING excision_rekeys s WHERE s.kind='failure' AND s.row_id=f.id::text AND s.replacement IS NULL")
        .execute(&mut *conn).await?;
    sqlx::query(
        "UPDATE raw.failures f SET reason=s.replacement #>> '{}' FROM excision_rekeys s
        WHERE s.kind='failure' AND s.row_id=f.id::text AND s.replacement IS NOT NULL",
    )
    .execute(&mut *conn)
    .await?;
    check()?;
    sqlx::query(
        "UPDATE raw.positions p SET carry=s.replacement FROM excision_rekeys s
        WHERE s.kind='position' AND s.row_id=p.binding",
    )
    .execute(conn)
    .await?;
    Ok(prepared
        .rotations
        .iter()
        .filter(|r| r.old.role == sealed::SecretRole::DataAndRecordNaming.as_str())
        .count())
}
