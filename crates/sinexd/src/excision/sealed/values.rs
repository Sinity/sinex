//! Dependent preparation holds one decoded value at a time. Its durable stage
//! contains only old/new ciphertext and exact immutable row coordinates.
use super::*;
use futures::TryStreamExt;
use sinex_store::excision::sealed::CiphertextChange;
type Keys = BTreeMap<String, (Option<crypto::DataKey>, Option<crypto::DataKey>)>;

pub(super) async fn dependents(
    conn: &mut PgConnection,
    plan: &Plan,
    keys: &Keys,
    file: &mut std::fs::File,
    authority: &Authority,
) -> Result<(), String> {
    let refs = keys.keys().cloned().collect::<Vec<_>>();
    for table in ["core.events", "telemetry.events"] {
        let query = format!(
            "SELECT id,material_id,payload FROM {table} WHERE class='sealed' AND payload->>'key'=ANY($1) ORDER BY id"
        );
        let mut rows = sqlx::query_as::<_, (uuid::Uuid, Option<uuid::Uuid>, Value)>(&query)
            .bind(&refs)
            .fetch(&mut *conn);
        while let Some((id, material, old)) = rows.try_next().await.map_err(|e| e.to_string())? {
            authority.check()?;
            let selected = table == "core.events" && plan.events.contains(&id);
            let replacement = if selected {
                None
            } else {
                Some(rekey(&old, keys)?)
            };
            write_change(
                file,
                &CiphertextChange::Event {
                    telemetry: table == "telemetry.events",
                    id,
                    material,
                    old,
                    replacement,
                },
            )?;
        }
    }
    let owners = plan
        .materials
        .iter()
        .filter(|m| m.sealed)
        .map(|m| m.id.0)
        .collect::<Vec<_>>();
    let mut rows = sqlx::query_as::<_, (i64, uuid::Uuid, i64, String)>("SELECT id,material_id,anchor,reason FROM raw.failures WHERE material_id=ANY($1) ORDER BY id").bind(owners).fetch(&mut *conn);
    while let Some((id, material, anchor, old)) =
        rows.try_next().await.map_err(|e| e.to_string())?
    {
        authority.check()?;
        let selected = plan
            .materials
            .iter()
            .any(|m| m.id.0 == material && (m.whole || m.anchors.contains(&anchor)));
        let replacement = if selected {
            None
        } else {
            let payload =
                crypto::sealed_text(&old).ok_or("sealed material has an unprotected failure")?;
            Some(rekey(&payload, keys)?.to_string())
        };
        write_change(
            file,
            &CiphertextChange::Failure {
                id,
                material,
                anchor,
                old,
                replacement,
            },
        )?;
    }
    Ok(())
}

fn rekey(value: &Value, keys: &Keys) -> Result<Value, String> {
    let (old, fresh) = keys
        .get(crypto::key_ref(value).ok_or("dependent value is not sealed")?)
        .ok_or("dependent value lacks its frozen key owner")?;
    let old = old
        .as_ref()
        .ok_or("dependent value uses a retired naming-only key")?;
    let fresh = fresh
        .as_ref()
        .ok_or("whole removal would strand an unrelated private dependent")?;
    let plain = crypto::unseal_payload(value, &old.key)
        .map_err(|_| "sealed dependent authentication failed")?;
    crypto::seal_value(&plain, fresh).map_err(|_| "sealed dependent re-encryption failed".into())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Position {
    binding: String,
    parser: String,
    version: i32,
    cursor: Value,
    committed: i64,
    old: Value,
    sanitized: Option<Vec<u8>>,
    owner: Option<MaterialId>,
    changed: bool,
}
pub(super) struct Positions {
    source: String,
    file: std::fs::File,
    rekey_owners: BTreeSet<MaterialId>,
}
impl Positions {
    pub async fn load(
        conn: &mut PgConnection,
        blobs: &Blobs,
        source: &str,
        archive: &Archive,
        selected: &BTreeMap<MaterialId, BTreeSet<i64>>,
        authority: &Authority,
    ) -> Result<Self, String> {
        let mut file = tempfile::tempfile().map_err(|_| "cannot stage protected carry changes")?;
        let mut rekey_owners = BTreeSet::new();
        let mut after = String::new();
        loop {
            let row:Option<(String,Value,Value,i64)>=sqlx::query_as("SELECT p.binding,p.cursor,p.carry,p.committed_seq FROM raw.positions p JOIN raw.bindings b USING(binding) WHERE b.source=$1 AND p.binding>$2 ORDER BY p.binding LIMIT 1")
                .bind(source).bind(&after).fetch_optional(&mut *conn).await.map_err(|e|e.to_string())?;
            let Some((binding, cursor, old, committed)) = row else {
                break;
            };
            after.clone_from(&binding);
            authority.check()?;
            let bound = sinex_store::bindings::load(conn, &binding)
                .await
                .map_err(|e| e.to_string())?
                .ok_or("source carry binding disappeared")?;
            let current: MaterialId = serde_json::from_value(cursor["material"].clone())
                .map_err(|_| "source carry lacks authenticated material coordinates")?;
            let candidate = archive
                .candidates
                .iter()
                .find(|c| c.material == current)
                .ok_or("source carry is outside its parser candidates")?;
            let source_parser = sinex_sources::sources::source_for(&sinex_sources::Binding {
                name: binding.clone(),
                source: bound.source,
                parser: candidate.parser.clone(),
                input: bound.input.into(),
                settings: serde_json::from_value(bound.settings)
                    .map_err(|_| "source carry has invalid recorded settings")?,
            })?;
            let mut owner = None;
            let plain = if old.is_null() {
                Value::Null
            } else if metadata::is_carry(&old) {
                let scope = metadata::Scope::Carry {
                    source: source.into(),
                    binding: binding.clone(),
                    parser: candidate.parser.clone(),
                    parser_version: candidate.parser_version,
                    cursor: cursor.clone(),
                };
                let ciphertext = metadata::carry_ciphertext(&old).map_err(|e| e.to_string())?;
                authority.open_current_carry(ciphertext.as_slice(), &scope)?
            } else {
                let reference = crypto::key_ref(&old)
                    .ok_or("source has an unsupported unprotected parser carry")?;
                let key_owner = archive
                    .candidates
                    .iter()
                    .find(|c| c.key.as_ref().is_some_and(|k| k.reference == reference))
                    .ok_or("source carry has no current material key owner")?;
                let owns_binding:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE m.id=$1 AND i.binding=$2)")
                    .bind(key_owner.material.0).bind(&binding).fetch_one(&mut *conn).await.map_err(|e|e.to_string())?;
                if !owns_binding {
                    return Err("source carry data key belongs to another binding".into());
                }
                let key = authority.material_key(conn, key_owner.material).await?;
                owner = Some(key_owner.material);
                crypto::unseal_payload(&old, &key.key)
                    .map_err(|_| "source carry authentication failed")?
            };
            let mut sanitized = plain.clone();
            for (material, anchors) in selected {
                archive
                    .walk(conn, blobs, *material, authority, |record| {
                        if anchors.contains(&(record.start as i64)) && !record.failed {
                            let context = sinex_sources::RecordContext {
                                manifest: &record.candidate.manifest,
                                member: sinex_sources::drain::member_at(
                                    &record.candidate.manifest,
                                    record.start,
                                ),
                            };
                            sanitized = source_parser
                                .erase_record(record.raw, &sanitized, &context)
                                .map_err(|e| e.to_string())?
                                .carry;
                        }
                        Ok(())
                    })
                    .await?;
            }
            let changed = sanitized != plain;
            let sanitized = if changed {
                let scope = metadata::Scope::Carry {
                    source: source.into(),
                    binding: binding.clone(),
                    parser: candidate.parser.clone(),
                    parser_version: candidate.parser_version,
                    cursor: cursor.clone(),
                };
                let encrypted = metadata::seal_carry(&sanitized, &authority.recipient(), &scope)
                    .map_err(|e| e.to_string())?;
                if authority.open_current_carry(encrypted.as_slice(), &scope)? != sanitized {
                    return Err("sanitized parser carry authentication failed".into());
                }
                Some(encrypted)
            } else {
                None
            };
            if changed && let Some(owner) = owner {
                rekey_owners.insert(owner);
            }
            let position = Position {
                binding,
                parser: candidate.parser.clone(),
                version: candidate.parser_version,
                cursor,
                committed,
                old,
                sanitized,
                owner,
                changed,
            };
            serde_json::to_writer(&mut file, &position)
                .map_err(|_| "cannot stage protected carry")?;
            file.write_all(b"\n")
                .map_err(|_| "cannot stage protected carry")?;
        }
        file.rewind()
            .map_err(|_| "cannot rewind protected carry stage")?;
        Ok(Self {
            source: source.into(),
            file,
            rekey_owners,
        })
    }
    pub fn data_owners(&self) -> impl Iterator<Item = MaterialId> + '_ {
        self.rekey_owners.iter().copied()
    }
    pub async fn prepare(
        self,
        keys: &Keys,
        file: &mut std::fs::File,
        authority: &Authority,
    ) -> Result<(), String> {
        use std::io::BufRead;
        for line in std::io::BufReader::new(self.file).lines() {
            let position: Position =
                serde_json::from_str(&line.map_err(|_| "cannot read protected carry stage")?)
                    .map_err(|_| "invalid protected carry stage")?;
            authority.check()?;
            let rotating = crypto::key_ref(&position.old).and_then(|key| keys.get(key));
            let scope = metadata::Scope::Carry {
                source: self.source.clone(),
                binding: position.binding.clone(),
                parser: position.parser.clone(),
                parser_version: position.version,
                cursor: position.cursor.clone(),
            };
            let plain = if let Some(ciphertext) = &position.sanitized {
                Some(authority.open_current_carry(ciphertext.as_slice(), &scope)?)
            } else if let Some((old, _)) = rotating {
                let old = old
                    .as_ref()
                    .ok_or("parser carry has no retiring data capability")?;
                Some(
                    crypto::unseal_payload(&position.old, &old.key)
                        .map_err(|_| "parser carry authentication failed")?,
                )
            } else {
                None
            };
            let replacement = if let Some((_, Some(fresh))) = rotating {
                crypto::seal_value(
                    plain.as_ref().ok_or("missing sanitized parser carry")?,
                    fresh,
                )
                .map_err(|_| "cannot re-encrypt parser carry")?
            } else if position.changed || rotating.is_some() {
                let plain = plain.as_ref().ok_or("missing sanitized parser carry")?;
                let ciphertext = metadata::seal_carry(plain, &authority.recipient(), &scope)
                    .map_err(|e| e.to_string())?;
                if authority.open_current_carry(ciphertext.as_slice(), &scope)? != *plain {
                    return Err("sanitized parser carry verification failed".into());
                }
                metadata::carry_envelope(&ciphertext)
            } else {
                position.old.clone()
            };
            write_change(
                file,
                &CiphertextChange::Position {
                    binding: position.binding,
                    cursor: position.cursor,
                    committed_seq: position.committed,
                    old: position.old,
                    replacement,
                },
            )?;
        }
        Ok(())
    }
}
