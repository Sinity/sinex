//! How drivers store a material's bytes: plainly, or, for a sealed source,
//! encrypted under a fresh data key per material. Every reading path (backfill,
//! the daemon's source tasks, replay) stores through one of these.
mod unlock;
use serde_json::Value;
use sinex_model::{MaterialId, RecordOutcome};
use sinex_source_core::drivers::MaterialSink;
use sinex_store::Blobs;
use sqlx::PgConnection;
pub(crate) use unlock::{Unlock, read_limits};

/// The class key's public half, from the file `$SINEX_CLASS_KEY_FILE` (an age
/// recipient, `age1…`). Sealed sources need it to capture; the daemon never
/// holds the secret half.
pub(crate) fn class_key() -> Result<Option<sinex_sealing::Recipient>, String> {
    let Some(path) = std::env::var_os("SINEX_CLASS_KEY_FILE") else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("{}: {e}", std::path::Path::new(&path).display()))?;
    sinex_sealing::parse_recipient(&text)
        .map(Some)
        .map_err(|e| e.to_string())
}

/// Drivers of a sealed source store a material's bytes through this: each
/// material gets a fresh data key, its bytes are sealed under it, and the key
/// waits here until the batch that registers the material is sealed with it.
pub(crate) struct SealingSink {
    blobs: Blobs,
    recipient: sinex_sealing::Recipient,
    state: std::sync::Mutex<SealingState>,
}

#[derive(Default)]
struct SealingState {
    unadmitted: std::collections::HashMap<String, SealingKey>,
    current: Option<(MaterialId, String, SealingKey)>,
}

#[derive(Clone)]
struct SealingKey {
    data: sinex_sealing::DataKey,
    wrapped: Vec<u8>,
}

impl SealingSink {
    pub(crate) fn new(blobs: Blobs, recipient: sinex_sealing::Recipient) -> Self {
        Self {
            blobs,
            recipient,
            state: Default::default(),
        }
    }

    pub(crate) fn key(&self, blob: &str) -> Result<sinex_sealing::DataKey, String> {
        Ok(self.sealing_key(blob)?.data)
    }

    fn sealing_key(&self, blob: &str) -> Result<SealingKey, String> {
        let state = self.state.lock().expect("sealing state");
        state
            .unadmitted
            .get(blob)
            .cloned()
            .or_else(|| {
                state
                    .current
                    .as_ref()
                    .filter(|(_, name, _)| name == blob)
                    .map(|(_, _, key)| key.clone())
            })
            .ok_or_else(|| "sealed data key is unavailable; explicit unlock is required".into())
    }

    pub(crate) fn open_carry(
        &self,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let Some(reference) = sinex_sealing::key_ref(value) else {
            return Ok(value.clone());
        };
        let state = self.state.lock().expect("sealing state");
        let key = state
            .current
            .as_ref()
            .filter(|(_, _, key)| key.data.key_ref == reference)
            .map(|(_, _, key)| key)
            .or_else(|| {
                state
                    .unadmitted
                    .values()
                    .find(|key| key.data.key_ref == reference)
            })
            .ok_or("sealed parser carry needs explicit operator unlock")?;
        sinex_sealing::unseal_payload(value, &key.data.key).map_err(|e| e.to_string())
    }

    /// Hold `key` for the material stored as `blob`, wrapped for the class.
    pub(crate) fn restore_key(
        &self,
        blob: String,
        key: sinex_sealing::DataKey,
    ) -> Result<(), String> {
        let wrapped = sinex_sealing::wrap(&key, &self.recipient).map_err(|e| e.to_string())?;
        self.state
            .lock()
            .expect("sealing state")
            .unadmitted
            .insert(blob, SealingKey { data: key, wrapped });
        Ok(())
    }

    pub(crate) fn restore_current(
        &self,
        material: MaterialId,
        blob: String,
        key: sinex_sealing::DataKey,
    ) -> Result<(), String> {
        let wrapped = sinex_sealing::wrap(&key, &self.recipient).map_err(|e| e.to_string())?;
        self.state.lock().expect("sealing state").current =
            Some((material, blob, SealingKey { data: key, wrapped }));
        Ok(())
    }

    pub(crate) fn put_reader(
        &self,
        reader: impl std::io::Read + std::io::Seek,
    ) -> Result<String, String> {
        let key = sinex_sealing::new_key().map_err(|e| e.to_string())?;
        let name = self
            .blobs
            .put_sealed_reader(reader, &key)
            .map_err(|e| e.to_string())?;
        self.restore_key(name.clone(), key)?;
        Ok(name)
    }

    /// Seal events and carry with their material's data key. The wrapped key travels
    /// with source progress so its row and the material commit atomically.
    pub(crate) fn seal(&self, batches: &mut [sinex_model::Batch]) -> Result<(), String> {
        for batch in batches {
            let material = match &batch.kind {
                sinex_model::BatchKind::Source(p) => p.material.clone(),
                _ => return Err("source sealing only accepts source progress".into()),
            };
            let checkpoint = material.is_none() && batch.records.is_empty();
            if let Some(material) = material {
                let sealing_key = self.sealing_key(&material.blob)?;
                let key = &sealing_key.data;
                let wrapped = sealing_key.wrapped.clone();
                let wrapped = sinex_model::WrappedDataKey {
                    material: material.id,
                    key_ref: key.key_ref.clone(),
                    wrapped,
                };
                if let sinex_model::BatchKind::Source(progress) = &mut batch.kind {
                    progress.data_keys.push(wrapped);
                }
                let mut state = self.state.lock().expect("sealing state");
                state.unadmitted.remove(&material.blob);
                state.current = Some((material.id, material.blob.clone(), sealing_key));
            }
            let current = self.state.lock().expect("sealing state").current.clone();
            let Some((material, _, sealing_key)) = current else {
                let sinex_model::BatchKind::Source(progress) = &batch.kind else {
                    unreachable!()
                };
                if batch.records.is_empty()
                    && (progress.carry.is_null() || sinex_sealing::is_sealed(&progress.carry))
                {
                    continue;
                }
                return Err(
                    "sealed source state needs its data key; explicit unlock is required".into(),
                );
            };
            let key = &sealing_key.data;
            for record in &mut batch.records {
                match record {
                    sinex_model::RecordOutcome::Events { events, .. } => {
                        for event in events {
                            if event
                                .location()
                                .is_none_or(|(location, _)| location.material != material)
                            {
                                return Err(
                                    "sealed batch cites a different material than its key".into()
                                );
                            }
                        }
                    }
                    sinex_model::RecordOutcome::Failure { location, .. } => {
                        if location.material != material {
                            return Err("sealed failure cites a different material".into());
                        }
                    }
                }
            }
            seal_records(&mut batch.records, Some(key))?;
            if let sinex_model::BatchKind::Source(progress) = &mut batch.kind {
                let already_sealed = checkpoint
                    && sinex_sealing::key_ref(&progress.carry) == Some(key.key_ref.as_str())
                    && sinex_sealing::unseal_payload(&progress.carry, &key.key).is_ok();
                // An empty carry says nothing; sealing it would make a later
                // material of a stateless parser need this material's key.
                if !already_sealed && !progress.carry.is_null() {
                    progress.carry = sinex_sealing::seal_value(&progress.carry, key)
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(())
    }
}

impl MaterialSink for SealingSink {
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        self.put_reader(file)
    }

    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.put_reader(std::io::Cursor::new(bytes))
    }
}

#[cfg(test)]
mod tests;

pub(crate) async fn open_carry(
    conn: &mut PgConnection,
    binding: &str,
    carry: &serde_json::Value,
    sealing: &crate::material_access::sealing::SealingSink,
    unlock: Option<&crate::material_access::sealing::Unlock>,
) -> Result<serde_json::Value, String> {
    if let Ok(value) = sealing.open_carry(carry) {
        return Ok(value);
    }
    let reference = sinex_sealing::key_ref(carry).ok_or("invalid sealed parser carry")?;
    let unlock = unlock.ok_or("sealed parser carry requires explicit operator unlock")?;
    let material = sinex_store::sealed::material_of_key(conn, reference, binding)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("sealed parser carry has no key of this binding")?;
    let key = unlock.data_key(conn, material).await?;
    let value = sinex_sealing::unseal_payload(carry, &key.key).map_err(|e| e.to_string())?;
    unlock.check()?;
    Ok(value)
}

pub(crate) fn seal_records(
    records: &mut [sinex_model::RecordOutcome],
    key: Option<&sinex_sealing::DataKey>,
) -> Result<(), String> {
    if let Some(key) = key {
        for record in records {
            match record {
                RecordOutcome::Events { events, .. } => {
                    for event in events {
                        sinex_sealing::seal_event(event, key).map_err(|e| e.to_string())?;
                    }
                }
                RecordOutcome::Failure { reason, .. } => {
                    *reason = sinex_sealing::seal_value(&Value::String(reason.clone()), key)
                        .map_err(|e| e.to_string())?
                        .to_string()
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn replay_keys(
    conn: &mut PgConnection,
    blobs: &Blobs,
    materials: Vec<(uuid::Uuid, String)>,
    unlock: Option<&Unlock>,
) -> Result<std::collections::HashMap<uuid::Uuid, sinex_sealing::DataKey>, String> {
    let mut keys = std::collections::HashMap::new();
    for (material, blob) in materials {
        if blobs.is_sealed(&blob).map_err(|e| e.to_string())? {
            let unlock = unlock.ok_or_else(|| {
                format!("material {material}: sealed replay needs an operator unlock and --sealed")
            })?;
            keys.insert(material, unlock.data_key(conn, MaterialId(material)).await?);
        }
    }
    Ok(keys)
}

pub(crate) fn collect_sealed(value: &Value, out: &mut Vec<String>) {
    if let Some(r) = sinex_sealing::key_ref(value) {
        out.push(r.to_string());
        return;
    }
    match value {
        Value::String(text) => {
            if let Some(value) = sinex_sealing::sealed_text(text) {
                collect_sealed(&value, out);
            }
        }
        Value::Array(items) => items.iter().for_each(|v| collect_sealed(v, out)),
        Value::Object(map) => map.values().for_each(|v| collect_sealed(v, out)),
        _ => {}
    }
}

pub(crate) fn unseal_in(
    value: &mut Value,
    keys: &std::collections::HashMap<String, sinex_sealing::Zeroizing<[u8; 32]>>,
) {
    if let Some(r) = sinex_sealing::key_ref(value) {
        if let Some(key) = keys.get(r)
            && let Ok(plain) = sinex_sealing::unseal_payload(value, key)
        {
            *value = plain;
        }
        return;
    }
    match value {
        Value::String(text) => {
            if let Some(mut sealed) = sinex_sealing::sealed_text(text) {
                unseal_in(&mut sealed, keys);
                if !sinex_sealing::is_sealed(&sealed) {
                    *value = sealed;
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|v| unseal_in(v, keys)),
        Value::Object(map) => map.values_mut().for_each(|v| unseal_in(v, keys)),
        _ => {}
    }
}
