//! Material privacy for capture, replay and stored reads. Readers pass their
//! operation's access here; encryption and key handling stay behind this seam.
mod access;
mod material;
mod sink;
pub(crate) use access::{Access, Unlock, read_limits};
pub(crate) use material::{MaterialOnDemand, open, open_checked};
use serde_json::Value;
use sinex_model::{Batch, BatchKind, Class, MaterialId, RecordOutcome};
use sinex_source_core::{Source, drivers};
use sinex_store::Blobs;
pub(crate) use sink::{SealingSink, Sink, class_key};
use sqlx::PgConnection;
use std::sync::Arc;

pub(crate) fn check(unlock: Option<&Unlock>) -> Result<(), String> {
    unlock.map_or(Ok(()), Unlock::check)
}

pub(crate) fn prepare_batch(
    batch: &mut Batch,
    class: Class,
    sealing: Option<&SealingSink>,
    stored: Option<MaterialId>,
) -> Result<(), String> {
    for record in &mut batch.records {
        if let sinex_model::RecordOutcome::Events { events, .. } = record {
            for event in events {
                event.class = event.class.max(class);
            }
        }
    }
    if let Some(sealing) = sealing {
        sealing.seal(std::slice::from_mut(batch))?;
        if let (Some(material), BatchKind::Source(progress)) = (stored, &mut batch.kind) {
            // A stored material's key must not be published after excision.
            progress.data_keys.retain(|key| key.material != material);
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct Materials {
    plain: Sink,
    class: Class,
    sealing: Option<Arc<SealingSink>>,
    unlock: Option<Unlock>,
}
impl Materials {
    pub(crate) fn new(blobs: Blobs, class: Class, access: Access<'_>) -> Result<Self, String> {
        let sealing = if class == Class::Sealed {
            let recipient = match access.recipient {
                Some(recipient) => Some(recipient.clone()),
                None => class_key()?,
            }
            .ok_or("its class is sealed, but no class key is configured (SINEX_CLASS_KEY_FILE)")?;
            Some(Arc::new(SealingSink::new(blobs.clone(), recipient)))
        } else {
            None
        };
        Ok(Self {
            plain: Sink(blobs),
            class,
            sealing,
            unlock: access.unlock.cloned(),
        })
    }
    pub(crate) fn archive(blobs: Blobs, class: Class, access: Access<'_>) -> Result<Self, String> {
        let recipient = if class == Class::Sealed {
            let configured = if access.recipient.is_none() {
                class_key()?
            } else {
                None
            };
            Some(
                access
                    .recipient
                    .cloned()
                    .or(configured)
                    .or_else(|| access.unlock.map(Unlock::recipient))
                    .ok_or("sealed capture requires the class key's public recipient")?,
            )
        } else {
            None
        };
        Self::new(
            blobs,
            class,
            Access {
                recipient: recipient.as_ref(),
                unlock: access.unlock,
            },
        )
    }
    pub(crate) async fn restore_archive(
        &self,
        conn: &mut PgConnection,
        stored: Option<&sinex_source_core::shared_pass::StoredFile>,
    ) -> Result<(), String> {
        if let (Some(stored), Some(sealing)) = (stored, &self.sealing) {
            let unlock = self
                .unlock
                .as_ref()
                .ok_or("continuing a sealed file needs an operator unlock")?;
            let material = stored
                .material
                .ok_or("a sealed stored copy names no material")?;
            let key = unlock.data_key(conn, material).await?;
            sealing.restore_key(stored.blob.clone(), key)?;
        }
        Ok(())
    }
    pub(crate) fn store_reader(
        &self,
        reader: impl std::io::Read + std::io::Seek,
    ) -> Result<String, String> {
        match &self.sealing {
            Some(sealing) => sealing.put_reader(reader),
            None => self.plain.0.put_seekable(reader).map_err(|e| e.to_string()),
        }
    }
    pub(crate) fn archive_input<C>(
        &self,
        blob: &str,
        length: u64,
        mut check: C,
    ) -> Result<sinex_source_core::shared_pass::Input, String>
    where
        C: FnMut() -> std::io::Result<()> + Send + 'static,
    {
        use sinex_source_core::shared_pass::Input;
        match &self.sealing {
            Some(sealing) => {
                let key = sealing.key(blob)?;
                Ok(Input::Seekable(Box::new(
                    self.plain
                        .0
                        .read_sealed(blob, Some(length), &key, read_limits(length)?, move || {
                            check().map_err(Into::into)
                        })
                        .map_err(|e| e.to_string())?,
                )))
            }
            None => Ok(Input::Sequential(Box::new(
                self.plain
                    .0
                    .read_plain(blob, Some(length), move || check().map_err(Into::into))
                    .map_err(|e| e.to_string())?,
            ))),
        }
    }
    pub(crate) async fn shared_carry(
        &self,
        conn: &mut PgConnection,
        binding: &str,
        carry: serde_json::Value,
        acquired: &drivers::Acquired,
    ) -> Result<serde_json::Value, String> {
        if acquired.starts_over().map_err(|e| e.to_string())? {
            return Ok(serde_json::Value::Null);
        }
        match (&self.sealing, acquired.material.is_some()) {
            (Some(sealing), true) => {
                open_carry(conn, binding, &carry, sealing, self.unlock.as_ref()).await
            }
            _ => Ok(carry),
        }
    }
    pub(crate) fn sink(&self) -> &dyn drivers::MaterialSink {
        match &self.sealing {
            Some(sink) => sink.as_ref(),
            None => &self.plain,
        }
    }
    pub(crate) fn check(&self) -> Result<(), String> {
        check(self.unlock.as_ref())
    }
    pub(crate) fn admission_check(
        &self,
        check: crate::writer::AdmissionCheck,
    ) -> crate::writer::AdmissionCheck {
        let unlock = self.unlock.clone();
        Arc::new(move || {
            check()?;
            self::check(unlock.as_ref())
        })
    }
    pub(crate) async fn reopen(
        &self,
        conn: &mut PgConnection,
        source: &dyn Source,
        acquired: &mut drivers::Acquired,
    ) -> Result<Option<MaterialId>, String> {
        let stored = reopen_stored(
            conn,
            &self.plain.0,
            source,
            acquired,
            self.sealing.as_deref(),
            self.unlock.as_ref(),
        )
        .await?;
        Ok(stored.then(|| acquired.material.as_ref().expect("reopened material").id))
    }
    pub(crate) fn prepare_batch(
        &self,
        batch: &mut Batch,
        stored: Option<MaterialId>,
    ) -> Result<(), String> {
        prepare_batch(batch, self.class, self.sealing.as_deref(), stored)
    }
    pub(crate) async fn carry(
        &self,
        conn: &mut PgConnection,
        binding: &str,
        carry: serde_json::Value,
        acquired: &drivers::Acquired,
    ) -> Result<serde_json::Value, String> {
        self.check()?;
        if let (Some(sealing), Some(material)) = (&self.sealing, &acquired.material)
            && let Some(drivers::StoredBytes::Blob {
                name,
                records: Some(_),
                ..
            }) = &material.stored
            && self.plain.0.is_sealed(name).map_err(|e| e.to_string())?
        {
            let unlock = self
                .unlock
                .as_ref()
                .ok_or("stored sealed material needs an operator unlock")?;
            let key = unlock.data_key(conn, material.id).await?;
            sealing.restore_key(name.clone(), key)?;
        }
        match (&self.sealing, acquired.material.is_some()) {
            (Some(sealing), true) => {
                open_carry(conn, binding, &carry, sealing, self.unlock.as_ref()).await
            }
            _ => Ok(carry),
        }
    }
}

/// A driver that offers a material already in the store (a restart before all
/// of it was admitted) reads it from the store, not from the source again.
/// True when it did.
pub(crate) async fn reopen_stored(
    conn: &mut PgConnection,
    blobs: &sinex_store::Blobs,
    source: &dyn Source,
    acquired: &mut drivers::Acquired,
    sealing: Option<&crate::sealing::SealingSink>,
    unlock: Option<&crate::sealing::Unlock>,
) -> Result<bool, String> {
    let Some(material) = &mut acquired.material else {
        return Ok(false);
    };
    let registered = sinex_store::material_bytes::registered(conn, &[material.id.0])
        .await
        .map_err(|e| e.to_string())?;
    if registered.is_empty() {
        return Ok(false);
    }
    let opened = crate::sealing::open(conn, blobs, material.id, unlock).await?;
    if opened.reference.parser != source.spec().parser
        || opened.reference.parser_version != source.spec().parser_version
    {
        return Err("a stored material was read by another parser version".into());
    }
    if let (Some(sealing), Some(key)) = (sealing, &opened.key) {
        sealing.restore_current(material.id, opened.reference.blob.clone(), key.clone())?;
    }
    material.stored = Some(opened.records());
    material.bytes.clear();
    material.manifest = opened.reference.manifest.clone();
    Ok(true)
}

pub(crate) async fn open_carry(
    conn: &mut PgConnection,
    binding: &str,
    carry: &serde_json::Value,
    sealing: &crate::sealing::SealingSink,
    unlock: Option<&crate::sealing::Unlock>,
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

/// Sealed acquisitions keep their own key and cannot share a stored copy.
pub(crate) fn can_share(class: Class) -> bool {
    class != Class::Sealed
}

pub(crate) async fn history_floor(conn: &mut PgConnection, binding: &str) -> Result<Class, String> {
    if sinex_store::positions::has_sealed_material(conn, binding)
        .await
        .map_err(|e| e.to_string())?
    {
        Ok(Class::Sealed)
    } else {
        Ok(Class::default())
    }
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

/// A reopened body already has its material and data key registered.
pub(crate) fn registered(acquired: &drivers::Acquired) -> Option<MaterialId> {
    acquired.material.as_ref().and_then(|material| {
        matches!(
            &material.stored,
            Some(drivers::StoredBytes::Blob {
                records: Some(_),
                ..
            })
        )
        .then_some(material.id)
    })
}
