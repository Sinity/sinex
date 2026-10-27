//! Store and open material bytes for capture, requests and replay.
mod material;
pub(crate) mod sealing;
pub(crate) use material::{MaterialOnDemand, open, open_checked};
use sealing::{SealingSink, Unlock, class_key, read_limits};
use serde_json::Value;
use sinex_model::{Batch, BatchKind, Class, MaterialId};
use sinex_source_core::{MaterialSink, Source, drivers};
use sinex_store::Blobs;
use sqlx::PgConnection;
use std::sync::Arc;

/// Capture can use a public recipient; reading stored sealed bytes needs the
/// operation's unlock.
#[derive(Clone, Copy, Default)]
pub(crate) struct Access<'a> {
    pub recipient: Option<&'a sinex_sealing::Recipient>,
    pub unlock: Option<&'a Unlock>,
}

/// Drivers store a material's bytes through this.
#[derive(Clone)]
pub(crate) struct Sink(pub Blobs);

impl MaterialSink for Sink {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.0.put(bytes).map_err(|e| e.to_string())
    }

    /// An export archive is streamed into the store, never kept in memory.
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        self.0
            .put_reader(std::io::BufReader::new(file))
            .map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
pub(crate) struct MaterialAccess {
    plain: Sink,
    class: Class,
    sealing: Option<Arc<SealingSink>>,
    unlock: Option<Unlock>,
}
impl MaterialAccess {
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
            None => self.plain.0.put_reader(reader).map_err(|e| e.to_string()),
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
    pub(crate) fn sink(&self) -> &dyn drivers::MaterialSink {
        match &self.sealing {
            Some(sink) => sink.as_ref(),
            None => &self.plain,
        }
    }
    pub(crate) fn check(&self) -> Result<(), String> {
        self.unlock.as_ref().map_or(Ok(()), Unlock::check)
    }
    pub(crate) fn admission_check(
        &self,
        check: crate::writer::AdmissionCheck,
    ) -> crate::writer::AdmissionCheck {
        let unlock = self.unlock.clone();
        Arc::new(move || {
            check()?;
            unlock.as_ref().map_or(Ok(()), Unlock::check)
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
        for record in &mut batch.records {
            if let sinex_model::RecordOutcome::Events { events, .. } = record {
                for event in events {
                    event.class = event.class.max(self.class);
                }
            }
        }
        if let Some(sealing) = self.sealing.as_deref() {
            sealing.seal(std::slice::from_mut(batch))?;
            if let (Some(material), BatchKind::Source(progress)) = (stored, &mut batch.kind) {
                // A stored material's key must not be published after excision.
                progress.data_keys.retain(|key| key.material != material);
            }
        }
        Ok(())
    }
    /// Reset carry for a new revision. Otherwise restore a stored sealed material's
    /// key when needed and open its carry using this operation's access.
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
        if acquired.starts_over().map_err(|e| e.to_string())? {
            return Ok(Value::Null);
        }
        match (&self.sealing, acquired.material.is_some()) {
            (Some(sealing), true) => {
                sealing::open_carry(conn, binding, &carry, sealing, self.unlock.as_ref()).await
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
    sealing: Option<&crate::material_access::sealing::SealingSink>,
    unlock: Option<&crate::material_access::sealing::Unlock>,
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
    let opened = crate::material_access::open(conn, blobs, material.id, unlock).await?;
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
