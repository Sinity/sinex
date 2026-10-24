//! Resolve current body/key/replay custody together before replacing a reader's pending view.
use super::Retained;
use crate::sealed_access::Authority;
use sinex_model::{MaterialId, MaterialRef};
use sinex_sources::drivers::RetainedRecords;
use sinex_store::{
    Blobs,
    sealed::{self, MaterialSecrets, metadata},
};
use sqlx::PgConnection;
use std::{io::Seek, sync::Arc};

pub(crate) struct Opened {
    pub reference: MaterialRef,
    pub body: Option<Arc<Retained>>,
    pub metadata: Vec<metadata::ErasedRecord>,
    pub append_end: Option<sinex_sources::binding::ArchivedAppendEnd>,
    pub secrets: Option<(String, MaterialSecrets)>,
}
impl Opened {
    pub fn records(&self) -> RetainedRecords {
        let bytes: Arc<dyn sinex_sources::drain::RecordRead> = match &self.body {
            Some(body) => body.clone(),
            None => Arc::new(Bodyless(self.reference.length)),
        };
        RetainedRecords {
            bodyless: self.body.is_none(),
            blob: self.reference.blob.clone(),
            bytes,
        }
    }
    pub fn transitions(
        self,
        through: usize,
    ) -> Result<Option<sinex_sources::drain::ErasedTransitions>, String> {
        if self.metadata.is_empty() && self.body.is_some() {
            return Ok(None);
        }
        let records = self
            .metadata
            .into_iter()
            .map(|r| {
                Ok((
                    usize::try_from(r.start).map_err(|_| "erased anchor exceeds address space")?,
                    usize::try_from(r.end).map_err(|_| "erased end exceeds address space")?,
                    r.transition,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        sinex_sources::drain::ErasedTransitions::from_archive(
            records,
            self.reference.length,
            self.body.is_none(),
        )
        .and_then(|e| e.after(through))
        .map(Some)
        .map_err(|e| e.to_string())
    }
}
struct Bodyless(u64);
impl sinex_sources::drain::RecordRead for Bodyless {
    fn length(&self) -> u64 {
        self.0
    }
    fn read_exact_at(&self, _start: u64, _bytes: &mut [u8]) -> std::io::Result<()> {
        Err(std::io::Error::other(
            "whole-erased material has no decryptable body",
        ))
    }
}

/// `source` is the caller's resolved native namespace, never a provider manifest field.
pub(crate) async fn open(
    conn: &mut PgConnection,
    blobs: &Blobs,
    source: &str,
    material: MaterialId,
    authority: Option<&Authority>,
) -> Result<Opened, String> {
    let reference = sinex_store::material_bytes::reference(conn, material)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("retained material is missing")?;
    let replay: Option<String> =
        sqlx::query_scalar("SELECT erased_replay_blob FROM raw.materials WHERE id=$1")
            .bind(material.0)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| e.to_string())?;
    let secrets = if blobs
        .is_sealed(&reference.blob)
        .map_err(|e| e.to_string())?
    {
        let authority =
            authority.ok_or("retained sealed material requires explicit scoped unlock")?;
        Some(authority.material_secrets(conn, material).await?)
    } else {
        None
    };
    let whole = secrets
        .as_ref()
        .is_some_and(|(_, s)| s.role() == sealed::SecretRole::RecordNamingOnly);
    if whole && replay.is_none() {
        return Err("whole-erased material lacks authenticated replay metadata".into());
    }
    let mut append_end = None;
    let records = if let Some(blob) = replay {
        let authority =
            authority.ok_or("erased replay metadata requires explicit scoped unlock")?;
        let scope = metadata::Scope::Replay {
            source: source.into(),
            material,
            parser: reference.parser.clone(),
            parser_version: i32::try_from(reference.parser_version)
                .map_err(|_| "stored parser version exceeds i32")?,
            length: reference.length,
            whole,
            append_end: reference
                .manifest
                .get("append_resume")
                .and_then(|r| r.get("end"))
                .cloned(),
        };
        let check = authority.clone();
        let input = blobs
            .read_plain(&blob, None, move || {
                check
                    .check()
                    .map_err(std::io::Error::other)
                    .map_err(Into::into)
            })
            .map_err(|e| e.to_string())?;
        let records = authority.open_replay_metadata(input, &scope)?;
        if let metadata::Scope::Replay {
            append_end: Some(end),
            ..
        } = &scope
        {
            append_end = Some(
                sinex_sources::binding::ArchivedAppendEnd::from_archive(end.clone())
                    .map_err(|e| e.to_string())?,
            );
        }
        records
    } else {
        Vec::new()
    };
    let body = if whole {
        None
    } else if let Some((key_ref, names)) = &secrets {
        let key = names.data_key(key_ref.clone()).map_err(|e| e.to_string())?;
        let authority = authority
            .ok_or("sealed read requires scoped authority")?
            .clone();
        let mut input = blobs
            .read_sealed(
                &reference.blob,
                Some(reference.length),
                &key,
                crate::sealed_access::read_limits(reference.length)?,
                move || {
                    authority
                        .check()
                        .map_err(std::io::Error::other)
                        .map_err(Into::into)
                },
            )
            .map_err(|e| e.to_string())?;
        std::io::copy(&mut input, &mut std::io::sink()).map_err(|e| e.to_string())?;
        input.rewind().map_err(|e| e.to_string())?;
        Some(Arc::new(Retained::new(input, reference.length)))
    } else {
        let authority = authority.cloned();
        let mut input = blobs
            .read_plain(&reference.blob, Some(reference.length), move || {
                if let Some(authority) = &authority {
                    authority.check().map_err(std::io::Error::other)?;
                }
                Ok(())
            })
            .map_err(|e| e.to_string())?;
        let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
        let copied = std::io::copy(&mut input, &mut file).map_err(|e| e.to_string())?;
        if copied != reference.length {
            return Err("retained material length differs".into());
        }
        Some(Arc::new(Retained::new(file, reference.length)))
    };
    Ok(Opened {
        reference,
        body,
        metadata: records,
        append_end,
        secrets,
    })
}
