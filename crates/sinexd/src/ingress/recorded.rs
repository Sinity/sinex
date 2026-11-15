//! A material Sinex recorded from this producer, and an independent check of
//! its headers against the current source declaration.
use super::*;
use crate::Result;
use sinex_protocol::{
    RecordedContinuation, RecordedContinuationRequest, RecordedHeaders, RecordedMaterialBinding,
};

impl Requests {
    pub(super) async fn recorded_continuation(
        &self,
        conn: &mut PgConnection,
        producer: &str,
        asked: &RecordedContinuationRequest,
    ) -> Result<RecordedContinuation> {
        let unknown = |reason: &str| RecordedContinuation::Unknown {
            request: asked.clone(),
            reason: reason.into(),
        };
        if asked.frame.is_empty() || asked.frame.contains('\0') {
            return Ok(unknown("material_ack_identity_unknown"));
        }
        let Some(saved) = commit_log::recorded_material(conn, producer, &asked.frame).await? else {
            return Ok(unknown("material_ack_not_recorded"));
        };
        if saved.stored_frame.number <= 0 || saved.stored_frame.refusal_kind().is_some() {
            return Ok(unknown("material_ack_not_positive"));
        }
        let content_hash = saved.stored_frame.content_hash();
        if content_hash.is_none_or(|hash| {
            hash.strip_prefix("blake3:")
                .is_none_or(|hex| blake3::Hash::from_hex(hex).is_err())
        }) {
            return Ok(unknown("material_ack_identity_unknown"));
        }
        if content_hash != Some(asked.frame_content_hash.as_str())
            || saved.stored_frame.material_id != Some(asked.material.0)
            || saved.blob.as_deref() != Some(asked.blob.as_str())
            || saved.length.and_then(|length| u64::try_from(length).ok()) != Some(asked.length)
        {
            return Ok(unknown("material_ack_binding_mismatch"));
        }
        if !saved.available {
            return Ok(unknown("material_custody_unavailable"));
        }
        let (Some(name), Some(version), Some(fingerprint), Some(family), Some(prefixes)) = (
            saved.source_name,
            saved
                .source_version
                .and_then(|version| u32::try_from(version).ok()),
            saved.source_fingerprint,
            saved.family,
            saved.write_kind_prefixes,
        ) else {
            return Ok(unknown("material_source_unknown"));
        };
        if name.is_empty() || family.is_empty() || blake3::Hash::from_hex(&fingerprint).is_err() {
            return Ok(unknown("material_source_unknown"));
        }
        let binding = RecordedMaterialBinding {
            ack: acknowledgment(saved.stored_frame)?,
            material: asked.material,
            blob: asked.blob.clone(),
            length: asked.length,
            source: sinex_model::SourceRef { name, version },
            source_fingerprint: fingerprint,
            family,
            write_kind_prefixes: prefixes,
            producer_host: saved.producer_host,
        };
        let headers = if asked.uses.is_empty() {
            RecordedHeaders::NotRequested
        } else {
            match self
                .continuation_headers(conn, producer, &binding, &asked.uses)
                .await
            {
                Ok(()) => RecordedHeaders::PermittedHeaders,
                Err(Check::Unavailable(error)) => {
                    return Err(Error::new(command_surface::FailureKind::NotRunning, error));
                }
                Err(reason) => RecordedHeaders::NotPermitted {
                    reason: reason.to_string(),
                },
            }
        };
        Ok(RecordedContinuation::Stored {
            request: asked.clone(),
            binding: Box::new(binding),
            headers,
        })
    }

    async fn continuation_headers(
        &self,
        conn: &mut PgConnection,
        producer: &str,
        binding: &RecordedMaterialBinding,
        uses: &[sinex_protocol::RecordedHeaderUse],
    ) -> Result<(), Check> {
        let registered =
            write_access::built_in_declaration(conn, &self.read_grants, producer, &binding.source)
                .await?;
        if registered.family != binding.family
            || registered.fingerprint() != binding.source_fingerprint
        {
            return Err(Check::Configuration(
                "recorded material's source differs from the current source declaration".into(),
            ));
        }
        for header in uses {
            write_access::event(&self.read_grants, producer, &header.kind.name, header.class)
                .map_err(|error| Check::Configuration(error.to_string()))?;
            if header.kind.name.contains('\0') {
                return Err(Check::Configuration("header kind name contains NUL".into()));
            }
            header
                .kind
                .database_version()
                .map_err(|error| Check::Configuration(error.to_string()))?;
        }
        let refs = uses
            .iter()
            .map(|header| header.kind.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let kinds = sinex_schema::kinds::load(conn, &refs.into_iter().collect::<Vec<_>>())
            .await
            .map_err(|error| Check::Unavailable(error.to_string()))?;
        for header in uses {
            let kind = kinds.get(&header.kind).ok_or_else(|| {
                Check::Configuration("header kind version is not registered".into())
            })?;
            let hash = kind.definition_hash();
            if hash != header.kind_definition_hash
                || !registered.allows(&header.kind, &hash)
                || (kind.declaring_producer != "sinex" && kind.declaring_producer != binding.family)
            {
                return Err(Check::Configuration(
                    "header is outside the recorded material's source declaration".into(),
                ));
            }
        }
        Ok(())
    }
}
