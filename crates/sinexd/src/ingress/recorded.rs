//! Retained own material evidence and an independent current header check.
use super::*;
use sinex_protocol::{
    RecordedContinuation, RecordedContinuationRequest, RecordedHeaders, RecordedMaterialBinding,
};

impl Requests {
    pub(super) async fn recorded_continuation(
        &self,
        conn: &mut PgConnection,
        producer: &str,
        claim: &RecordedContinuationRequest,
    ) -> Result<RecordedContinuation, String> {
        let unknown = |reason: &str| RecordedContinuation::Unknown {
            request: claim.clone(),
            reason: reason.into(),
        };
        if claim.frame.is_empty() || claim.frame.contains('\0') {
            return Ok(unknown("material_receipt_identity_unknown"));
        }
        let Some(saved) = commit_log::recorded_material(conn, producer, &claim.frame)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(unknown("material_receipt_not_recorded"));
        };
        if saved.receipt.number <= 0
            || saved.receipt.refusal.is_some()
            || saved
                .receipt
                .outcome
                .get("refusal_kind")
                .is_some_and(|kind| !kind.is_null())
        {
            return Ok(unknown("material_receipt_not_positive"));
        }
        let content_hash = saved
            .receipt
            .outcome
            .get("frame_content_hash")
            .and_then(Value::as_str);
        if content_hash.is_none_or(|hash| {
            hash.strip_prefix("blake3:")
                .is_none_or(|hex| blake3::Hash::from_hex(hex).is_err())
        }) {
            return Ok(unknown("material_receipt_identity_unknown"));
        }
        if content_hash != Some(claim.frame_content_hash.as_str())
            || saved.receipt.material_id != Some(claim.material.0)
            || saved.blob.as_deref() != Some(claim.blob.as_str())
            || saved.length.and_then(|length| u64::try_from(length).ok()) != Some(claim.length)
        {
            return Ok(unknown("material_receipt_binding_mismatch"));
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
            return Ok(unknown("material_source_authority_unknown"));
        };
        if name.is_empty()
            || family.is_empty()
            || prefixes.is_empty()
            || blake3::Hash::from_hex(&fingerprint).is_err()
        {
            return Ok(unknown("material_source_authority_unknown"));
        }
        let binding = RecordedMaterialBinding {
            receipt: acknowledgment(saved.receipt),
            material: claim.material,
            blob: claim.blob.clone(),
            length: claim.length,
            source: sinex_model::SourceRef { name, version },
            source_fingerprint: fingerprint,
            family,
            write_kind_prefixes: prefixes,
            producer_host: saved.producer_host,
        };
        let headers = if claim.uses.is_empty() {
            RecordedHeaders::NotRequested
        } else {
            match self
                .continuation_headers(conn, producer, &binding, &claim.uses)
                .await
            {
                Ok(()) => RecordedHeaders::PermittedHeaders,
                Err(Check::Unavailable(error)) => return Err(error),
                Err(reason) => RecordedHeaders::NotPermitted {
                    reason: reason.to_string(),
                },
            }
        };
        Ok(RecordedContinuation::Retained {
            request: claim.clone(),
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
        let native =
            authority::native_declaration(conn, &self.read_grants, producer, &binding.source)
                .await?;
        if native.family != binding.family || native.fingerprint() != binding.source_fingerprint {
            return Err(Check::Configuration(
                "retained source differs from the current owner declaration".into(),
            ));
        }
        for header in uses {
            authority::event(&self.read_grants, producer, &header.kind.name, header.class)
                .map_err(Check::Configuration)?;
            if header.kind.name.contains('\0') {
                return Err(Check::Configuration("header kind name contains NUL".into()));
            }
            header
                .kind
                .database_version()
                .map_err(Check::Configuration)?;
        }
        let refs = uses
            .iter()
            .map(|header| header.kind.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let kinds = sinex_schema::kinds::load(conn, &refs.into_iter().collect::<Vec<_>>())
            .await
            .map_err(|error| Check::Unavailable(error.to_string()))?;
        for header in uses {
            if !authority::matches(&binding.write_kind_prefixes, &header.kind.name) {
                return Err(Check::Configuration(
                    "header is outside retained write prefixes".into(),
                ));
            }
            let registered = kinds.get(&header.kind).ok_or_else(|| {
                Check::Configuration("header kind version is not registered".into())
            })?;
            let hash = registered.definition_hash();
            if hash != header.kind_definition_hash
                || !native.allows(&header.kind, &hash)
                || (registered.declaring_producer != "sinex"
                    && registered.declaring_producer != binding.family)
            {
                return Err(Check::Configuration(
                    "header differs from the exact retained source contract".into(),
                ));
            }
        }
        Ok(())
    }
}
