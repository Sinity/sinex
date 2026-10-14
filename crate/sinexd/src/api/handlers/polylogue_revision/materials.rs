//! Immutable native custody for prepared Polylogue publication artifacts.
//!
//! These locators transport bytes into CAS. Unlike `sources.stage` file
//! authority, a temporary locator is allowed because publication always pins
//! and verifies native custody before returning. Replay never reads that locator.

use std::path::{Path, PathBuf};

use serde_json::{Map, Value as JsonValue, json};
use sha2::{Digest, Sha256};
use sinex_db::repositories::SourceMaterial;
use sinex_db::{DbPoolExt, SourceMaterialRecord};
use sinex_primitives::domain::{
    MaterialStatus, SourceIdentifier, SourceMaterialFormat, SourceMaterialTimingInfoType,
};
use sinex_primitives::privacy::MaterialCaptureClass;
use sinex_primitives::rpc::polylogue::PolylogueArtifactInput;
use sinex_primitives::rpc::sources::{
    SourceAdmissionDecision, SourceMaterialMetadataContract, SourceMaterialStatistics,
    SourceOrigin, SourcePolicyEvidence,
};
use sinex_primitives::{Result, SinexError};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::api::service_container::ServiceContainer;
use crate::runtime::content_store::VerifiedPath;

const ARTIFACT_LOGICAL_SOURCE: &str = "integration.polylogue.publication-artifact";

/// Stage or recover custody under the material ID already durably prepared by
/// the controller. A retry validates the native registered bytes and never
/// requires the original transport locator to remain available.
pub(super) async fn stage_artifact(
    services: &ServiceContainer,
    id: Uuid,
    input: &PolylogueArtifactInput,
    metadata: JsonValue,
) -> Result<SourceMaterialRecord> {
    validate_descriptor(input)?;
    services
        .content
        .content_store()
        .ensure_ingest_size_allowed(input.size_bytes)?;
    let expected = expected_metadata(input, metadata)?;
    let repository = services.pool().source_materials();
    if let Some(record) = repository.get_by_id(id.into()).await? {
        return verify_registered_artifact(services, record, input, &expected).await;
    }

    let path = transport_path(&input.file_path)?;
    verify_bytes(path.as_path().as_std_path(), input).await?;
    let capture_class = MaterialCaptureClass::AllowedPlaintext;
    // Like sources.stage, this authenticated manual ingress has no binding
    // capture policy to consult and records the default allowed class.
    if capture_class.is_rejected() || !capture_class.allows_byte_storage() {
        return Err(SinexError::validation(
            "Material capture policy rejects byte custody",
        ));
    }

    // The native manager retains its configured/default blob-size limit. Its
    // existing BLAKE3 ingestion hash reads the whole bounded file into memory;
    // the SHA-256 checks here stream and do not change that native implementation.
    let blob = services
        .content
        .content_store()
        .ingest_external_file(&path, None)
        .await?;
    if u64::try_from(blob.size_bytes).ok() != Some(input.size_bytes) {
        return Err(SinexError::validation(
            "Stored blob size contradicts artifact descriptor",
        ));
    }
    let stored_path = services
        .content
        .content_store()
        .get_blob_path(&blob.content_key())
        .await
        .map_err(|error| unavailable("Cannot resolve stored artifact CAS path", &error))?;
    verify_retained_bytes(stored_path.as_std_path(), input).await?;

    let mut contract = SourceMaterialMetadataContract::new(
        SourceMaterialFormat::infer_from_path(path.as_str()),
        SourceMaterialTimingInfoType::Intrinsic,
    );
    contract.origin = Some(SourceOrigin {
        source_uri: Some(path.as_str().to_owned()),
        ..SourceOrigin::default()
    });
    contract.statistics = Some(SourceMaterialStatistics {
        total_bytes: Some(blob.size_bytes),
        checksum_blake3: blob.checksum_blake3.clone(),
        ..SourceMaterialStatistics::default()
    });
    contract.policy = Some(SourcePolicyEvidence {
        capture_class: Some(capture_class),
        admission_decision: Some(SourceAdmissionDecision::Admitted),
        quarantine_reason: None,
    });
    // A transport path may be reused for another revision. The existing typed
    // identifier binds this registry entry to its durable prepared material ID;
    // the actual locator is origin metadata, never the registry's natural key.
    let identifier = SourceIdentifier::new(ARTIFACT_LOGICAL_SOURCE, Some(id.into())).to_wire();
    let material = SourceMaterial::file(identifier)
        .with_blob_id(blob.id)
        .with_metadata_contract(&contract)
        .with_metadata(JsonValue::Object(expected.clone()));
    let record = repository.register_external_material(id, material).await?;
    finalize_verified_size(services, record, input.size_bytes, &expected).await
}

/// Resolve the completed registry's blob through the native manager. The
/// registered source identifier and caller's source URI are never read here.
pub(super) async fn artifact_path(
    services: &ServiceContainer,
    record: &SourceMaterialRecord,
) -> Result<PathBuf> {
    native_artifact(services, record)
        .await
        .map(|(path, _)| path)
}

async fn native_artifact(
    services: &ServiceContainer,
    record: &SourceMaterialRecord,
) -> Result<(PathBuf, i64)> {
    if record.status != MaterialStatus::Completed {
        return Err(SinexError::io("Artifact custody is not completed"));
    }
    let blob_id = record
        .optional_blob_id
        .ok_or_else(|| SinexError::io("Registered artifact has no native blob custody"))?;
    let blob = services
        .pool()
        .blobs()
        .get_by_id(blob_id.into())
        .await?
        .ok_or_else(|| SinexError::io("Registered artifact blob is unavailable"))?;
    services
        .content
        .content_store()
        .get_blob_path(&blob.content_key())
        .await
        .map(|path| (path.into_std_path_buf(), blob.size_bytes))
        .map_err(|error| unavailable("Cannot resolve registered artifact CAS path", &error))
}

async fn verify_registered_artifact(
    services: &ServiceContainer,
    record: SourceMaterialRecord,
    input: &PolylogueArtifactInput,
    expected: &Map<String, JsonValue>,
) -> Result<SourceMaterialRecord> {
    verify_metadata(&record.metadata, expected)?;
    if SourceIdentifier::from_wire(&record.source_identifier)?
        != SourceIdentifier::new(ARTIFACT_LOGICAL_SOURCE, Some(record.id.into()))
    {
        return Err(SinexError::validation(
            "Registered artifact identity contradicts prepared material",
        ));
    }
    if record
        .metadata
        .get("material_type")
        .and_then(JsonValue::as_str)
        != Some("file")
    {
        return Err(SinexError::validation(
            "Registered artifact is not file material",
        ));
    }
    let (path, blob_size) = native_artifact(services, &record).await?;
    if u64::try_from(blob_size).ok() != Some(input.size_bytes) {
        return Err(SinexError::validation(
            "Registered blob size contradicts artifact descriptor",
        ));
    }
    verify_retained_bytes(&path, input).await?;
    finalize_verified_size(services, record, input.size_bytes, expected).await
}

async fn finalize_verified_size(
    services: &ServiceContainer,
    mut record: SourceMaterialRecord,
    size_bytes: u64,
    expected: &Map<String, JsonValue>,
) -> Result<SourceMaterialRecord> {
    verify_metadata(&record.metadata, expected)?;
    let expected_size = i64::try_from(size_bytes)
        .map_err(|_| SinexError::validation("Artifact size exceeds registry range"))?;
    if record.total_bytes.is_some_and(|size| size != expected_size) {
        return Err(SinexError::validation(
            "Registered artifact size contradicts descriptor",
        ));
    }
    if record.total_bytes.is_none() {
        let blob_id = record
            .optional_blob_id
            .ok_or_else(|| SinexError::io("Registered artifact has no native blob custody"))?;
        // register_external_material stores the prepared ID and blob together,
        // but leaves total_bytes NULL. The existing native finalization owner
        // permits a completed row to bind its verified size. Retry resumes here
        // if registration committed before that update or its reply was lost.
        services
            .pool()
            .source_materials()
            .finalize_in_flight(
                record.id.into(),
                Some(blob_id.into()),
                None,
                None,
                Some(expected_size),
            )
            .await?;
        record = services
            .pool()
            .source_materials()
            .get_by_id(record.id.into())
            .await?
            .ok_or_else(|| {
                SinexError::io("Registered artifact disappeared during size finalization")
            })?;
        verify_metadata(&record.metadata, expected)?;
        if record.optional_blob_id != Some(blob_id) {
            return Err(SinexError::validation(
                "Artifact blob custody changed during size finalization",
            ));
        }
        if record.status != MaterialStatus::Completed || record.total_bytes != Some(expected_size) {
            return Err(SinexError::io("Artifact size finalization is not visible"));
        }
    }
    Ok(record)
}

fn validate_descriptor(input: &PolylogueArtifactInput) -> Result<()> {
    if input.sha256.len() != 64
        || !input
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(SinexError::validation(
            "Artifact SHA-256 must be lowercase hexadecimal",
        ));
    }
    i64::try_from(input.size_bytes)
        .map_err(|_| SinexError::validation("Artifact size exceeds registry range"))?;
    Ok(())
}

fn expected_metadata(
    input: &PolylogueArtifactInput,
    metadata: JsonValue,
) -> Result<Map<String, JsonValue>> {
    let mut expected = metadata
        .as_object()
        .cloned()
        .ok_or_else(|| SinexError::validation("Artifact metadata must be an object"))?;
    for (key, value) in [
        ("polylogue_sha256", json!(input.sha256)),
        ("file_size_bytes", json!(input.size_bytes)),
    ] {
        if expected.get(key).is_some_and(|declared| declared != &value) {
            return Err(SinexError::validation(format!(
                "Artifact metadata contradicts {key}"
            )));
        }
        expected.insert(key.to_owned(), value);
    }
    Ok(expected)
}

fn verify_metadata(actual: &JsonValue, expected: &Map<String, JsonValue>) -> Result<()> {
    for (key, value) in expected {
        if actual.get(key) != Some(value) {
            return Err(SinexError::validation(format!(
                "Registered artifact metadata contradicts {key}"
            )));
        }
    }
    Ok(())
}

fn transport_path(file_path: &str) -> Result<VerifiedPath> {
    let absolute = std::path::absolute(file_path).map_err(SinexError::io)?;
    let path = absolute
        .to_str()
        .ok_or_else(|| SinexError::validation("Artifact path is not UTF-8"))?;
    VerifiedPath::parse(path)
}

// A contradiction at the caller locator refuses that input. Once native custody
// exists, missing or damaged bytes are unavailable evidence, not a new refusal.
async fn verify_retained_bytes(path: &Path, input: &PolylogueArtifactInput) -> Result<()> {
    verify_bytes(path, input).await.map_err(|error| {
        unavailable(
            "Registered artifact bytes are unavailable or damaged",
            &error,
        )
    })
}

async fn verify_bytes(path: &Path, input: &PolylogueArtifactInput) -> Result<()> {
    let metadata = tokio::fs::metadata(path).await.map_err(SinexError::io)?;
    if !metadata.is_file() || metadata.len() != input.size_bytes {
        return Err(SinexError::validation(
            "Artifact regularity or length contradicts descriptor",
        ));
    }
    let mut file = tokio::fs::File::open(path).await.map_err(SinexError::io)?;
    let opened = file.metadata().await.map_err(SinexError::io)?;
    if !opened.is_file() || opened.len() != input.size_bytes {
        return Err(SinexError::validation(
            "Opened artifact regularity or length contradicts descriptor",
        ));
    }
    verify_stream(&mut file, input).await
}

async fn verify_stream<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    input: &PolylogueArtifactInput,
) -> Result<()> {
    let limit = input
        .size_bytes
        .checked_add(1)
        .ok_or_else(|| SinexError::validation("Artifact size exceeds verification range"))?;
    let mut reader = reader.take(limit);
    let mut digest = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).await.map_err(SinexError::io)?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| SinexError::validation("Artifact byte size overflows u64"))?;
        if size > input.size_bytes {
            return Err(SinexError::validation("Artifact grew beyond declared size"));
        }
        digest.update(&buffer[..read]);
    }
    if size != input.size_bytes || format!("{:x}", digest.finalize()) != input.sha256 {
        return Err(SinexError::validation(
            "Artifact bytes contradict SHA-256 or size descriptor",
        ));
    }
    Ok(())
}

fn unavailable(message: &str, error: &SinexError) -> SinexError {
    SinexError::io(message).with_std_error(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_db::DbPoolExt;
    use xtask::sandbox::prelude::*;

    #[sinex_test(timeout = 120)]
    async fn distinct_publication_requests_reuse_transport_path_without_aliasing_custody(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let directory = tempfile::tempdir()?;
        let locator = directory.path().join("reusable-transport.json");
        let mut config = crate::api::config::GatewayConfig::load()?;
        config.database_url = ctx.database_url().to_owned();
        config.nats.url = ctx.nats_handle()?.client_url().to_owned();
        config.content_store_path = directory.path().join("cas").to_string_lossy().into_owned();
        config = config.with_nats_namespace(Some(ctx.pipeline_namespace().prefix().to_owned()));
        let services = ServiceContainer::new(&config).await?;
        let mut retained = Vec::new();
        let mut operations = Vec::new();
        for (request_id, bytes) in [
            (
                "fixture-first-publication",
                br#"{"revision":"first"}"#.as_slice(),
            ),
            (
                "fixture-second-publication",
                br#"{"revision":"second"}"#.as_slice(),
            ),
        ] {
            tokio::fs::write(&locator, bytes).await?;
            let input = PolylogueArtifactInput {
                file_path: locator.to_string_lossy().into_owned(),
                sha256: format!("{:x}", Sha256::digest(bytes)),
                size_bytes: bytes.len() as u64,
            };
            let operation = services
                .publication_coordination_pool()
                .state()
                .prepare_polylogue_publication(
                    "fixture-operator",
                    request_id,
                    &input.sha256,
                    json!({"fixture_material_id": Uuid::now_v7()}),
                )
                .await?;
            let id: Uuid = serde_json::from_value(
                operation.scope.as_ref().unwrap()["fixture_material_id"].clone(),
            )?;
            let metadata = json!({"fixture_request_id": request_id});
            let record = stage_artifact(&services, id, &input, metadata.clone()).await?;
            let identity = SourceIdentifier::from_wire(&record.source_identifier)?;
            assert_eq!(identity.material_id.map(|id| id.to_uuid()), Some(id));
            assert_ne!(record.source_identifier, input.file_path);
            let origin = SourceMaterialMetadataContract::from_metadata(&record.metadata)
                .unwrap()
                .origin
                .unwrap();
            assert_eq!(origin.source_uri.as_deref(), Some(input.file_path.as_str()));
            operations.push(operation.id);
            retained.push((id, input, metadata, bytes.to_vec()));
        }
        assert_ne!(operations[0], operations[1]);
        assert_ne!(retained[0].0, retained[1].0);
        // The locator now contains the second request's bytes. Retrying the first
        // must resolve its existing CAS material, without reading that locator.
        let first = stage_artifact(
            &services,
            retained[0].0,
            &retained[0].1,
            retained[0].2.clone(),
        )
        .await?;
        assert_eq!(
            tokio::fs::read(artifact_path(&services, &first).await?).await?,
            retained[0].3
        );
        tokio::fs::remove_file(&locator).await?;
        for (id, input, metadata, bytes) in retained {
            let record = stage_artifact(&services, id, &input, metadata).await?;
            assert_eq!(
                tokio::fs::read(artifact_path(&services, &record).await?).await?,
                bytes
            );
        }
        Ok(())
    }

    #[test]
    fn artifact_metadata_preserves_exact_source_and_normalizer_contract() {
        let input = PolylogueArtifactInput {
            file_path: "unused".into(),
            sha256: "a".repeat(64),
            size_bytes: 3,
        };
        let supplied = json!({"role":"provider_artifact", "source_ref":"synthetic:retained-version", "acquisition":{"status":"available"}, "normalizer_version":"demo-v1"});
        let expected = expected_metadata(&input, supplied.clone()).unwrap();
        for (key, value) in supplied.as_object().unwrap() {
            assert_eq!(expected.get(key), Some(value));
        }
        let actual = JsonValue::Object(expected.clone());
        verify_metadata(&actual, &expected).unwrap();
        let mut contradictory = actual;
        contradictory["source_ref"] = json!("synthetic:other-version");
        assert!(
            !verify_metadata(&contradictory, &expected)
                .unwrap_err()
                .is_retryable()
        );
        assert!(expected_metadata(&input, json!({"polylogue_sha256":"wrong"})).is_err());
    }

    #[tokio::test]
    async fn growing_artifact_read_stops_at_first_byte_beyond_declared_size() {
        let mut reader = std::io::Cursor::new(vec![b'x'; 1024]);
        let input = PolylogueArtifactInput {
            file_path: "unused".into(),
            sha256: format!("{:x}", Sha256::digest(b"xxx")),
            size_bytes: 3,
        };
        assert!(
            !verify_stream(&mut reader, &input)
                .await
                .unwrap_err()
                .is_retryable()
        );
        assert_eq!(reader.position(), 4, "must not drain the growing remainder");
    }

    #[tokio::test]
    async fn streamed_artifact_check_distinguishes_unavailability_from_contradiction() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("synthetic-artifact.ndjson");
        let bytes = b"synthetic bytes\n";
        tokio::fs::write(&path, bytes).await.unwrap();
        let input = PolylogueArtifactInput {
            file_path: path.to_string_lossy().into(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            size_bytes: bytes.len() as u64,
        };
        verify_bytes(&path, &input).await.unwrap();
        tokio::fs::write(&path, b"contradictory bytes\n")
            .await
            .unwrap();
        assert!(
            !verify_bytes(&path, &input)
                .await
                .unwrap_err()
                .is_retryable()
        );
        assert!(
            verify_retained_bytes(&path, &input)
                .await
                .unwrap_err()
                .is_retryable()
        );
        tokio::fs::remove_file(&path).await.unwrap();
        let error = verify_bytes(&path, &input).await.unwrap_err();
        assert_eq!(error.kind(), sinex_primitives::error::SinexErrorKind::Io);
        assert!(error.is_retryable());
    }
}
