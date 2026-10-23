//! The sealed class (architecture §3.5; spec `privacy.md` §4). A sealed
//! material's bytes are encrypted in the content store under a fresh data key;
//! the events read from it carry their payload encrypted under the same key,
//! with the envelope (type, time, source, provenance) left readable. Equivalence
//! keys and record hashes of sealed events are keyed hashes under a naming key
//! normally derived from the data key. A replacement data key can retain the
//! existing record-naming capability without retaining old decryption keys.
//!
//! Data keys are wrapped with the class key's public half (an age X25519
//! recipient), which the daemon holds, so capture never needs the secret. The
//! operator's identity unwraps them only during an unlock session. Excision
//! destroys the data capability: the bytes and sealed fields become unreadable
//! everywhere they were copied. Naming-only capabilities cannot decrypt content.
use crate::{Error, Result};
use base64::Engine;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde_json::{Value, json};
use sinex_model::{Class, EquivalenceKey, Event, MaterialId};
use sqlx::PgConnection;

pub use age::x25519::{Identity, Recipient};
pub use hashed_blobs::DataKey;
mod secrets;
pub use secrets::{MaterialSecrets, SecretRole, unwrap_secrets, wrap_secrets};

/// A fresh data key, named by a new key reference.
pub fn new_key() -> Result<DataKey> {
    let mut key = [0u8; 32];
    getrandom::fill(&mut key).map_err(|e| Error::Invalid(format!("random key: {e}")))?;
    Ok(DataKey {
        key_ref: uuid::Uuid::now_v7().to_string(),
        key,
    })
}

/// The class key's public half, as the daemon holds it.
pub fn parse_recipient(text: &str) -> Result<age::x25519::Recipient> {
    text.trim()
        .parse()
        .map_err(|e| Error::Invalid(format!("class key recipient: {e}")))
}

/// The operator's identity, as an unlock session holds it.
pub fn parse_identity(text: &str) -> Result<age::x25519::Identity> {
    text.lines()
        .map(str::trim)
        .find(|l| l.starts_with("AGE-SECRET-KEY-"))
        .ok_or_else(|| Error::Invalid("no AGE-SECRET-KEY- line in the identity".into()))?
        .parse()
        .map_err(|e| Error::Invalid(format!("identity: {e}")))
}

/// Wrap a data key and its record-naming capability for the sealed class.
pub fn wrap(key: &DataKey, recipient: &Recipient) -> Result<Vec<u8>> {
    wrap_secrets(&MaterialSecrets::from_data_key(key), recipient)
}

/// Unwrap only a data capability. Naming-only envelopes are never data keys.
pub fn unwrap(wrapped: &[u8], identity: &Identity) -> Result<[u8; 32]> {
    unwrap_secrets(wrapped, identity, Class::Sealed)?.data_bytes()
}

fn naming_key(key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("sinex 2026 sealed naming key", key)
}

fn payload_key(key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("sinex 2026 sealed payload key", key)
}

/// Whether `payload` is a sealed payload (`{"sealed": .., "key": ..}`).
pub fn is_sealed(payload: &Value) -> bool {
    payload.as_object().is_some_and(|o| {
        o.len() == 2
            && o.get("sealed").is_some_and(Value::is_string)
            && o.get("key").is_some_and(Value::is_string)
    })
}

/// The key reference a sealed payload names.
pub fn key_ref(payload: &Value) -> Option<&str> {
    is_sealed(payload)
        .then(|| payload["key"].as_str())
        .flatten()
}

/// Seal an event read from a sealed material: encrypt its payload, key its
/// equivalence key and record hash, and set its class.
pub fn seal_event(event: &mut Event, key: &DataKey) -> Result<()> {
    seal_event_with_secrets(event, key, &MaterialSecrets::from_data_key(key))
}

/// Encrypt with the current data key while retaining the material's record identities.
pub fn seal_event_with_secrets(
    event: &mut Event,
    key: &DataKey,
    secrets: &MaterialSecrets,
) -> Result<()> {
    secrets.require_data_key(key)?;
    event.payload = seal_value(&event.payload, key)?;
    let naming = secrets.record_naming();
    if let Some(k) = &event.equivalence_key {
        event.equivalence_key = Some(EquivalenceKey(format!(
            "sealed:{}",
            blake3::keyed_hash(naming, k.0.as_bytes()).to_hex()
        )));
    }
    event.record_hash = *blake3::keyed_hash(naming, &event.record_hash).as_bytes();
    event.class = Class::Sealed;
    Ok(())
}

/// Seal one persisted interpretation value, including parser carry or failure details.
pub fn seal_value(value: &Value, key: &DataKey) -> Result<Value> {
    let plain = serde_json::to_vec(value).map_err(|e| Error::Invalid(e.to_string()))?;
    let mut nonce = [0u8; 24];
    getrandom::fill(&mut nonce).map_err(|e| Error::Invalid(format!("random nonce: {e}")))?;
    let cipher = XChaCha20Poly1305::new((&payload_key(&key.key)).into());
    let sealed = cipher
        .encrypt(&XNonce::from(nonce), plain.as_slice())
        .map_err(|_| Error::Invalid("payload encryption failed".into()))?;
    let mut body = nonce.to_vec();
    body.extend(sealed);
    Ok(json!({
        "sealed": base64::engine::general_purpose::STANDARD.encode(body),
        "key": key.key_ref,
    }))
}

/// A sealed value carried by a text column, such as a failure reason.
pub fn sealed_text(text: &str) -> Option<Value> {
    let value: Value = serde_json::from_str(text).ok()?;
    is_sealed(&value).then_some(value)
}

/// The plaintext of a sealed payload.
pub fn unseal_payload(payload: &Value, key: &[u8; 32]) -> Result<Value> {
    let body = payload["sealed"]
        .as_str()
        .ok_or_else(|| Error::Invalid("not a sealed payload".into()))?;
    let body = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| Error::Invalid(format!("sealed payload: {e}")))?;
    if body.len() < 24 {
        return Err(Error::Invalid("sealed payload is too short".into()));
    }
    let (nonce, ct) = body.split_at(24);
    let nonce: [u8; 24] = nonce.try_into().expect("split at 24");
    let cipher = XChaCha20Poly1305::new((&payload_key(key)).into());
    let plain = cipher
        .decrypt(&XNonce::from(nonce), ct)
        .map_err(|_| Error::Invalid("sealed payload does not decrypt under its key".into()))?;
    serde_json::from_slice(&plain).map_err(|e| Error::Invalid(format!("sealed payload: {e}")))
}

/// Record a material's wrapped data key. Written before the material row, so
/// a sealed material never exists without its key.
pub async fn store_key(
    conn: &mut PgConnection,
    material: MaterialId,
    key_ref: &str,
    wrapped: &[u8],
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO privacy.material_keys (material_id, key_ref, class, wrapped_key)
         VALUES ($1, $2, 'sealed', $3) ON CONFLICT (material_id) DO NOTHING",
        material.0,
        key_ref,
        wrapped
    )
    .execute(&mut *conn)
    .await?;
    let actual = sqlx::query_scalar!(
        "SELECT key_ref FROM privacy.material_keys WHERE material_id=$1",
        material.0
    )
    .fetch_one(conn)
    .await?;
    if actual != key_ref {
        return Err(Error::Invalid(
            "material is already bound to a different sealed key".into(),
        ));
    }
    Ok(())
}

/// A wrapped key by its reference; `None` once excised.
pub async fn wrapped_key(conn: &mut PgConnection, key_ref: &str) -> Result<Option<Vec<u8>>> {
    Ok(sqlx::query_scalar!(
        "SELECT wrapped_key FROM privacy.material_keys WHERE key_ref = $1",
        key_ref
    )
    .fetch_optional(&mut *conn)
    .await?)
}

#[cfg(test)]
mod tests;
