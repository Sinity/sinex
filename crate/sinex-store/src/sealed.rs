//! The sealed class (architecture §3.5; spec `privacy.md` §4). A sealed
//! material's bytes are encrypted in the content store under a fresh data key;
//! the events read from it carry their payload encrypted under the same key,
//! with the envelope (type, time, source, provenance) left readable. Equivalence
//! keys and record hashes of sealed events are keyed hashes under a naming key
//! derived from the data key, so equal values still deduplicate without
//! revealing them.
//!
//! Data keys are wrapped with the class key's public half (an age X25519
//! recipient), which the daemon holds, so capture never needs the secret. The
//! operator's identity unwraps them only during an unlock session. Excision
//! destroys the wrapped key: the bytes and sealed fields become unreadable
//! everywhere they were copied.
use crate::{Error, Result};
use base64::Engine;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde_json::{Value, json};
use sinex_model::{Class, EquivalenceKey, Event, MaterialId};
use sqlx::PgConnection;
use std::io::{Read, Write};

pub use age::x25519::{Identity, Recipient};
pub use hashed_blobs::DataKey;

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

/// Wrap a data key for storage: age-encrypted to the class key.
pub fn wrap(key: &DataKey, recipient: &age::x25519::Recipient) -> Result<Vec<u8>> {
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(recipient as &dyn age::Recipient))
            .map_err(|e| Error::Invalid(format!("wrap: {e}")))?;
    let mut out = Vec::new();
    let mut writer = encryptor
        .wrap_output(&mut out)
        .map_err(|e| Error::Invalid(format!("wrap: {e}")))?;
    writer
        .write_all(&key.key)
        .and_then(|()| writer.finish().map(drop))
        .map_err(|e| Error::Invalid(format!("wrap: {e}")))?;
    Ok(out)
}

/// Unwrap a stored data key with the operator's identity.
pub fn unwrap(wrapped: &[u8], identity: &age::x25519::Identity) -> Result<[u8; 32]> {
    let decryptor =
        age::Decryptor::new(wrapped).map_err(|e| Error::Invalid(format!("unwrap: {e}")))?;
    let mut reader = decryptor
        .decrypt(std::iter::once(identity as &dyn age::Identity))
        .map_err(|e| Error::Invalid(format!("unwrap: {e}")))?;
    let mut key = Vec::new();
    reader
        .read_to_end(&mut key)
        .map_err(|e| Error::Invalid(format!("unwrap: {e}")))?;
    key.try_into()
        .map_err(|_| Error::Invalid("unwrapped key is not 32 bytes".into()))
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
    let plain = serde_json::to_vec(&event.payload).map_err(|e| Error::Invalid(e.to_string()))?;
    let mut nonce = [0u8; 24];
    getrandom::fill(&mut nonce).map_err(|e| Error::Invalid(format!("random nonce: {e}")))?;
    let cipher = XChaCha20Poly1305::new((&payload_key(&key.key)).into());
    let sealed = cipher
        .encrypt(&XNonce::from(nonce), plain.as_slice())
        .map_err(|_| Error::Invalid("payload encryption failed".into()))?;
    let mut body = nonce.to_vec();
    body.extend(sealed);
    event.payload = json!({
        "sealed": base64::engine::general_purpose::STANDARD.encode(body),
        "key": key.key_ref,
    });
    let naming = naming_key(&key.key);
    if let Some(k) = &event.equivalence_key {
        event.equivalence_key = Some(EquivalenceKey(format!(
            "sealed:{}",
            blake3::keyed_hash(&naming, k.0.as_bytes()).to_hex()
        )));
    }
    event.record_hash = *blake3::keyed_hash(&naming, &event.record_hash).as_bytes();
    event.class = Class::Sealed;
    Ok(())
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
mod tests {
    use super::*;
    use sinex_model::{Origin, ReadingOrder, RecordLocation, Timestamp, kinds::CommandImported};

    #[test]
    fn wrap_unwrap_and_seal_round_trip() {
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public();
        let key = new_key().unwrap();
        let wrapped = wrap(&key, &recipient).unwrap();
        assert_eq!(unwrap(&wrapped, &identity).unwrap(), key.key);
        let other = age::x25519::Identity::generate();
        assert!(
            unwrap(&wrapped, &other).is_err(),
            "a wrong identity is refused"
        );

        let mut e = Event::build(
            &CommandImported {
                command: "gpg --decrypt diary".into(),
                hostname: "h".into(),
                started_at: Timestamp::from_unix_secs(1).unwrap(),
                header_elapsed: 0,
            },
            "s",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                ordinal: 0,
            },
            ReadingOrder(0),
            [1; 32],
            Class::Open,
        )
        .unwrap();
        let plain = e.payload.clone();
        seal_event(&mut e, &key).unwrap();
        assert!(is_sealed(&e.payload));
        assert!(!e.payload.to_string().contains("diary"));
        assert_eq!(e.class, Class::Sealed);
        assert_eq!(key_ref(&e.payload), Some(key.key_ref.as_str()));
        assert_eq!(unseal_payload(&e.payload, &key.key).unwrap(), plain);
        assert!(unseal_payload(&e.payload, &[0; 32]).is_err());
    }

    #[test]
    fn parse_identity_from_a_key_file() {
        let identity = age::x25519::Identity::generate();
        use age::secrecy::ExposeSecret;
        let file = format!(
            "# created: now\n# public key: {}\n{}\n",
            identity.to_public(),
            identity.to_string().expose_secret()
        );
        let parsed = parse_identity(&file).unwrap();
        assert_eq!(
            parsed.to_public().to_string(),
            identity.to_public().to_string()
        );
        assert!(parse_recipient(&identity.to_public().to_string()).is_ok());
    }
}
