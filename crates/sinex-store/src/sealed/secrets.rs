//! Wrapped capabilities are authenticated as a role before any caller receives data keys.
use super::{DataKey, Identity, Recipient, naming_key};
use crate::{Error, Result};
use age::secrecy::{ExposeSecret, SecretBox, zeroize::Zeroizing};
use sinex_model::Class;
use std::io::{Read, Write};

const MAGIC: &[u8; 8] = b"SXKEY\0\0\0";
const VERSION: u8 = 1;
const SEALED: u8 = 1;
const HEADER: usize = 11;
const MAX_BYTES: usize = HEADER + 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretRole {
    DataAndRecordNaming,
    RecordNamingOnly,
}

/// No serialization or debug representation: capabilities leave this type only
/// through a class-wrapped envelope or an explicitly checked data-key request.
#[derive(Clone)]
pub struct MaterialSecrets {
    data: Option<SecretBox<[u8; 32]>>,
    record_naming: SecretBox<[u8; 32]>,
}
impl MaterialSecrets {
    pub fn from_data_key(key: &DataKey) -> Self {
        Self {
            data: Some(SecretBox::new(Box::new(key.key))),
            record_naming: SecretBox::new(Box::new(naming_key(&key.key))),
        }
    }
    pub fn role(&self) -> SecretRole {
        if self.data.is_some() {
            SecretRole::DataAndRecordNaming
        } else {
            SecretRole::RecordNamingOnly
        }
    }
    /// Retains candidate record-hash/equivalence confirmation for class holders;
    /// it retains no capability to decrypt old blobs or interpretation payloads.
    pub fn naming_only(&self) -> Self {
        Self {
            data: None,
            record_naming: self.record_naming.clone(),
        }
    }
    /// Fresh content encryption preserves record identity through excision.
    pub fn with_data_key(&self, key: &DataKey) -> Self {
        Self {
            data: Some(SecretBox::new(Box::new(key.key))),
            record_naming: self.record_naming.clone(),
        }
    }
    pub fn data_key(&self, key_ref: String) -> Result<DataKey> {
        Ok(DataKey {
            key_ref,
            key: self.data_bytes()?,
        })
    }
    pub(super) fn data_bytes(&self) -> Result<[u8; 32]> {
        self.data
            .as_ref()
            .map(|s| *s.expose_secret())
            .ok_or_else(|| invalid("record-naming-only capability cannot decrypt content"))
    }
    pub(super) fn record_naming(&self) -> &[u8; 32] {
        self.record_naming.expose_secret()
    }
    pub(super) fn require_data_key(&self, key: &DataKey) -> Result<()> {
        if self.data_bytes()? != key.key {
            return Err(invalid("data key does not match material capabilities"));
        }
        Ok(())
    }
}
fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

pub fn wrap_secrets(secrets: &MaterialSecrets, recipient: &Recipient) -> Result<Vec<u8>> {
    let mut plain = Zeroizing::new(Vec::with_capacity(MAX_BYTES));
    plain.extend_from_slice(MAGIC);
    plain.extend_from_slice(&[
        VERSION,
        SEALED,
        match secrets.role() {
            SecretRole::DataAndRecordNaming => 1,
            SecretRole::RecordNamingOnly => 2,
        },
    ]);
    if let Some(data) = &secrets.data {
        plain.extend_from_slice(data.expose_secret());
    }
    plain.extend_from_slice(secrets.record_naming());
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(recipient as &dyn age::Recipient))
            .map_err(|e| Error::Invalid(format!("wrap: {e}")))?;
    let mut out = Vec::new();
    let mut writer = encryptor
        .wrap_output(&mut out)
        .map_err(|e| Error::Invalid(format!("wrap: {e}")))?;
    writer
        .write_all(&plain)
        .and_then(|()| writer.finish().map(drop))
        .map_err(|e| Error::Invalid(format!("wrap: {e}")))?;
    Ok(out)
}

pub fn unwrap_secrets(
    wrapped: &[u8],
    identity: &Identity,
    class: Class,
) -> Result<MaterialSecrets> {
    if class != Class::Sealed {
        return Err(invalid("wrapped capability belongs to the sealed class"));
    }
    let decryptor =
        age::Decryptor::new(wrapped).map_err(|e| Error::Invalid(format!("unwrap: {e}")))?;
    let reader = decryptor
        .decrypt(std::iter::once(identity as &dyn age::Identity))
        .map_err(|e| Error::Invalid(format!("unwrap: {e}")))?;
    let mut plain = Zeroizing::new(Vec::new());
    reader
        .take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut plain)
        .map_err(|e| Error::Invalid(format!("unwrap: {e}")))?;
    decode(&plain)
}
fn decode(plain: &[u8]) -> Result<MaterialSecrets> {
    // The predecessor stored exactly one raw data key inside age.
    if plain.len() == 32 {
        let data: [u8; 32] = plain.try_into().expect("checked length");
        return Ok(MaterialSecrets {
            data: Some(SecretBox::new(Box::new(data))),
            record_naming: SecretBox::new(Box::new(naming_key(&data))),
        });
    }
    if plain.len() < HEADER || &plain[..8] != MAGIC || plain[8] != VERSION || plain[9] != SEALED {
        return Err(invalid(
            "unsupported wrapped capability format, version or class",
        ));
    }
    let (data, naming) = match (plain[10], plain.len()) {
        (1, MAX_BYTES) => (
            Some(SecretBox::new(Box::new(
                plain[HEADER..HEADER + 32]
                    .try_into()
                    .expect("checked length"),
            ))),
            &plain[HEADER + 32..],
        ),
        (2, n) if n == HEADER + 32 => (None, &plain[HEADER..]),
        _ => return Err(invalid("invalid wrapped capability role or length")),
    };
    Ok(MaterialSecrets {
        data,
        record_naming: SecretBox::new(Box::new(naming.try_into().expect("checked length"))),
    })
}

#[cfg(test)]
mod tests;
