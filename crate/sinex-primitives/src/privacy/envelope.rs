//! Token formats for encrypted and hashed privacy output.
//!
//! Encrypted tokens:  `⌜enc:v1:<base64url(nonce ‖ ciphertext ‖ tag)>⌝`
//! Hashed tokens:     `⌜hash:<hex[0..32]>⌝`

use super::PrivacyError;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::XChaCha20Poly1305;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};

/// Token delimiters — visually distinctive Unicode corner brackets.
const TOKEN_OPEN: &str = "\u{231c}"; // ⌜
const TOKEN_CLOSE: &str = "\u{231d}"; // ⌝

/// Encrypt plaintext with XChaCha20-Poly1305 and wrap in envelope token.
pub fn encrypt_token(plaintext: &str, key: &[u8; 32]) -> Result<String, PrivacyError> {
    let cipher = XChaCha20Poly1305::new_from_slice(key)
        .map_err(|e| PrivacyError::InvalidKey(e.to_string()))?;
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|e| PrivacyError::EncryptionFailed(e.to_string()))?;

    // nonce (24 bytes) ‖ ciphertext+tag
    let mut blob = Vec::with_capacity(24 + ciphertext.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&ciphertext);

    Ok(format!(
        "{TOKEN_OPEN}enc:v1:{}{TOKEN_CLOSE}",
        URL_SAFE_NO_PAD.encode(&blob)
    ))
}

/// Decrypt a `⌜enc:v1:...⌝` token back to plaintext.
pub fn decrypt_token(token: &str, key: &[u8; 32]) -> Result<String, PrivacyError> {
    let inner = strip_envelope(token, "enc:v1:")?;
    let blob = URL_SAFE_NO_PAD
        .decode(inner)
        .map_err(|e| PrivacyError::DecryptionFailed(format!("base64: {e}")))?;
    if blob.len() < 24 {
        return Err(PrivacyError::DecryptionFailed("too short".into()));
    }
    let (nonce_bytes, ciphertext) = blob.split_at(24);
    let nonce_arr: [u8; 24] = nonce_bytes
        .try_into()
        .map_err(|_| PrivacyError::DecryptionFailed("invalid nonce length".into()))?;
    let cipher = XChaCha20Poly1305::new_from_slice(key)
        .map_err(|e| PrivacyError::InvalidKey(e.to_string()))?;
    let plaintext = cipher
        .decrypt(&nonce_arr.into(), ciphertext)
        .map_err(|e| PrivacyError::DecryptionFailed(e.to_string()))?;
    String::from_utf8(plaintext)
        .map_err(|e| PrivacyError::DecryptionFailed(format!("invalid utf-8: {e}")))
}

/// Produce a keyed BLAKE3 MAC hash token.
pub fn hash_token(input: &str, key: &[u8; 32]) -> String {
    let mac = blake3::keyed_hash(key, input.as_bytes());
    // Truncate to 128 bits (32 hex chars) for readability
    let hex = mac.to_hex();
    format!("{TOKEN_OPEN}hash:{}{TOKEN_CLOSE}", &hex[..32])
}

/// Return spans of encrypted tokens whose envelope is valid and authentic for
/// `key`. Without the key, no token is trusted: a shape-valid base64 payload
/// is still forgeable user content and must not disable redaction.
pub(crate) fn find_encrypted_token_spans(
    input: &str,
    key: Option<&[u8; 32]>,
) -> Vec<(usize, usize)> {
    let Some(key) = key else {
        return Vec::new();
    };
    let open_prefix = format!("{TOKEN_OPEN}enc:v1:");
    let mut search_from = 0;
    let mut spans = Vec::new();
    while let Some(rel) = input[search_from..].find(open_prefix.as_str()) {
        let token_start = search_from + rel;
        let payload_start = search_from + rel + open_prefix.len();
        let Some(close_rel) = input[payload_start..].find(TOKEN_CLOSE) else {
            // An unterminated/forged marker is ordinary user content. Keep
            // scanning after its prefix so a later authenticated token is
            // still protected rather than letting the malformed candidate
            // hide it from the redaction engine.
            search_from = payload_start;
            continue;
        };
        let token_end = payload_start + close_rel + TOKEN_CLOSE.len();
        let token = &input[token_start..token_end];
        if decrypt_token(token, key).is_ok() {
            spans.push((token_start, token_end));
            search_from = token_end;
        } else {
            // Do not skip possible nested or adjacent candidates after an
            // unauthenticated marker. Only an authenticated envelope owns
            // its span and may advance the scan past its closing delimiter.
            search_from = token_start + TOKEN_OPEN.len();
        }
    }
    spans
}

/// Find and decrypt all `⌜enc:v1:...⌝` tokens in a string.
pub fn decrypt_all(input: &str, key: &[u8; 32]) -> Result<String, PrivacyError> {
    let open = format!("{TOKEN_OPEN}enc:v1:");
    let mut result = input.to_string();
    while let Some(start) = result.find(&open) {
        let rest = &result[start + open.len()..];
        let end_offset = rest
            .find(TOKEN_CLOSE)
            .ok_or_else(|| PrivacyError::InvalidToken("unterminated token".into()))?;
        let full_token = &result[start..start + open.len() + end_offset + TOKEN_CLOSE.len()];
        let plaintext = decrypt_token(full_token, key)?;
        result = format!(
            "{}{plaintext}{}",
            &result[..start],
            &result[start + full_token.len()..]
        );
    }
    Ok(result)
}

/// Strip envelope delimiters and prefix, returning the inner payload.
fn strip_envelope<'a>(token: &'a str, prefix: &str) -> Result<&'a str, PrivacyError> {
    let stripped = token
        .strip_prefix(TOKEN_OPEN)
        .and_then(|s| s.strip_suffix(TOKEN_CLOSE))
        .ok_or_else(|| PrivacyError::InvalidToken("missing delimiters".into()))?;
    stripped
        .strip_prefix(prefix)
        .ok_or_else(|| PrivacyError::InvalidToken(format!("expected prefix '{prefix}'")))
}

#[cfg(test)]
#[path = "envelope_test.rs"]
mod tests;
