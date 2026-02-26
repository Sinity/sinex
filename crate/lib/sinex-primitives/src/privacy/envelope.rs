//! Token formats for encrypted and hashed privacy output.
//!
//! Encrypted tokens:  `⌜enc:v1:<base64url(nonce ‖ ciphertext ‖ tag)>⌝`
//! Hashed tokens:     `⌜hash:<hex[0..32]>⌝`

use super::PrivacyError;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::XChaCha20Poly1305;

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

/// Check if a string contains one or more encrypted tokens.
pub fn contains_encrypted_token(input: &str) -> bool {
    input.contains("\u{231c}enc:v1:")
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
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0x42; 32]
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = test_key();
        let plaintext = "ghp_abcdefghij1234567890ABCDEFGHIJKLMN";
        let token = encrypt_token(plaintext, &key).unwrap();
        assert!(token.starts_with('\u{231c}'));
        assert!(token.ends_with('\u{231d}'));
        assert!(token.contains("enc:v1:"));
        let decrypted = decrypt_token(&token, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn decrypt_wrong_key_fails() {
        let key = test_key();
        let token = encrypt_token("secret", &key).unwrap();
        let wrong_key = [0x99; 32];
        assert!(decrypt_token(&token, &wrong_key).is_err());
    }

    #[test]
    fn hash_is_deterministic() {
        let key = test_key();
        let h1 = hash_token("hello@example.com", &key);
        let h2 = hash_token("hello@example.com", &key);
        assert_eq!(h1, h2);
        assert!(h1.starts_with('\u{231c}'));
        assert!(h1.contains("hash:"));
    }

    #[test]
    fn hash_different_inputs_differ() {
        let key = test_key();
        let h1 = hash_token("alice@example.com", &key);
        let h2 = hash_token("bob@example.com", &key);
        assert_ne!(h1, h2);
    }

    #[test]
    fn decrypt_all_multiple_tokens() {
        let key = test_key();
        let t1 = encrypt_token("secret1", &key).unwrap();
        let t2 = encrypt_token("secret2", &key).unwrap();
        let input = format!("before {t1} middle {t2} after");
        let decrypted = decrypt_all(&input, &key).unwrap();
        assert_eq!(decrypted, "before secret1 middle secret2 after");
    }
}
