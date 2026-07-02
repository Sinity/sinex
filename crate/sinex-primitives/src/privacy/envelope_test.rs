use super::*;
use xtask::sandbox::sinex_test;

fn test_key() -> [u8; 32] {
    [0x42; 32]
}

#[sinex_test]
async fn encrypt_decrypt_roundtrip() -> ::xtask::sandbox::TestResult<()> {
    let key = test_key();
    let plaintext = ["ghp_", "abcdefghij1234567890ABCDEFGHIJKLMN"].concat();
    let token = encrypt_token(&plaintext, &key).unwrap();
    assert!(token.starts_with('\u{231c}'));
    assert!(token.ends_with('\u{231d}'));
    assert!(token.contains("enc:v1:"));
    let decrypted = decrypt_token(&token, &key).unwrap();
    assert_eq!(decrypted, plaintext);
    Ok(())
}

#[sinex_test]
async fn decrypt_wrong_key_fails() -> ::xtask::sandbox::TestResult<()> {
    let key = test_key();
    let token = encrypt_token("secret", &key).unwrap();
    let wrong_key = [0x99; 32];
    assert!(decrypt_token(&token, &wrong_key).is_err());
    Ok(())
}

#[sinex_test]
async fn hash_is_deterministic() -> ::xtask::sandbox::TestResult<()> {
    let key = test_key();
    let h1 = hash_token("hello@example.com", &key);
    let h2 = hash_token("hello@example.com", &key);
    assert_eq!(h1, h2);
    assert!(h1.starts_with('\u{231c}'));
    assert!(h1.contains("hash:"));
    Ok(())
}

#[sinex_test]
async fn hash_different_inputs_differ() -> ::xtask::sandbox::TestResult<()> {
    let key = test_key();
    let h1 = hash_token("alice@example.com", &key);
    let h2 = hash_token("bob@example.com", &key);
    assert_ne!(h1, h2);
    Ok(())
}

#[sinex_test]
async fn decrypt_all_multiple_tokens() -> ::xtask::sandbox::TestResult<()> {
    let key = test_key();
    let t1 = encrypt_token("secret1", &key).unwrap();
    let t2 = encrypt_token("secret2", &key).unwrap();
    let input = format!("before {t1} middle {t2} after");
    let decrypted = decrypt_all(&input, &key).unwrap();
    assert_eq!(decrypted, "before secret1 middle secret2 after");
    Ok(())
}
