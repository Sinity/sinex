//! Unicode security testing
//!
//! This module tests for various unicode-based security vulnerabilities including:
//! - Homograph attacks (visual spoofing)
//! - Unicode normalization attacks
//! - Direction override exploits
//! - Zero-width character injection
//! - Encoding-based attacks

use sinex_db::sanitization::EventSanitizer;
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use unicode_normalization::UnicodeNormalization;

// =============================================================================
// Unicode Homograph Attack Tests
// =============================================================================

#[sinex_test]
async fn test_unicode_homograph_attacks(ctx: TestContext) -> anyhow::Result<()> {
    println!("Testing unicode homograph attacks...");

    // Collection of visually similar characters that could be used in attacks
    let homograph_pairs = vec![
        // Latin vs Cyrillic
        ("admin", "аdmin", "Cyrillic 'а' looks like Latin 'a'"),
        ("test", "tеst", "Cyrillic 'е' looks like Latin 'e'"),
        ("root", "rооt", "Cyrillic 'о' looks like Latin 'o'"),
        // Latin vs Greek
        ("Alpha", "Αlpha", "Greek 'Α' looks like Latin 'A'"),
        ("Beta", "Βeta", "Greek 'Β' looks like Latin 'B'"),
        // Other confusables
        (
            "file.exe",
            "fiⅼe.exe",
            "Small Roman numeral 'ⅼ' looks like 'l'",
        ),
        (
            "google.com",
            "goog1e.com",
            "Number '1' looks like letter 'l'",
        ),
        (
            "microsoft",
            "rniсrosoft",
            "'rn' looks like 'm', Cyrillic 'с'",
        ),
        // Mixed scripts
        ("PayPal", "РayРal", "Cyrillic 'Р' looks like Latin 'P'"),
        ("Amazon", "Аmazon", "Cyrillic 'А' looks like Latin 'A'"),
    ];

    let pool = ctx.pool();

    for (legitimate, attack, description) in homograph_pairs {
        println!("\nTesting: {} vs {} - {}", legitimate, attack, description);

        // Create event with homograph attack
        let event = EventBuilder::new()
            .source("unicode_security_test")
            .event_type("homograph.test")
            .payload(json!({
                "username": attack,
                "legitimate": legitimate,
                "description": description
            }))
            .build();

        // Insert event
        let insert_result = insert_event(pool, &event).await?;

        // Query back and check if it was normalized or flagged
        let retrieved = sqlx::query!(
            r#"
            SELECT payload,
                   payload->>'username' as username
            FROM core.events
            WHERE event_id::uuid = $1::uuid
            "#,
            event.id.to_uuid()
        )
        .fetch_one(pool)
        .await?;

        let stored_username = retrieved.username.unwrap_or_default();

        // Check if the system detected the homograph
        if stored_username == attack {
            println!("  ⚠️  Homograph stored as-is: {}", attack);

            // Check if we can detect it programmatically
            let is_mixed_script = contains_mixed_scripts(&attack);
            let has_confusables = contains_confusable_chars(&attack);

            println!("  Mixed scripts: {}", is_mixed_script);
            println!("  Has confusables: {}", has_confusables);
        } else {
            println!("  ✓ Homograph was normalized or rejected");
        }
    }

    Ok(())
}

// =============================================================================
// Unicode Normalization Attack Tests
// =============================================================================

#[sinex_test]
async fn test_unicode_normalization_attacks(ctx: TestContext) -> anyhow::Result<()> {
    println!("Testing unicode normalization attacks...");

    // Different unicode normalization forms can represent the same visual string
    let normalization_tests = vec![
        // NFD vs NFC
        (
            "café",
            "cafe\u{0301}",
            "NFC vs NFD (combining acute accent)",
        ),
        ("naïve", "nai\u{0308}ve", "NFC vs NFD (combining diaeresis)"),
        // Multiple combining characters
        ("é", "e\u{0301}", "Precomposed vs decomposed"),
        ("ñ", "n\u{0303}", "Precomposed vs decomposed tilde"),
        // Canonical equivalence
        ("Ω", "Ω", "Ohm sign vs Greek capital omega"),
        ("ﬁ", "fi", "Ligature vs separate characters"),
        // Compatibility equivalence
        ("①", "1", "Circled number vs regular number"),
        ("½", "1/2", "Fraction vs regular characters"),
    ];

    let pool = ctx.pool();

    for (normalized, variant, description) in normalization_tests {
        println!("\nTesting: {} - {}", description, variant);

        // Test all normalization forms
        let nfc = variant.nfc().collect::<String>();
        let nfd = variant.nfd().collect::<String>();
        let nfkc = variant.nfkc().collect::<String>();
        let nfkd = variant.nfkd().collect::<String>();

        println!("  Original: {:?} (len={})", variant, variant.len());
        println!("  NFC: {:?} (len={})", nfc, nfc.len());
        println!("  NFD: {:?} (len={})", nfd, nfd.len());
        println!("  NFKC: {:?} (len={})", nfkc, nfkc.len());
        println!("  NFKD: {:?} (len={})", nfkd, nfkd.len());

        // Create events with different normalizations
        for (form_name, form_value) in [("NFC", nfc), ("NFD", nfd), ("NFKC", nfkc), ("NFKD", nfkd)]
        {
            let event = EventBuilder::new()
                .source("unicode_normalization_test")
                .event_type("normalization.test")
                .payload(json!({
                    "original": variant,
                    "normalized": normalized,
                    "form": form_name,
                    "value": form_value,
                    "description": description
                }))
                .build();

            insert_event(pool, &event).await?;
        }
    }

    // Query to see how different forms are stored
    let stored_forms = sqlx::query!(
        r#"
        SELECT 
            payload->>'form' as form,
            payload->>'value' as value,
            LENGTH(payload->>'value') as value_length
        FROM core.events
        WHERE source = 'unicode_normalization_test'
        ORDER BY ts_ingest DESC
        LIMIT 20
        "#
    )
    .fetch_all(pool)
    .await?;

    println!("\nStored normalization forms:");
    for form in stored_forms {
        println!(
            "  {}: {} (len={})",
            form.form.unwrap_or_default(),
            form.value.unwrap_or_default(),
            form.value_length.unwrap_or(0)
        );
    }

    Ok(())
}

// =============================================================================
// Zero-Width Character Attack Tests
// =============================================================================

#[sinex_test]
async fn test_zero_width_character_attacks(ctx: TestContext) -> anyhow::Result<()> {
    println!("Testing zero-width character attacks...");

    let zero_width_tests = vec![
        // Zero-width characters
        ("test", "te\u{200B}st", "Zero-width space (U+200B)"),
        ("admin", "ad\u{200C}min", "Zero-width non-joiner (U+200C)"),
        ("password", "pass\u{200D}word", "Zero-width joiner (U+200D)"),
        (
            "secret",
            "se\u{FEFF}cret",
            "Zero-width no-break space (U+FEFF)",
        ),
        // Multiple zero-width characters
        (
            "data",
            "d\u{200B}a\u{200C}t\u{200D}a",
            "Multiple zero-width chars",
        ),
        // Zero-width at boundaries
        ("\u{200B}start", "start", "Zero-width at start"),
        ("end\u{200B}", "end", "Zero-width at end"),
        ("\u{200B}both\u{200B}", "both", "Zero-width at both ends"),
    ];

    let pool = ctx.pool();

    for (clean, injected, description) in zero_width_tests {
        println!("\nTesting: {} - {}", description, clean);
        println!(
            "  Clean: {:?} (len={}, bytes={})",
            clean,
            clean.len(),
            clean.as_bytes().len()
        );
        println!(
            "  Injected: {:?} (len={}, bytes={})",
            injected,
            injected.len(),
            injected.as_bytes().len()
        );

        // Create event with zero-width characters
        let event = EventBuilder::new()
            .source("zero_width_test")
            .event_type("zw.injection")
            .payload(json!({
                "username": injected,
                "clean_username": clean,
                "description": description,
                "char_count": injected.chars().count(),
                "byte_count": injected.as_bytes().len()
            }))
            .build();

        // Test sanitization
        let mut sanitizable_event = event.clone();
        let was_sanitized = EventSanitizer::sanitize_event(&mut sanitizable_event)?;

        if was_sanitized {
            println!("  ✓ Event was sanitized");
            let sanitized_username = sanitizable_event.payload["username"].as_str().unwrap_or("");
            println!("  Sanitized to: {:?}", sanitized_username);
        } else {
            println!("  ⚠️  Event was not sanitized");
        }

        // Insert and verify
        insert_event(pool, &event).await?;
    }

    Ok(())
}

// =============================================================================
// Direction Override Attack Tests
// =============================================================================

#[sinex_test]
async fn test_direction_override_attacks(ctx: TestContext) -> anyhow::Result<()> {
    println!("Testing direction override attacks...");

    let direction_tests = vec![
        // Right-to-left override
        (
            "file.txt",
            "file\u{202E}txt.exe",
            "RLO hides real extension",
        ),
        (
            "invoice.pdf",
            "invoice\u{202E}fdp.exe",
            "RLO makes exe look like pdf",
        ),
        // Left-to-right override
        ("مستند.exe", "\u{202D}مستند.exe", "LRO on RTL text"),
        // Embedding attacks
        ("normal", "nor\u{202A}mal", "Left-to-right embedding"),
        ("normal", "nor\u{202B}mal", "Right-to-left embedding"),
        // Pop directional formatting
        ("test\u{202C}", "test", "Pop directional formatting"),
        // Complex bidirectional text
        (
            "user@domain.com",
            "user@\u{202E}moc.niamod",
            "Email spoofing with RLO",
        ),
    ];

    let pool = ctx.pool();

    for (legitimate, attack, description) in direction_tests {
        println!("\nTesting: {}", description);
        println!("  Legitimate: {:?}", legitimate);
        println!("  Attack: {:?}", attack);

        // Visual representation (approximate)
        println!("  Visual: {}", attack);

        let event = EventBuilder::new()
            .source("direction_override_test")
            .event_type("bidi.attack")
            .payload(json!({
                "filename": attack,
                "legitimate": legitimate,
                "description": description,
                "contains_bidi": contains_bidi_override(&attack)
            }))
            .build();

        insert_event(pool, &event).await?;

        // Check if the attack would be caught
        if contains_bidi_override(&attack) {
            println!("  ✓ Bidirectional override detected");
        } else {
            println!("  ⚠️  Bidirectional override not detected");
        }
    }

    Ok(())
}

// =============================================================================
// Encoding Attack Tests
// =============================================================================

#[sinex_test]
async fn test_encoding_based_attacks(ctx: TestContext) -> anyhow::Result<()> {
    println!("Testing encoding-based attacks...");

    let encoding_tests = vec![
        // Overlong UTF-8 sequences (invalid but sometimes accepted)
        ("A", vec![0xC1, 0x81], "Overlong encoding of 'A'"),
        ("/", vec![0xC0, 0xAF], "Overlong encoding of '/'"),
        // Invalid UTF-8 sequences
        ("", vec![0xFF, 0xFE], "Invalid UTF-8 start bytes"),
        ("", vec![0x80, 0x80], "Orphaned continuation bytes"),
        // UTF-16 surrogates in UTF-8 (invalid)
        ("", vec![0xED, 0xA0, 0x80], "UTF-16 high surrogate"),
        ("", vec![0xED, 0xB0, 0x80], "UTF-16 low surrogate"),
    ];

    let pool = ctx.pool();

    for (expected, bytes, description) in encoding_tests {
        println!("\nTesting: {}", description);
        println!("  Bytes: {:?}", bytes);

        // Try to create string from bytes
        match String::from_utf8(bytes.clone()) {
            Ok(s) => {
                println!("  ⚠️  Invalid UTF-8 was accepted: {:?}", s);

                // Try to insert into database
                let event = EventBuilder::new()
                    .source("encoding_attack_test")
                    .event_type("encoding.invalid")
                    .payload(json!({
                        "value": s,
                        "description": description,
                        "bytes": bytes
                    }))
                    .build();

                match insert_event(pool, &event).await {
                    Ok(_) => println!("  ⚠️  Database accepted invalid encoding"),
                    Err(e) => println!("  ✓ Database rejected: {}", e),
                }
            }
            Err(e) => {
                println!("  ✓ Invalid UTF-8 correctly rejected: {}", e);
            }
        }
    }

    Ok(())
}

// =============================================================================
// Combined Attack Tests
// =============================================================================

#[sinex_test]
async fn test_combined_unicode_attacks(ctx: TestContext) -> anyhow::Result<()> {
    println!("Testing combined unicode attacks...");

    let combined_attacks = vec![
        // Homograph + zero-width
        ("admin", "аdm\u{200B}in", "Homograph 'а' + zero-width space"),
        // Normalization + direction override
        ("café.exe", "cafe\u{0301}\u{202E}exe.", "NFD + RLO"),
        // Multiple attack vectors
        (
            "PayPal",
            "Р\u{200B}ay\u{202E}laP\u{200C}al",
            "Homograph + ZW + RLO + ZWNJ",
        ),
    ];

    let pool = ctx.pool();

    for (legitimate, attack, description) in combined_attacks {
        println!("\nTesting combined attack: {}", description);
        println!("  Legitimate: {:?}", legitimate);
        println!(
            "  Attack: {:?} (len={}, bytes={})",
            attack,
            attack.len(),
            attack.as_bytes().len()
        );

        let attack_vectors = analyze_unicode_attacks(&attack);
        println!("  Detected vectors: {:?}", attack_vectors);

        let event = EventBuilder::new()
            .source("combined_unicode_test")
            .event_type("unicode.combined")
            .payload(json!({
                "input": attack,
                "legitimate": legitimate,
                "description": description,
                "attack_vectors": attack_vectors
            }))
            .build();

        insert_event(pool, &event).await?;
    }

    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

fn contains_mixed_scripts(s: &str) -> bool {
    use unicode_script::{Script, UnicodeScript};

    let scripts: std::collections::HashSet<Script> = s
        .chars()
        .filter(|c| c.is_alphabetic())
        .map(|c| c.script())
        .collect();

    // Common script doesn't count as mixing
    let meaningful_scripts = scripts
        .into_iter()
        .filter(|s| *s != Script::Common && *s != Script::Inherited)
        .count();

    meaningful_scripts > 1
}

fn contains_confusable_chars(s: &str) -> bool {
    // Simple check for common confusables
    let confusables = [
        ('а', 'a'),
        ('е', 'e'),
        ('о', 'o'),
        ('р', 'p'),
        ('х', 'x'),
        ('Α', 'A'),
        ('Β', 'B'),
        ('Ε', 'E'),
        ('Ζ', 'Z'),
        ('Η', 'H'),
        ('１', '1'),
        ('０', '0'),
        ('ｌ', 'l'),
        ('Ｉ', 'I'),
    ];

    s.chars()
        .any(|c| confusables.iter().any(|(conf, _)| *conf == c))
}

fn contains_bidi_override(s: &str) -> bool {
    s.chars().any(|c| {
        matches!(
            c,
            '\u{202A}'
                | '\u{202B}'
                | '\u{202C}'
                | '\u{202D}'
                | '\u{202E}'
                | '\u{2066}'
                | '\u{2067}'
                | '\u{2068}'
                | '\u{2069}'
        )
    })
}

fn contains_zero_width(s: &str) -> bool {
    s.chars().any(|c| {
        matches!(
            c,
            '\u{200B}'
                | '\u{200C}'
                | '\u{200D}'
                | '\u{FEFF}'
                | '\u{200E}'
                | '\u{200F}'
                | '\u{2060}'
                | '\u{2061}'
                | '\u{2062}'
                | '\u{2063}'
                | '\u{2064}'
        )
    })
}

fn analyze_unicode_attacks(s: &str) -> HashMap<String, bool> {
    let mut vectors = HashMap::new();

    vectors.insert("mixed_scripts".to_string(), contains_mixed_scripts(s));
    vectors.insert("confusables".to_string(), contains_confusable_chars(s));
    vectors.insert("bidi_override".to_string(), contains_bidi_override(s));
    vectors.insert("zero_width".to_string(), contains_zero_width(s));
    vectors.insert("non_nfc".to_string(), s != &s.nfc().collect::<String>());

    vectors
}

async fn insert_event(pool: &DbPool, event: &RawEvent) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r#"
        INSERT INTO core.events (event_id, source, event_type, payload, ts_orig, ts_ingest)
        VALUES ($1::uuid, $2, $3, $4, $5, $6)
        "#,
        event.id.to_uuid(),
        event.source,
        event.event_type,
        event.payload,
        event.ts_orig,
        event.ts_ingest
    )
    .execute(pool)
    .await
    .context("Failed to insert event")?;
    Ok(())
}
