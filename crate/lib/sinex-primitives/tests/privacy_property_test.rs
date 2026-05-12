use proptest::prelude::*;
use sinex_primitives::privacy::{self, ProcessingContext};
use xtask::sandbox::prelude::*;

fn all_contexts() -> Vec<ProcessingContext> {
    vec![
        ProcessingContext::Command,
        ProcessingContext::Clipboard,
        ProcessingContext::WindowTitle,
        ProcessingContext::Journal,
        ProcessingContext::Dbus,
        ProcessingContext::Notification,
        ProcessingContext::Document,
        ProcessingContext::Metadata,
    ]
}

#[allow(clippy::expect_used)]
fn engine() -> &'static sinex_primitives::privacy::PrivacyEngine {
    privacy::engine().expect("privacy engine must initialize")
}

fn arb_context() -> impl Strategy<Value = ProcessingContext> {
    prop_oneof![
        Just(ProcessingContext::Command),
        Just(ProcessingContext::Clipboard),
        Just(ProcessingContext::WindowTitle),
        Just(ProcessingContext::Journal),
        Just(ProcessingContext::Dbus),
        Just(ProcessingContext::Notification),
        Just(ProcessingContext::Document),
        Just(ProcessingContext::Metadata),
    ]
}

fn luhn_valid_card() -> impl Strategy<Value = String> {
    (0u64..999_999_999_999_999u64).prop_map(|base| {
        let partial = format!("{base:015}");
        let digits: Vec<u8> = partial.chars().map(|c| c as u8 - b'0').collect();
        let mut sum: u32 = 0;
        let mut double = true;
        for &d in digits.iter().rev() {
            let mut val = u32::from(d);
            if double {
                val *= 2;
                if val > 9 {
                    val -= 9;
                }
            }
            sum += val;
            double = !double;
        }
        let check = (10 - (sum % 10)) % 10;
        format!("{partial}{check}")
    })
}

fn arb_github_token() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9]{36}".prop_map(|suffix| format!("ghp_{suffix}"))
}

fn arb_email() -> impl Strategy<Value = String> {
    (
        "[a-z]{3,10}",
        "[a-z]{3,8}",
        prop_oneof![
            Just("com".to_string()),
            Just("org".to_string()),
            Just("net".to_string()),
            Just("io".to_string()),
        ],
    )
        .prop_map(|(local, domain, tld)| format!("{local}@{domain}.{tld}"))
}

fn arb_all_context_secret() -> impl Strategy<Value = String> {
    prop_oneof![
        "[0-9A-Z]{16}".prop_map(|suffix| format!("AKIA{suffix}")),
        arb_github_token(),
        "[A-Za-z0-9_\\-]{20,40}".prop_map(|suffix| format!("glpat-{suffix}")),
        "[A-Za-z0-9]{36,48}".prop_map(|suffix| format!("npm_{suffix}")),
        "[A-Za-z0-9]{24,48}".prop_map(|suffix| format!("sk_live_{suffix}")),
        "[A-Za-z0-9\\-]{20,48}".prop_map(|suffix| format!("xoxb-{suffix}")),
        (
            "[A-Za-z0-9_\\-]{8,24}",
            "[A-Za-z0-9_\\-]{8,24}",
            "[A-Za-z0-9_\\-]{12,32}",
        )
            .prop_map(|(header, claims, sig)| format!("eyJ{header}.eyJ{claims}.{sig}")),
        "[A-Za-z0-9_\\-]{35}".prop_map(|suffix| format!("AIza{suffix}")),
        "[A-Za-z0-9+/=]{44,64}".prop_map(|suffix| format!("AccountKey={suffix}")),
        "[A-Za-z0-9._~+/=\\-]{20,48}".prop_map(|token| format!("Bearer {token}")),
        "[a-z]{3,10}".prop_map(|db| format!("postgres://user:pass@localhost/{db}")),
        "[A-Za-z0-9]{34,48}".prop_map(|suffix| format!("hf_{suffix}")),
        "[A-Za-z0-9]{48}".prop_map(|suffix| format!("sk-{suffix}")),
    ]
}

fn arb_command_or_journal_secret_assignment() -> impl Strategy<Value = String> {
    "[A-Za-z0-9_\\-]{16,48}".prop_map(|value| format!("TOKEN={value}"))
}

fn arb_command_secret_flag() -> impl Strategy<Value = String> {
    prop_oneof![
        "[A-Za-z0-9_\\-]{16,48}".prop_map(|value| format!("--token {value}")),
        "[A-Za-z0-9_\\-]{16,48}".prop_map(|value| format!("--api-key={value}")),
    ]
}

sinex_proptest! {
    fn privacy_never_panics_on_arbitrary_utf8(
        text: String in "\\PC{0,500}",
        context: ProcessingContext in arb_context()
    ) -> Result<()> {
        let result = engine().process(&text, context);
        // result.text may be empty when:
        // - text itself was empty, or
        // - the privacy engine suppressed the entire input (result.suppressed = true)
        prop_assert!(
            !result.text.is_empty() || text.is_empty() || result.suppressed,
            "Non-empty input produced empty output without suppression"
        );
        Ok(())
    }
}

sinex_proptest! {
    fn privacy_handles_all_contexts(text: String in "\\PC{0,200}") -> Result<()> {
        for ctx in all_contexts() {
            let _ = engine().process(&text, ctx);
        }
        Ok::<(), proptest::test_runner::TestCaseError>(())
    }
}

sinex_proptest! {
    fn credit_cards_always_detected(
        card: String in luhn_valid_card(),
        prefix: String in "[a-zA-Z ]{0,20}",
        suffix: String in "[a-zA-Z ]{0,20}"
    ) -> Result<()> {
        let input = format!("{prefix} {card} {suffix}");
        let result = engine().process(&input, ProcessingContext::Command);
        prop_assert!(
            !result.text.contains(&card),
            "Credit card {} should be redacted in output: {}",
            card, result.text,
        );
        Ok(())
    }
}

sinex_proptest! {
    fn github_tokens_always_detected(
        token: String in arb_github_token(),
        context: ProcessingContext in arb_context()
    ) -> Result<()> {
        let input = format!("export TOKEN={token}");
        let result = engine().process(&input, context);
        prop_assert!(
            !result.text.contains(&token),
            "GitHub token should be redacted in context {:?}: {}",
            context, result.text,
        );
        Ok(())
    }
}

sinex_proptest! {
    fn known_all_context_secrets_are_removed_wherever_embedded(
        secret: String in arb_all_context_secret(),
        prefix: String in "[a-zA-Z0-9 _./:-]{0,40}",
        suffix: String in "[a-zA-Z0-9 _./:-]{0,40}",
        context: ProcessingContext in arb_context()
    ) -> Result<()> {
        let input = format!("{prefix} {secret} {suffix}");
        let result = engine().process(&input, context);

        prop_assert!(
            result.suppressed || !result.text.contains(&secret),
            "secret shape should be removed in context {:?}: input={:?} output={:?}",
            context,
            input,
            result.text,
        );
        Ok(())
    }
}

sinex_proptest! {
    fn command_scoped_secret_assignments_are_removed_wherever_embedded(
        secret: String in arb_command_or_journal_secret_assignment(),
        prefix: String in "[a-zA-Z0-9 _./:-]{0,40}",
        suffix: String in "[a-zA-Z0-9 _./:-]{0,40}"
    ) -> Result<()> {
        let input = format!("{prefix} {secret} {suffix}");

        for context in [ProcessingContext::Command, ProcessingContext::Journal] {
            let result = engine().process(&input, context);
            prop_assert!(
                result.suppressed || !result.text.contains(&secret),
                "command/journal secret shape should be removed in context {:?}: input={:?} output={:?}",
                context,
                input,
                result.text,
            );
        }
        Ok(())
    }
}

sinex_proptest! {
    fn command_secret_flags_are_removed_wherever_embedded(
        secret: String in arb_command_secret_flag(),
        prefix: String in "[a-zA-Z0-9 _./:-]{0,40}",
        suffix: String in "[a-zA-Z0-9 _./:-]{0,40}"
    ) -> Result<()> {
        let input = format!("{prefix} {secret} {suffix}");
        let result = engine().process(&input, ProcessingContext::Command);

        prop_assert!(
            result.suppressed || !result.text.contains(&secret),
            "command secret flag should be removed: input={:?} output={:?}",
            input,
            result.text,
        );
        Ok(())
    }
}

sinex_proptest! {
    fn emails_detected_across_contexts(
        email: String in arb_email(),
        context: ProcessingContext in arb_context()
    ) -> Result<()> {
        let input = format!("contact: {email}");
        let result = engine().process(&input, context);
        prop_assert!(
            !result.text.contains(&email),
            "Email {} should be redacted in context {:?}: {}",
            email, context, result.text,
        );
        Ok(())
    }
}

sinex_proptest! {
    fn redaction_is_idempotent(
        text: String in "\\PC{0,300}",
        context: ProcessingContext in arb_context()
    ) -> Result<()> {
        let first = engine().process(&text, context);
        let second = engine().process(&first.text, context);
        prop_assert_eq!(
            first.text.as_ref(),
            second.text.as_ref(),
            "Double-processing should not change output"
        );
        Ok(())
    }
}

sinex_proptest! {
    fn output_length_bounded(
        text: String in "\\PC{0,500}",
        context: ProcessingContext in arb_context()
    ) -> Result<()> {
        let result = engine().process(&text, context);
        let max_overhead = 4096;
        prop_assert!(
            result.text.len() <= text.len() + max_overhead,
            "Output length {} exceeds input {} + overhead {}",
            result.text.len(), text.len(), max_overhead,
        );
        Ok(())
    }
}

sinex_proptest! {
    fn json_processing_preserves_object_keys(
        (key1, key2) in ("[a-z_]{1,10}", "[a-z_]{1,10}")
            .prop_filter("object keys must be distinct", |(key1, key2)| key1 != key2),
        val1: String in "\\PC{0,50}",
        val2: String in "\\PC{0,50}",
        context: ProcessingContext in arb_context()
    ) -> Result<()> {
        let input = serde_json::json!({ &key1: val1, &key2: val2 });
        let output = engine().process_json(&input, context);
        prop_assert!(output.is_object(), "Output should remain an object");
        #[allow(clippy::expect_used)]
        let obj = output.as_object().expect("verified above");
        prop_assert!(obj.contains_key(&key1), "Key '{}' should be preserved", key1);
        prop_assert!(obj.contains_key(&key2), "Key '{}' should be preserved", key2);
        Ok(())
    }
}

sinex_proptest! {
    fn suppressed_result_has_empty_text(
        text: String in "\\PC{1,100}",
        context: ProcessingContext in arb_context()
    ) -> Result<()> {
        let result = engine().process(&text, context);
        // Assert unconditionally: suppression must imply empty text.
        // The original guard `if result.suppressed` allowed the property to pass vacuously
        // on every non-suppressed case without actually checking anything.
        if result.suppressed {
            prop_assert!(
                result.text.is_empty(),
                "Suppressed result should have empty text, got: {}",
                result.text,
            );
        } else {
            prop_assert!(
                !result.text.is_empty(),
                "Non-suppressed result with non-empty input should have non-empty text"
            );
        }
        Ok(())
    }
}
