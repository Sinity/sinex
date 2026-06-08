#![no_main]

use libfuzzer_sys::fuzz_target;
use sinex_primitives::privacy::{CategorySet, PrivacyConfig, PrivacyEngine, ProcessingContext};
use std::sync::OnceLock;

/// Build (once) a privacy engine seeded with the full builtin category set.
///
/// `PrivacyConfig::default()` now defaults to `CategorySet::None`, so an engine
/// built from it exercises zero rules — the fuzzer would only walk the no-match
/// path. Seeding `CategorySet::All` keeps fuzz coverage on the seed matcher and
/// action executor without making seed rules ambient runtime policy.
fn seed_engine() -> Option<&'static PrivacyEngine> {
    static ENGINE: OnceLock<Option<PrivacyEngine>> = OnceLock::new();
    ENGINE
        .get_or_init(|| {
            let mut config = PrivacyConfig::default();
            config.builtin_categories = CategorySet::All;
            PrivacyEngine::new(config).ok()
        })
        .as_ref()
}

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let Some(engine) = seed_engine() else {
            return;
        };
        // Exercise all processing contexts — must never panic
        for ctx in [
            ProcessingContext::Command,
            ProcessingContext::Clipboard,
            ProcessingContext::Journal,
            ProcessingContext::Dbus,
            ProcessingContext::Notification,
            ProcessingContext::Document,
            ProcessingContext::Metadata,
        ] {
            let result = engine.process(s, ctx);
            // Basic sanity: if suppressed, we still got a result
            let _ = result.any_matched();
            let _ = result.suppressed;
            let _ = result.text.len();
        }
    }
});
