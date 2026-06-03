#![no_main]

use libfuzzer_sys::fuzz_target;
use sinex_primitives::privacy::{PrivacyConfig, PrivacyEngine, ProcessingContext};

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let Ok(engine) = PrivacyEngine::new(PrivacyConfig::default()) else {
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
