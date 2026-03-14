#![no_main]

use libfuzzer_sys::fuzz_target;
use sinex_primitives::privacy::{self, ProcessingContext};

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        // Exercise all processing contexts — must never panic
        for ctx in [
            ProcessingContext::Command,
            ProcessingContext::Clipboard,
            ProcessingContext::WindowTitle,
            ProcessingContext::Journal,
            ProcessingContext::Dbus,
            ProcessingContext::Notification,
            ProcessingContext::Document,
            ProcessingContext::Metadata,
        ] {
            let result = privacy::engine().process(s, ctx);
            // Basic sanity: if suppressed, we still got a result
            let _ = result.any_matched();
            let _ = result.suppressed;
            let _ = result.text.len();
        }
    }
});
