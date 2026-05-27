#![no_main]

use libfuzzer_sys::fuzz_target;
use sinex_primitives::validation::{validate_json, validate_path};

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        // Must never panic, only return Ok/Err
        let _ = validate_path(s);
        let _ = validate_json(s);
    }
});
