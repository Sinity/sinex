#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|bytes: &[u8]| sinex_fuzz::manifest_roundtrip(bytes));
