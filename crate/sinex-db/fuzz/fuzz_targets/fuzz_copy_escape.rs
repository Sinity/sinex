#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let mut buf = Vec::new();
        sinex_db::postgres_copy::escape_copy_str(&mut buf, s);

        // Verify invariant: no unescaped control characters in output
        let output = &buf;
        let mut i = 0;
        while i < output.len() {
            match output[i] {
                b'\\' => {
                    // Must be followed by an escape code
                    assert!(i + 1 < output.len(), "trailing backslash at byte {i}");
                    match output[i + 1] {
                        b't' | b'n' | b'\\' | b'r' => i += 2,
                        other => panic!("unknown escape sequence \\{} at byte {i}", other as char),
                    }
                }
                b'\t' => panic!("unescaped tab at byte {i}"),
                b'\n' => panic!("unescaped newline at byte {i}"),
                b'\r' => panic!("unescaped carriage return at byte {i}"),
                _ => i += 1,
            }
        }
    }
});
