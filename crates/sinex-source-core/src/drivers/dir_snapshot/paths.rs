//! Reversible directory paths. Valid Unicode is kept as written; percent signs
//! and bytes outside UTF-8 are escaped as percent followed by two hex digits.
use super::DriverError;
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

pub(super) fn display(path: &Path) -> String {
    let mut bytes = path.as_os_str().as_bytes();
    let mut text = String::new();
    while !bytes.is_empty() {
        let (valid, invalid) = match std::str::from_utf8(bytes) {
            Ok(_) => (bytes.len(), 0),
            Err(error) => (
                error.valid_up_to(),
                error
                    .error_len()
                    .unwrap_or(bytes.len() - error.valid_up_to()),
            ),
        };
        text.push_str(
            &std::str::from_utf8(&bytes[..valid])
                .expect("valid prefix")
                .replace('%', "%25"),
        );
        for byte in &bytes[valid..valid + invalid] {
            use std::fmt::Write;
            write!(text, "%{byte:02X}").expect("write to string");
        }
        bytes = &bytes[valid + invalid..];
    }
    text
}

pub(super) fn native(text: &str) -> Result<PathBuf, DriverError> {
    let mut bytes = Vec::new();
    let mut rest = text.as_bytes();
    while let Some((&byte, after)) = rest.split_first() {
        if byte == b'%' {
            let hex = after
                .get(..2)
                .and_then(|hex| std::str::from_utf8(hex).ok())
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .ok_or_else(|| DriverError::Cursor("directory path escape is invalid".into()))?;
            bytes.push(hex);
            rest = &after[2..];
        } else {
            bytes.push(byte);
            rest = after;
        }
    }
    Ok(PathBuf::from(OsString::from_vec(bytes)))
}
