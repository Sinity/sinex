//! Unicode paths keep their spelling. Other paths carry their native bytes.
use super::DriverError;
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

pub(super) fn display(path: &Path) -> String {
    if let Some(text) = path.to_str() {
        return text.to_owned();
    }
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

pub(super) fn add_native(member: &mut serde_json::Value, path: &Path) {
    use base64::Engine;
    if path.to_str().is_none() {
        member["native_path_bytes"] = base64::engine::general_purpose::STANDARD
            .encode(path.as_os_str().as_bytes())
            .into();
    }
}

pub(super) fn native(member: &sinex_model::Member) -> Result<PathBuf, DriverError> {
    use base64::Engine;
    match member.details.get("native_path_bytes") {
        None => Ok(PathBuf::from(&member.path)),
        Some(value) => {
            let bytes = value
                .as_str()
                .and_then(|text| base64::engine::general_purpose::STANDARD.decode(text).ok())
                .ok_or_else(|| {
                    DriverError::Cursor("directory native path bytes are invalid".into())
                })?;
            Ok(PathBuf::from(OsString::from_vec(bytes)))
        }
    }
}
