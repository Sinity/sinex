//! Connecting to NATS. The URL names the server and the user
//! (`nats://user@host:port`, the user percent-encoded); the password is read
//! from a file. A password in the URL is refused: process arguments are
//! readable by every local user.
use std::path::Path;

/// Connect, as the URL's user with the password in `password_file` when the
/// URL names a user.
pub async fn connect(
    url: &str,
    password_file: Option<&Path>,
) -> Result<async_nats::Client, String> {
    let options = options(url, password_file)?;
    let (server, _) = split(url)?;
    options
        .connect(server.as_str())
        .await
        .map_err(|e| format!("NATS at {url}: {e}"))
}

fn options(url: &str, password_file: Option<&Path>) -> Result<async_nats::ConnectOptions, String> {
    let (_, user) = split(url)?;
    match (user, password_file) {
        (Some(user), Some(file)) => {
            let password = std::fs::read_to_string(file)
                .map_err(|e| format!("NATS password {}: {e}", file.display()))?;
            Ok(async_nats::ConnectOptions::with_user_and_password(
                user.clone(),
                password.trim_end_matches('\n').to_owned(),
            ).custom_inbox_prefix(format!("_INBOX.{user}")))
        }
        (None, None) => Ok(async_nats::ConnectOptions::new()),
        (Some(user), None) => Err(format!(
            "NATS user {user} needs its password file (--nats-password-file)"
        )),
        (None, Some(_)) => Err(format!(
            "a NATS password file needs a user in the URL (nats://USER@HOST:PORT), not {url}"
        )),
    }
}

/// The URL without its user, and the decoded user if present.
fn split(url: &str) -> Result<(String, Option<String>), String> {
    let (scheme, rest) = match url.split_once("://") {
        Some((scheme, rest)) => (Some(scheme), rest),
        None => (None, url),
    };
    let Some((user, host)) = rest.rsplit_once('@') else {
        return Ok((url.to_owned(), None));
    };
    if user.contains(':') {
        return Err(
            "the NATS URL holds a password; give it in a file with --nats-password-file".into(),
        );
    }
    let server = match scheme {
        Some(scheme) => format!("{scheme}://{host}"),
        None => host.to_owned(),
    };
    Ok((server, Some(decode(user))))
}

/// Percent-decoding; an invalid escape is kept as written.
fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3])
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests;
