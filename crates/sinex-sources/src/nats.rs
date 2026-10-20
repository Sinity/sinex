//! Connecting to NATS from one URL. Deployments pass each process its
//! credentials in the URL (`nats://user:password@host:port`, the password
//! percent-encoded), but `async_nats::connect` does not authenticate with
//! URL credentials, so they are taken out here and passed explicitly.

/// Connect, authenticating with the URL's user and password when it has them.
pub async fn connect(url: &str) -> Result<async_nats::Client, String> {
    let (server, credentials) = split(url);
    let options = match credentials {
        Some((user, password)) => {
            async_nats::ConnectOptions::with_user_and_password(user, password)
        }
        None => async_nats::ConnectOptions::new(),
    };
    options
        .connect(server.as_str())
        .await
        .map_err(|e| format!("NATS at {}: {e}", redacted(url)))
}

/// The URL with any password replaced, for messages and logs.
pub fn redacted(url: &str) -> String {
    match split(url) {
        (server, Some((user, _))) => {
            let (scheme, host) = server.split_once("://").unwrap_or(("nats", &server));
            format!("{scheme}://{user}:***@{host}")
        }
        (server, None) => server,
    }
}

/// The URL without credentials, and the decoded user and password if present.
fn split(url: &str) -> (String, Option<(String, String)>) {
    let (scheme, rest) = match url.split_once("://") {
        Some((scheme, rest)) => (Some(scheme), rest),
        None => (None, url),
    };
    let Some((userinfo, host)) = rest.rsplit_once('@') else {
        return (url.to_owned(), None);
    };
    let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    let server = match scheme {
        Some(scheme) => format!("{scheme}://{host}"),
        None => host.to_owned(),
    };
    (server, Some((decode(user), decode(password))))
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
