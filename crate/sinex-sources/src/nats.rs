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
mod tests {
    use super::*;

    #[test]
    fn credentials_are_taken_out_and_decoded() {
        let (server, creds) =
            split("nats://sinexd:daemon%20secret%3A%20with%20%40%20and%20%2F@127.0.0.1:4222");
        assert_eq!(server, "nats://127.0.0.1:4222");
        assert_eq!(
            creds,
            Some(("sinexd".into(), "daemon secret: with @ and /".into()))
        );
    }

    #[test]
    fn a_url_without_credentials_is_unchanged() {
        assert_eq!(
            split("nats://127.0.0.1:4222"),
            ("nats://127.0.0.1:4222".into(), None)
        );
        assert_eq!(split("127.0.0.1:4222"), ("127.0.0.1:4222".into(), None));
    }

    #[test]
    fn messages_never_show_the_password() {
        let shown = redacted("nats://session-x:s3cret@host:4222");
        assert_eq!(shown, "nats://session-x:***@host:4222");
        assert!(!shown.contains("s3cret"));
    }

    #[test]
    fn a_bad_escape_is_kept() {
        assert_eq!(decode("a%zzb%4"), "a%zzb%4");
        assert_eq!(decode("%41"), "A");
    }
}
