use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct SubjectRef {
    raw: &'static str,
}

impl SubjectRef {
    #[must_use]
    pub const fn from_static(raw: &'static str) -> Self {
        assert!(
            is_valid_subject_expr(raw, false),
            "invalid source subject reference"
        );
        Self { raw }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.raw
    }

    #[must_use]
    pub fn kind(self) -> &'static str {
        let bytes = self.raw.as_bytes();
        let mut index = 0usize;
        while index < bytes.len() {
            if bytes[index] == b':' {
                return &self.raw[..index];
            }
            index += 1;
        }
        self.raw
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct SubjectQuery {
    raw: &'static str,
}

impl SubjectQuery {
    #[must_use]
    pub const fn from_static(raw: &'static str) -> Self {
        assert!(
            is_valid_subject_expr(raw, true),
            "invalid source subject query"
        );
        Self { raw }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.raw
    }

    #[must_use]
    pub fn matches(self, subject: SubjectRef) -> bool {
        let query = self.raw;
        if query == "*" {
            return true;
        }
        if let Some(prefix) = query.strip_suffix(":*") {
            return subject.as_str().starts_with(prefix)
                && subject.as_str().as_bytes().get(prefix.len()) == Some(&b':');
        }
        query == subject.as_str()
    }
}

const fn is_valid_subject_expr(raw: &str, allow_wildcard: bool) -> bool {
    let bytes = raw.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    if allow_wildcard && bytes.len() == 1 && bytes[0] == b'*' {
        return true;
    }

    let mut index = 0usize;
    let mut colon_count = 0usize;
    let mut last_was_colon = true;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b':' {
            if last_was_colon {
                return false;
            }
            colon_count += 1;
            last_was_colon = true;
            index += 1;
            continue;
        }
        if allow_wildcard && byte == b'*' {
            return index + 1 == bytes.len()
                && index > 0
                && bytes[index - 1] == b':'
                && colon_count > 0;
        }
        if !is_subject_char(byte) {
            return false;
        }
        last_was_colon = false;
        index += 1;
    }

    colon_count > 0 && !last_was_colon
}

const fn is_subject_char(byte: u8) -> bool {
    byte.is_ascii_lowercase()
        || byte.is_ascii_uppercase()
        || byte.is_ascii_digit()
        || matches!(byte, b'-' | b'_' | b'.' | b'/')
}

#[macro_export]
macro_rules! subject_ref {
    ($value:literal) => {{
        const SUBJECT: $crate::source_contracts::SubjectRef =
            $crate::source_contracts::SubjectRef::from_static($value);
        SUBJECT
    }};
}

#[macro_export]
macro_rules! subject_query {
    ($value:literal) => {{
        const QUERY: $crate::source_contracts::SubjectQuery =
            $crate::source_contracts::SubjectQuery::from_static($value);
        QUERY
    }};
}
