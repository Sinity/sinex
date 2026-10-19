//! Splitting query text into tokens. A quoted string is one token and never a
//! keyword, so `"recall"` searches for the word.
use super::QueryError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A bare word: keyword, field, value or search word.
    Word,
    /// A double-quoted string, JSON escapes allowed.
    Quoted,
    /// `= != < <= > >= ~ ^ : ( ) [ ] , ->`.
    Symbol,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Token {
    pub kind: Kind,
    pub text: String,
    /// Byte offset in the query text, for error messages.
    pub at: usize,
}

impl Token {
    pub fn is_word(&self, word: &str) -> bool {
        self.kind == Kind::Word && self.text.eq_ignore_ascii_case(word)
    }

    pub fn is_symbol(&self, symbol: &str) -> bool {
        self.kind == Kind::Symbol && self.text == symbol
    }
}

const SYMBOL_CHARS: &str = "=!<>~^()[],";

pub(crate) fn tokens(text: &str) -> Result<Vec<Token>, QueryError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < text.len() {
        let rest = &text[i..];
        let c = rest.chars().next().expect("not at the end");
        if c.is_whitespace() {
            i += c.len_utf8();
            continue;
        }
        if c == '"' {
            let mut stream = serde_json::Deserializer::from_str(rest).into_iter::<String>();
            let value = match stream.next() {
                Some(Ok(v)) => v,
                _ => return Err(QueryError::at(i, "unterminated or invalid quoted string")),
            };
            out.push(Token {
                kind: Kind::Quoted,
                text: value,
                at: i,
            });
            i += stream.byte_offset();
            continue;
        }
        if rest.starts_with("->") {
            out.push(symbol("->", i));
            i += 2;
            continue;
        }
        if SYMBOL_CHARS.contains(c) {
            let two = rest.get(..2).unwrap_or("");
            let len = if matches!(two, "!=" | "<=" | ">=") {
                2
            } else {
                1
            };
            if c == '!' && len == 1 {
                return Err(QueryError::at(i, "`!` must be part of `!=`"));
            }
            out.push(symbol(&rest[..len], i));
            i += len;
            continue;
        }
        // A bare word runs to whitespace, a quote or a symbol character.
        let mut end = i;
        while end < text.len() {
            let ch = text[end..].chars().next().expect("in bounds");
            if ch.is_whitespace() || ch == '"' || SYMBOL_CHARS.contains(ch) {
                break;
            }
            end += ch.len_utf8();
        }
        let word = &text[i..end];
        // `field:value` splits at the colon when the left side is a field-like
        // name; times (`14:05`) and citations (`sinex:ev/...`) stay whole.
        if let Some(colon) = word.find(':') {
            let name = &word[..colon];
            let field_like = !name.is_empty()
                && name != "sinex"
                && name.bytes().all(|b| {
                    b.is_ascii_lowercase() || b == b'_' || b == b'.' || b.is_ascii_digit()
                })
                && name.as_bytes()[0].is_ascii_lowercase();
            if field_like {
                out.push(Token {
                    kind: Kind::Word,
                    text: name.to_string(),
                    at: i,
                });
                out.push(symbol(":", i + colon));
                // `field:"quoted value"` leaves the quote as the next token.
                let value_start = i + colon + 1;
                if value_start < end {
                    out.push(Token {
                        kind: Kind::Word,
                        text: text[value_start..end].to_string(),
                        at: value_start,
                    });
                }
                i = end;
                continue;
            }
        }
        out.push(Token {
            kind: Kind::Word,
            text: word.to_string(),
            at: i,
        });
        i = end;
    }
    Ok(out)
}

fn symbol(text: &str, at: usize) -> Token {
    Token {
        kind: Kind::Symbol,
        text: text.to_string(),
        at,
    }
}

#[cfg(test)]
mod tests;
