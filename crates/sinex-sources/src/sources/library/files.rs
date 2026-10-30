//! File digests, media types and dimensions.
use serde_json::Value;
use std::io::Read;
use std::path::Path;

pub struct Digests {
    pub blake3: String,
    pub sha256: String,
    pub md5: String,
    /// The first bytes, for sniffing the type and reading image dimensions.
    pub head: Vec<u8>,
    pub media_type: String,
}

/// Hash a file once with all three digests, streaming it.
pub fn digest(path: &Path) -> std::io::Result<Digests> {
    use md5::Digest as _;
    let mut file = std::fs::File::open(path)?;
    let mut b3 = blake3::Hasher::new();
    let mut sha = sha2::Sha256::new();
    let mut md = md5::Md5::new();
    let mut head = Vec::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let chunk = &buf[..n];
        if head.len() < 64 * 1024 {
            let take = (64 * 1024 - head.len()).min(n);
            head.extend_from_slice(&chunk[..take]);
        }
        b3.update(chunk);
        sha.update(chunk);
        md.update(chunk);
    }
    let media_type = sniff(&head).to_owned();
    Ok(Digests {
        blake3: b3.finalize().to_hex().to_string(),
        sha256: hex(&sha.finalize()),
        md5: hex(&md.finalize()),
        head,
        media_type,
    })
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The media type from a file's first bytes. Names are never trusted: a
/// `.jpg` that is a PNG is a PNG.
pub(super) fn sniff(head: &[u8]) -> &'static str {
    let starts = |magic: &[u8]| head.starts_with(magic);
    let at = |offset: usize, magic: &[u8]| head.get(offset..offset + magic.len()) == Some(magic);
    if starts(b"\xFF\xD8\xFF") {
        "image/jpeg"
    } else if starts(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if starts(b"GIF87a") || starts(b"GIF89a") {
        "image/gif"
    } else if starts(b"RIFF") && at(8, b"WEBP") {
        "image/webp"
    } else if starts(b"RIFF") && at(8, b"WAVE") {
        "audio/wav"
    } else if starts(b"RIFF") && at(8, b"AVI ") {
        "video/x-msvideo"
    } else if at(4, b"ftyp") {
        match head.get(8..12) {
            Some(b"avif") | Some(b"avis") => "image/avif",
            Some(b"heic") | Some(b"heix") | Some(b"mif1") => "image/heic",
            Some(b"qt  ") => "video/quicktime",
            Some(b"M4A ") => "audio/mp4",
            _ => "video/mp4",
        }
    } else if starts(b"\x1A\x45\xDF\xA3") {
        if head.windows(4).any(|w| w == b"webm") {
            "video/webm"
        } else {
            "video/x-matroska"
        }
    } else if starts(b"%PDF-") {
        "application/pdf"
    } else if starts(b"ID3") || (head.len() > 1 && head[0] == 0xFF && head[1] & 0xE0 == 0xE0) {
        "audio/mpeg"
    } else if starts(b"fLaC") {
        "audio/flac"
    } else if starts(b"OggS") {
        "audio/ogg"
    } else if starts(b"BM") && head.len() > 26 {
        "image/bmp"
    } else if starts(b"II*\0") || starts(b"MM\0*") {
        "image/tiff"
    } else if starts(b"PK\x03\x04") {
        "application/zip"
    } else if head.iter().take(4096).all(|b| *b != 0) && std::str::from_utf8(head).is_ok() {
        if head.starts_with(b"WEBVTT") {
            "text/vtt"
        } else {
            "text/plain"
        }
    } else {
        "application/octet-stream"
    }
}

/// Image dimensions from the header bytes; `(width, height, duration)`.
pub fn dimensions(head: &[u8], media_type: &str) -> (Option<u32>, Option<u32>, Option<u64>) {
    let be16 = |o: usize| {
        head.get(o..o + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]) as u32)
    };
    let le16 = |o: usize| {
        head.get(o..o + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as u32)
    };
    let be32 = |o: usize| {
        head.get(o..o + 4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    };
    let le24 = |o: usize| {
        head.get(o..o + 3)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], 0]))
    };
    let (w, h) = match media_type {
        "image/png" => (be32(16), be32(20)),
        "image/gif" => (le16(6), le16(8)),
        "image/bmp" => (
            head.get(18..22)
                .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            head.get(22..26)
                .map(|b| i32::from_le_bytes([b[0], b[1], b[2], b[3]]).unsigned_abs()),
        ),
        "image/webp" => match head.get(12..16) {
            Some(b"VP8 ") => (le16(26).map(|v| v & 0x3FFF), le16(28).map(|v| v & 0x3FFF)),
            Some(b"VP8L") => head
                .get(21..25)
                .map(|b| {
                    let v = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                    (Some((v & 0x3FFF) + 1), Some(((v >> 14) & 0x3FFF) + 1))
                })
                .unwrap_or((None, None)),
            Some(b"VP8X") => (le24(24).map(|v| v + 1), le24(27).map(|v| v + 1)),
            _ => (None, None),
        },
        "image/jpeg" => {
            // Walk the segments to the first start-of-frame marker.
            let mut i = 2;
            let mut found = (None, None);
            while i + 9 < head.len() {
                if head[i] != 0xFF {
                    break;
                }
                let marker = head[i + 1];
                let len = be16(i + 2).unwrap_or(0) as usize;
                if matches!(marker, 0xC0..=0xC3 | 0xC5..=0xC7 | 0xC9..=0xCB | 0xCD..=0xCF) {
                    found = (be16(i + 7), be16(i + 5));
                    break;
                }
                if len < 2 {
                    break;
                }
                i += 2 + len;
            }
            found
        }
        _ => (None, None),
    };
    (w.filter(|v| *v > 0), h.filter(|v| *v > 0), None)
}

/// Width, height and duration from `ffprobe`, when it is installed.
pub(super) fn ffprobe(path: &Path) -> Option<(Option<u32>, Option<u32>, Option<u64>)> {
    let out = std::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_entries",
            "format=duration:stream=width,height",
        ])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let v: Value = serde_json::from_slice(&out.stdout).ok()?;
    let stream = v
        .get("streams")
        .and_then(Value::as_array)
        .and_then(|s| s.iter().find(|s| s.get("width").is_some()));
    let dim = |k: &str| {
        stream
            .and_then(|s| s.get(k))
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
    };
    let duration = v
        .pointer("/format/duration")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<f64>().ok())
        .map(|s| (s * 1000.0).round() as u64);
    Some((dim("width"), dim("height"), duration))
}
