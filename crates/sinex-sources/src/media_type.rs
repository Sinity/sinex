//! Media signatures shared by source families.
/// The media type from a file's first bytes. Names are never trusted: a
/// `.jpg` that is a PNG is a PNG. Generic UTF-8 text is refined by
/// the caller after sniffing.
pub(crate) fn sniff(head: &[u8]) -> &'static str {
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
