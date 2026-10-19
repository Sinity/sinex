//! Renditions and handoff. Media decoding and playing use existing programs
//! (spec `library.md` §1, §4): previews are made with `ffmpeg`, and an open
//! request hands a file to a player chosen by media type.
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The longest side of a preview, in pixels.
pub const PREVIEW_SIDE: u32 = 512;

/// Whether `ffmpeg` can make a still preview of this media type.
pub fn previewable(media_type: &str) -> bool {
    media_type.starts_with("image/") || media_type.starts_with("video/")
}

/// The tool and settings a preview is made with, recorded on the rendition.
pub fn preview_settings() -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    m.insert("tool".into(), "ffmpeg".into());
    m.insert(
        "filter".into(),
        format!("thumbnail,scale='min({PREVIEW_SIDE},iw)':-2").into(),
    );
    m.insert("format".into(), "webp".into());
    m
}

/// Make a WebP preview of `input` at `output`: the most representative frame
/// of a video, or the image itself, no wider than `PREVIEW_SIDE`.
pub fn make_preview(input: &Path, output: &Path) -> Result<(), String> {
    let out = Command::new("ffmpeg")
        .args(["-v", "error", "-nostdin", "-y", "-i"])
        .arg(input)
        .args([
            "-vf",
            &format!("thumbnail,scale='min({PREVIEW_SIDE},iw)':-2"),
            "-frames:v",
            "1",
            "-c:v",
            "libwebp",
            "-quality",
            "80",
        ])
        .arg(output)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("ffmpeg: {e}"))?;
    if out.status.success() && output.exists() {
        Ok(())
    } else {
        Err(format!(
            "ffmpeg could not make a preview of {}: {}",
            input.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// The program to hand a media type to: the operator's setting for it, or
/// the desktop's default opener.
pub fn player_for(media_type: &str, configured: Option<&str>) -> String {
    if let Some(c) = configured {
        return c.to_owned();
    }
    if media_type.starts_with("video/") || media_type.starts_with("audio/") {
        "mpv".into()
    } else {
        "xdg-open".into()
    }
}

/// A path that holds exactly `content` now: the file exists, has the size
/// recorded, and hashes to the content name. Each handoff rechecks, so a
/// moved or edited file is never opened in place of the requested bytes.
pub fn verified(path: &Path, size: u64, content: &str) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if meta.len() != size {
        return false;
    }
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut hasher = blake3::Hasher::new();
    if std::io::copy(&mut file, &mut hasher).is_err() {
        return false;
    }
    format!("blake3:{}", hasher.finalize().to_hex()) == content
}

/// Where stored bytes are written out for a player when no verified copy
/// exists on disk: a cache file named by the content, with the extension the
/// media type implies so players recognize it.
pub fn cache_path(cache: &Path, content: &str, media_type: &str) -> PathBuf {
    let digest = content.strip_prefix("blake3:").unwrap_or(content);
    let ext = match media_type {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/avif" => "avif",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        "video/x-matroska" => "mkv",
        "video/quicktime" => "mov",
        "audio/mpeg" => "mp3",
        "audio/flac" => "flac",
        "audio/ogg" => "ogg",
        "application/pdf" => "pdf",
        _ => "bin",
    };
    cache.join(format!("{digest}.{ext}"))
}

#[cfg(test)]
mod tests;
