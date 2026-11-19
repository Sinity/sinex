//! Renditions and opening files. Media decoding and playing use existing
//! programs: previews are made with `ffmpeg`, and an open request hands a file
//! to a player chosen by media type.
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The longest side of a preview, in pixels.
const PREVIEW_SIDE: u32 = 512;

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
/// recorded, and hashes to the content name. Each open rechecks, so a moved or
/// edited file is never opened in place of the requested bytes.
pub fn verified(path: &Path, size: u64, content: &str) -> bool {
    VerifiedFile::open(path, size, content).is_some()
}

/// The open file and its unchanged on-disk identity after hashing.
pub struct VerifiedFile {
    path: PathBuf,
    file: std::fs::File,
    identity: FileIdentity,
}

impl VerifiedFile {
    pub fn open(path: &Path, size: u64, content: &str) -> Option<Self> {
        let expected = identity(std::fs::metadata(path).ok()?)?;
        if expected.len != size {
            return None;
        }
        let mut file = std::fs::File::open(path).ok()?;
        if identity(file.metadata().ok()?)? != expected {
            return None;
        }
        let mut hasher = blake3::Hasher::new();
        std::io::copy(&mut file, &mut hasher).ok()?;
        let verified = Self {
            path: path.to_owned(),
            file,
            identity: expected,
        };
        (format!("blake3:{}", hasher.finalize().to_hex()) == content && verified.still_present())
            .then_some(verified)
    }

    pub fn still_present(&self) -> bool {
        std::fs::metadata(&self.path).ok().and_then(identity) == Some(self.identity)
            && self.file.metadata().ok().and_then(identity) == Some(self.identity)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    len: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

fn identity(meta: std::fs::Metadata) -> Option<FileIdentity> {
    use std::os::unix::fs::MetadataExt;
    meta.is_file().then(|| FileIdentity {
        device: meta.dev(),
        inode: meta.ino(),
        len: meta.len(),
        modified: (meta.mtime(), meta.mtime_nsec()),
        changed: (meta.ctime(), meta.ctime_nsec()),
    })
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
