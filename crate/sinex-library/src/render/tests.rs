use super::*;

#[test]
fn a_copy_is_verified_by_its_bytes() {
    let dir = std::env::temp_dir().join(format!("sinex-render-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("a");
    std::fs::write(&p, b"abc").unwrap();
    let name = format!("blake3:{}", blake3::hash(b"abc").to_hex());
    assert!(verified(&p, 3, &name));
    assert!(!verified(&p, 4, &name));
    std::fs::write(&p, b"abd").unwrap();
    assert!(!verified(&p, 3, &name));
    assert!(!verified(&dir.join("missing"), 3, &name));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn players_and_cache_names() {
    assert_eq!(player_for("video/mp4", None), "mpv");
    assert_eq!(player_for("image/png", None), "xdg-open");
    assert_eq!(player_for("image/png", Some("imv")), "imv");
    assert_eq!(
        cache_path(Path::new("/c"), "blake3:ab", "image/png"),
        PathBuf::from("/c/ab.png")
    );
    assert!(previewable("video/webm") && !previewable("audio/flac"));
}

#[test]
fn previews_are_made_with_ffmpeg_when_present() {
    if Command::new("ffmpeg").arg("-version").output().is_err() {
        return;
    }
    let dir = std::env::temp_dir().join(format!("sinex-preview-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let input = dir.join("in.png");
    let made = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=red:s=1024x768",
            "-frames:v",
            "1",
        ])
        .arg(&input)
        .output()
        .unwrap();
    assert!(made.status.success());
    let out = dir.join("p.webp");
    make_preview(&input, &out).unwrap();
    assert!(std::fs::metadata(&out).unwrap().len() > 0);
    std::fs::remove_dir_all(&dir).unwrap();
}
