use super::*;
use xtask::sandbox::prelude::sinex_test;

fn fake_png(width: u32, height: u32) -> Vec<u8> {
    let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
    // length + "IHDR" (8 bytes) then width/height big-endian.
    bytes.extend_from_slice(&[0, 0, 0, 13]);
    bytes.extend_from_slice(b"IHDR");
    bytes.extend_from_slice(&width.to_be_bytes());
    bytes.extend_from_slice(&height.to_be_bytes());
    bytes.extend_from_slice(&[8, 2, 0, 0, 0]); // bit depth, color type, etc.
    bytes
}

#[sinex_test]
async fn png_dimensions_reads_ihdr() -> xtask::sandbox::TestResult<()> {
    assert_eq!(png_dimensions(&fake_png(1920, 1080)), Some((1920, 1080)));
    assert_eq!(png_dimensions(b"not a png"), None);
    assert_eq!(png_dimensions(&[]), None);
    Ok(())
}

#[sinex_test]
async fn parse_region_reads_x_y_w_h() -> xtask::sandbox::TestResult<()> {
    assert_eq!(parse_region("10,20 800x600"), Some(vec![10, 20, 800, 600]));
    assert_eq!(parse_region("garbage"), None);
    Ok(())
}

#[sinex_test]
async fn build_screenshot_event_anchors_material_and_geometry()
-> xtask::sandbox::TestResult<()> {
    let captured = CapturedImage {
        bytes: fake_png(800, 600),
        width_px: 800,
        height_px: 600,
    };
    let material_id = Id::<SourceMaterial>::from_uuid(uuid::Uuid::new_v4());
    let config = MediaScreenCaptureConfig {
        policy_posture: "metadata_only".to_string(),
        ..MediaScreenCaptureConfig::default()
    };
    let request = ScreenCaptureRequest {
        region: Some("10,20 800x600".to_string()),
        display: Some("DP-1".to_string()),
    };
    let event =
        build_screenshot_event(&captured, material_id, &config, &request, Timestamp::now())?;
    let payload = &event.payload;
    assert_eq!(payload["raw_material_id"], material_id.to_string());
    assert_eq!(payload["width_px"], 800);
    assert_eq!(payload["height_px"], 600);
    assert_eq!(payload["display_id"], "DP-1");
    assert_eq!(payload["policy_posture"], "metadata_only");
    assert_eq!(payload["region"], serde_json::json!([10, 20, 800, 600]));
    Ok(())
}

struct FakeBackend {
    image: CapturedImage,
}

#[async_trait]
impl ScreenCaptureBackend for FakeBackend {
    async fn capture(&self, _request: &ScreenCaptureRequest) -> RuntimeResult<CapturedImage> {
        Ok(self.image.clone())
    }
}

#[sinex_test]
async fn command_backend_reads_stdout_png() -> xtask::sandbox::TestResult<()> {
    // A fake "capture command" (`cat <fixture>`) emits a PNG to stdout,
    // exercising the real command backend without a display.
    let dir = std::env::temp_dir().join(format!("sinex-screen-capture-{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await?;
    let png_path = dir.join("fixture.png");
    tokio::fs::write(&png_path, fake_png(640, 480)).await?;
    let backend = CommandScreenCaptureBackend::new(
        "cat".to_string(),
        vec![png_path.to_string_lossy().into_owned()],
        Duration::from_secs(5),
    );
    let captured = backend.capture(&ScreenCaptureRequest::default()).await?;
    assert_eq!((captured.width_px, captured.height_px), (640, 480));
    assert_eq!(captured.bytes, fake_png(640, 480));
    tokio::fs::remove_dir_all(&dir).await.ok();
    Ok(())
}

#[sinex_test]
async fn command_backend_reports_missing_program() -> xtask::sandbox::TestResult<()> {
    let backend = CommandScreenCaptureBackend::new(
        "sinex-nonexistent-grim-binary".to_string(),
        Vec::new(),
        Duration::from_secs(5),
    );
    let error = backend
        .capture(&ScreenCaptureRequest::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not found"));
    Ok(())
}

#[sinex_test]
async fn fake_backend_capture_is_usable() -> xtask::sandbox::TestResult<()> {
    let backend = FakeBackend {
        image: CapturedImage {
            bytes: fake_png(100, 50),
            width_px: 100,
            height_px: 50,
        },
    };
    let captured = backend.capture(&ScreenCaptureRequest::default()).await?;
    assert_eq!(captured.width_px, 100);
    Ok(())
}
