//! Positive raw capture through the real media spool reader and writer. No
//! model policy or model execution is needed to retain screen and audio bytes.
use super::*;
use sinex_sources::sources::media_capture;

fn wav() -> Vec<u8> {
    let samples = 8_000u32;
    let data = samples * 2;
    let mut bytes = Vec::new();
    bytes.extend(b"RIFF");
    bytes.extend((36 + data).to_le_bytes());
    bytes.extend(b"WAVEfmt ");
    bytes.extend(16u32.to_le_bytes());
    bytes.extend(1u16.to_le_bytes());
    bytes.extend(1u16.to_le_bytes());
    bytes.extend(8_000u32.to_le_bytes());
    bytes.extend(16_000u32.to_le_bytes());
    bytes.extend(2u16.to_le_bytes());
    bytes.extend(16u16.to_le_bytes());
    bytes.extend(b"data");
    bytes.extend(data.to_le_bytes());
    bytes.resize(44 + data as usize, 0);
    bytes
}
#[devkit::test]
async fn model_disabled_capture_keeps_exact_screen_and_audio_materials_after_restart(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-model-off-media-{}", uuid::Uuid::now_v7()));
    let store = dir.join("store");
    let frame = b"P6\n1 1\n255\n\0\0\0".to_vec();
    let audio = wav();
    let settings = sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
        .expect("fixture day setting");
    let configuration = crate::binding_runner::Config::default();
    assert!(!configuration.model_accounting.actual_receipts);
    assert_eq!(
        sinex_model::model_calls::execute::<(), ()>(
            || panic!("model input selection must remain disabled"),
            |_: ()| panic!("provider construction must remain disabled")
        ),
        Err::<(), _>(sinex_model::model_calls::Refusal::ProviderDisabled)
    );
    let mut bindings = Vec::new();
    for (kind, extension, mime, bytes, end) in [
        (
            "screen",
            "ppm",
            "image/x-portable-pixmap",
            &frame,
            100_000_000_000i64,
        ),
        ("audio", "wav", "audio/wav", &audio, 101_000_000_000i64),
    ] {
        let spool = dir.join(kind);
        std::fs::create_dir_all(&spool).unwrap();
        let partial = spool.join("capture.partial");
        std::fs::write(&partial, bytes).unwrap();
        sinex_source_core::drivers::media::publish_segment(&spool,&partial,&format!("100000000000-{end}.{kind}"),extension,
          &json!({"kind":kind,"started_ns":"100000000000","ended_ns":end.to_string(),"mime":mime,"media_type":mime,"width":if kind=="screen"{Some(1)}else{None},"height":if kind=="screen"{Some(1)}else{None},"accessibility_text":if kind=="screen"{Some("synthetic widget")}else{None}})).unwrap();
        let mut settings = Settings::new("synthetic-media-host");
        settings.options = json!({"kind":kind});
        let binding = Binding::of(&media_capture::SPEC, &spool, settings).named(kind);
        bindings.push(BindingReader::new(&media_capture::FORMAT, binding).unwrap());
    }
    let mut ctx = ReaderContext::open(&url, &store, settings.clone())
        .await
        .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let results = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        &sink,
        &mut bindings,
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await;
    assert_eq!(results.len(), 2);
    for (name, result) in results {
        if let Err(reason) = result {
            panic!("{name}: {reason}")
        }
    }
    let rows=sqlx::query!(r#"SELECT e.id,e.payload,e.material_id,e.anchor,e.output_index,m.blob,m.length
      FROM core.events e JOIN raw.materials m ON m.id=e.material_id JOIN core.live_locations l ON l.event_id=e.id
      WHERE e.event_type='media.segment' ORDER BY e.payload->>'kind'"#).fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(rows.len(), 2);
    let mut retained = Vec::new();
    for row in rows {
        let kind = row.payload["kind"].as_str().unwrap();
        let original = if kind == "screen" { &frame } else { &audio };
        assert!(row.material_id.is_some());
        assert_eq!(row.anchor, Some(0));
        assert_eq!(row.output_index, 0);
        assert_eq!(row.length as usize, original.len());
        assert_eq!(ctx.blobs.read(&row.blob).unwrap(), *original);
        assert_eq!(row.payload["bytes"], original.len() as u64);
        retained.push((row.id, row.blob, original.clone()));
    }
    assert_eq!(sqlx::query_scalar!("SELECT count(*) FROM core.events WHERE event_type IN ('model.request','model.response','model.receipt')").fetch_one(&mut ctx.read_conn).await.unwrap(),Some(0));
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap(),
        Some(0)
    );
    ctx.close().await.unwrap();
    let mut fresh = ReaderContext::open(&url, &store, settings).await.unwrap();
    for (id, blob, bytes) in retained {
        assert_eq!(fresh.blobs.read(&blob).unwrap(), bytes);
        assert_eq!(
            sqlx::query_scalar!(
                "SELECT material_id IS NOT NULL FROM core.events WHERE id=$1",
                id
            )
            .fetch_one(&mut fresh.read_conn)
            .await
            .unwrap(),
            Some(true)
        );
    }
    fresh.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
