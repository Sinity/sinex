use futures::StreamExt;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeAdapter, MaterialAnchor};
use sinexd::runtime::parser::{EmailMboxFileAdapter, EmailMboxFileConfig, all_adapter_schemas};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

async fn mbox_file(bytes: &[u8]) -> xtask::sandbox::TestResult<NamedTempFile> {
    let file = NamedTempFile::new()?;
    let mut async_file = tokio::fs::File::create(file.path()).await?;
    async_file.write_all(bytes).await?;
    async_file.flush().await?;
    Ok(file)
}

fn config_for(file: &NamedTempFile) -> EmailMboxFileConfig {
    EmailMboxFileConfig {
        paths: vec![
            camino::Utf8PathBuf::from_path_buf(file.path().to_path_buf())
                .expect("test temp path should be utf8"),
        ],
        folder: Some("Inbox".to_string()),
        max_message_bytes: 1024 * 1024,
    }
}

#[sinex_test]
async fn streams_mbox_messages_as_byte_range_records() -> xtask::sandbox::TestResult<()> {
    let file = mbox_file(
        b"From sender@example.com Sat Jan 01 00:00:00 2022\n\
Message-ID: <one@example.com>\n\
\n\
first\n\
From sender@example.com Sun Jan 02 00:00:00 2022\n\
Message-ID: <two@example.com>\n\
\n\
second\n",
    )
    .await?;

    let adapter = EmailMboxFileAdapter;
    let config = config_for(&file);
    let mut stream = adapter.open(dummy_material_id(), &config, None).await?;

    let first = stream.next().await.expect("first record")?;
    let second = stream.next().await.expect("second record")?;
    assert!(stream.next().await.is_none());

    assert_eq!(first.metadata["mailbox_format"], "mbox-staged");
    assert_eq!(first.metadata["folder"], "Inbox");
    assert_eq!(first.metadata["mbox_message_index"], 0);
    assert_eq!(second.metadata["mbox_message_index"], 1);
    assert!(first.bytes.starts_with(b"Message-ID: <one@example.com>"));
    assert!(second.bytes.starts_with(b"Message-ID: <two@example.com>"));

    assert!(matches!(
        first.anchor,
        MaterialAnchor::ByteRange { start: 49, .. }
    ));
    assert!(
        second.metadata["mbox_byte_start"]
            .as_u64()
            .expect("second start should be numeric")
            > first.metadata["mbox_byte_start"]
                .as_u64()
                .expect("first start should be numeric")
    );
    Ok(())
}

#[sinex_test]
async fn cursor_resumes_after_last_consumed_mbox_message() -> xtask::sandbox::TestResult<()> {
    let file = mbox_file(
        b"From sender@example.com Sat Jan 01 00:00:00 2022\n\
Message-ID: <one@example.com>\n\
\n\
first\n\
From sender@example.com Sun Jan 02 00:00:00 2022\n\
Message-ID: <two@example.com>\n\
\n\
second\n",
    )
    .await?;

    let adapter = EmailMboxFileAdapter;
    let config = config_for(&file);
    let mut stream = adapter.open(dummy_material_id(), &config, None).await?;
    let first = stream.next().await.expect("first record")?;
    let cursor = adapter.cursor_after(&first)?;

    let mut resumed = adapter
        .open(dummy_material_id(), &config, Some(cursor))
        .await?;
    let second = resumed.next().await.expect("second record")?;
    assert!(resumed.next().await.is_none());
    assert!(second.bytes.starts_with(b"Message-ID: <two@example.com>"));
    Ok(())
}

#[sinex_test]
async fn escaped_mboxrd_from_lines_are_body_content() -> xtask::sandbox::TestResult<()> {
    let file = mbox_file(
        b"From sender@example.com Sat Jan 01 00:00:00 2022\n\
Message-ID: <one@example.com>\n\
\n\
>From escaped@example.com Sat Jan 01 00:01:00 2022\n\
body\n",
    )
    .await?;

    let adapter = EmailMboxFileAdapter;
    let config = config_for(&file);
    let mut stream = adapter.open(dummy_material_id(), &config, None).await?;
    let first = stream.next().await.expect("first record")?;
    assert!(stream.next().await.is_none());
    assert!(
        first
            .bytes
            .windows(b">From escaped".len())
            .any(|window| window == b">From escaped")
    );
    Ok(())
}

#[sinex_test]
async fn adapter_schema_exposes_mbox_paths_and_budget() -> xtask::sandbox::TestResult<()> {
    let schemas = all_adapter_schemas();
    let schema = schemas
        .get("EmailMboxFileAdapter")
        .expect("email MBOX adapter schema should be registered");

    assert!(schema.required.iter().any(|field| field == "paths"));
    assert!(
        schema
            .schema
            .pointer("/properties/max_message_bytes")
            .is_some()
    );
    Ok(())
}
