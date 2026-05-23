use sinex_gateway::handlers::handle_documents_search;
use sinex_primitives::Uuid;
use sinex_primitives::rpc::documents::DocumentsSearchRequest;
use xtask::sandbox::prelude::*;

async fn seed_document(
    ctx: &TestContext,
    id: Uuid,
    kind: &str,
    natural_key: &str,
) -> TestResult<()> {
    let parsed_event_id = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO core.documents
            (id, kind, natural_key, parsed_event_id, extraction_version,
             chunk_count, text_byte_len, side_data, created_at, updated_at)
        VALUES ($1, $2, $3, $4, 1, 0, 0, '{}'::jsonb, now(), now())
        "#,
        id as Uuid,
        kind,
        natural_key,
        parsed_event_id as Uuid,
    )
    .execute(ctx.pool())
    .await?;
    Ok(())
}

async fn seed_chunk(
    ctx: &TestContext,
    document_id: Uuid,
    chunk_index: i32,
    text: &str,
) -> TestResult<()> {
    let chunked_event_id = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO core.document_chunks
            (document_id, chunk_index, text, byte_offset_start, byte_offset_end,
             source_anchor_start, source_anchor_end, chunked_event_id)
        VALUES ($1, $2, $3, 0, $4, NULL, NULL, $5)
        "#,
        document_id as Uuid,
        chunk_index,
        text,
        text.len() as i64,
        chunked_event_id as Uuid,
    )
    .execute(ctx.pool())
    .await?;
    Ok(())
}

#[sinex_test]
async fn documents_search_response_exposes_next_offset(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/gateway-pagination").await?;
    for index in 0..3 {
        seed_chunk(
            &ctx,
            doc_id,
            index,
            &format!("gateway pagination keyword chunk {index}"),
        )
        .await?;
    }

    let response = handle_documents_search(
        ctx.pool(),
        DocumentsSearchRequest {
            query: "pagination".to_string(),
            kind: Some("dendron_markdown".to_string()),
            document_ids: Some(vec![doc_id]),
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: Some(2),
            offset: Some(0),
        },
    )
    .await?;

    assert_eq!(response.results.len(), 2);
    assert!(response.has_more);
    assert_eq!(response.next_offset, Some(2));
    assert_eq!(response.empty_reason, None);
    Ok(())
}

#[sinex_test]
async fn documents_search_response_exposes_empty_reason(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/gateway-empty").await?;
    seed_chunk(&ctx, doc_id, 0, "gateway indexed text").await?;

    let response = handle_documents_search(
        ctx.pool(),
        DocumentsSearchRequest {
            query: "quantum".to_string(),
            kind: Some("dendron_markdown".to_string()),
            document_ids: Some(vec![doc_id]),
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: Some(10),
            offset: Some(0),
        },
    )
    .await?;

    assert!(response.results.is_empty());
    assert!(!response.has_more);
    assert_eq!(response.next_offset, None);
    assert_eq!(response.empty_reason.as_deref(), Some("no_match"));
    Ok(())
}
