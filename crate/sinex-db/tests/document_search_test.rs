//! Integration tests for `DocumentSearchRepository`.
//!
//! Seeds `core.documents` + `core.document_chunks` via direct SQL inserts
//! (not through the event pipeline — we are testing the read path, not the
//! projection trigger) and then exercises FTS, trigram fallback, metadata
//! filters, and pagination.

use sinex_db::repositories::{DbPoolExt, DocumentSearchQuery, SearchEmptyReason};
use sinex_primitives::Uuid;
use xtask::sandbox::prelude::*;

// ---------------------------------------------------------------------------
// Seed helpers
// ---------------------------------------------------------------------------

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
        ON CONFLICT (id) DO NOTHING
        "#,
        id as Uuid,
        kind,
        natural_key,
        parsed_event_id as Uuid,
    )
    .execute(&ctx.pool)
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
        ON CONFLICT (document_id, chunk_index) DO NOTHING
        "#,
        document_id as Uuid,
        chunk_index,
        text,
        text.len() as i64,
        chunked_event_id as Uuid,
    )
    .execute(&ctx.pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// FTS: single-word query finds the matching chunk.
#[sinex_test]
async fn document_search_fts_single_word(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/rust-async").await?;
    seed_chunk(
        &ctx,
        doc_id,
        0,
        "Rust async programming with tokio and futures",
    )
    .await?;
    seed_chunk(&ctx, doc_id, 1, "Unrelated content about cooking recipes").await?;

    let repo = ctx.pool.documents();
    let results = repo
        .search(&DocumentSearchQuery {
            query: "tokio".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: None,
            offset: None,
            vector_params: None,
        })
        .await?;

    assert!(!results.results.is_empty(), "expected at least one FTS hit");
    assert_eq!(
        results.search_mode,
        sinex_db::repositories::SearchMode::Fts,
        "expected FTS path"
    );
    // chunk 0 should appear
    assert!(
        results.results.iter().any(|r| r.chunk_index == 0),
        "chunk 0 should be in results"
    );
    Ok(())
}

/// FTS: multi-word query (AND semantics by default in websearch_to_tsquery).
#[sinex_test]
async fn document_search_fts_multi_word(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/async-rust").await?;
    seed_chunk(
        &ctx,
        doc_id,
        0,
        "Async Rust channels allow communication between tasks",
    )
    .await?;
    seed_chunk(&ctx, doc_id, 1, "Only async here without the other keyword").await?;

    let repo = ctx.pool.documents();
    let results = repo
        .search(&DocumentSearchQuery {
            query: "async channels".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: None,
            offset: None,
            vector_params: None,
        })
        .await?;

    assert_eq!(results.search_mode, sinex_db::repositories::SearchMode::Fts);
    // chunk 0 has both "async" and "channels"
    let hit_chunk_indices: Vec<i32> = results
        .results
        .iter()
        .filter(|r| r.document_id == doc_id)
        .map(|r| r.chunk_index)
        .collect();
    assert!(
        hit_chunk_indices.contains(&0),
        "chunk 0 (both terms) should be in results"
    );
    Ok(())
}

/// FTS: websearch negation operator `-excluded`.
#[sinex_test]
async fn document_search_fts_exclusion_operator(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/systems").await?;
    seed_chunk(&ctx, doc_id, 0, "Memory management in systems programming").await?;
    seed_chunk(
        &ctx,
        doc_id,
        1,
        "Memory management without garbage collection",
    )
    .await?;

    let repo = ctx.pool.documents();
    // Exclude chunks mentioning "systems"
    let results = repo
        .search(&DocumentSearchQuery {
            query: "memory -systems".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: None,
            offset: None,
            vector_params: None,
        })
        .await?;

    // chunk 1 should appear (has "memory", no "systems")
    // chunk 0 might be excluded (has "systems")
    if results.search_mode == sinex_db::repositories::SearchMode::Fts {
        for r in &results.results {
            if r.document_id == doc_id {
                assert_ne!(r.chunk_index, 0, "chunk 0 contains excluded term 'systems'");
            }
        }
    }
    Ok(())
}

/// Trigram fallback: typo'd query returns no FTS results, falls back to similarity.
#[sinex_test]
async fn document_search_trigram_fallback(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/postgres").await?;
    seed_chunk(
        &ctx,
        doc_id,
        0,
        "PostgreSQL full-text search with websearch_to_tsquery",
    )
    .await?;

    let repo = ctx.pool.documents();
    // "postgresl" is a deliberate typo — FTS won't match, trigram should
    let results = repo
        .search(&DocumentSearchQuery {
            query: "postgresl".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: None,
            offset: None,
            vector_params: None,
        })
        .await?;

    // With enough text in the chunk, trigram similarity should fire
    assert_eq!(
        results.search_mode,
        sinex_db::repositories::SearchMode::TrigramFallback,
        "typo'd query should trigger trigram fallback"
    );
    Ok(())
}

/// Metadata filter: `kind` restricts results to the specified kind.
#[sinex_test]
async fn document_search_kind_filter(ctx: TestContext) -> TestResult<()> {
    let md_doc = Uuid::now_v7();
    let term_doc = Uuid::now_v7();
    seed_document(&ctx, md_doc, "dendron_markdown", "notes/filtering-md").await?;
    seed_document(&ctx, term_doc, "terminal_output", "term/filtering-term").await?;
    seed_chunk(
        &ctx,
        md_doc,
        0,
        "database indexing strategies for performance",
    )
    .await?;
    seed_chunk(
        &ctx,
        term_doc,
        0,
        "database indexing command output for performance",
    )
    .await?;

    let repo = ctx.pool.documents();
    let results = repo
        .search(&DocumentSearchQuery {
            query: "indexing".to_string(),
            kind: Some("dendron_markdown".to_string()),
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: None,
            offset: None,
            vector_params: None,
        })
        .await?;

    for r in &results.results {
        assert_eq!(
            r.kind, "dendron_markdown",
            "kind filter should exclude terminal_output results"
        );
    }
    Ok(())
}

/// Metadata filter: `natural_key_prefix` scopes to path prefix.
#[sinex_test]
async fn document_search_natural_key_prefix_filter(ctx: TestContext) -> TestResult<()> {
    let doc_a = Uuid::now_v7();
    let doc_b = Uuid::now_v7();
    seed_document(&ctx, doc_a, "dendron_markdown", "projects/sinex/overview").await?;
    seed_document(&ctx, doc_b, "dendron_markdown", "journal/2026-05-01").await?;
    seed_chunk(&ctx, doc_a, 0, "sinex architecture design patterns").await?;
    seed_chunk(&ctx, doc_b, 0, "sinex daily progress update notes").await?;

    let repo = ctx.pool.documents();
    let results = repo
        .search(&DocumentSearchQuery {
            query: "sinex".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: Some("projects/sinex/".to_string()),
            updated_after: None,
            updated_before: None,
            limit: None,
            offset: None,
            vector_params: None,
        })
        .await?;

    assert_eq!(results.search_mode, sinex_db::repositories::SearchMode::Fts);
    for r in &results.results {
        assert!(
            r.natural_key.starts_with("projects/sinex/"),
            "natural_key_prefix filter should restrict results, got: {}",
            r.natural_key
        );
    }
    Ok(())
}

/// Metadata filter: `document_ids` scopes to specific documents.
#[sinex_test]
async fn document_search_document_ids_filter(ctx: TestContext) -> TestResult<()> {
    let doc_a = Uuid::now_v7();
    let doc_b = Uuid::now_v7();
    seed_document(&ctx, doc_a, "dendron_markdown", "notes/a-doc-ids").await?;
    seed_document(&ctx, doc_b, "dendron_markdown", "notes/b-doc-ids").await?;
    seed_chunk(&ctx, doc_a, 0, "network protocols and latency measurements").await?;
    seed_chunk(&ctx, doc_b, 0, "network protocols and throughput analysis").await?;

    let repo = ctx.pool.documents();
    let results = repo
        .search(&DocumentSearchQuery {
            query: "protocols".to_string(),
            kind: None,
            document_ids: Some(vec![doc_a]),
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: None,
            offset: None,
            vector_params: None,
        })
        .await?;

    for r in &results.results {
        assert_eq!(
            r.document_id, doc_a,
            "document_ids filter should restrict to doc_a only"
        );
    }
    Ok(())
}

/// Pagination: page 1 and page 2 are non-overlapping.
#[sinex_test]
async fn document_search_pagination_non_overlap(ctx: TestContext) -> TestResult<()> {
    // Insert 5 chunks on the same document with the same keyword so we have
    // enough results to paginate.
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/pagination").await?;
    for i in 0..5i32 {
        seed_chunk(
            &ctx,
            doc_id,
            i,
            &format!("pagination keyword appears in chunk number {i}"),
        )
        .await?;
    }

    let repo = ctx.pool.documents();
    let page1 = repo
        .search(&DocumentSearchQuery {
            query: "pagination".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: Some(2),
            offset: Some(0),
            vector_params: None,
        })
        .await?;

    let page2 = repo
        .search(&DocumentSearchQuery {
            query: "pagination".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: Some(2),
            offset: Some(2),
            vector_params: None,
        })
        .await?;

    assert!(
        page1.has_more,
        "page 1 should advertise another page when more rows exist"
    );
    assert!(
        page2.has_more,
        "page 2 should advertise another page when more rows exist"
    );
    let p1_indices: std::collections::HashSet<i32> =
        page1.results.iter().map(|r| r.chunk_index).collect();
    let p2_indices: std::collections::HashSet<i32> =
        page2.results.iter().map(|r| r.chunk_index).collect();

    assert_eq!(
        p1_indices.intersection(&p2_indices).count(),
        0,
        "page 1 and page 2 must not overlap"
    );
    assert!(!page1.results.is_empty(), "page 1 should have results");
    Ok(())
}

#[sinex_test]
async fn document_search_empty_reason_distinguishes_no_match_from_no_index(
    ctx: TestContext,
) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/no-match").await?;
    seed_chunk(&ctx, doc_id, 0, "indexed text about deterministic systems").await?;

    let repo = ctx.pool.documents();
    let no_match = repo
        .search(&DocumentSearchQuery {
            query: "quantum".to_string(),
            kind: Some("dendron_markdown".to_string()),
            document_ids: Some(vec![doc_id]),
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: Some(10),
            offset: Some(0),
            vector_params: None,
        })
        .await?;
    assert!(no_match.results.is_empty());
    assert_eq!(no_match.empty_reason, Some(SearchEmptyReason::NoMatch));
    assert!(!no_match.has_more);

    let no_indexed_text = repo
        .search(&DocumentSearchQuery {
            query: "anything".to_string(),
            kind: Some("dendron_markdown".to_string()),
            document_ids: Some(vec![Uuid::now_v7()]),
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: Some(10),
            offset: Some(0),
            vector_params: None,
        })
        .await?;
    assert!(no_indexed_text.results.is_empty());
    assert_eq!(
        no_indexed_text.empty_reason,
        Some(SearchEmptyReason::NoIndexedText)
    );
    assert!(!no_indexed_text.has_more);
    Ok(())
}

/// Pagination: results are ordered by score DESC, stable across pages.
#[sinex_test]
async fn document_search_pagination_ordering_stable(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/ordering").await?;
    for i in 0..5i32 {
        seed_chunk(&ctx, doc_id, i, &format!("ordering test keyword chunk {i}")).await?;
    }

    let repo = ctx.pool.documents();
    let all_results = repo
        .search(&DocumentSearchQuery {
            query: "ordering".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: Some(10),
            offset: Some(0),
            vector_params: None,
        })
        .await?;

    // Verify non-increasing score order
    let scores: Vec<f64> = all_results.results.iter().map(|r| r.score).collect();
    for window in scores.windows(2) {
        assert!(
            window[0] >= window[1],
            "results must be ordered by score DESC, got {} before {}",
            window[0],
            window[1]
        );
    }
    Ok(())
}

/// `get_document` returns the expected document record.
#[sinex_test]
async fn document_search_get_document(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "terminal_output", "term/get-document-test").await?;

    let repo = ctx.pool.documents();
    let doc = repo.get_document(doc_id).await?;

    assert!(
        doc.is_some(),
        "get_document should find the seeded document"
    );
    let doc = doc.unwrap();
    assert_eq!(doc.kind, "terminal_output");
    assert_eq!(doc.natural_key, "term/get-document-test");
    Ok(())
}

/// `get_document` returns None for an unknown ID.
#[sinex_test]
async fn document_search_get_document_not_found(ctx: TestContext) -> TestResult<()> {
    let unknown_id = Uuid::now_v7();
    let repo = ctx.pool.documents();
    let doc = repo.get_document(unknown_id).await?;
    assert!(
        doc.is_none(),
        "get_document should return None for unknown id"
    );
    Ok(())
}

/// `get_document_chunks` returns chunks in chunk_index order.
#[sinex_test]
async fn document_search_get_document_chunks(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/chunks-test").await?;
    seed_chunk(&ctx, doc_id, 2, "third chunk").await?;
    seed_chunk(&ctx, doc_id, 0, "first chunk").await?;
    seed_chunk(&ctx, doc_id, 1, "second chunk").await?;

    let repo = ctx.pool.documents();
    let chunks = repo.get_document_chunks(doc_id, 10, 0).await?;

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].chunk_index, 0);
    assert_eq!(chunks[1].chunk_index, 1);
    assert_eq!(chunks[2].chunk_index, 2);
    Ok(())
}

/// `get_document_chunks` paginates correctly with limit + offset.
#[sinex_test]
async fn document_search_get_document_chunks_pagination(ctx: TestContext) -> TestResult<()> {
    let doc_id = Uuid::now_v7();
    seed_document(&ctx, doc_id, "dendron_markdown", "notes/chunks-pagination").await?;
    for i in 0..5i32 {
        seed_chunk(&ctx, doc_id, i, &format!("chunk text {i}")).await?;
    }

    let repo = ctx.pool.documents();
    let page1 = repo.get_document_chunks(doc_id, 2, 0).await?;
    let page2 = repo.get_document_chunks(doc_id, 2, 2).await?;
    let page3 = repo.get_document_chunks(doc_id, 2, 4).await?;

    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0].chunk_index, 0);
    assert_eq!(page2.len(), 2);
    assert_eq!(page2[0].chunk_index, 2);
    assert_eq!(page3.len(), 1);
    assert_eq!(page3[0].chunk_index, 4);
    Ok(())
}

/// Empty query returns a validation error (not a DB error).
#[sinex_test]
async fn document_search_empty_query_error(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.documents();
    let result = repo
        .search(&DocumentSearchQuery {
            query: "   ".to_string(),
            kind: None,
            document_ids: None,
            natural_key_prefix: None,
            updated_after: None,
            updated_before: None,
            limit: None,
            offset: None,
            vector_params: None,
        })
        .await;

    assert!(result.is_err(), "empty query should return an error");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("cannot be empty"),
        "error message should mention empty query, got: {err}"
    );
    Ok(())
}
