//! Shared test scaffolding for the material assembler.
//!
//! Each test module previously rolled its own `test_assembler` helper with
//! identical content-store / state-dir setup. This module centralises the
//! boilerplate behind a small builder so tweaks (size limits, slice timeout)
//! stay readable.

#![cfg(test)]

use std::sync::Arc;

use camino::Utf8PathBuf;
use sinex_node_sdk::content_store::{ContentStoreConfig, MaterialContentStore};
use xtask::sandbox::prelude::*;

use super::MaterialAssembler;
use crate::MaterialReadySet;

const DEFAULT_MAX_MATERIAL_SIZE_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_SLICE_TIMEOUT_SECS: u64 = 300;
const DEFAULT_ORPHAN_THRESHOLD_SECS: u64 = 3_600;
const DEFAULT_DISK_THRESHOLD_PERCENT: u8 = 90;
const DEFAULT_SLICES_MAX_ACK_PENDING: i64 = 1_000;
const DEFAULT_FRAME_BUFFER: usize = 100;

/// Builder for `(MaterialAssembler, TempDir, TempDir)` triples used in tests.
///
/// The two returned tempdirs are kept by the caller so they outlive the
/// assembler — dropping them too early racks the assembler against an
/// unlinked state/content directory.
pub(super) struct TestAssemblerBuilder {
    label: &'static str,
    max_material_size_bytes: u64,
    slice_timeout_secs: u64,
    buffered_slice_limit: usize,
}

impl TestAssemblerBuilder {
    pub(super) fn new(label: &'static str) -> Self {
        Self {
            label,
            max_material_size_bytes: DEFAULT_MAX_MATERIAL_SIZE_BYTES,
            slice_timeout_secs: DEFAULT_SLICE_TIMEOUT_SECS,
            buffered_slice_limit: DEFAULT_FRAME_BUFFER,
        }
    }

    pub(super) fn max_material_size_bytes(mut self, v: u64) -> Self {
        self.max_material_size_bytes = v;
        self
    }

    pub(super) fn slice_timeout_secs(mut self, v: u64) -> Self {
        self.slice_timeout_secs = v;
        self
    }

    pub(super) fn buffered_slice_limit(mut self, v: usize) -> Self {
        self.buffered_slice_limit = v;
        self
    }

    pub(super) async fn build(
        self,
        ctx: &TestContext,
    ) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
        let content_store_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(content_store_dir.path().to_path_buf())
            .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
        MaterialContentStore::init(&repo_path, Some(self.label)).await?;
        let content_store = Arc::new(MaterialContentStore::new(ContentStoreConfig {
            root_path: repo_path,
            num_copies: None,
            large_files: None,
            ..Default::default()
        })?);

        let state_dir = tempfile::tempdir()?;
        let assembler = MaterialAssembler::new(
            ctx.nats_client(),
            ctx.pool.clone(),
            content_store,
            state_dir.path().to_path_buf(),
            Some(ctx.pipeline_namespace().prefix().to_string()),
            DEFAULT_SLICES_MAX_ACK_PENDING,
            Some(MaterialReadySet::default()),
            self.buffered_slice_limit,
            self.max_material_size_bytes,
            self.slice_timeout_secs,
            DEFAULT_ORPHAN_THRESHOLD_SECS,
            DEFAULT_DISK_THRESHOLD_PERCENT,
        )?;

        Ok((assembler, content_store_dir, state_dir))
    }
}

/// Convenience wrapper: default configuration with a caller-supplied label.
pub(super) async fn build_test_assembler(
    ctx: &TestContext,
    label: &'static str,
) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
    TestAssemblerBuilder::new(label).build(ctx).await
}

/// Build a content-store rooted at a fresh tempdir with the given label.
///
/// Used by tests that need just the content store (e.g. the
/// `assembler_rejects_unrepresentable_max_material_size` case which probes
/// constructor behaviour without ever touching the assembler proper).
pub(super) async fn build_test_content_store(
    label: &'static str,
) -> TestResult<(Arc<MaterialContentStore>, tempfile::TempDir)> {
    let content_store_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(content_store_dir.path().to_path_buf())
        .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
    MaterialContentStore::init(&repo_path, Some(label)).await?;
    let content_store = Arc::new(MaterialContentStore::new(ContentStoreConfig {
        root_path: repo_path,
        num_copies: None,
        large_files: None,
        ..Default::default()
    })?);
    Ok((content_store, content_store_dir))
}
