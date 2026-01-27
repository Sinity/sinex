use xtask::sandbox::prelude::*;
use std::path::{Path, PathBuf};

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out)?;
        } else if path.extension().map(|ext| ext == "rs").unwrap_or(false) {
            out.push(path);
        }
    }
    Ok(())
}

#[sinex_test]
async fn pipeline_stream_naming_lint(_ctx: TestContext) -> TestResult<()> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let roots = [
        "crate/core/sinex-ingestd/tests",
        "tests/e2e/tests",
        "crate/lib/sinex-services/tests",
    ];

    let mut offenders = Vec::new();
    for root in roots {
        let mut files = Vec::new();
        collect_rs_files(&repo_root.join(root), &mut files)?;
        for file in files {
            let contents = std::fs::read_to_string(&file)?;
            if contents.contains("nats_stream_name_with_namespace(") {
                offenders.push(file);
            }
        }
    }

    ensure!(
        offenders.is_empty(),
        "Direct nats_stream_name_with_namespace usage found:\n{}",
        offenders
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join("\n")
    );

    Ok(())
}
