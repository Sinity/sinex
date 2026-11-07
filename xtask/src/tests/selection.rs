use super::*;
use std::{fs, path::Path};

fn package(root: &Path, directory: &str, name: &str, manifest: &str, source: &str, bin: bool) {
    let path = root.join(directory);
    fs::create_dir_all(path.join("src")).unwrap();
    fs::write(
        path.join("Cargo.toml"),
        format!("[package]\nname='{name}'\nversion='0.1.0'\nedition='2024'\n{manifest}"),
    )
    .unwrap();
    fs::write(
        path.join(if bin { "src/main.rs" } else { "src/lib.rs" }),
        source,
    )
    .unwrap();
}

#[devkit::test]
fn native_packages_and_targets_keep_daemon_unit_tests_without_unrelated_builds() {
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path();
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers=['model','store','sources','daemon','unrelated']\nresolver='3'\n",
    )
    .unwrap();
    package(
        root,
        "model",
        "sinex-model",
        "",
        "pub const VALUE:usize=42;\n#[cfg(test)] mod query { #[test] fn selected_model() {} }\n",
        false,
    );
    package(
        root,
        "sources",
        "sinex-sources",
        "[features]\ndefault=[]\nfull=[]\n",
        "pub fn value()->usize { if cfg!(feature=\"full\") {42} else {0} }\n#[cfg(test)] mod sources { mod analysis { #[test] fn selected_source() { assert_eq!(crate::value(),42); } } }\n",
        false,
    );
    package(
        root,
        "store",
        "sinex-store",
        "[dependencies]\nsinex-model={path='../model'}\nsinex-sources={path='../sources'}\n",
        "#[cfg(test)] mod query { #[test] fn selected_store() { assert_eq!(sinex_sources::value(),sinex_model::VALUE); } }\n",
        false,
    );
    package(
        root,
        "daemon",
        "sinexd",
        "[dependencies]\nsinex-sources={path='../sources',features=['full']}\n",
        "#[cfg(not(test))] compile_error!(\"the normal daemon executable is not needed\");\nfn main() {}\n#[cfg(test)] mod analysis { mod recall { #[test] fn selected_request() { assert_eq!(sinex_sources::value(),42); } } }\n",
        true,
    );
    package(
        root,
        "unrelated",
        "unrelated",
        "",
        "compile_error!(\"an unrelated package must not compile\");\n",
        false,
    );
    for (path, message) in [
        (
            "daemon/tests/unselected.rs",
            "an unselected integration target must not compile",
        ),
        (
            "daemon/src/bin/helper.rs",
            "an unselected helper binary must not compile",
        ),
        (
            "daemon/examples/unselected.rs",
            "an unselected example must not compile",
        ),
    ] {
        let path = root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            path,
            format!("compile_error!({message:?});\nfn main() {{}}\n"),
        )
        .unwrap();
    }
    let lock = devkit::cargo_command()
        .args(["generate-lockfile", "--offline"])
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        lock.status.success(),
        "{}",
        String::from_utf8_lossy(&lock.stderr)
    );

    let mut selected = options(&[
        "-p",
        "sinex-model",
        "-p",
        "sinex-store",
        "-p",
        "sinex-sources",
        "-p",
        "sinexd",
        "--lib",
        "--bin",
        "sinexd",
        "-E",
        "test(query::) | test(sources::analysis::) | test(analysis::recall::)",
    ]);
    // The nested fixture owns its build output and does not need durability scratch.
    selected.tmpfs = false;
    selected.env.push((
        "CARGO_TARGET_DIR".into(),
        root.join("build").display().to_string(),
    ));
    let mut run = Run::unrecorded("test-selection", "test", &[]);
    run.root = root.to_owned();
    let result = run.nextest("selected", &selected);
    assert!(result.is_ok(), "{result:?}\n{:?}", run.failure);
    let mut names: Vec<_> = run.tests.iter().map(|test| test.name.as_str()).collect();
    names.sort();
    assert_eq!(
        names,
        [
            "analysis::recall::selected_request",
            "query::selected_model",
            "query::selected_store",
            "sources::analysis::selected_source",
        ],
    );
    assert!(
        run.tests
            .iter()
            .all(|test| test.status == devkit::TestStatus::Passed)
    );
}
