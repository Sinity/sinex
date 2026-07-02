use super::*;
use xtask_macros::sinex_test;

#[sinex_test]
async fn dev_bindings_manifest_contains_dogfood_source_families()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    std::fs::write(home.path().join(".zsh_history"), ": 1:0;echo test\n")?;
    let qutebrowser_dir = home.path().join(".local/share/qutebrowser");
    std::fs::create_dir_all(&qutebrowser_dir)?;
    std::fs::write(qutebrowser_dir.join("history.sqlite"), "")?;
    let chrome_dir = home.path().join(".config/chrome-ws/Default");
    std::fs::create_dir_all(&chrome_dir)?;
    std::fs::write(chrome_dir.join("History"), "")?;
    let manifest = generate_dev_source_bindings_manifest_for_home(
        Path::new("/workspace/sinex"),
        home.path(),
    );
    let source_ids = manifest
        .bindings
        .iter()
        .map(|binding| binding.source_id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        source_ids,
        vec![
            "terminal.zsh-history",
            "terminal.atuin-history",
            "browser.history",
            "browser.history",
            "git-commit-history",
            "fs",
            "system.journald",
        ]
    );
    assert!(
        manifest.comment.contains("SINEX_SOURCE_BINDINGS_PATH"),
        "manifest comment should include the env var needed to start the dogfood loop"
    );
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_skips_absent_zsh_history()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    let manifest = generate_dev_source_bindings_manifest_for_home(
        Path::new("/workspace/sinex"),
        home.path(),
    );
    let source_ids = manifest
        .bindings
        .iter()
        .map(|binding| binding.source_id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        source_ids,
        vec![
            "terminal.atuin-history",
            "git-commit-history",
            "fs",
            "system.journald",
        ]
    );
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_adds_qutebrowser_history_when_material_exists()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    let qutebrowser_dir = home.path().join(".local/share/qutebrowser");
    std::fs::create_dir_all(&qutebrowser_dir)?;
    let qutebrowser_history = qutebrowser_dir.join("history.sqlite");
    std::fs::write(&qutebrowser_history, "")?;
    let dump_dir = tempfile::tempdir()?;
    let dump_path = dump_dir.path().join("full_history.ndjson");
    std::fs::write(&dump_path, "{}\n")?;

    let manifest = generate_dev_source_bindings_manifest_for_home_and_exports(
        Path::new("/workspace/sinex"),
        home.path(),
        Some(&dump_path),
        None,
    );
    let browser = manifest
        .bindings
        .iter()
        .find(|binding| binding.source_id == "browser.history")
        .expect("browser binding exists");

    assert_eq!(
        browser.runtime_config["primary"]["path"],
        qutebrowser_history.to_string_lossy().as_ref()
    );
    assert_eq!(
        browser.runtime_config["primary"]["query"],
        "SELECT rowid, * FROM History"
    );
    assert_eq!(browser.runtime_config["primary"]["table"], "History");
    assert_eq!(
        browser.runtime_config["primary"]["read_only"], false,
        "qutebrowser history is WAL-backed; dev bindings must allow SQLite \
         sidecar recovery while still issuing only SELECT queries"
    );
    assert_eq!(browser.runtime_config["primary"]["immutable"], false);
    assert_eq!(
        browser.runtime_config["secondary"]["path"],
        dump_path.to_string_lossy().as_ref()
    );
    assert_eq!(browser.runtime_config["secondary"]["skip_empty"], true);
    assert_eq!(browser.runtime_config["interleaved"], false);
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_adds_chrome_history_when_material_exists()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    let chrome_dir = home.path().join(".config/chrome-ws/Default");
    std::fs::create_dir_all(&chrome_dir)?;
    let chrome_history = chrome_dir.join("History");
    std::fs::write(&chrome_history, "")?;

    let manifest = generate_dev_source_bindings_manifest_for_home(
        Path::new("/workspace/sinex"),
        home.path(),
    );
    let browser = manifest
        .bindings
        .iter()
        .find(|binding| binding.source_id == "browser.history")
        .expect("browser binding exists");

    assert_eq!(browser.instance_idx, 1);
    assert_eq!(
        browser.runtime_config["primary"]["path"],
        chrome_history.to_string_lossy().as_ref()
    );
    assert!(
        browser.runtime_config["primary"]["query"]
            .as_str()
            .expect("query string")
            .contains("visits JOIN urls"),
        "Chrome/Chromium history must use the visits+urls projection"
    );
    assert_eq!(browser.runtime_config["primary"]["table"], "visits");
    assert_eq!(browser.runtime_config["primary"]["read_only"], false);
    assert_eq!(browser.runtime_config["primary"]["immutable"], false);
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_adds_raindrop_bookmarks_when_export_exists()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    let export_dir = tempfile::tempdir()?;
    let export_path = export_dir.path().join("bookmarks.csv");
    std::fs::write(
        &export_path,
        "id,url,created,favorite\n1,https://example.com,2026-01-01T00:00:00Z,false\n",
    )?;

    let manifest = generate_dev_source_bindings_manifest_for_home_and_exports(
        Path::new("/workspace/sinex"),
        home.path(),
        None,
        Some(&export_path),
    );
    let raindrop = manifest
        .bindings
        .iter()
        .find(|binding| binding.source_id == "raindrop-bookmarks")
        .expect("raindrop binding exists");

    assert_eq!(
        raindrop.runtime_config["path"],
        export_path.to_string_lossy().as_ref()
    );
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_can_focus_selected_sources()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    let export_dir = tempfile::tempdir()?;
    let export_path = export_dir.path().join("bookmarks.csv");
    std::fs::write(&export_path, "id,url,created,favorite\n")?;
    let manifest = generate_dev_source_bindings_manifest_for_home_and_exports(
        Path::new("/workspace/sinex"),
        home.path(),
        None,
        Some(&export_path),
    );

    let focused = filter_dev_source_bindings_manifest(
        manifest,
        &[String::from("raindrop-bookmarks")],
        &[],
    )?;

    let source_ids = focused
        .bindings
        .iter()
        .map(|binding| binding.source_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(source_ids, vec!["raindrop-bookmarks"]);
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_can_exclude_heavy_sources()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    let qutebrowser_dir = home.path().join(".local/share/qutebrowser");
    std::fs::create_dir_all(&qutebrowser_dir)?;
    std::fs::write(qutebrowser_dir.join("history.sqlite"), "")?;
    let manifest = generate_dev_source_bindings_manifest_for_home(
        Path::new("/workspace/sinex"),
        home.path(),
    );

    let filtered =
        filter_dev_source_bindings_manifest(manifest, &[], &[String::from("browser.history")])?;

    assert!(
        filtered
            .bindings
            .iter()
            .all(|binding| binding.source_id != "browser.history")
    );
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_rejects_unknown_source_filter()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    let manifest = generate_dev_source_bindings_manifest_for_home(
        Path::new("/workspace/sinex"),
        home.path(),
    );

    let error =
        filter_dev_source_bindings_manifest(manifest, &[String::from("missing.source")], &[])
            .expect_err("unknown source filters must fail loudly");

    assert!(format!("{error:#}").contains("unknown --source value"));
    assert!(format!("{error:#}").contains("available dev sources"));
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_uses_watch_root_for_git_and_fs()
-> crate::sandbox::prelude::TestResult<()> {
    let home = tempfile::tempdir()?;
    let manifest = generate_dev_source_bindings_manifest_for_home(
        Path::new("/workspace/sinex"),
        home.path(),
    );
    let git = manifest
        .bindings
        .iter()
        .find(|binding| binding.source_id == "git-commit-history")
        .expect("git binding exists");
    let fs = manifest
        .bindings
        .iter()
        .find(|binding| binding.source_id == "fs")
        .expect("fs binding exists");

    assert_eq!(git.runtime_config["path"], "/workspace/sinex");
    assert_eq!(fs.runtime_config["watch_paths"][0], "/workspace/sinex");
    assert_eq!(git.runtime_config["continuous_poll_interval_secs"], 30);
    Ok(())
}

#[sinex_test]
async fn dev_bindings_manifest_uses_stable_service_names()
-> crate::sandbox::prelude::TestResult<()> {
    let manifest = generate_dev_source_bindings_manifest(Path::new("/workspace/sinex"));

    for binding in &manifest.bindings {
        assert_eq!(
            binding.service_name,
            format!("source-driver-{}-{}", binding.source_id, binding.instance_idx)
        );
        assert!(binding.extra_args.is_empty());
        assert!(binding.extra_env.is_empty());
    }
    Ok(())
}
