use super::builders::{
    def, step, v_arr_min, v_contains, v_empty, v_eq, v_has, v_json, v_lines, v_stderr,
};
use super::types::{ExerciseDef, InfraReq, Tier};

// ═══════════════════════════════════════════════════════════════════════════════
// Exercise catalog (~65 exercises)
// ═══════════════════════════════════════════════════════════════════════════════

#[allow(clippy::vec_init_then_push)] // 65-item catalog is clearer with push than vec![]
#[must_use]
pub fn build_catalog() -> Vec<ExerciseDef> {
    use super::types::ExpectedExit::{Any, Failure};
    use Tier::{T1, T2, T3, T4};

    let mut v = Vec::with_capacity(65);

    // ─── Tier 1: Fast / Read-Only (~30s total) ──────────────────────────────

    v.push(
        def("t1.help_root", "Root --help output", T1)
            .step(step("help", &["--help"]).v(v_contains("Developer tasks"))),
    );

    v.push(
        def("t1.help_check", "Check --help output", T1)
            .step(step("help", &["check", "--help"]).v(v_contains("--full"))),
    );

    v.push(
        def("t1.help_test", "Test --help output", T1)
            .step(step("help", &["test", "--help"]).v(v_contains("--debug"))),
    );

    v.push(
        def("t1.help_build", "Build --help output", T1)
            .step(step("help", &["build", "--help"]).v(v_contains("--release"))),
    );

    v.push(
        def("t1.list_commands_human", "List commands (human)", T1).step(
            step("list", &["--list-commands"])
                .v(v_contains("check"))
                .v(v_contains("test"))
                .v(v_contains("build"))
                .v(v_contains("status")),
        ),
    );

    v.push(
        def("t1.list_commands_json", "List commands (JSON)", T1).step(
            step("list", &["--list-commands", "--json"])
                .v(v_json())
                .v(v_has(&["commands", "version"])),
        ),
    );

    v.push(
        def("t1.list_commands_count", "Command count >= 15", T1).step(
            step("count", &["--list-commands", "--json"])
                .v(v_json())
                .v(v_arr_min("commands", 15)),
        ),
    );

    v.push(
        def("t1.status_summary_human", "Status summary (human)", T1)
            .step(step("summary", &["status", "--summary"]).v(v_lines(Some(1), None))),
    );

    v.push(
        def("t1.status_summary_json", "Status summary (JSON)", T1).step(
            step("summary", &["status", "--summary", "--json"])
                .v(v_json())
                .v(v_has(&["status"])),
        ),
    );

    v.push(
        def("t1.status_doctor_json", "Status doctor (JSON)", T1).step(
            step("doctor", &["status", "--doctor", "--json"])
                .v(v_json())
                .v(v_has(&["status", "data"])),
        ),
    );

    v.push(
        def("t1.deps_list_human", "Deps list (human)", T1)
            .step(step("deps", &["deps", "list"]).v(v_contains("sinex-primitives"))),
    );

    v.push(
        def("t1.deps_list_json", "Deps list (JSON)", T1)
            .step(step("deps", &["deps", "list", "--json"]).v(v_json())),
    );

    v.push(
        def("t1.deps_duplicates", "Deps duplicates (JSON)", T1)
            .step(step("dups", &["deps", "duplicates", "--json"]).v(v_json())),
    );

    v.push(
        def("t1.history_list_human", "History list (human)", T1)
            .step(step("history", &["history", "list", "--limit", "3"])),
    );

    v.push(
        def("t1.history_list_json", "History list (JSON)", T1)
            .step(step("history", &["history", "list", "--limit", "3", "--json"]).v(v_json())),
    );

    v.push(
        def("t1.jobs_list_human", "Jobs list (human)", T1).step(step("jobs", &["jobs", "list"])),
    );

    v.push(
        def("t1.jobs_list_json", "Jobs list (JSON)", T1).step(
            step("jobs", &["jobs", "list", "--json"])
                .v(v_json())
                .v(v_has(&["status", "data"])),
        ),
    );

    v.push(
        def("t1.jobs_active_json", "Jobs active (JSON)", T1)
            .step(step("active", &["jobs", "active", "--json"]).v(v_json())),
    );

    v.push(
        def("t1.format_silent", "Silent format produces no output", T1)
            .step(step("silent", &["status", "--summary", "--format", "silent"]).v(v_empty())),
    );

    v.push(
        def("t1.format_compact", "Compact format is 1-3 lines", T1).step(
            step("compact", &["status", "--summary", "--format", "compact"])
                .v(v_lines(Some(1), Some(3))),
        ),
    );

    v.push(
        def("t1.no_command_error", "No subcommand exits non-zero", T1).step(
            step("nocommand", &[])
                .exit(Failure)
                .v(v_stderr("No command")),
        ),
    );

    v.push(
        def("t1.invalid_flag", "Invalid flag exits non-zero", T1)
            .step(step("badflag", &["check", "--nonexistent"]).exit(Failure)),
    );

    v.push(
        def("t1.test_dry_run", "Test dry-run (JSON)", T1).step(
            step(
                "dryrun",
                &["test", "--dry-run", "--skip-preflight", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(def("t1.infra_env", "Infra env prints vars", T1).step(step("env", &["infra", "env"])));

    v.push(
        def("t1.fix_help", "Fix --help output", T1)
            .step(step("help", &["fix", "--help"]).v(v_contains("fix"))),
    );

    v.push(
        def("t1.docs_build_help", "Docs build --help output", T1)
            .step(step("help", &["docs", "build", "--help"]).v(v_contains("--open"))),
    );

    v.push(
        def(
            "t1.privacy_catalog_json",
            "Privacy catalog returns rules (JSON)",
            T1,
        )
        .step(
            step("catalog", &["--json", "privacy", "catalog"])
                .v(v_json())
                .v(v_has(&["status", "data"]))
                .v(v_arr_min("data", 1)),
        ),
    );

    v.push(
        def(
            "t1.privacy_test_clean_json",
            "Privacy test clean input (JSON)",
            T1,
        )
        .step(
            step("test", &["--json", "privacy", "test", "hello world"])
                .v(v_json())
                .v(v_has(&["status", "data"])),
        ),
    );

    v.push(
        def(
            "t1.privacy_key_generate_json",
            "Privacy key generate (JSON)",
            T1,
        )
        .step(
            step("key", &["--json", "privacy", "key", "--generate"])
                .v(v_json())
                .v(v_has(&["status", "data"])),
        ),
    );

    // ─── Tier 2: Moderate (~5min) ───────────────────────────────────────────

    v.push(
        def("t2.check_json", "Check with JSON output", T2).step(
            step("check", &["check", "--json", "--skip-tests"])
                .v(v_json())
                .v(v_eq("status", serde_json::json!("success"))),
        ),
    );

    v.push(
        def("t2.check_with_lint", "Check with clippy (--lint)", T2)
            .step(step("check", &["check", "--lint", "--skip-tests", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.check_with_fmt", "Check with fmt (--fmt)", T2)
            .step(step("check", &["check", "--fmt", "--skip-tests", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.check_package", "Check single package", T2)
            .step(step("check", &["check", "-p", "sinex-primitives", "--json"]).v(v_json())),
    );

    v.push(
        def(
            "t2.build_package",
            "Build single package (debug + release)",
            T2,
        )
        .step(step("debug", &["build", "-p", "sinex-primitives", "--json"]).v(v_json()))
        .step(
            step(
                "release",
                &["build", "-p", "sinex-primitives", "--release", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def(
            "t2.test_suite",
            "Test xtask: full suite + filter expression",
            T2,
        )
        .step(
            step(
                "full",
                &["test", "-p", "xtask", "--json", "--skip-preflight"],
            )
            .v(v_json()),
        )
        .step(
            step(
                "filter",
                &[
                    "test",
                    "-E",
                    "test(test_status_symbol)",
                    "-p",
                    "xtask",
                    "--skip-preflight",
                    "--json",
                ],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def("t2.deps_tree", "Deps tree (JSON)", T2).step(
            step(
                "tree",
                &["deps", "tree", "--package", "sinex-primitives", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def("t2.deps_unused", "Deps unused (JSON)", T2)
            .step(step("unused", &["deps", "unused", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.deps_timings", "Deps timings (JSON)", T2)
            .step(step("timings", &["deps", "timings", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.deps_impact", "Deps impact analysis", T2).step(
            step(
                "impact",
                &["deps", "impact", "--package", "sinex-primitives", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def("t2.status_full_json", "Full status (JSON)", T2).step(
            step("status", &["status", "--json"])
                .v(v_json())
                .v(v_has(&["status", "data"])),
        ),
    );

    v.push(
        def("t2.history_stats", "History stats (JSON)", T2).step(
            step(
                "stats",
                &["history", "stats", "--command", "check", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def("t2.history_export", "History export (JSON)", T2)
            .step(step("export", &["history", "export", "--limit", "3", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.infra_status", "Infra status (JSON)", T2)
            .infra(InfraReq::Both)
            .step(step("status", &["infra", "status", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.contracts_info", "Contracts info", T2)
            .infra(InfraReq::Postgres)
            .step(step("info", &["contracts", "info", "--json"]).exit(Any)),
    );

    v.push(
        def("t2.json_vs_human", "JSON vs human consistency", T2)
            .step(step("json", &["status", "--doctor", "--json"]).v(v_json()))
            .step(step("human", &["status", "--doctor"])),
    );

    v.push(
        def("t2.json_vs_compact", "JSON vs compact format", T2)
            .step(
                step("compact", &["status", "--doctor", "--format", "compact"])
                    .v(v_lines(Some(1), Some(5))),
            )
            .step(step("json", &["status", "--doctor", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.check_lint_breakdown", "Check lint-breakdown", T2).step(
            step(
                "check",
                &["check", "--lint-breakdown", "--json", "--skip-tests"],
            )
            .v(v_json()),
        ),
    );

    // ─── Tier 3: Heavy (~10min) ─────────────────────────────────────────────

    v.push(
        def("t3.check_full", "Full workspace check", T3)
            .step(step("check", &["check", "--all", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.build_workspace", "Full workspace build", T3)
            .step(step("build", &["build", "--all", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.test_primitives", "Test sinex-primitives", T3)
            .infra(InfraReq::Postgres)
            .step(step("test", &["test", "-p", "sinex-primitives", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.test_schema", "Test sinex-schema", T3)
            .infra(InfraReq::Postgres)
            .step(step("test", &["test", "-p", "sinex-schema", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.check_by_file", "Check by-file breakdown", T3)
            .step(step("check", &["check", "--by-file", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.history_analyze", "Test failure analysis", T3)
            .step(step("analyze", &["history", "tests", "analyze", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.history_slowest", "Slowest tests", T3)
            .step(step("slowest", &["history", "tests", "slowest", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.history_eta", "Test runtime ETA", T3)
            .step(step("eta", &["history", "tests", "eta", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.history_diagnostics", "Compiler diagnostic history", T3)
            .step(step("diags", &["history", "diagnostics", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.contracts_ready", "Schema verification", T3)
            .infra(InfraReq::Postgres)
            .step(step("ready", &["ci", "check-ready", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.infra_cycle", "Infra stop/start/status cycle", T3)
            .infra(InfraReq::Both)
            .step(step("stop", &["infra", "stop"]).exit(Any))
            .step(step("start", &["infra", "start"]))
            .step(step("status", &["infra", "status", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.deps_graph", "Dependency graph", T3)
            .step(step("graph", &["deps", "graph", "--json"]).v(v_json())),
    );

    // ─── Tier 4: Advanced Multi-Step ────────────────────────────────────────

    v.push(def("t4.bg_job_lifecycle", "Background job full lifecycle", T4).custom());
    v.push(def("t4.affected_clean", "Affected: clean state", T4).custom());
    v.push(def("t4.affected_leaf", "Affected: leaf crate changed", T4).custom());
    v.push(
        def(
            "t4.affected_foundation",
            "Affected: foundation crate changed",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.affected_workspace",
            "Affected: workspace-wide trigger",
            T4,
        )
        .custom(),
    );
    v.push(def("t4.history_roundtrip", "History tracking roundtrip", T4).custom());
    v.push(def("t4.output_format_matrix", "Output format matrix", T4).custom());
    v.push(def("t4.jobs_prune", "Jobs prune safety boundary", T4).custom());

    // Coordinator exercises — validate deduplication decision matrix
    v.push(
        def(
            "t4.coord_fresh_check",
            "Coordinator: Fresh detection (check→re-check)",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.coord_attach_check",
            "Coordinator: Attach to running job",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.coord_scope_isolation",
            "Coordinator: Scope key isolates packages",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.coord_state_update",
            "Coordinator: State updated with real job_id+pid",
            T4,
        )
        .custom(),
    );

    // Extended coordinator exercises — validate FIFO queue and supersede behavior
    v.push(
        def(
            "t4.coord_supersede",
            "Coordinator: Supersede stale bg job on tree change",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.coord_queue_no_overwrite",
            "Coordinator: Multiple queued jobs are preserved (FIFO)",
            T4,
        )
        .custom(),
    );

    // Extended affected exercise
    v.push(
        def(
            "t4.affected_transitive",
            "Affected: transitive dependents included",
            T4,
        )
        .custom(),
    );

    // Extended job exercise
    v.push(
        def(
            "t4.jobs_output_while_running",
            "Jobs: output readable while job is running",
            T4,
        )
        .custom(),
    );

    // F6: Observability and query contract exercises
    v.push(
        def(
            "t4.preflight_stages_in_history",
            "History: preflight stage appears in stage timings after check",
            T4,
        )
        .custom(),
    );

    v.push(
        def(
            "t4.live_stage_visible_during_run",
            "History: live_stage field queryable via jobs status during bg run",
            T4,
        )
        .custom(),
    );

    v.push(
        def(
            "t4.diagnostic_delta_roundtrip",
            "History: diagnostic query returns valid JSON after a check run",
            T4,
        )
        .custom(),
    );

    v.push(
        def(
            "t4.history_stages_populated",
            "History: stage timings non-empty for latest check invocation",
            T4,
        )
        .custom(),
    );

    v.push(
        def(
            "t4.analytics_recommend_runs",
            "Analytics: recommend subcommand returns valid JSON",
            T4,
        )
        .custom(),
    );

    v
}
