use crate::command_catalog::{ArgInfo, CommandInfo, collect_global_args, find_command};

struct HelpCategory {
    title: &'static str,
    command_paths: &'static [&'static str],
}

struct GuideSection {
    title: &'static str,
    intro: &'static str,
    entries: &'static [GuideEntry],
}

struct GuideEntry {
    path: &'static str,
    fallback_summary: &'static str,
    when: &'static str,
    examples: &'static [&'static str],
    notes: &'static [&'static str],
}

const HELP_CATEGORIES: &[HelpCategory] = &[
    HelpCategory {
        title: "Development",
        command_paths: &["fix", "check", "test", "build", "work"],
    },
    HelpCategory {
        title: "Runtime",
        command_paths: &["run", "infra", "jobs", "status"],
    },
    HelpCategory {
        title: "Analysis",
        command_paths: &["deps", "history", "analytics", "git-stack"],
    },
    HelpCategory {
        title: "Diagnostics",
        command_paths: &["doctor", "privacy"],
    },
    HelpCategory {
        title: "Documentation",
        command_paths: &["docs"],
    },
    HelpCategory {
        title: "Maintenance",
        command_paths: &["exercise", "reset"],
    },
];

const GUIDE_SECTIONS: &[GuideSection] = &[
    GuideSection {
        title: "Core Loop",
        intro: "These are the commands worth remembering for day-to-day code changes.",
        entries: &[
            GuideEntry {
                path: "check",
                fallback_summary: "Fast compile/lint validation",
                when: "you changed Rust code and want the default fast correctness pass",
                examples: &["xtask check", "xtask check --lint", "xtask check --full"],
                notes: &[],
            },
            GuideEntry {
                path: "fix",
                fallback_summary: "Apply automatic formatting and lint fixes",
                when: "formatting or clippy fixes are mechanical and you want the repo-approved autofix pass first",
                examples: &["xtask fix", "xtask fix --check"],
                notes: &[],
            },
            GuideEntry {
                path: "test",
                fallback_summary: "Main test entrypoint",
                when: "you need package tests, a targeted filter, or the normal preflight-aware local test loop",
                examples: &[
                    "xtask test",
                    "xtask test -p sinex-primitives",
                    "xtask test --debug -E 'test(name)'",
                    "xtask test --heavy",
                ],
                notes: &[
                    "Do not use bare cargo test when xtask already exposes the surface you need.",
                ],
            },
            GuideEntry {
                path: "work",
                fallback_summary: "Run the standard local workflow pipeline",
                when: "you want the standard end-to-end local validation path",
                examples: &["xtask work"],
                notes: &[],
            },
            GuideEntry {
                path: "build",
                fallback_summary: "Build workspace packages",
                when: "you need binaries or build artifacts",
                examples: &["xtask build", "xtask build -p sinex-gateway"],
                notes: &[],
            },
        ],
    },
    GuideSection {
        title: "Runtime & Infra",
        intro: "Use these when the local stack or a running process is part of the work.",
        entries: &[
            GuideEntry {
                path: "infra start",
                fallback_summary: "Start local infrastructure",
                when: "local Postgres and NATS need to be available for checks, tests, or manual runs",
                examples: &[
                    "xtask infra start",
                    "xtask infra status",
                    "xtask infra stop",
                ],
                notes: &[],
            },
            GuideEntry {
                path: "infra flake-stage",
                fallback_summary: "Stage a flake-safe checkout copy",
                when: "you need a dirty checkout, untracked files, or a runtime-socket-filled repo to work as a local Nix or nixos-rebuild input",
                examples: &[
                    "xtask infra flake-stage",
                    "xtask infra flake-stage --output-dir /tmp/sinex-flake-verify --force",
                ],
                notes: &[
                    "The staged tree excludes local-only runtime and build artifacts such as `.git`, `.sinex`, `target`, `result*`, and `.direnv`.",
                ],
            },
            GuideEntry {
                path: "run",
                fallback_summary: "Run sinex binaries",
                when: "you need to launch a node, ingest daemon, or gateway process during development",
                examples: &[
                    "xtask run node terminal-ingestor --watch",
                    "xtask run gateway",
                ],
                notes: &[],
            },
            GuideEntry {
                path: "status",
                fallback_summary: "Show workspace and service health",
                when: "you want a quick read on infra/runtime state before or after a change",
                examples: &["xtask status", "xtask status --summary"],
                notes: &[],
            },
            GuideEntry {
                path: "doctor",
                fallback_summary: "Health check and auto-remediation",
                when: "the environment may be broken, stale, or missing expected dependencies",
                examples: &[
                    "xtask doctor",
                    "xtask doctor --fix",
                    "xtask doctor --runtime",
                ],
                notes: &[],
            },
            GuideEntry {
                path: "jobs output",
                fallback_summary: "Read output for a background job",
                when: "you launched a long-running command with --bg and need its logs or current state",
                examples: &[
                    "xtask check --bg",
                    "xtask jobs status 42",
                    "xtask jobs output 42",
                ],
                notes: &[
                    "Use xtask history when you need trends, diagnostics, or durable execution records beyond the live job handle.",
                ],
            },
            GuideEntry {
                path: "reset",
                fallback_summary: "Wipe developer state",
                when: "local state is genuinely corrupted and you need a deliberate clean-slate reset",
                examples: &[
                    "xtask reset --yes",
                    "xtask reset --yes --db",
                    "xtask reset --yes --target",
                ],
                notes: &[
                    "This is destructive. Scope it to the smallest reset that fixes the problem.",
                ],
            },
        ],
    },
    GuideSection {
        title: "Investigation & Verification",
        intro: "Reach for these after failures or when you need to understand impact and trends.",
        entries: &[
            GuideEntry {
                path: "history diagnostics",
                fallback_summary: "Inspect recorded diagnostics",
                when: "a check or build failed and you want package-scoped errors, trends, or fixable subsets",
                examples: &[
                    "xtask history diagnostics --level error",
                    "xtask history diagnostics --fixable",
                    "xtask history diagnostics --package sinex-primitives",
                ],
                notes: &[],
            },
            GuideEntry {
                path: "history tests analyze",
                fallback_summary: "Analyze recent test execution history",
                when: "a test surface failed and you need buckets, flaky tests, slow tests, or captured output",
                examples: &[
                    "xtask history tests analyze",
                    "xtask history tests failures --output",
                    "xtask history tests output test_name",
                ],
                notes: &[],
            },
            GuideEntry {
                path: "analytics workspace-health",
                fallback_summary: "Compute a composite workspace health score",
                when: "you want a compact signal about repo health or follow-up recommendations",
                examples: &[
                    "xtask analytics workspace-health",
                    "xtask analytics recommend",
                ],
                notes: &[],
            },
            GuideEntry {
                path: "deps impact",
                fallback_summary: "Analyze rebuild impact",
                when: "a dependency change might widen the rebuild/test blast radius",
                examples: &["xtask deps impact", "xtask deps impact sinex-gateway"],
                notes: &[],
            },
            GuideEntry {
                path: "git-stack split",
                fallback_summary: "Plan and materialize a stacked PR split from the current branch",
                when: "a long local commit train needs to be broken into reviewable slices with generated PR bodies and squash commits",
                examples: &[
                    "xtask git-stack plan --base origin/master",
                    "xtask git-stack split --base origin/master --branch-prefix pr-stack",
                ],
                notes: &[
                    "The planner records dirty-worktree loose ends and materializes branches in a temporary worktree so the active checkout is not rewritten.",
                ],
            },
            GuideEntry {
                path: "git-stack publish",
                fallback_summary: "Push a materialized stack and open or reuse chained PRs",
                when: "you already generated stack branches locally and want to publish them to a remote with generated PR bodies",
                examples: &[
                    "xtask git-stack publish --plan .sinex/git-stack/master-split/plan.yaml",
                    "xtask git-stack publish --plan .sinex/git-stack/master-split/plan.yaml --push-only",
                ],
                notes: &[
                    "PR creation reuses existing pull requests when possible and uses the generated per-slice `pr-body.md` files.",
                ],
            },
            GuideEntry {
                path: "test vm",
                fallback_summary: "Run NixOS VM checks",
                when: "a change touches deployment or runtime behavior and you need VM coverage beyond the normal Rust/package loop",
                examples: &[
                    "xtask test vm --category smoke",
                    "xtask test vm --category integration",
                ],
                notes: &["The default GitHub Actions gate does not run the full VM suite."],
            },
        ],
    },
    GuideSection {
        title: "Docs & Context",
        intro: "These keep generated repo surfaces current and produce scoped AI context when needed.",
        entries: &[
            GuideEntry {
                path: "docs sync",
                fallback_summary: "Refresh generated repo surfaces",
                when: "you changed CLAUDE transclusions, xtask docs plumbing, or the Rust EventPayload schema registry and want the generated surfaces refreshed together",
                examples: &["xtask docs sync"],
                notes: &[],
            },
            GuideEntry {
                path: "docs check",
                fallback_summary: "Verify generated repo-surface drift",
                when: "you want CI-style drift detection for generated docs and the checked-in schema bundle without rewriting files",
                examples: &["xtask docs check"],
                notes: &[],
            },
            GuideEntry {
                path: "docs agents",
                fallback_summary: "Regenerate AGENTS.md",
                when: "you only changed CLAUDE.md or its transcluded includes and need the local agent surface refreshed",
                examples: &["xtask docs agents"],
                notes: &[],
            },
            GuideEntry {
                path: "docs snapshot",
                fallback_summary: "Generate an AI context snapshot",
                when: "you need a scoped workspace snapshot for another agent or context-heavy debugging task",
                examples: &[
                    "xtask docs snapshot --changed --context",
                    "xtask docs snapshot --scope sinex-db",
                ],
                notes: &[],
            },
        ],
    },
];

#[must_use]
pub fn render_commands_help(commands: &[CommandInfo]) -> String {
    use std::io::IsTerminal;

    let use_color = std::io::stdout().is_terminal();
    let mut out = String::from("Commands:\n");

    for (index, category) in HELP_CATEGORIES.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        if use_color {
            out.push_str(&format!("  \x1b[1;4m{}\x1b[0m\n", category.title));
        } else {
            out.push_str(&format!("  {}:\n", category.title));
        }
        for path in category.command_paths {
            let command = lookup_command(commands, path);
            let summary = command_summary(command, "No summary available");
            out.push_str(&format!("    {path:<12}{summary}\n"));
        }
    }

    out
}

#[must_use]
pub fn render_command_guide(commands: &[CommandInfo]) -> String {
    let mut out = String::new();
    out.push_str("# xtask Command Guide\n\n");
    out.push_str(
        "<!-- Auto-generated by `xtask docs command-guide`. Do not edit manually. -->\n\n",
    );
    out.push_str(
        "This guide covers the public xtask commands that humans and agents are expected to reach for during local work.\n",
    );
    out.push_str(
        "It is intentionally selective: hidden automation plumbing and one-off implementation details stay out of this surface.\n\n",
    );
    out.push_str("## Agent Defaults\n\n");
    out.push_str("- Prefer `--json` or `--format json` when another tool will parse the output.\n");
    out.push_str("- Use `--bg` for long-running work you want to inspect through `xtask jobs`.\n");
    out.push_str("- Use `xtask <command> --help` only to confirm the exact live flags for commands already named below.\n\n");

    for section in GUIDE_SECTIONS {
        out.push_str(&format!("## {}\n\n", section.title));
        out.push_str(section.intro);
        out.push_str("\n\n");
        for entry in section.entries {
            let command = lookup_command(commands, entry.path);
            let summary = sentence(command_summary(command, entry.fallback_summary));
            out.push_str(&format!(
                "- `{}`: {} Use when {}.",
                invocation(entry.path),
                summary,
                entry.when
            ));
            if !entry.examples.is_empty() {
                out.push_str(" Common forms: ");
                out.push_str(
                    &entry
                        .examples
                        .iter()
                        .map(|example| format!("`{example}`"))
                        .collect::<Vec<_>>()
                        .join("; "),
                );
                out.push('.');
            }
            if !entry.notes.is_empty() {
                out.push_str(" Notes: ");
                out.push_str(&entry.notes.join(" "));
            }
            out.push('\n');
        }
        out.push('\n');
    }

    out
}

#[must_use]
pub fn render_command_reference(commands: &[CommandInfo]) -> String {
    let mut out = String::new();
    out.push_str("# xtask Command Reference\n\n");
    out.push_str(
        "<!-- Auto-generated by `xtask docs command-reference`. Do not edit manually. -->\n\n",
    );
    out.push_str("This reference is generated from xtask's public clap command tree.\n");
    out.push_str(
        "It deliberately excludes hidden automation-only plumbing that is not part of the supported operator surface.\n\n",
    );
    out.push_str(
        "Regenerate with `xtask docs sync` or `xtask docs command-reference`; verify drift with `xtask docs check`.\n\n",
    );

    let global_args = collect_global_args();
    if !global_args.is_empty() {
        out.push_str("## Global Flags\n\n");
        out.push_str("| Flag | Value | Description |\n|---|---|---|\n");
        for arg in &global_args {
            out.push_str(&format!(
                "| `{}` | {} | {} |\n",
                render_flag(arg),
                if arg.takes_value { "yes" } else { "no" },
                markdown_cell(arg.help.as_deref().unwrap_or(""))
            ));
        }
        out.push('\n');
    }

    out.push_str("## Top-Level Commands\n\n");
    out.push_str("| Command | Purpose |\n|---|---|\n");
    for command in commands {
        out.push_str(&format!(
            "| `{}` | {} |\n",
            command.name,
            markdown_cell(command.about.as_deref().unwrap_or(""))
        ));
    }

    for command in commands {
        render_command_section(&mut out, command, 2, &command.name);
    }

    out
}

fn render_command_section(
    out: &mut String,
    command: &CommandInfo,
    heading_level: usize,
    path: &str,
) {
    let hashes = "#".repeat(heading_level);
    out.push_str(&format!("\n{hashes} `{}`\n\n", invocation(path)));

    if let Some(about) = command.about.as_deref() {
        out.push_str(about);
        out.push_str("\n\n");
    }

    let local_args: Vec<&ArgInfo> = command.args.iter().filter(|arg| !arg.global).collect();
    if !local_args.is_empty() {
        out.push_str("**Arguments**\n\n");
        out.push_str("| Flag | Value | Required | Description |\n|---|---|---|---|\n");
        for arg in local_args {
            out.push_str(&format!(
                "| `{}` | {} | {} | {} |\n",
                render_flag(arg),
                if arg.takes_value { "yes" } else { "no" },
                if arg.required { "yes" } else { "no" },
                markdown_cell(arg.help.as_deref().unwrap_or(""))
            ));
        }
        out.push('\n');
    }

    if !command.subcommands.is_empty() {
        out.push_str("**Subcommands**\n\n");
        out.push_str("| Command | Purpose |\n|---|---|\n");
        for subcommand in &command.subcommands {
            out.push_str(&format!(
                "| `{}` | {} |\n",
                subcommand.name,
                markdown_cell(subcommand.about.as_deref().unwrap_or(""))
            ));
        }
        for subcommand in &command.subcommands {
            render_command_section(
                out,
                subcommand,
                heading_level + 1,
                &format!("{path} {}", subcommand.name),
            );
        }
    }
}

fn invocation(path: &str) -> String {
    format!("xtask {path}")
}

#[allow(
    clippy::panic,
    reason = "Doc-build fatal: a documented command path that isn't registered is a build-config bug"
)]
fn lookup_command<'a>(commands: &'a [CommandInfo], path: &str) -> &'a CommandInfo {
    find_command(commands, path)
        .unwrap_or_else(|| panic!("documented command path missing: {path}"))
}

fn command_summary<'a>(command: &'a CommandInfo, fallback: &'a str) -> &'a str {
    command
        .about
        .as_deref()
        .map(str::trim)
        .filter(|about| !about.is_empty())
        .unwrap_or(fallback)
}

fn sentence(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.ends_with('.') {
        trimmed.to_string()
    } else {
        format!("{trimmed}.")
    }
}

fn render_flag(arg: &ArgInfo) -> String {
    let mut parts = Vec::new();
    if let Some(short) = arg.short {
        parts.push(format!("-{short}"));
    }
    if let Some(long) = &arg.long {
        parts.push(format!("--{long}"));
    }
    if parts.is_empty() {
        arg.name.clone()
    } else {
        parts.join(", ")
    }
}

fn markdown_cell(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_catalog::collect_command_catalog;

    #[test]
    fn help_category_paths_exist() {
        let commands = collect_command_catalog();
        for category in HELP_CATEGORIES {
            for path in category.command_paths {
                assert!(
                    find_command(&commands, path).is_some(),
                    "missing help path: {path}"
                );
            }
        }
    }

    #[test]
    fn guide_paths_exist() {
        let commands = collect_command_catalog();
        for section in GUIDE_SECTIONS {
            for entry in section.entries {
                assert!(
                    find_command(&commands, entry.path).is_some(),
                    "missing guide path: {}",
                    entry.path
                );
            }
        }
    }

    #[test]
    fn reference_renders_global_flags() {
        let rendered = render_command_reference(&[CommandInfo {
            name: "check".to_string(),
            about: Some("Compile verification".to_string()),
            args: vec![ArgInfo {
                name: "package".to_string(),
                short: Some('p'),
                long: Some("package".to_string()),
                help: Some("Check specific package(s) only".to_string()),
                required: false,
                global: false,
                possible_values: vec![],
                takes_value: true,
            }],
            subcommands: vec![],
        }]);

        assert!(rendered.contains("# xtask Command Reference"));
        assert!(rendered.contains("## Global Flags"));
        assert!(rendered.contains("## `xtask check`"));
        assert!(
            rendered.contains("| `-p, --package` | yes | no | Check specific package(s) only |")
        );
    }
}
