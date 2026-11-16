"""Temporary batch checks; remove this helper after collecting the artifacts."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import time


BASE = "0cb9b5c1ea48243b129cc9721c3001df31f15b13"
ROOT = Path.cwd()
OUTPUT = Path(os.environ["RUNNER_TEMP"]) / "store-verification"
STATEMENTS = Path("/tmp/sinex-query-statements.jsonl")
PACKAGES = [
    "sinex-store",
    "sinex-query",
    "sinex-derivations",
    "sinex-library",
    "sinex-model",
    "sinex-sources",
    "sinexd",
]
SELECT = [argument for package in PACKAGES for argument in ("-p", package)]
QUERY_FILES = [
    "crates/sinex-store/src/query/compile.rs",
    "crates/sinex-store/src/query/compile/calendar.rs",
    "crates/sinex-store/src/query/compile/expr.rs",
]
TESTS = {
    "stored_keys_round_trip": "rename the stored DayKey day field to date",
    "unknown_last_duration_leaves_end_a_lower_bound": "clear the session lower-bound flag",
    "a_day_with_an_unknown_hour_has_an_unknown_sum": "discard unknown hourly sum fields",
    "suspend_closes_open_focus_interval": "remove suspend from interval endings",
    "open_afk_span_clamped_at_creation": "remove the AFK current-time clamp",
    "an_undecodable_key_reports_the_stored_key_error": "discard a stored key decoding error",
    "a_configuration_names_every_built_derivation": "omit derivation configuration validation",
    "sql_hour_matches_the_daemon_zone_across_both_transitions": "use UTC offset spans",
    "transition_window_stops_at_a_suspend_for_the_same_host": "omit host interruption matching",
    "a_record_that_fails_on_replay_keeps_its_old_reading": "archive failed replay records",
    "a_material_that_yields_nothing_keeps_its_readings": "archive every selected replay material",
    "undo_after_a_partly_failed_replay_restores_the_prior_readings": "archive failed replay records",
}


def write_json(name: str, value: object) -> None:
    (OUTPUT / name).write_text(json.dumps(value, indent=2) + "\n")


def run(arguments: list[str], log: str, *, expected_failure: bool = False) -> tuple[int, str]:
    """Keep full child output and its direct exit status while streaming progress."""
    destination = OUTPUT / log
    destination.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    print("+ " + shlex.join(arguments), flush=True)
    environment = {**os.environ, "CARGO_TERM_COLOR": "never", "NEXTEST_COLOR": "never"}
    with destination.open("w") as output:
        output.write("+ " + shlex.join(arguments) + "\n")
        output.flush()
        child = subprocess.Popen(
            arguments,
            cwd=ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            bufsize=1,
        )
        try:
            assert child.stdout is not None
            for line in child.stdout:
                output.write(line)
                output.flush()
                print(line, end="", flush=True)
            status = child.wait()
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait()
        output.write(f"\nexit_status={status}\nwall_seconds={time.monotonic() - started:.3f}\n")
    text = destination.read_text()
    if status and not expected_failure:
        raise RuntimeError(f"{log} failed with exit status {status}")
    return status, text


@contextlib.contextmanager
def restore_files(paths: list[str]):
    originals = {name: (ROOT / name).read_bytes() for name in paths}
    try:
        yield
    finally:
        for name, content in originals.items():
            (ROOT / name).write_bytes(content)


def change(path: str, before: str, after: str, *, count: int = 1) -> None:
    file = ROOT / path
    text = file.read_text()
    found = text.count(before)
    if found != count:
        raise RuntimeError(f"{path}: expected {count} copies of mutation text, found {found}")
    file.write_text(text.replace(before, after))


def base_source(path: str) -> str:
    return subprocess.check_output(["git", "show", f"{BASE}:{path}"], cwd=ROOT, text=True).replace(
        "sinex_model::query::", "sinex_query::"
    )


def add_sql_dump(text: str) -> str:
    before = "        Ok(Compiled {\n"
    if text.count(before) != 1:
        raise RuntimeError("the compiler needs exactly one compiled-answer return for SQL collection")
    dump = '''        #[cfg(test)]
        {
            use std::io::Write as _;
            let record = serde_json::to_string(&(answer.render().0, count.render().0)).unwrap();
            let mut file = std::fs::OpenOptions::new().create(true).append(true)
                .open("/tmp/sinex-query-statements.jsonl").unwrap();
            file.lock().unwrap();
            writeln!(file, "{record}").unwrap();
        }
'''
    return text.replace(before, dump + before)


def compare_sql() -> None:
    final_compiler = (ROOT / QUERY_FILES[0]).read_text()
    with restore_files(QUERY_FILES):
        for version, compiler in [("before", base_source(QUERY_FILES[0])), ("after", final_compiler)]:
            (ROOT / QUERY_FILES[0]).write_text(add_sql_dump(compiler))
            for path in QUERY_FILES[1:]:
                (ROOT / path).write_text(base_source(path))
            STATEMENTS.unlink(missing_ok=True)
            run(
                ["cargo", "xtask", "test", *SELECT, "-E", "package(sinex-store) & test(query::)", "--no-fail-fast"],
                f"sql-{version}.log",
            )
            statements = sorted(set(STATEMENTS.read_text().splitlines()))
            if not statements:
                raise RuntimeError(f"the {version} query tests produced no SQL statements")
            (OUTPUT / f"sql-{version}.txt").write_text("\n".join(statements) + "\n")
            STATEMENTS.unlink()
    before = (OUTPUT / "sql-before.txt").read_text()
    after = (OUTPUT / "sql-after.txt").read_text()
    import difflib

    difference = "".join(difflib.unified_diff(
        before.splitlines(keepends=True), after.splitlines(keepends=True),
        fromfile="sql-before.txt", tofile="sql-after.txt",
    ))
    (OUTPUT / "sql.diff").write_text(difference)
    write_json("sql-summary.json", {"unique_statements": len(before.splitlines()), "equal": not difference})
    if difference:
        raise RuntimeError("the compiler split changed SQL; see sql.diff")


def catalog_and_query_plan() -> None:
    run(
        ["cargo", "xtask", "db", "exec", "--", "bash", "-c",
         'cargo run --locked --quiet -p sinexd -- admin catalog > "$1"',
         "catalog", str(OUTPUT / "catalog.json")],
        "catalog.log",
    )
    run(
        ["cargo", "xtask", "test", *SELECT, "-E", "test(committed_catalog_is_current)", "--no-fail-fast"],
        "catalog-test.log",
    )
    if (OUTPUT / "catalog.json").read_bytes() != (ROOT / "nix/catalog.json").read_bytes():
        raise RuntimeError("the generated catalog differs from the tracked catalog")
    run(
        ["cargo", "xtask", "db", "exec", "--", "bash", "-c",
         'psql --no-psqlrc --set=ON_ERROR_STOP=1 "$DATABASE_URL" --file "$1"',
         "query-plan", str(ROOT / ".github/explain-window.sql")],
        "explain-window.log",
    )


def mutation_checks() -> None:
    paths = [
        "crates/sinex-derivations/src/keys.rs",
        "crates/sinex-derivations/src/session.rs",
        "crates/sinex-derivations/src/figures.rs",
        "crates/sinex-derivations/src/state_interval.rs",
        "crates/sinex-derivations/src/lib.rs",
        "crates/sinexd/src/binding_runner.rs",
        "crates/sinex-store/src/query/compile/calendar.rs",
        "crates/sinex-store/src/derivations/state_transitions.rs",
        "crates/sinex-store/src/admission/archive.rs",
        "crates/sinexd/src/replay.rs",
    ]
    write_json("mutations.json", TESTS)
    with restore_files(paths):
        change(paths[0], "pub struct DayKey {\n    pub day: Date,", 'pub struct DayKey {\n    #[serde(rename = "date")]\n    pub day: Date,')
        change(paths[1], "                    end_is_lower_bound,", "                    end_is_lower_bound: false,")
        for field in ["value", "numerator", "denominator"]:
            change(paths[2], f"            sum.unknown_{field} |= f.{field}.is_none();\n", "")
        change(paths[3], "ended_by: &[Interruption::Afk, Interruption::Suspend],", "ended_by: &[Interruption::Afk],", count=2)
        change(paths[3], "end.min(env.now).max(interval.open.at)", "end.max(interval.open.at)")
        change(paths[4], '''            Err(error) => Computed {
                error: Some(sinex_model::BuildError::Key(error.to_string())),
                outputs: vec![],
                waiting: false,
            },''', "            Err(_) => Computed::default(),")
        change(paths[5], "        config.validate_derivations()?;\n", "")
        change(paths[6], "offset_seconds: self.ctx.zone.to_offset(from).seconds(),", "offset_seconds: 0,")
        change(paths[7], "if name == &scope_name || matching {", "if name == &scope_name {")
        change(paths[8], '''AND NOT EXISTS (
                 SELECT 1 FROM unnest($5::uuid[], $6::bigint[]) failed(material_id, record_offset)
                 WHERE failed.material_id = e.material_id AND failed.record_offset = e.record_offset)''',
               "AND (cardinality($5::uuid[])>=0 AND cardinality($6::bigint[])>=0)")
        change(paths[9], "                materials: read_materials,", "                materials: sequence.iter().filter(|m| in_scope.contains(&m.id)).map(|m| MaterialId(m.id)).collect(),")
        expression = " | ".join(f"test(/::{name}$/)" for name in TESTS)
        status, log = run(
            ["cargo", "xtask", "test", *SELECT, "-E", expression, "--no-fail-fast"],
            "mutations.log", expected_failure=True,
        )
        plain = re.sub(r"\x1b\[[0-9;]*m", "", log)
        failures = re.findall(r"\bFAIL\s+\[[^\]]*\]\s+(?:\([^)]*\)\s+)?\S+\s+(\S+)", plain)
        failed_names = {name.rsplit("::", 1)[-1] for name in failures}
        missing = sorted(set(TESTS) - failed_names)
        write_json("mutation-results.json", {
            "exit_status": status, "failed_tests": failures,
            "missing_assertion_failures": missing,
        })
        if not status or missing:
            raise RuntimeError(f"every named mutation needs an observed test failure; missing: {missing}")


def save_timing(label: str, kind: str) -> None:
    target = Path(os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")))
    timing = target / "cargo-timings/cargo-timing.html"
    destination = OUTPUT / "timings" / label
    destination.mkdir(parents=True, exist_ok=True)
    if not timing.exists():
        write_json(f"timings/{label}/{kind}.json", {"timing_html": "not produced"})
        return
    text = timing.read_text()
    (destination / f"{kind}.html").write_text(text)
    match = re.search(r"\b(?:const|var|let)\s+UNIT_DATA\s*=\s*", text)
    summary: dict[str, object] = {"timing_html": f"{kind}.html"}
    if match:
        try:
            units, _ = json.JSONDecoder().raw_decode(text[match.end():])
            summary.update({
                "units": len(units),
                "unit_seconds": sum(unit["duration"] for unit in units),
                "unit_data": units,
            })
        except (ValueError, KeyError, TypeError) as error:
            summary["parse_error"] = str(error)
    else:
        summary["parse_error"] = "UNIT_DATA not found in Cargo timing HTML"
    write_json(f"timings/{label}/{kind}.json", summary)


def measure_one(label: str, template: str) -> None:
    # Both children inherit this one db-exec process's unchanged DATABASE_URL.
    run(["cargo", "check", "--locked", "--workspace", "--timings"], f"timings/{label}/warm.log")
    save_timing(label, "warm")
    (ROOT / template).touch()
    run(["cargo", "check", "--locked", "--workspace", "--timings"], f"timings/{label}/touched.log")
    save_timing(label, "touched")


def measure_edit_cascade() -> None:
    initial = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    write_json("measurement-revisions.json", {"before": BASE, "after": initial})
    helper = OUTPUT / "measure-edit.py"
    shutil.copyfile(Path(__file__), helper)
    try:
        for label, revision, template in [
            ("before", BASE, "crates/sinex-model/src/query/template.rs"),
            ("after", initial, "crates/sinex-query/src/template.rs"),
        ]:
            run(["git", "checkout", "--detach", revision], f"timings/{label}/checkout.log")
            run(
                ["cargo", "xtask", "db", "exec", "--", "python3", str(helper),
                 "--measure-one", label, template],
                f"timings/{label}/run.log",
            )
    finally:
        run(["git", "checkout", "--detach", initial], "timings/restore.log")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--measure-one", nargs=2, metavar=("LABEL", "TEMPLATE"))
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    if args.measure_one:
        measure_one(*args.measure_one)
        return
    run(["git", "fetch", "--no-tags", "--depth=1", "origin", BASE], "fetch-base.log")
    catalog_and_query_plan()
    compare_sql()
    measure_edit_cascade()
    (OUTPUT / "complete.txt").write_text("SQL comparison, catalog, query plans and edit measurements completed.\n")


if __name__ == "__main__":
    main()
