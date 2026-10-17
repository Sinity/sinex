#!/usr/bin/env python3
"""Run the real foundation candidate in owned disposable state. No production defaults."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import threading
import traceback
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--recovery-context", action="store_true", help="qualify bounded mixed-context project recall through configured Atuin and public clients")
parser.add_argument("--source-investigation", action="store_true", help="qualify bounded Atuin acquisition timing and exact capture receipts")
parser.add_argument("--privacy-rules", action="store_true", help="qualify admission-once optional rules and retained evidence through actual clients")
parser.add_argument("--reversible-exclusion",action="store_true",help="qualify packaged exact occurrence exclusion with an explicit immutable package pin")
parser.add_argument("--package-pin",type=Path,help="reuse the exact clean qualification/package/binary SHA256 pin for packaged exclusion, fusion, privacy rules, source investigation or configured continuation; never build or fall back")
parser.add_argument("--recall-coverage",action="store_true",help="qualify narrow recall polling/error coverage through packaged CLI and MCP")
parser.add_argument("--recall-coverage-before-pin",type=Path,help="exact immutable pre-fix package for recall coverage contrast")
parser.add_argument("--sessions",action="store_true",help="qualify selected logical-day shell reconciliation and project recall")
parser.add_argument("--session-resume",type=Path,help="continue remaining controls from an exact failed owned session report; no build or source reset")
parser.add_argument("--session-predecessor-pin",type=Path,help="exact immutable existing stage-2 package pin for guarded session upgrade")
parser.add_argument("--configured-continuation",action="store_true",help="measure finite ongoing Atuin acquisition and same-state recovery through pinned ordinary clients")
parser.add_argument("--exclusion-contracts",action="store_true",help="qualify typed single-writer exclusion without public-client or package claims")
parser.add_argument("--fusion",action="store_true",help="qualify selected Atuin/zsh fusion lifecycle using a pinned independent specification")
parser.add_argument("--fusion-focus",choices=["changed-seams","source-codec","maintenance","source-privacy"],help="focused projection/head-race, source-codec, packaged maintenance or remaining source/privacy controls; reuse separately retained journey proof")
parser.add_argument("--fusion-spec",type=Path,help="read-only specification Git repository for selected fusion fixture")
parser.add_argument("--indexed",action="store_true",help="qualify indexed shell retrieval and actual MCP transport")
parser.add_argument("--retained-replay", action="store_true", help="qualify retained Atuin replay through actual packaged clients")
parser.add_argument("--packaged", action="store_true", help="build and exercise the Nix package entrypoints")
parser.add_argument("--read-lifecycle", action="store_true", help="qualify blocked reads and cancellation through actual packaged clients")
parser.add_argument("--live-cursor", action="store_true", help="qualify current-state raw-event continuation through packaged clients")
parser.add_argument("--startup-coverage", action="store_true", help="qualify configured initial source coverage before its first read")
parser.add_argument("--credential-output", action="store_true", help="qualify fixed credential presentation with no optional rules")
parser.add_argument("--atuin", action="store_true", help="qualify retained synthetic Atuin through real clients")
parser.add_argument("--demo-spec", type=Path, help="read-only specification Git repository for the pinned Kestrel driver")
parser.add_argument("--demo-boundaries", action="store_true", help="qualify observed writer boundary kills using the pinned fixture")
parser.add_argument("--demo-provider-loss", action="store_true", help="qualify committed retained reads after observed kill and provider loss")
parser.add_argument("--demo-control", choices=["launch-failure", "corrupt-answer"])
OPTIONS = parser.parse_args()
if OPTIONS.recovery_context:
    incompatible = [value for key,value in vars(OPTIONS).items() if key not in ('recovery_context','packaged','package_pin') and value]
    if incompatible: parser.error("recovery context owns its scenario selector")
    if OPTIONS.packaged != bool(OPTIONS.package_pin): parser.error("packaged recovery context requires an explicit immutable package pin")
if OPTIONS.recall_coverage_before_pin and not OPTIONS.recall_coverage: parser.error("recall coverage contrast requires --recall-coverage")
if OPTIONS.recall_coverage and (not OPTIONS.packaged or not OPTIONS.recall_coverage_before_pin or any((OPTIONS.package_pin,OPTIONS.sessions,OPTIONS.session_resume,OPTIONS.indexed,OPTIONS.fusion,OPTIONS.configured_continuation,OPTIONS.atuin,OPTIONS.retained_replay,OPTIONS.reversible_exclusion,OPTIONS.exclusion_contracts,OPTIONS.read_lifecycle,OPTIONS.live_cursor,OPTIONS.demo_spec,OPTIONS.startup_coverage,OPTIONS.credential_output))):
    parser.error("recall coverage requires --packaged --recall-coverage-before-pin FILE and its own selector")
if OPTIONS.session_resume and not OPTIONS.sessions: parser.error("session resume requires --sessions")
if OPTIONS.configured_continuation and (not OPTIONS.packaged or not OPTIONS.package_pin or any([OPTIONS.fusion,OPTIONS.fusion_focus,OPTIONS.fusion_spec,OPTIONS.reversible_exclusion,OPTIONS.indexed,OPTIONS.exclusion_contracts,OPTIONS.retained_replay,OPTIONS.atuin,OPTIONS.read_lifecycle,OPTIONS.live_cursor,OPTIONS.demo_spec,OPTIONS.startup_coverage,OPTIONS.credential_output,OPTIONS.demo_control,OPTIONS.demo_boundaries,OPTIONS.demo_provider_loss])):
    parser.error("configured continuation requires --packaged --package-pin FILE and its own scenario selector")
if OPTIONS.fusion_focus and not OPTIONS.fusion: parser.error("fusion focus requires --fusion")
if OPTIONS.packaged and OPTIONS.fusion_focus in ("changed-seams","source-codec"): parser.error("this fusion focus is development-only; packaged focus supports maintenance or source-privacy")
if OPTIONS.fusion != bool(OPTIONS.fusion_spec): parser.error("fusion requires --fusion --fusion-spec GIT_REPOSITORY")
if OPTIONS.fusion and any([OPTIONS.reversible_exclusion,OPTIONS.indexed,OPTIONS.exclusion_contracts,OPTIONS.retained_replay,OPTIONS.atuin,OPTIONS.read_lifecycle,OPTIONS.live_cursor,OPTIONS.demo_spec,OPTIONS.startup_coverage,OPTIONS.credential_output]): parser.error("fusion owns its own scenario selector")
if OPTIONS.package_pin and not (OPTIONS.reversible_exclusion or OPTIONS.fusion or OPTIONS.configured_continuation or OPTIONS.privacy_rules or OPTIONS.source_investigation or OPTIONS.recovery_context):
    parser.error("prebuilt package pins are supported only by exclusion, fusion, privacy rules, source investigation, configured continuation or recovery context qualification")
if OPTIONS.fusion and OPTIONS.package_pin and not OPTIONS.packaged:
    parser.error("fusion prebuilt package pins require --packaged")
if OPTIONS.reversible_exclusion and (not OPTIONS.packaged or not OPTIONS.package_pin or OPTIONS.exclusion_contracts or OPTIONS.indexed or OPTIONS.retained_replay or OPTIONS.atuin or OPTIONS.read_lifecycle or OPTIONS.live_cursor or OPTIONS.demo_spec or OPTIONS.startup_coverage or OPTIONS.credential_output):
    parser.error("reversible exclusion requires --packaged --package-pin FILE and its own scenario selector")

if OPTIONS.exclusion_contracts and (OPTIONS.packaged or OPTIONS.indexed or OPTIONS.retained_replay or OPTIONS.atuin or OPTIONS.read_lifecycle or OPTIONS.live_cursor or OPTIONS.demo_spec or OPTIONS.startup_coverage or OPTIONS.credential_output):
    parser.error("typed exclusion contracts require their own development selector")
if OPTIONS.credential_output: OPTIONS.atuin = True
if OPTIONS.indexed and (OPTIONS.retained_replay or OPTIONS.atuin or OPTIONS.read_lifecycle or OPTIONS.live_cursor or OPTIONS.demo_spec or OPTIONS.startup_coverage): parser.error("indexed qualification owns its own scenarios")
if OPTIONS.retained_replay and (not OPTIONS.packaged or OPTIONS.read_lifecycle or OPTIONS.live_cursor or OPTIONS.atuin or OPTIONS.demo_spec or OPTIONS.startup_coverage):
    parser.error("retained replay requires --packaged and its own scenario selector")
if OPTIONS.read_lifecycle and (not OPTIONS.packaged or OPTIONS.live_cursor or OPTIONS.atuin or OPTIONS.demo_spec or OPTIONS.startup_coverage):
    parser.error("read lifecycle qualification requires --packaged and its own scenario selector")
if OPTIONS.live_cursor and (not OPTIONS.packaged or OPTIONS.atuin or OPTIONS.demo_spec or OPTIONS.startup_coverage):
    parser.error("live cursor qualification requires --packaged and its own scenario selector")
if OPTIONS.startup_coverage and not OPTIONS.packaged:
    parser.error("initial coverage qualification requires --packaged")
if OPTIONS.startup_coverage and (OPTIONS.atuin or OPTIONS.demo_spec):
    parser.error("initial coverage qualification cannot combine with other scenario selectors")
if OPTIONS.demo_spec and not OPTIONS.packaged:
    parser.error("the demonstration requires --packaged")
if (OPTIONS.demo_control or OPTIONS.demo_boundaries or OPTIONS.demo_provider_loss) and not OPTIONS.demo_spec:
    parser.error("demonstration controls require --demo-spec")
if OPTIONS.demo_provider_loss and (OPTIONS.demo_boundaries or OPTIONS.demo_control):
    parser.error("provider-loss qualification cannot combine with other demonstration controls")
if OPTIONS.source_investigation and any((OPTIONS.privacy_rules,OPTIONS.recall_coverage,OPTIONS.sessions,OPTIONS.session_resume,OPTIONS.indexed,OPTIONS.fusion,OPTIONS.configured_continuation,OPTIONS.atuin,OPTIONS.retained_replay,OPTIONS.reversible_exclusion,OPTIONS.exclusion_contracts,OPTIONS.read_lifecycle,OPTIONS.live_cursor,OPTIONS.demo_spec,OPTIONS.startup_coverage,OPTIONS.credential_output,OPTIONS.fusion_focus,OPTIONS.fusion_spec,OPTIONS.session_predecessor_pin,OPTIONS.recall_coverage_before_pin,OPTIONS.demo_control,OPTIONS.demo_boundaries,OPTIONS.demo_provider_loss)):
    parser.error("source investigation owns its scenario selector")
if OPTIONS.source_investigation and OPTIONS.packaged and not OPTIONS.package_pin:
    parser.error("packaged source investigation requires an explicit immutable package pin")
if OPTIONS.source_investigation and OPTIONS.package_pin and not OPTIONS.packaged:
    parser.error("source investigation package pin requires --packaged")
BIN_DIR = Path(os.environ.get("SINEX_FOUNDATION_BIN_DIR", str(ROOT/"target/debug")))
SESSION_RESUME = None
if OPTIONS.session_resume:
    prior=json.loads(OPTIONS.session_resume.read_text())
    assert prior['success'] is False and prior['postgres_stop_exit']==0, "resume requires a stopped failed owned run"
    WORK=Path(prior['runtime_state']).resolve(strict=True)
    assert WORK.parent==Path('/realm/tmp/work') and WORK.name.startswith('sinex-foundation-') and (WORK/'pg/PG_VERSION').is_file() and not (WORK/'pg/postmaster.pid').exists(), "resume state is not a stopped owned foundation cluster"
    observed_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    prior_tree=subprocess.check_output(['git','rev-parse',prior['candidate_head']+'^{tree}'],cwd=ROOT,text=True).strip()
    assert prior_tree==prior['candidate_index_tree'] and prior['candidate_tracked_diff_sha256']==hashlib.sha256(b'').hexdigest(), "prior runtime source was not clean"
    changed=subprocess.check_output(['git','diff','--name-only',prior['candidate_head'],observed_head],cwd=ROOT,text=True).splitlines()
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True) and all(x.startswith(('tests/','docs/')) or x in ('README.md','.agentctl/project.toml') for x in changed), "resume requires unchanged runtime/build source and a clean harness candidate"
    BIN_DIR=WORK/'runtime-bin'
    for name in ['sinexd','sinexctl']:
        assert hashlib.sha256((BIN_DIR/name).read_bytes()).hexdigest()==prior['binary_sha256'][name], "retained runtime differs from failed run"
    SESSION_RESUME={'report':str(OPTIONS.session_resume.resolve()),'report_sha256':hashlib.sha256(OPTIONS.session_resume.read_bytes()).hexdigest(),'runtime_candidate':prior['candidate_head'],'runtime_tree':prior_tree,'harness_candidate':observed_head,'harness_only_difference':changed,'binaries':prior['binary_sha256']}
else:
    WORK = Path(tempfile.mkdtemp(prefix="sinex-foundation-", dir="/realm/tmp/work"))
REPORT = Path(os.environ.get("SINEX_FOUNDATION_REPORT_DIR", str(WORK.parent / (WORK.name + ("-resume-"+str(time.time_ns()) if SESSION_RESUME else "-result")))))
REPORT.mkdir(parents=True, exist_ok=False)
if SESSION_RESUME: (REPORT/'runtime-identity.json').write_text(json.dumps(SESSION_RESUME,indent=2)+'\n')
COMMANDS = []
COMMAND_LOCK = threading.Lock()
SCENARIOS = []
PLANNED = ["cold query-free admin build", "canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "checked runtime build and pure contracts", "canonical hypertables, real role grants, writer admission and restart", "invalid and unsupported config fail before startup", "occupied endpoint leaves unowned resources untouched", "actual daemon CLI transport and truthful scope", "partial start cannot steal writer and clean shutdown releases ownership"]
if OPTIONS.atuin:
    PLANNED = PLANNED[:4] + ["Atuin unchanged-source parser recipe readmission", "Atuin retained source and real-client query", "Atuin repeat and late open-row update", "Atuin soft delete and source evidence", "Atuin empty malformed and unavailable outcomes", "Atuin restart and bounded retained cursor"]
if OPTIONS.startup_coverage:
    PLANNED = ["canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "unconfigured empty coverage", "configured blocked first read and recovery", "configured blocked empty read and restart", "missing source stays unavailable without creation"]
if OPTIONS.demo_spec:
    PLANNED = ["canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "pinned independent fixture preparation", "Kestrel base actual answers", "Kestrel repeated snapshot", "Kestrel append at the same source path", "Kestrel empty actual answers", "Kestrel malformed actual answers"]
if OPTIONS.demo_boundaries:
    PLANNED = PLANNED[:2] + ["pinned independent fixture preparation", "uninterrupted boundary baseline", "observed retained-fsynced kill and restart", "observed transaction-staged kill and restart", "observed transaction-committed kill and restart"]
if OPTIONS.demo_provider_loss:
    PLANNED = PLANNED[:2] + ["pinned independent fixture preparation", "uninterrupted boundary baseline", "observed transaction-staged kill and provider loss", "observed transaction-committed kill and provider loss"]
if OPTIONS.live_cursor:
    PLANNED = ["canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "raw current-state continuation and change disclosure", "cursor refusal and compatible restart"]
if OPTIONS.read_lifecycle:
    PLANNED = ["canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "disconnected actual client releases blocked snapshot", "API default deadline returns budget and releases snapshot", "CLI default deadline returns budget and releases snapshot", "CLI configured deadline returns budget and releases snapshot", "queued deadline and concurrent disconnect reclaim both reader slots"]
if OPTIONS.retained_replay:
    PLANNED = ["canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "retained fixture and independent bad-interpretation contrast", "bounded replay removed-range progress and later precommit recovery", "postcommit kill and request recovery", "disconnected client leaves admitted replay running", "changed parser artifact refuses mixed-operation continuation", "retained evidence refusal and durable outcomes", "retained request and outcome authority refuse unavailable comparison", "explicit authority cancellation and request conflicts", "malformed retained material preserves prior answers"]
if OPTIONS.credential_output:
    PLANNED = PLANNED[:4] + ["Atuin retained source and real-client query"]
if OPTIONS.indexed:
    PLANNED=["canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract","bound SQL null semantics","restricted profile and store binding","indexed fixture, independent full-population ranking and semantic controls","actual MCP transport and cross-client identity","project component-depth ambiguity and wildcard boundary","projection interruption and same-state reconstruction","source replacement removal replay and project rebuild","retained navigation without provider after restart","indexed deadlines cancellation and credential presentation"]
if OPTIONS.exclusion_contracts:
    PLANNED=["cold query-free admin build","canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract","checked runtime build and pure contracts","typed occurrence exclusion retained evidence and action recovery"]
if OPTIONS.reversible_exclusion:
    import exclusion_cases
    PLANNED=["canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract"]+exclusion_cases.PLANNED
if OPTIONS.fusion:
    import fusion_cases
    PLANNED=["canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract"]+fusion_cases.planned(OPTIONS.fusion_focus)
    if not OPTIONS.packaged: PLANNED=["cold query-free admin build"]+PLANNED[:2]+["checked runtime build and pure contracts"]+PLANNED[2:]
if OPTIONS.recovery_context:
    import recovery_context_cases
    PLANNED=["canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract"]+recovery_context_cases.PLANNED
    if not OPTIONS.packaged: PLANNED=["cold query-free admin build"]+PLANNED[:2]+["checked runtime build and pure contracts"]+PLANNED[2:]
if OPTIONS.source_investigation:
    import source_investigation_cases
    PLANNED=["canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract"]+source_investigation_cases.PLANNED
    if not OPTIONS.packaged: PLANNED=["cold query-free admin build"]+PLANNED[:2]+["checked runtime build and pure contracts"]+PLANNED[2:]
if OPTIONS.configured_continuation:
    import configured_continuation_cases
    PLANNED=["canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract"]+configured_continuation_cases.PLANNED
if OPTIONS.sessions:
    if not OPTIONS.session_predecessor_pin or OPTIONS.packaged or any((OPTIONS.indexed,OPTIONS.fusion,OPTIONS.configured_continuation,OPTIONS.atuin,OPTIONS.retained_replay)):
        parser.error("sessions requires --session-predecessor-pin FILE and its own development selector")
    import session_cases
    PLANNED=["cold query-free admin build","canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract","checked runtime build and pure contracts"]+session_cases.PLANNED
    if SESSION_RESUME: PLANNED=["retained owned schema check","owned PostgreSQL extension and checksum contract"]+session_cases.RESUME_PLANNED
if OPTIONS.recall_coverage:
    import recall_coverage_cases
    PLANNED=["canonical schema cold apply and idempotent check","owned PostgreSQL extension and checksum contract"]+recall_coverage_cases.PLANNED
if OPTIONS.privacy_rules:
    PLANNED=["canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "optional privacy rules preserve admission meaning and retained protection", "zsh retained original protection uses current rules once"]
if OPTIONS.packaged:
    PLANNED.insert(0, "Nix package entrypoint selection")
print(json.dumps({"report":str(REPORT/"result.json"),"owned_state":str(WORK)}),flush=True)
PROCESSES = []
CHILDREN = []
ENV = os.environ.copy()
for key in ("DATABASE_URL", "SQLX_OFFLINE", "PGHOST", "PGPORT", "PGUSER", "PGDATABASE"):
    ENV.pop(key, None)
PG_STARTED = False
POSTGRES_STOP_EXIT = None
PREBUILT_PIN = None


def run(args, *, expected=0, env=None, label=None):
    with COMMAND_LOCK:
        index = len(COMMANDS)
        COMMANDS.append({"argv":[str(a) for a in args],"exit":None,"expected_exit":expected,"label":label})
    try:
        result = subprocess.run([str(a) for a in args], cwd=ROOT, env=env or ENV, capture_output=True, text=True, timeout=900)
    except subprocess.TimeoutExpired as error:
        def text(value): return value.decode(errors="replace") if isinstance(value, bytes) else (value or "")
        (REPORT / f"{index:03}.stdout").write_text(text(error.stdout))
        (REPORT / f"{index:03}.stderr").write_text(text(error.stderr))
        COMMANDS[index]["timed_out"] = True
        raise AssertionError(f"command {index} timed out; partial output retained") from error
    (REPORT / f"{index:03}.stdout").write_text(result.stdout)
    (REPORT / f"{index:03}.stderr").write_text(result.stderr)
    COMMANDS[index]["exit"] = result.returncode
    if result.returncode not in (expected if isinstance(expected, tuple) else (expected,)):
        raise AssertionError(f"command {index} ({label or args[0]}) exit {result.returncode}, expected {expected}; see retained output")
    return result


def scenario(name, fn):
    started = time.monotonic()
    try:
        fn()
    except Exception as error:
        (REPORT/"scenario-traceback.txt").write_text(traceback.format_exc())
        SCENARIOS.append({"name": name, "outcome": "failed", "reason": str(error), "seconds": time.monotonic()-started})
        raise
    SCENARIOS.append({"name": name, "outcome": "passed", "seconds": time.monotonic()-started})


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def rpc(endpoint, token, method="status"):
    request = urllib.request.Request(endpoint, data=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":{}}).encode(), headers={"Authorization": "Bearer "+token, "Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        return json.load(error)


def start_daemon(config, name, *extra, binary=None):
    stdout = (REPORT / (name+".stdout")).open("w")
    stderr = (REPORT / (name+".stderr")).open("w")
    (REPORT/(name+"-config.toml")).write_text(Path(config).read_text())
    proc = subprocess.Popen([str(binary or BIN_DIR/"sinexd"),"serve","--config",str(config), *map(str,extra)], env=ENV, stdout=stdout, stderr=stderr)
    CHILDREN.append((proc,stdout,stderr))
    PROCESSES.append({"pid":proc.pid,"argv":[str(binary or BIN_DIR/"sinexd"),"serve","--config",str(config), *map(str,extra)],"stdout":name+".stdout","stderr":name+".stderr"})
    return proc


def stop(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=15)
    assert proc.returncode == 0, f"daemon did not exit cleanly: {proc.returncode}"


try:
    if OPTIONS.packaged:
        def select_package():
            global BIN_DIR,PREBUILT_PIN
            if OPTIONS.package_pin:
                pin_bytes=OPTIONS.package_pin.read_bytes()
                pin=json.loads(pin_bytes)
                package=Path(pin['package'])
                assert package.parent==Path('/nix/store') and package.resolve(strict=True)==package and package.is_dir(), "prebuilt package must name one immutable /nix/store output"
                assert pin['exit_code']==0 and pin['dirty'] is False, "package pin must record a successful clean build"
                observed_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
                observed_tree=subprocess.check_output(['git','write-tree'],cwd=ROOT,text=True).strip()
                assert pin['candidate']==observed_head and pin['tree']==observed_tree and not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True), "prebuilt package pin differs from the clean current candidate"
                if OPTIONS.configured_continuation or OPTIONS.privacy_rules:
                    package_candidate=pin['package_candidate']
                    package_tree=subprocess.check_output(['git','rev-parse',package_candidate+'^{tree}'],cwd=ROOT,text=True).strip()
                    assert package_tree==pin['package_tree'], "recorded package source tree differs"
                    changed=subprocess.check_output(['git','diff','--name-only',package_candidate,observed_head],cwd=ROOT,text=True).splitlines()
                    assert all(p.startswith(('tests/','docs/')) or p in ('README.md','.agentctl/project.toml') for p in changed), "configured continuation requires a package with unchanged runtime/build source"
                    (REPORT/'runtime-identity.json').write_text(json.dumps({'package_candidate':package_candidate,'package_tree':package_tree,'qualification_candidate':observed_head,'qualification_tree':observed_tree,'harness_and_docs_only_difference':changed},indent=2)+'\n')
                BIN_DIR=package/'bin'
                for name in ['sinexd','sinexctl']:
                    binary=BIN_DIR/name
                    assert pin['binaries'][name]['path']==str(binary) and binary.is_file() and binary.resolve().is_relative_to(package), "prebuilt entrypoint is outside its specified immutable package"
                    assert hashlib.sha256(binary.read_bytes()).hexdigest()==pin['binaries'][name]['sha256'], "prebuilt entrypoint hash differs from package pin"
                (REPORT/'prebuilt-package-pin.json').write_bytes(pin_bytes)
                PREBUILT_PIN={'selection':'explicit_immutable_prebuilt','pin_sha256':hashlib.sha256(pin_bytes).hexdigest(),'pin':pin,'builds_invoked':False}
                run([BIN_DIR/'sinexctl','--version'],label='actual explicitly pinned CLI entrypoint')
                run([BIN_DIR/'sinexd','--version'],label='actual explicitly pinned daemon entrypoint')
            else:
                built = run(["nix", "build", "--no-link", ".#default", "--print-out-paths", "--max-jobs", "1"], label="actual Nix package")
                outputs = built.stdout.splitlines()
                assert len(outputs) == 1, "package build did not return exactly one output"
                BIN_DIR = Path(outputs[0]) / "bin"
            assert all((BIN_DIR/name).is_file() for name in ["sinexd", "sinexctl"]), "package entrypoints absent"
            refused = run([BIN_DIR/"sinexctl", "--endpoint", "http://127.0.0.1:1/rpc", "--token-file", WORK/"not-created", "--query-timeout-ms", "500", "status"], expected=2, label="query deadline refused on non-query command")
            assert json.loads(refused.stderr)["kind"] == "bad_query"
            if OPTIONS.demo_spec:
                run([BIN_DIR/"xtask", "demo", ROOT, "--help"], label="packaged xtask driver forwarding")
                run([BIN_DIR/"xtask", "demo"], expected=2, label="packaged xtask missing explicit paths")
        scenario("Nix package entrypoint selection", select_package)
    # Build admin before the first cluster exists, with no ambient SQLx/PG routing.
    if not (SESSION_RESUME or ((OPTIONS.privacy_rules or OPTIONS.source_investigation or OPTIONS.recovery_context) and OPTIONS.packaged) or OPTIONS.recall_coverage or OPTIONS.configured_continuation or OPTIONS.reversible_exclusion or OPTIONS.demo_spec or OPTIONS.startup_coverage or OPTIONS.live_cursor or OPTIONS.read_lifecycle or OPTIONS.retained_replay or ((OPTIONS.indexed or OPTIONS.fusion) and OPTIONS.packaged)): scenario("cold query-free admin build", lambda: run(["cargo","build","--locked","-p","sinexd","--no-default-features"], label="cold admin"))
    if not SESSION_RESUME:
        (WORK/"pg").mkdir()
        (WORK/"socket").mkdir()
    ENV.update(PGDATA=str(WORK/"pg"), PGHOST=str(WORK/"socket"), PGPORT=str(free_port()))
    if not SESSION_RESUME: run(["initdb","--no-locale","--auth=trust","--data-checksums"],label="isolated cluster")
    run(["pg_ctl","-D",WORK/"pg","-l",REPORT/"postgres.log","-o",f"-k {WORK/'socket'} -c listen_addresses= -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on -p {ENV['PGPORT']}","-w","start"])
    PG_STARTED = True
    if not SESSION_RESUME:
        run(["createdb","foundation"])
        run(["createuser","sinex_writer"])
        run(["createuser","sinex_reader"])
    admin = f"postgresql:///foundation?host={WORK/'socket'}&port={ENV['PGPORT']}"
    writer = f"postgresql://sinex_writer@localhost/foundation?host={WORK/'socket'}&port={ENV['PGPORT']}"
    reader = f"postgresql://sinex_reader@localhost/foundation?host={WORK/'socket'}&port={ENV['PGPORT']}"
    for name,value in [("admin",admin),("writer",writer),("reader",reader),("token","synthetic-foundation-token")]:
        (WORK/name).write_text(value)
        (WORK/name).chmod(0o600)
    admin_binary=BIN_DIR/"sinexd" if SESSION_RESUME or ((OPTIONS.privacy_rules or OPTIONS.source_investigation or OPTIONS.recovery_context) and OPTIONS.packaged) or OPTIONS.recall_coverage or OPTIONS.configured_continuation or OPTIONS.reversible_exclusion or OPTIONS.demo_spec or OPTIONS.startup_coverage or OPTIONS.live_cursor or OPTIONS.read_lifecycle or OPTIONS.retained_replay or ((OPTIONS.indexed or OPTIONS.fusion) and OPTIONS.packaged) else ROOT/"target/debug/sinexd"
    daemon=BIN_DIR/"sinexd"
    if SESSION_RESUME:
        for database in ['foundation','session_predecessor']:
            dump=REPORT/(database+'-before-continuation.dump')
            run(['pg_dump',admin.replace('/foundation?','/'+database+'?'),'--format=custom','--file',dump],label='preserve failed owned database before continuation')
            dump.chmod(0o600)
            with dump.open('rb') as held: os.fsync(held.fileno())
        directory_fd=os.open(REPORT,os.O_DIRECTORY)
        try: os.fsync(directory_fd)
        finally: os.close(directory_fd)
        scenario("retained owned schema check",lambda:run([admin_binary,'admin','schema','--connection-file',WORK/'admin','--check']))
    else:
        scenario("canonical schema cold apply and idempotent check", lambda: (
            run([admin_binary,"admin","schema","--connection-file",WORK/"admin"]),
            run([admin_binary,"admin","schema","--connection-file",WORK/"admin","--check"]),
            run([admin_binary,"admin","schema","--connection-file",WORK/"admin"])))
    def database_contract():
        actual = run(["psql", admin, "--no-psqlrc", "--tuples-only", "--no-align", "-v", "ON_ERROR_STOP=1", "--command", "SELECT json_build_object('postgres_version',version(),'server_version_num',current_setting('server_version_num'),'data_checksums',current_setting('data_checksums'),'shared_preload_libraries',current_setting('shared_preload_libraries'),'timescaledb_version',(SELECT extversion FROM pg_extension WHERE extname='timescaledb'))"], label="actual isolated database contract")
        observed = json.loads(actual.stdout)
        assert int(observed["server_version_num"]) >= 180000
        assert observed["data_checksums"] == "on"
        assert "timescaledb" in [name.strip() for name in observed["shared_preload_libraries"].split(",")]
        assert observed["timescaledb_version"], "canonical extension absent"
    scenario("owned PostgreSQL extension and checksum contract", database_contract)
    ENV["DATABASE_URL"] = admin
    if not (SESSION_RESUME or ((OPTIONS.privacy_rules or OPTIONS.source_investigation or OPTIONS.recovery_context) and OPTIONS.packaged) or OPTIONS.recall_coverage or OPTIONS.configured_continuation or OPTIONS.reversible_exclusion or OPTIONS.demo_spec or OPTIONS.startup_coverage or OPTIONS.live_cursor or OPTIONS.read_lifecycle or OPTIONS.retained_replay or ((OPTIONS.indexed or OPTIONS.fusion) and OPTIONS.packaged)): scenario("checked runtime build and pure contracts", lambda: (
        run(["cargo","build","--locked","--workspace"],label="runtime against isolated schema"),
        run(["cargo","test","--locked","-p","sinex-model","-p","sinex-protocol","-p","sinexd","-p","sinex-store"],label="pure and configuration contracts")))
    if not OPTIONS.packaged and not SESSION_RESUME:
        # Admin-only builds use the same Cargo output name. Bind execution to a retained
        # per-run copy so another build cannot change the tested executable mid-scenario.
        runtime_bin=WORK/"runtime-bin"; runtime_bin.mkdir()
        for name in ["sinexd","sinexctl"]: shutil.copy2(BIN_DIR/name,runtime_bin/name)
        BIN_DIR=runtime_bin
        daemon=BIN_DIR/"sinexd"
    if OPTIONS.recovery_context:
        recovery_context_cases.exercise(globals())
    elif OPTIONS.source_investigation:
        source_investigation_cases.exercise(globals())
    elif OPTIONS.privacy_rules:
        import privacy_rule_cases
        privacy_rule_cases.exercise(globals())
    elif OPTIONS.recall_coverage:
        recall_coverage_cases.exercise(globals())
    elif OPTIONS.sessions:
        session_cases.exercise(globals())
    elif OPTIONS.configured_continuation:
        configured_continuation_cases.exercise(globals())
    elif OPTIONS.fusion:
        fusion_cases.exercise(globals())
    elif OPTIONS.reversible_exclusion:
        exclusion_cases.exercise(globals())
    elif OPTIONS.exclusion_contracts:
        def exclusion_contract():
            isolated=ENV.copy()
            isolated.update(SINEX_TEST_ADMIN_URL=admin,SINEX_TEST_WRITER_URL=writer,SINEX_TEST_READER_URL=reader,SINEX_TEST_STORE_DIR=str(WORK/'exclusion-contract-store'))
            run(["cargo","test","--locked","-p","sinex-store","--features","product","--lib","exclusion::tests::occurrence_exclusion_preserves_evidence_and_finalized_actions","--","--ignored","--nocapture"],env=isolated,label="typed writer exclusion, retained evidence, undo and finalized request recovery")
        scenario("typed occurrence exclusion retained evidence and action recovery",exclusion_contract)
    elif OPTIONS.indexed:
        scenario("bound SQL null semantics",lambda:run(["cargo","test","--locked","-p","sinex-store","--features","product","--lib","search::tests::bound_unknown_time_preserves_three_valued_negation","--","--ignored"],label="actual typed SQL null truth control"))
        import indexed_cases
        indexed_cases.exercise(globals())
    elif OPTIONS.retained_replay:
        import retained_replay_cases
        retained_replay_cases.exercise(globals())
    elif OPTIONS.read_lifecycle:
        import read_lifecycle_cases
        read_lifecycle_cases.exercise(globals())
    elif OPTIONS.live_cursor:
        import live_cursor_cases
        live_cursor_cases.exercise(globals())
    elif OPTIONS.startup_coverage:
        import startup_coverage_cases
        startup_coverage_cases.exercise(globals())
    elif OPTIONS.demo_spec:
        if OPTIONS.demo_boundaries or OPTIONS.demo_provider_loss:
            import boundary_cases
            boundary_cases.exercise(globals(), provider_loss=OPTIONS.demo_provider_loss)
        else:
            import demo_cases
            demo_cases.exercise(globals())
    elif OPTIONS.atuin:
        if not OPTIONS.credential_output:
            def atuin_recipe_contract():
                run(["createdb","atuin_recipe"],label="isolated recipe discrimination database")
                isolated=ENV.copy()
                isolated.update(SINEX_TEST_ADMIN_URL=admin.replace('/foundation?','/atuin_recipe?'),SINEX_TEST_WRITER_URL=writer.replace('/foundation?','/atuin_recipe?'),SINEX_TEST_READER_URL=reader.replace('/foundation?','/atuin_recipe?'),SINEX_TEST_STORE_DIR=str(WORK/'recipe-proof-store'))
                run(["cargo","test","--locked","-p","sinex-store","--features","product","--lib","atuin::tests::unchanged_source_new_recipe_readmits_and_preserves_old_evidence","--","--ignored","--nocapture"],env=isolated,label="typed prior recipe, identical source hash and retained history")
            scenario("Atuin unchanged-source parser recipe readmission",atuin_recipe_contract)
        import atuin_cases
        atuin_cases.exercise(globals())
    else:
        ENV.update(SINEX_TEST_ADMIN_URL=admin,SINEX_TEST_WRITER_URL=writer,SINEX_TEST_READER_URL=reader,SINEX_TEST_STORE_DIR=str(WORK/"proof-store"))
        scenario("canonical hypertables, real role grants, writer admission and restart", lambda: run(["cargo","test","--locked","-p","sinex-store","--features","product","--test","postgres","--","--ignored","--nocapture"], label="real restricted roles and durable admission"))
        port=free_port()
        endpoint=f"http://127.0.0.1:{port}/rpc"
        conf=WORK/"config.toml"
        def config_text(port_value=port, suffix="service"):
            return f'''listen = "127.0.0.1:{port_value}"
    state_dir = "{WORK/suffix}"
    store_dir = "{WORK/(suffix+'-blobs')}"
    writer_connection_file = "{WORK/'writer'}"
    reader_connection_file = "{WORK/'reader'}"
    token_file = "{WORK/'token'}"
    sources = []
    automata = []
    '''
        conf.write_text(config_text())
        def bad_config():
            bad=WORK/"bad.toml"; bad.write_text(config_text()+"unexpected = true\n")
            run([daemon,"serve","--config",bad],expected=1,label="unknown config rejected")
            assert not (WORK/"service").exists(), "invalid config acquired runtime state"
            bad.write_text(config_text().replace("sources = []",'sources = ["terminal.atuin-history"]'))
            run([daemon,"serve","--config",bad],expected=1,label="source without Atuin configuration rejected")
        scenario("invalid and unsupported config fail before startup",bad_config)
        def occupied_endpoint():
            with socket.socket() as occupied:
                occupied.bind(("127.0.0.1",0)); occupied.listen()
                occupied_config=WORK/"occupied.toml"; occupied_config.write_text(config_text(occupied.getsockname()[1],"occupied-state"))
                run([daemon,"serve","--config",occupied_config],expected=1,label="occupied endpoint")
                assert not (WORK/"occupied-state").exists(), "occupied endpoint acquired state"
        scenario("occupied endpoint leaves unowned resources untouched",occupied_endpoint)
        proc=start_daemon(conf,"daemon")
        deadline=time.monotonic()+15
        while True:
            if proc.poll() is not None: raise AssertionError("daemon failed before actual client response")
            try:
                status=rpc(endpoint,"synthetic-foundation-token")
                if "result" in status: break
            except (OSError,ValueError): pass
            if time.monotonic()>deadline: raise AssertionError("actual RPC did not become ready")
            time.sleep(0.05)
        def client_routes():
            cli=BIN_DIR/"sinexctl"
            for command in ["status","version","catalog","config-show"]:
                answer=run([cli,"--endpoint",endpoint,"--token-file",WORK/"token",command,"--json"],label="actual CLI "+command)
                decoded=json.loads(answer.stdout)
                if command=="status": assert decoded["supported_sources"]==[] and decoded["writer"]["state"]=="owned"
                assert "synthetic-foundation-token" not in answer.stdout and writer not in answer.stdout
            (WORK/"wrong-token").write_text("wrong-token")
            refused=run([cli,"--endpoint",endpoint,"--token-file",WORK/"wrong-token","status","--json"],expected=4,label="actual CLI token refusal")
            assert json.loads(refused.stderr)["kind"]=="refused"
            assert rpc(endpoint,"wrong-token")["error"]["kind"]=="refused"
            assert rpc(endpoint,"synthetic-foundation-token","replay")["error"]["kind"]=="not_found"
        scenario("actual daemon CLI transport and truthful scope",client_routes)
        def collision_and_shutdown():
            other=WORK/"second.toml"; other.write_text(config_text(free_port(),"other-state"))
            run([daemon,"serve","--config",other],expected=1,label="same database writer refused")
            assert proc.poll() is None and "result" in rpc(endpoint,"synthetic-foundation-token")
            stop(proc)
            replacement=start_daemon(conf,"restart")
            deadline=time.monotonic()+15
            while True:
                if replacement.poll() is not None: raise AssertionError("restart failed")
                try:
                    if "result" in rpc(endpoint,"synthetic-foundation-token"): break
                except OSError: pass
                if time.monotonic()>deadline: raise AssertionError("restart unavailable")
                time.sleep(0.05)
            stop(replacement)
        scenario("partial start cannot steal writer and clean shutdown releases ownership",collision_and_shutdown)
    success=True
except Exception as error:
    success=False
    failure=str(error)
finally:
    if not success:
        for proc,_,_ in CHILDREN:
            if proc.poll() is not None: continue
            observed={"pid":proc.pid}
            for entry in ["wchan","status","cgroup","syscall"]:
                try: observed[entry]=(Path("/proc")/str(proc.pid)/entry).read_text()
                except OSError as error: observed[entry]=str(error)
            (REPORT/f"failure-process-{proc.pid}.json").write_text(json.dumps(observed,indent=2)+"\n")
    if PG_STARTED and not success:
        try:
            diagnostic=subprocess.run(["psql",admin,"--no-psqlrc","--tuples-only","--no-align","-c","SELECT pid,datname,usename,state,wait_event_type,wait_event,query FROM pg_stat_activity"],env=ENV,capture_output=True,text=True,timeout=10)
            (REPORT/"failure-pg-activity.stdout").write_text(diagnostic.stdout)
            (REPORT/"failure-pg-activity.stderr").write_text(diagnostic.stderr)
        except Exception as diagnostic_error:
            (REPORT/"failure-pg-activity.stderr").write_text(str(diagnostic_error))
    for proc,out,err in CHILDREN:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try: proc.wait(timeout=10)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait()
        out.close(); err.close()
    if PG_STARTED:
        result=subprocess.run(["pg_ctl","-D",str(WORK/"pg"),"-m","immediate","-w","stop"],env=ENV,capture_output=True,text=True)
        POSTGRES_STOP_EXIT=result.returncode
        (REPORT/"postgres-stop.stderr").write_text(result.stderr)
        if result.returncode: success=False; failure="owned PostgreSQL shutdown failed"
    revision=subprocess.run(["git","rev-parse","HEAD"],cwd=ROOT,capture_output=True,text=True).stdout.strip()
    used = {c["argv"][0] for c in COMMANDS} | {p["argv"][0] for p in PROCESSES}
    binary_hashes={name:hashlib.sha256((BIN_DIR/name).read_bytes()).hexdigest() for name in ["sinexd","sinexctl","xtask"] if (BIN_DIR/name).exists() and str(BIN_DIR/name) in used}
    for name in PLANNED:
        if name not in {s["name"] for s in SCENARIOS}:
            SCENARIOS.append({"name":name,"outcome":"unrun","reason":failure if not success else "not exercised"})
    for record,(proc,_,_) in zip(PROCESSES,CHILDREN): record["exit"]=proc.returncode
    report={"candidate_head":revision,"candidate_index_tree":subprocess.run(["git","write-tree"],cwd=ROOT,capture_output=True,text=True).stdout.strip(),"candidate_tracked_diff_sha256":hashlib.sha256(subprocess.check_output(["git","diff","--binary","HEAD"],cwd=ROOT)).hexdigest(),"entrypoint_kind":"nix_package" if OPTIONS.packaged else ("retained_development_build" if SESSION_RESUME else "development_build"),"prebuilt_package":PREBUILT_PIN,"retained_runtime":SESSION_RESUME,"binary_sha256":binary_hashes,"success":success,"failure":None if success else failure,"scenarios":SCENARIOS,"commands":COMMANDS,"owned_processes":PROCESSES,"postgres_stop_exit":POSTGRES_STOP_EXIT,"runtime_state":"owned disposable state removed after captured evidence" if success and not SESSION_RESUME else str(WORK),"unimplemented":["formal M1 acceptance", "full query/surface/fusion scope", "snapshot bootstrap after pruning and purge/cutover", "remaining MCP tools","confirmed delivery","full target schema and Timescale","sealed lifecycle"]}
    if OPTIONS.demo_spec:
        report["demo"]={"oracle_revision":__import__("demo_cases").ORACLE_REVISION,"control":OPTIONS.demo_control,"provider_loss":OPTIONS.demo_provider_loss,"scope":"packaged Atuin raw-event driver; canonical row material, no SQLite snapshot retention or read-model index"}
    (REPORT/"result.json").write_text(json.dumps(report,indent=2)+"\n")
    if success and not SESSION_RESUME: shutil.rmtree(WORK)
    print(json.dumps({"success":success,"report":str(REPORT/"result.json"),"scenarios":len(SCENARIOS)}))
    sys.exit(0 if success else 1)
