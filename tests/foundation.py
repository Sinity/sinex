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
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--packaged", action="store_true", help="build and exercise the Nix package entrypoints")
parser.add_argument("--read-lifecycle", action="store_true", help="qualify blocked reads and cancellation through actual packaged clients")
parser.add_argument("--live-cursor", action="store_true", help="qualify current-state raw-event continuation through packaged clients")
parser.add_argument("--startup-coverage", action="store_true", help="qualify configured initial source coverage before its first read")
parser.add_argument("--atuin", action="store_true", help="qualify retained synthetic Atuin through real clients")
parser.add_argument("--demo-spec", type=Path, help="read-only specification Git repository for the pinned Kestrel driver")
parser.add_argument("--demo-boundaries", action="store_true", help="qualify observed writer boundary kills using the pinned fixture")
parser.add_argument("--demo-provider-loss", action="store_true", help="qualify committed retained reads after observed kill and provider loss")
parser.add_argument("--demo-control", choices=["launch-failure", "corrupt-answer"])
OPTIONS = parser.parse_args()
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
BIN_DIR = Path(os.environ.get("SINEX_FOUNDATION_BIN_DIR", str(ROOT/"target/debug")))
WORK = Path(tempfile.mkdtemp(prefix="sinex-foundation-", dir="/realm/tmp/work"))
REPORT = Path(os.environ.get("SINEX_FOUNDATION_REPORT_DIR", str(WORK.parent / (WORK.name + "-result"))))
REPORT.mkdir(parents=True, exist_ok=False)
COMMANDS = []
SCENARIOS = []
PLANNED = ["cold query-free admin build", "canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "checked runtime build and pure contracts", "canonical hypertables, real role grants, writer admission and restart", "invalid and unsupported config fail before startup", "occupied endpoint leaves unowned resources untouched", "actual daemon CLI transport and truthful scope", "partial start cannot steal writer and clean shutdown releases ownership"]
if OPTIONS.atuin:
    PLANNED = PLANNED[:4] + ["Atuin retained source and real-client query", "Atuin repeat and late open-row update", "Atuin soft delete and source evidence", "Atuin empty malformed and unavailable outcomes", "Atuin restart and bounded retained cursor"]
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
if OPTIONS.packaged:
    PLANNED.insert(0, "Nix package entrypoint selection")
print(json.dumps({"report":str(REPORT/"result.json"),"owned_state":str(WORK)}),flush=True)
PROCESSES = []
CHILDREN = []
ENV = os.environ.copy()
for key in ("DATABASE_URL", "SQLX_OFFLINE", "PGHOST", "PGPORT", "PGUSER", "PGDATABASE"):
    ENV.pop(key, None)
PG_STARTED = False


def run(args, *, expected=0, env=None, label=None):
    index = len(COMMANDS)
    try:
        result = subprocess.run([str(a) for a in args], cwd=ROOT, env=env or ENV, capture_output=True, text=True, timeout=900)
    except subprocess.TimeoutExpired as error:
        def text(value): return value.decode(errors="replace") if isinstance(value, bytes) else (value or "")
        (REPORT / f"{index:03}.stdout").write_text(text(error.stdout))
        (REPORT / f"{index:03}.stderr").write_text(text(error.stderr))
        COMMANDS.append({"argv":[str(a) for a in args],"exit":None,"expected_exit":expected,"label":label,"timed_out":True})
        raise AssertionError(f"command {index} timed out; partial output retained") from error
    (REPORT / f"{index:03}.stdout").write_text(result.stdout)
    (REPORT / f"{index:03}.stderr").write_text(result.stderr)
    COMMANDS.append({"argv": [str(a) for a in args], "exit": result.returncode, "expected_exit": expected, "label": label})
    if result.returncode not in (expected if isinstance(expected, tuple) else (expected,)):
        raise AssertionError(f"command {index} ({label or args[0]}) exit {result.returncode}, expected {expected}; see retained output")
    return result


def scenario(name, fn):
    started = time.monotonic()
    try:
        fn()
    except Exception as error:
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


def start_daemon(config, name, *extra):
    stdout = (REPORT / (name+".stdout")).open("w")
    stderr = (REPORT / (name+".stderr")).open("w")
    (REPORT/(name+"-config.toml")).write_text(Path(config).read_text())
    proc = subprocess.Popen([str(BIN_DIR/"sinexd"),"serve","--config",str(config), *map(str,extra)], env=ENV, stdout=stdout, stderr=stderr)
    CHILDREN.append((proc,stdout,stderr))
    PROCESSES.append({"pid":proc.pid,"argv":[str(BIN_DIR/"sinexd"),"serve","--config",str(config), *map(str,extra)],"stdout":name+".stdout","stderr":name+".stderr"})
    return proc


def stop(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=15)
    assert proc.returncode == 0, f"daemon did not exit cleanly: {proc.returncode}"


try:
    if OPTIONS.packaged:
        def select_package():
            global BIN_DIR
            built = run(["nix", "build", "--no-link", ".#default", "--print-out-paths", "--max-jobs", "1"], label="actual Nix package")
            outputs = built.stdout.splitlines()
            assert len(outputs) == 1, "package build did not return exactly one output"
            BIN_DIR = Path(outputs[0]) / "bin"
            assert all((BIN_DIR/name).is_file() for name in ["sinexd", "sinexctl"]), "package entrypoints absent"
            if OPTIONS.demo_spec:
                run([BIN_DIR/"xtask", "demo", ROOT, "--help"], label="packaged xtask driver forwarding")
                run([BIN_DIR/"xtask", "demo"], expected=2, label="packaged xtask missing explicit paths")
        scenario("Nix package entrypoint selection", select_package)
    # Build admin before the first cluster exists, with no ambient SQLx/PG routing.
    if not (OPTIONS.demo_spec or OPTIONS.startup_coverage or OPTIONS.live_cursor or OPTIONS.read_lifecycle): scenario("cold query-free admin build", lambda: run(["cargo","build","--locked","-p","sinexd","--no-default-features"], label="cold admin"))
    (WORK/"pg").mkdir()
    (WORK/"socket").mkdir()
    ENV.update(PGDATA=str(WORK/"pg"), PGHOST=str(WORK/"socket"), PGPORT=str(free_port()))
    run(["initdb","--no-locale","--auth=trust","--data-checksums"],label="isolated cluster")
    run(["pg_ctl","-D",WORK/"pg","-l",REPORT/"postgres.log","-o",f"-k {WORK/'socket'} -c listen_addresses= -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on -p {ENV['PGPORT']}","-w","start"])
    PG_STARTED = True
    run(["createdb","foundation"])
    run(["createuser","sinex_writer"])
    run(["createuser","sinex_reader"])
    admin = f"postgresql:///foundation?host={WORK/'socket'}&port={ENV['PGPORT']}"
    writer = f"postgresql://sinex_writer@localhost/foundation?host={WORK/'socket'}&port={ENV['PGPORT']}"
    reader = f"postgresql://sinex_reader@localhost/foundation?host={WORK/'socket'}&port={ENV['PGPORT']}"
    for name,value in [("admin",admin),("writer",writer),("reader",reader),("token","synthetic-foundation-token")]:
        (WORK/name).write_text(value)
        (WORK/name).chmod(0o600)
    admin_binary=BIN_DIR/"sinexd" if OPTIONS.demo_spec or OPTIONS.startup_coverage or OPTIONS.live_cursor or OPTIONS.read_lifecycle else ROOT/"target/debug/sinexd"
    daemon=BIN_DIR/"sinexd"
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
    if not (OPTIONS.demo_spec or OPTIONS.startup_coverage or OPTIONS.live_cursor or OPTIONS.read_lifecycle): scenario("checked runtime build and pure contracts", lambda: (
        run(["cargo","build","--locked","--workspace"],label="runtime against isolated schema"),
        run(["cargo","test","--locked","-p","sinex-model","-p","sinex-protocol","-p","sinexd","-p","sinex-store"],label="pure and configuration contracts")))
    if not OPTIONS.packaged:
        # Admin-only builds use the same Cargo output name. Bind execution to a retained
        # per-run copy so another build cannot change the tested executable mid-scenario.
        runtime_bin=WORK/"runtime-bin"; runtime_bin.mkdir()
        for name in ["sinexd","sinexctl"]: shutil.copy2(BIN_DIR/name,runtime_bin/name)
        BIN_DIR=runtime_bin
        daemon=BIN_DIR/"sinexd"
    if OPTIONS.read_lifecycle:
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
        (REPORT/"postgres-stop.stderr").write_text(result.stderr)
        if result.returncode: success=False; failure="owned PostgreSQL shutdown failed"
    revision=subprocess.run(["git","rev-parse","HEAD"],cwd=ROOT,capture_output=True,text=True).stdout.strip()
    used = {c["argv"][0] for c in COMMANDS} | {p["argv"][0] for p in PROCESSES}
    binary_hashes={name:hashlib.sha256((BIN_DIR/name).read_bytes()).hexdigest() for name in ["sinexd","sinexctl","xtask"] if (BIN_DIR/name).exists() and str(BIN_DIR/name) in used}
    for name in PLANNED:
        if name not in {s["name"] for s in SCENARIOS}:
            SCENARIOS.append({"name":name,"outcome":"unrun","reason":failure if not success else "not exercised"})
    for record,(proc,_,_) in zip(PROCESSES,CHILDREN): record["exit"]=proc.returncode
    report={"candidate_head":revision,"candidate_index_tree":subprocess.run(["git","write-tree"],cwd=ROOT,capture_output=True,text=True).stdout.strip(),"candidate_tracked_diff_sha256":hashlib.sha256(subprocess.check_output(["git","diff","--binary","HEAD"],cwd=ROOT)).hexdigest(),"entrypoint_kind":"nix_package" if OPTIONS.packaged else "development_build","binary_sha256":binary_hashes,"success":success,"failure":None if success else failure,"scenarios":SCENARIOS,"commands":COMMANDS,"owned_processes":PROCESSES,"runtime_state":"owned disposable state removed after captured evidence" if success else str(WORK),"unimplemented":["formal M1 acceptance", "general read model/index and its future fault qualification","MCP","confirmed delivery","full target schema and Timescale","sealed lifecycle","owner-tag shared dependency publication"]}
    if OPTIONS.demo_spec:
        report["demo"]={"oracle_revision":__import__("demo_cases").ORACLE_REVISION,"control":OPTIONS.demo_control,"provider_loss":OPTIONS.demo_provider_loss,"scope":"packaged Atuin raw-event driver; canonical row material, no SQLite snapshot retention or read-model index"}
    (REPORT/"result.json").write_text(json.dumps(report,indent=2)+"\n")
    if success: shutil.rmtree(WORK)
    print(json.dumps({"success":success,"report":str(REPORT/"result.json"),"scenarios":len(SCENARIOS)}))
    sys.exit(0 if success else 1)
