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
OPTIONS = parser.parse_args()
BIN_DIR = Path(os.environ.get("SINEX_FOUNDATION_BIN_DIR", str(ROOT/"target/debug")))
WORK = Path(tempfile.mkdtemp(prefix="sinex-foundation-", dir="/realm/tmp/work"))
REPORT = Path(os.environ.get("SINEX_FOUNDATION_REPORT_DIR", str(WORK.parent / (WORK.name + "-result"))))
REPORT.mkdir(parents=True, exist_ok=False)
COMMANDS = []
SCENARIOS = []
PLANNED = ["cold query-free admin build", "canonical schema cold apply and idempotent check", "owned PostgreSQL extension and checksum contract", "checked runtime build and pure contracts", "canonical hypertables, real role grants, writer admission and restart", "invalid and unsupported config fail before startup", "occupied endpoint leaves unowned resources untouched", "actual daemon CLI transport and truthful scope", "partial start cannot steal writer and clean shutdown releases ownership"]
if OPTIONS.packaged:
    PLANNED.insert(0, "Nix package entrypoint selection")
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
    if result.returncode != expected:
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


def start_daemon(config, name):
    stdout = (REPORT / (name+".stdout")).open("w")
    stderr = (REPORT / (name+".stderr")).open("w")
    proc = subprocess.Popen([str(BIN_DIR/"sinexd"),"serve","--config",str(config)], env=ENV, stdout=stdout, stderr=stderr)
    CHILDREN.append((proc,stdout,stderr))
    PROCESSES.append({"pid":proc.pid,"argv":[str(BIN_DIR/"sinexd"),"serve","--config",str(config)],"stdout":name+".stdout","stderr":name+".stderr"})
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
        scenario("Nix package entrypoint selection", select_package)
    # Build admin before the first cluster exists, with no ambient SQLx/PG routing.
    scenario("cold query-free admin build", lambda: run(["cargo","build","--locked","-p","sinexd","--no-default-features"], label="cold admin"))
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
    admin_binary=ROOT/"target/debug/sinexd"
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
    scenario("checked runtime build and pure contracts", lambda: (
        run(["cargo","build","--locked","--workspace"],label="runtime against isolated schema"),
        run(["cargo","test","--locked","-p","sinex-model","-p","sinex-protocol","-p","sinexd"],label="pure and configuration contracts")))
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
        run([daemon,"serve","--config",bad],expected=1,label="unsupported source rejected")
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
        assert rpc(endpoint,"synthetic-foundation-token","query.run")["error"]["kind"]=="not_found"
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
    binary_hashes={name:hashlib.sha256((BIN_DIR/name).read_bytes()).hexdigest() for name in ["sinexd","sinexctl"] if (BIN_DIR/name).exists() and str(BIN_DIR/name) in used}
    for name in PLANNED:
        if name not in {s["name"] for s in SCENARIOS}:
            SCENARIOS.append({"name":name,"outcome":"unrun","reason":failure if not success else "not exercised"})
    for record,(proc,_,_) in zip(PROCESSES,CHILDREN): record["exit"]=proc.returncode
    report={"candidate_head":revision,"candidate_index_tree":subprocess.run(["git","write-tree"],cwd=ROOT,capture_output=True,text=True).stdout.strip(),"candidate_tracked_diff_sha256":hashlib.sha256(subprocess.check_output(["git","diff","--binary","HEAD"],cwd=ROOT)).hexdigest(),"entrypoint_kind":"nix_package" if OPTIONS.packaged else "development_build","binary_sha256":binary_hashes,"success":success,"failure":None if success else failure,"scenarios":SCENARIOS,"commands":COMMANDS,"owned_processes":PROCESSES,"runtime_state":"owned disposable state removed after captured evidence","unimplemented":["Atuin/M1","MCP","confirmed delivery","full target schema and Timescale","sealed lifecycle","owner-tag shared dependency publication"]}
    (REPORT/"result.json").write_text(json.dumps(report,indent=2)+"\n")
    shutil.rmtree(WORK)
    print(json.dumps({"success":success,"report":str(REPORT/"result.json"),"scenarios":len(SCENARIOS)}))
    sys.exit(0 if success else 1)
