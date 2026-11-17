"""Synthetic wire roundtrips and allocated-memory control for ARRAY receive."""
import base64
import json
import os
from pathlib import Path
import struct
import subprocess
import sys

COUNTS = [0, 64, 32, 21, 16, 12, 10, 9, 8, 6, 5, 4, 3, 2, 1]
WIDTHS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 16, 21, 32, 64]

def bitmap(values):
    # A single selector-1 block holds up to 64 one-bit NULL indicators.
    assert 0 < len(values) <= 64
    bits = sum((value is None) << n for n, value in enumerate(values))
    return struct.pack("!IIQQ", len(values), 1, 1, bits)

def datum(kind, value):
    if kind == "jsonb":
        return b"\x01" + json.dumps(value, ensure_ascii=False).encode()
    if kind == "text":
        return value.encode()
    return struct.pack("!q", value)

def encode(kind, values):
    has_nulls = any(value is None for value in values)
    header = bytes([1, has_nulls]) + b"pg_catalog\0" + kind.encode() + b"\0"
    header += bytes([has_nulls])
    if has_nulls:
        header += bitmap(values)
    present = [datum(kind, value) for value in values if value is not None]
    wire = header + b"\x01" + struct.pack("!I", len(present))
    wire += b"".join(struct.pack("!I", len(value)) + value for value in present)
    return base64.b64encode(wire).decode()

def decode(encoded):
    data = base64.b64decode(encoded)
    offset = 2
    assert data[0] == 1
    def take(size):
        nonlocal offset
        result = data[offset:offset + size]
        assert len(result) == size
        offset += size
        return result
    def number(fmt):
        return struct.unpack(fmt, take(struct.calcsize(fmt)))[0]
    def string():
        nonlocal offset
        end = data.index(0, offset)
        result = data[offset:end].decode()
        offset = end + 1
        return result
    assert string() == "pg_catalog"
    kind = string()
    has_nulls = number("!B")
    assert has_nulls == data[1]
    nulls = None
    if has_nulls:
        count, blocks = number("!I"), number("!I")
        selectors = [number("!Q") for _ in range((blocks + 15) // 16)]
        nulls = []
        for n in range(blocks):
            selector = (selectors[n // 16] >> ((n % 16) * 4)) & 15
            value = number("!Q")
            if selector == 15:
                nulls.extend([value & ((1 << 36) - 1)] * (value >> 36))
            else:
                width = WIDTHS[selector]
                nulls.extend((value >> (i * width)) & ((1 << width) - 1)
                             for i in range(COUNTS[selector]))
        nulls = nulls[:count]
        assert all(value in (0, 1) for value in nulls)
    assert number("!B") == 1
    count = number("!I")
    values = []
    for absent in nulls if nulls is not None else [False] * count:
        if absent:
            values.append(None)
            continue
        raw = take(number("!I"))
        if kind == "jsonb":
            assert raw[0] == 1
            values.append(json.loads(raw[1:]))
        elif kind == "text":
            values.append(raw.decode())
        else:
            values.append(struct.unpack("!q", raw)[0])
    assert offset == len(data)
    return kind, values

def cluster(label, binary, probe, large):
    root = Path.cwd() / label
    root.mkdir()
    data, socket = root / "data", root / "socket"
    socket.mkdir()
    env = dict(os.environ, PGHOST=str(socket), PGPORT="5432", PGDATABASE="postgres", PGUSER="receiver")
    def run(*args):
        return subprocess.check_output([str(binary / args[0]), *map(str, args[1:])], env=env, text=True)
    def sql(query):
        return subprocess.check_output([str(binary / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1"],
                                       input=query + ";\n", env=env, text=True).strip()
    started = False
    try:
        run("initdb", "-D", data, "--no-locale", "--encoding=UTF8", "--auth=trust", "-U", "receiver")
        run("pg_ctl", "-D", data, "-l", root / "server.log", "-o",
            "-k " + str(socket) + " -c listen_addresses= -c shared_buffers=16MB -c max_connections=10 -c shared_preload_libraries=timescaledb", "-w", "start")
        started = True
        sql("CREATE EXTENSION timescaledb")
        sql("CREATE FUNCTION receive_memory(regtype,text) RETURNS bigint AS '" + str(probe)
            + "','receive_memory' LANGUAGE C STRICT")
        cases = [("jsonb", [{"items": [[1, False], [2, "neutral"]]}, None, {"other": []}]),
                 ("text", ["", None, "neutral " * 8192, "end"]),
                 ("int8", [1, None, -3, 1 << 52])]
        for kind, values in cases:
            wire = encode(kind, values)
            output = sql("SELECT ('" + wire + "'::_timescaledb_internal.compressed_data)::text")
            assert decode(output) == (kind, values), (label, kind)
        # Prewarm the receiver before measuring its per-call allocations.
        sql("SELECT ('" + encode("jsonb", [{"warm": True}]) + "'::_timescaledb_internal.compressed_data)::text")
        allocated = int(sql("SELECT receive_memory('_timescaledb_internal.compressed_data'::regtype,'" + large + "')"))
        output = sql("SELECT ('" + large + "'::_timescaledb_internal.compressed_data)::text")
        assert decode(output) == decode(large)
        return {"allocated": allocated, "wire_bytes": len(base64.b64decode(large)), "cases": len(cases) + 1}
    finally:
        if started:
            run("pg_ctl", "-D", data, "-m", "fast", "-w", "stop")
            assert not (data / "postmaster.pid").exists()

def main():
    # Many small JSON objects produce receive parse trees larger than their datum.
    value = {"items": [{"a": n, "b": False} for n in range(2048)]}
    large = encode("jsonb", [value] * 128)
    before = cluster("before", Path(sys.argv[1]), Path(sys.argv[3]), large)
    after = cluster("after", Path(sys.argv[2]), Path(sys.argv[3]), large)
    assert before["allocated"] > after["allocated"] * 3, (before, after)
    result = {"before": before, "after": after, "ratio": before["allocated"] / after["allocated"]}
    Path(sys.argv[4]).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))

if __name__ == "__main__":
    main()
