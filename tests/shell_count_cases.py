#!/usr/bin/env python3
"""Ordinary PostgreSQL controls for the private two-source candidate-count query."""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import tempfile
import time
import traceback
import uuid

ROOT = Path(__file__).resolve().parents[1]
OLD = "UPDATE sinex_shell_nodes n SET count2=(SELECT count(*) FROM sinex_shell_nodes x WHERE x.source<>n.source AND x.host=n.host AND x.command_hash=n.command_hash AND x.command=n.command AND abs(x.at_ns-n.at_ns)<=$2::bigint AND (NOT $3 OR (NOT x.paired AND NOT n.paired))) WHERE n.id=ANY($1)"
SCHEMA = """CREATE TEMP TABLE sinex_shell_nodes(id uuid PRIMARY KEY,source smallint NOT NULL,host text,command text NOT NULL,command_hash text NOT NULL,at_ns numeric,paired boolean NOT NULL DEFAULT false,count2 bigint NOT NULL DEFAULT 0,count60 bigint NOT NULL DEFAULT 0);
CREATE INDEX ON sinex_shell_nodes(source,command_hash,at_ns);"""


def literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return "'" + str(value).replace("'", "''") + "'"


def nodes(plan):
    yield plan
    for child in plan.get('Plans', []):
        yield from nodes(child)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    args.report.mkdir(parents=True, exist_ok=False)
    work = Path(tempfile.mkdtemp(prefix='sinex-shell-count-', dir='/realm/tmp/work'))
    env = dict(os.environ)
    for key in ('DATABASE_URL', 'PGDATABASE', 'PGUSER', 'PGHOST', 'PGPORT'):
        env.pop(key, None)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    (work / 'socket').mkdir()
    env.update(PGDATA=str(work / 'pg'), PGHOST=str(work / 'socket'), PGPORT=str(port), PGDATABASE="postgres")
    commands = []
    started = False
    success = False
    failure = None
    def run(argv):
        result = subprocess.run(list(map(str, argv)), env=env, capture_output=True, text=True, timeout=120)
        index = len(commands)
        commands.append({'argv': list(map(str, argv)), 'exit': result.returncode})
        (args.report / f'{index:03}.stdout').write_text(result.stdout)
        (args.report / f'{index:03}.stderr').write_text(result.stderr)
        assert result.returncode == 0, f'command {index} exit {result.returncode}'
        return result.stdout
    try:
        run(['initdb', '--no-locale', '--auth=trust'])
        run(['pg_ctl', '-D', work / 'pg', '-l', args.report / 'postgres.log', '-o', f'-k {work / "socket"} -c listen_addresses= -p {port}', '-w', 'start'])
        started = True
        template = (ROOT / 'crate/sinex-store/src/shell-count.sql').read_text()
        new = template.replace('{field}', 'count2')
        rule = json.loads((ROOT / 'crate/sinex-store/src/shell-rule.json').read_text())
        assert rule['sources'] == ['terminal.atuin-history', 'terminal.zsh-history']
        # A literal oracle covers boundaries, duplicate endpoints, fake equal hashes,
        # null equality and both extrema without fixed-width timestamp arithmetic.
        rows = []
        def add(source, at, host='h', command='a', digest='same', paired=False):
            rows.append({'id': str(uuid.UUID(int=len(rows)+1)), 'source': source, 'at': at,
                         'host': host, 'command': command, 'hash': digest, 'paired': paired})
        for at in [0, 2_000_000_000, -2_000_000_000, 2_000_000_001, -2_000_000_001,
                   60_000_000_000, -60_000_000_000, 60_000_000_001,
                   -(2**63), 2**63-1, -(2**63)-60_000_000_000,
                   2**63-1+60_000_000_000, 10**30, -(10**30), None]:
            for source in (0, 1):
                add(source, at)
        add(1, 0); add(1, 0)
        add(0, 0, host=None); add(1, 0, host=None)
        add(1, 0, command='different full command')  # Deliberate hash collision.
        add(1, 0, host='other'); add(1, 0, paired=True); add(0, 0, paired=True)
        # Fractional numeric operands also distinguish accidental integer casts.
        add(0, Decimal('0.5')); add(1, Decimal('2000000000.5'))
        rng = random.Random(8429)
        for _ in range(96):
            add(rng.randrange(2), rng.choice([None, 0, 2_000_000_000, 2**63-1]),
                host=rng.choice(['h', 'other', None]), command=rng.choice(['a', 'b']),
                digest=rng.choice(['same', 'different']), paired=bool(rng.randrange(2)))
        values = ','.join('('+','.join(map(literal, [r['id'], r['source'], r['host'], r['command'], r['hash'], r['at'], r['paired']]))+')' for r in rows)
        seed = SCHEMA + '\nINSERT INTO sinex_shell_nodes(id,source,host,command,command_hash,at_ns,paired) VALUES '+values+';'
        cases = []
        for bound in [0, 2_000_000_000, 60_000_000_000, 2**63-1]:
            for fallback in (False, True):
                expected = {}
                for n in rows:
                    expected[n['id']] = sum(x['source'] != n['source'] and
                        x['host'] is not None and n['host'] is not None and x['host'] == n['host'] and
                        x['hash'] == n['hash'] and x['command'] == n['command'] and
                        x['at'] is not None and n['at'] is not None and
                        abs(Decimal(x['at'])-Decimal(n['at'])) <= bound and
                        (not fallback or (not x['paired'] and not n['paired'])) for x in rows)
                field = 'count60' if fallback else 'count2'
                for name, query in [('predecessor', OLD.replace('SET count2=', 'SET '+field+'=')), ('indexed', template.replace('{field}', field))]:
                    statements = [seed, 'PREPARE candidate_count(uuid[],bigint,boolean) AS '+query+';']
                    for offset in range(0, len(rows), 64):
                        keys = ','.join(literal(r['id']) for r in rows[offset:offset+64])
                        statements.append(f'EXECUTE candidate_count(ARRAY[{keys}]::uuid[],{bound},{literal(fallback)});')
                    statements.append(f"SELECT jsonb_object_agg(id::text,{field}) FROM sinex_shell_nodes;")
                    fixture = args.report / f'semantics-{bound}-{fallback}-{name}.sql'
                    fixture.write_text('\n'.join(statements))
                    result = run(['psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '--file', fixture])
                    actual = json.loads(result)
                    assert actual == expected, (name, bound, fallback, 'independent count mismatch')
                cases.append({'bound': bound, 'fallback': fallback, 'nodes': len(rows), 'old_new_oracle_equal': True})
        (args.report / 'semantics.json').write_text(json.dumps(cases, indent=2))
        # Match the owning temp-table lifecycle: indexes precede population; no
        # forced planner settings, added index or manual ANALYZE hides the plan.
        population = SCHEMA + """
INSERT INTO sinex_shell_nodes(id,source,host,command,command_hash,at_ns)
SELECT md5(n::text)::uuid,0,'h','command '||n,md5(n::text),n::numeric*1000000000 FROM generate_series(1,20000) n;
"""
        keys = ','.join("md5('"+str(n)+"')::uuid" for n in range(1, 65))
        plans = {}
        for name, query in [('predecessor', OLD), ('indexed', new)]:
            fixture = args.report / (name+'-plan.sql')
            fixture.write_text(population + 'PREPARE candidate_count(uuid[],bigint,boolean) AS '+query+';\nEXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE candidate_count(ARRAY['+keys+'],2000000000,false);')
            plan = json.loads(run(['psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '--file', fixture]))[0]
            plans[name] = plan
            if name == 'indexed':
                conditions = [n.get('Index Cond', '') for n in nodes(plan['Plan']) if 'source_command_hash_at_ns' in n.get('Index Name', '')]
                assert any(all(part in c for part in ['source', 'command_hash', 'at_ns >=', 'at_ns <=']) for c in conditions), conditions
        (args.report / 'plans.json').write_text(json.dumps(plans, indent=2))
        success = True
    except BaseException as error:
        failure = repr(error)
        (args.report / 'failure.txt').write_text(traceback.format_exc())
    finally:
        if started:
            try:
                run(['pg_ctl', '-D', work / 'pg', '-m', 'fast', '-w', 'stop'])
            except BaseException as error:
                success = False; failure = repr(error)
        result = {'success': success, 'failure': failure, 'commands': commands,
                  'work': str(work), 'state_removed': False,
                  'scope': 'ordinary PostgreSQL synthetic count equality and representative plans, not native/public capacity'}
        if success:
            shutil.rmtree(work); result['state_removed'] = True
        (args.report / 'result.json').write_text(json.dumps(result, indent=2))
        print(json.dumps({'success': success, 'report': str(args.report)}))
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
