"""Bounded observations at named recovery phases; no sampler or durable monitor."""
import hashlib
import json
import os
from pathlib import Path
import resource
import time


def usage():
    def one(who):
        u=resource.getrusage(who)
        return {'user_seconds':u.ru_utime,'system_seconds':u.ru_stime,'max_rss_kib':u.ru_maxrss,
            'minor_faults':u.ru_minflt,'major_faults':u.ru_majflt,'voluntary_switches':u.ru_nvcsw,
            'involuntary_switches':u.ru_nivcsw}
    return {'monotonic':time.monotonic(),'self':one(resource.RUSAGE_SELF),'reaped_children':one(resource.RUSAGE_CHILDREN)}


def usage_delta(before, after):
    return {'elapsed_seconds':after['monotonic']-before['monotonic'],
        'cpu_seconds':{kind:{key:after[kind][key]-before[kind][key] for key in ('user_seconds','system_seconds')}
            for kind in ('self','reaped_children')},
        'raw_before':before,'raw_after':after,
        'scope':'harness interval from before package selection through owned child reaping/PostgreSQL stop; excludes managed-environment setup, final report serialization and successful workspace deletion; child CPU includes only descendants accounted by Linux wait/rusage, not an inclusive process-tree CPU total',
        'rss_scope':'Linux ru_maxrss is an individual high-water statistic, not simultaneous aggregate; child value is the largest accounted child high-water, not a sum; do not subtract maxima',
        'physical_io':{'value':None,'reason':'no device-level attribution; file lengths and ru_maxrss do not measure physical IO'},
        'simultaneous_aggregate_peak_rss':{'value':None,'reason':'no continuous simultaneous process-tree RSS sampler; named phase samples and individual VmHWM cannot recover this peak'}}


def proc_fields(stat, status):
    fields=stat[stat.rindex(')')+2:].split()
    values={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in status.splitlines() if ':' in line}
    ticks=os.sysconf('SC_CLK_TCK')
    return {'start_ticks':int(fields[19]),'user_cpu_seconds':int(fields[11])/ticks,
        'system_cpu_seconds':int(fields[12])/ticks,
        'VmRSS':values.get('VmRSS'),'VmHWM':values.get('VmHWM'),
        'scope':'one process at sample time; VmHWM is that process lifetime high-water, not simultaneous aggregate'}


def process(pid, role):
    path=Path('/proc')/str(pid)
    try:
        stat=(path/'stat').read_text();status=(path/'status').read_text();value=proc_fields(stat,status)
        assert proc_fields((path/'stat').read_text(),status)['start_ticks']==value['start_ticks']
        return dict(value,pid=pid,role=role,sampled_monotonic=time.monotonic())
    except (OSError, ValueError, AssertionError) as error:
        return {'pid':pid,'role':role,'unavailable':str(error),'sampled_monotonic':time.monotonic()}


def filesystem(path):
    began=time.monotonic();size=allocated=count=unavailable=0
    if path.exists():
        for p in path.rglob('*'):
            try:
                if not p.is_file():continue
                stat=p.stat();size+=stat.st_size;allocated+=stat.st_blocks*512;count+=1
            except OSError:unavailable+=1
    return {'path':str(path),'file_length_bytes':size,'allocated_bytes':allocated,'files':count,
        'unavailable_files':unavailable,'sample_seconds':time.monotonic()-began,
        'scope':'recursive file stats over this interval; not an atomic snapshot or physical IO'}


def reuse_accounting(path, pin):
    report=json.loads(path.read_text());account_path=path.parent/'retention-accounting.json';account=json.loads(account_path.read_text())
    assert report['success'] and report['postgres_stop_exit']==0 and report['candidate_tracked_diff_sha256']==hashlib.sha256(b'').hexdigest()
    assert report['prebuilt_package']['pin']['package']==pin['package']
    assert report['binary_sha256']=={name:pin['binaries'][name]['sha256'] for name in ['sinexd','sinexctl']}, 'prior accounting used different runtime binaries'
    assert len(account['snapshots'])>=3 and account['same_state_restarts']==0
    assert all(s['source']['last_run']['status']=='completed' for s in account['snapshots'])
    assert any(s['name'].startswith('three held completed') and s['outcome']=='passed' for s in report['scenarios'])
    return {'report':str(path.resolve()),'report_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
        'accounting':str(account_path.resolve()),'accounting_sha256':hashlib.sha256(account_path.read_bytes()).hexdigest(),
        'candidate':report['candidate_head'],'tree':report['candidate_index_tree'],
        'package':pin['package'],'binary_sha256':report['binary_sha256'],
        'scope':'delivered three-poll content/custody accounting reused, not rerun; exact per-poll bytes stay owned by this receipt'}


def phase_snapshot(h, label, daemon, started, calls, transport, interventions):
    run=h['run'];work=h['WORK'];report=h['REPORT']
    sql="SELECT jsonb_build_object('database_bytes',pg_database_size(current_database()),'event_rows',(SELECT count(*) FROM core.events),'materials',(SELECT count(*) FROM raw.source_material_registry),'blob_logical_bytes',(SELECT coalesce(sum(size_bytes),0) FROM core.blobs),'observations',(SELECT count(*) FROM raw.observations),'source_rows_read',(SELECT coalesce(sum(rows_read),0) FROM raw.observations),'repeated',(SELECT coalesce(sum(repeated),0) FROM raw.observations))"
    database=json.loads(run(['psql',h['admin'],'-XqAt','-v','ON_ERROR_STOP=1','-c',sql],label='whole-loop read-only storage observation').stdout)
    processes=[process(daemon.pid,'sinexd')]
    pid_file=work/'pg/postmaster.pid'
    try:
        postmaster=int(pid_file.read_text().splitlines()[0]);pg=[postmaster]
        children=Path('/proc')/str(postmaster)/'task'/str(postmaster)/'children'
        pg.extend(map(int,children.read_text().split()))
        processes.extend(process(pid,'postgres postmaster' if pid==postmaster else 'postgres child') for pid in pg)
    except (OSError,ValueError) as error:processes.append({'role':'postgres','unavailable':str(error)})
    return {'phase':label,'elapsed_from_journey_start_seconds':time.monotonic()-started,'database':database,
        'files':{name:filesystem(path) for name,path in [('retained',work/'recovery-blobs'),('workspace_total',work),('report',report)]},
        'processes':processes,'harness_usage':usage(),'CLI_call_count':len(calls),'CLI_stdout_bytes':sum(c['bytes'] for c in calls),
        'API_MCP_response_bytes':sum(c.get('response_body_bytes',c.get('response_line_bytes',0)) for c in transport),
        'API_MCP_response_count':len(transport),'interventions_so_far':len(interventions),
        'scratch_only_bytes':{'value':None,'reason':'no separate scratch directory: workspace_total overlaps PostgreSQL, retained store, provider and runtime fixtures; do not sum it with those components'},
        'scope':'DB statement then filesystem and individual process samples; these are not one atomic or simultaneous snapshot; transient open/tmp files may contribute to retained/scratch lengths'}
