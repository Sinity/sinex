#!/usr/bin/env python3
"""Independent loopback wire discriminator. No daemon/database or actual mutation."""
import argparse
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import tempfile
import threading

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--expect',choices=['vulnerable','bound'],required=True)
args=parser.parse_args()
action='01800000-0000-7000-8000-000000000011'
target='01800000-0000-7000-8000-000000000022'
other='01800000-0000-7000-8000-000000000033'
base={'operation_id':'01800000-0000-7000-8000-000000000044','action_id':action,'method':'delete.run','state':'completed','target':target,'occurrence':{'source':'terminal.atuin-history','event_type':'command.executed','equivalence_key':'["01800000-0000-7000-8000-000000000055","synthetic-a"]'},'exclusion_operation':'01800000-0000-7000-8000-000000000044','removed_exclusion_operation':None,'archived':[target],'reason':None,'raw_ref':None}
fixtures=[('valid',base)]
for field,value in [('action_id',other),('method','delete.undo'),('target',other)]:
    wrong=copy.deepcopy(base);wrong[field]=value;fixtures.append(('wrong_'+field,wrong))
    missing=copy.deepcopy(base);missing.pop(field);fixtures.append(('missing_'+field,missing))
fixtures += [('untyped_empty',{}),('untyped_null',None)]
observations=[]
with tempfile.TemporaryDirectory(prefix='sinex-delete-mock-',dir='/realm/tmp/work') as scratch:
    token=Path(scratch)/'token';token.write_text('synthetic-mock-client-token');token.chmod(0o600)
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append(received)
            encoded=json.dumps({'jsonrpc':'2.0','id':1,'result':answer}).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(encoded)));self.end_headers();self.wfile.write(encoded)
        def log_message(self,*_): pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        for name,answer in fixtures:
            requests=[]
            argv=[str(args.binary),'--endpoint',f'http://127.0.0.1:{server.server_port}/rpc','--token-file',str(token),'delete','sinex:ev/'+target,'--action-id',action,'--json']
            result=subprocess.run(argv,capture_output=True,text=True,timeout=10)
            observations.append({'case':name,'argv':argv,'request':requests,'mock_response':{'jsonrpc':'2.0','id':1,'result':answer},'exit':result.returncode,'stdout':result.stdout,'stderr':result.stderr})
    finally:
        server.shutdown();thread.join();server.server_close()
receipt={'scope':'Independent real CLI against mock HTTP success replies, not actual daemon/write qualification','expectation':args.expect,'binary':str(args.binary),'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest(),'action_id':action,'target':target,'observations':observations}
args.output.write_text(json.dumps(receipt,indent=2)+'\n')
assert observations[0]['exit']==0,observations[0]
for row in observations[1:]:
    expected=0 if args.expect=='vulnerable' else 5
    assert row['exit']==expected,(row['case'],row['exit'],row['stderr'])
    if args.expect=='bound':
        assert not row['stdout']
        error=json.loads(row['stderr']);assert error['kind']=='partial' and error['details']['action_id']==action,error
print(json.dumps({'receipt':str(args.output),'binary_sha256':receipt['binary_sha256'],'cases':len(observations),'exits':{r['case']:r['exit'] for r in observations}}))
