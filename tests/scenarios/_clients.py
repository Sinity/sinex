"""CLI, RPC and MCP clients for scenarios that drive a running daemon."""
import json
import select
import subprocess
import time


class Clients:
    """The public clients of one daemon endpoint."""

    def __init__(self, h, endpoint, name):
        self.h, self.endpoint, self.name = h, endpoint, name
        self.token = (h['WORK']/'token').read_text()
        self.mcp = None
        self.request_id = 0

    def cli(self, command, *args, expected=(0, 5), token_file=None):
        """Run `sinexctl COMMAND ARGS --json`; return its JSON answer (stdout, or stderr on failure)."""
        result = self.h['run'](
            [self.h['BIN_DIR']/'sinexctl', '--endpoint', self.endpoint,
             '--token-file', token_file or self.h['WORK']/'token', command, *args, '--json'],
            expected=expected, label=f'{self.name} CLI {command}')
        return json.loads(result.stdout or result.stderr)

    def api(self, method='status', params=None):
        return self.h['rpc'](self.endpoint, self.token, method, params, timeout=30)

    def connect_mcp(self):
        """Start `sinexctl mcp` and initialize it; returns the tool names."""
        self.close_mcp()
        h = self.h
        stderr = (h['REPORT']/f'{self.name}-mcp.stderr').open('a')
        argv = [str(h['BIN_DIR']/'sinexctl'), '--endpoint', self.endpoint,
                '--token-file', str(h['WORK']/'token'), 'mcp']
        self.mcp = subprocess.Popen(argv, env=h['ENV'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=stderr, text=True, bufsize=1)
        h['CHILDREN'].append((self.mcp, self.mcp.stdout, stderr))
        h['PROCESSES'].append({'pid': self.mcp.pid, 'argv': argv})
        assert 'result' in self.mcp_call('initialize', {
            'protocolVersion': '2025-03-26', 'capabilities': {},
            'clientInfo': {'name': 'sinex-scenario', 'version': '1'}})
        self.mcp.stdin.write(json.dumps({'jsonrpc': '2.0', 'method': 'notifications/initialized'})+'\n')
        self.mcp.stdin.flush()
        return {t['name'] for t in self.mcp_call('tools/list', {})['result']['tools']}

    def mcp_call(self, method, params, seconds=35):
        self.request_id += 1
        request = {'jsonrpc': '2.0', 'id': self.request_id, 'method': method, 'params': params}
        self.mcp.stdin.write(json.dumps(request)+'\n')
        self.mcp.stdin.flush()
        deadline = time.monotonic()+seconds
        with (self.h['REPORT']/f'{self.name}-mcp.jsonl').open('a') as log:
            while True:
                ready, _, _ = select.select([self.mcp.stdout], [], [], max(0, deadline-time.monotonic()))
                assert ready, f'MCP gave no answer to {method} within {seconds}s'
                line = self.mcp.stdout.readline()
                assert line, 'MCP server exited'
                response = json.loads(line)
                log.write(json.dumps({'request': request, 'response': response})+'\n')
                if response.get('id') == self.request_id:
                    return response

    def tool(self, name, arguments, error=False):
        """Call an MCP tool; check whether it reported an error; return its structured answer."""
        result = self.mcp_call('tools/call', {'name': name, 'arguments': arguments})['result']
        assert bool(result.get('isError')) == error, result
        return result.get('structuredContent') or json.loads(result['content'][0]['text'])

    def close_mcp(self):
        if self.mcp is not None:
            if self.mcp.poll() is None:
                self.mcp.stdin.close()
                self.mcp.wait(timeout=30)
            assert self.mcp.returncode == 0, f'MCP server exited {self.mcp.returncode}'
            self.mcp = None


def wait(proc, label, fn, seconds=120, context=None):
    """Poll `fn` until it returns something truthy, while `proc` keeps running.

    Connection and decoding errors count as not yet; assertion errors fail at once.
    """
    deadline = time.monotonic()+seconds
    while time.monotonic() < deadline:
        assert proc.poll() is None, f'{label}: daemon exited {proc.returncode}'
        try:
            value = fn()
        except (OSError, KeyError, ValueError):
            value = None
        if value:
            return value
        time.sleep(.05)
    raise AssertionError(f'{label}: not reached within {seconds}s' + (f'; last seen {context}' if context else ''))
