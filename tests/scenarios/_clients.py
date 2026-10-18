"""CLI and RPC clients of one daemon endpoint, with its MCP server, for scenarios that
write their own daemon config; and a wait that fails when the daemon exits."""
import json
import time

from _atuin_daemon import Mcp


class Clients:
    """The public clients of one daemon endpoint."""

    def __init__(self, h, endpoint, name):
        self.h, self.endpoint, self.name = h, endpoint, name
        self.token = (h['WORK']/'token').read_text()
        self.mcp = None

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
        """Start and initialize `sinexctl mcp`; returns its tool names."""
        self.close_mcp()
        self.mcp = Mcp(self.h, self)
        return self.mcp.tool_names()

    def tool(self, name, arguments, error=False):
        return self.mcp.tool(name, arguments, error)

    def close_mcp(self):
        if self.mcp is not None:
            self.mcp.close()
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
