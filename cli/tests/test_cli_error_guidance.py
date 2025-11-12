import pathlib
import sys

from click.testing import CliRunner
import pytest

# Ensure cli/ package import works whether running via pytest -k or python -m pytest
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import exo  # type: ignore  # pylint: disable=import-error
from rpc_client import SinexRPCError  # type: ignore  # pylint: disable=import-error


@pytest.fixture()
def runner():
    return CliRunner()


def test_query_surfaces_rate_limit_guidance(monkeypatch, runner):
    def _raise_rate_limit(*_args, **_kwargs):
        raise SinexRPCError(429, "Too Many Requests")

    monkeypatch.setattr(exo, "_query_with_rpc", _raise_rate_limit)

    result = runner.invoke(exo.cli, ["query", "--source", "fs-watcher"])

    assert result.exit_code == 1
    assert (
        "rate limit" in result.output.lower()
    ), "CLI should mention rate limiting guidance when the gateway returns HTTP 429"


def test_query_prompts_for_auth_on_unauthorized(monkeypatch, runner):
    def _raise_unauthorized(*_args, **_kwargs):
        raise SinexRPCError(401, "Unauthorized")

    monkeypatch.setattr(exo, "_query_with_rpc", _raise_unauthorized)

    result = runner.invoke(exo.cli, ["query", "--source", "fs-watcher"])

    assert result.exit_code == 1
    assert (
        "auth" in result.output.lower()
    ), "CLI should reference authentication or tokens when RPC returns HTTP 401"
