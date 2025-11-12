import pathlib
import sys

from click.testing import CliRunner
import pytest

# Allow importing the local CLI package when pytest runs from workspace root.
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import exo  # type: ignore  # pylint: disable=import-error


@pytest.fixture()
def runner():
    return CliRunner()


def test_dlq_list_command_exists(runner):
    result = runner.invoke(exo.cli, ["dlq", "list"])

    assert result.exit_code == 0, (
        "exo dlq list should be implemented so operators can inspect backlog,"
        " but the command is still missing"
    )


def test_dlq_purge_command_exists(runner):
    result = runner.invoke(exo.cli, ["dlq", "purge"])

    assert result.exit_code == 0, (
        "exo dlq purge should exist to clear stuck entries,"
        " but invoking it still errors"
    )


def test_confirmations_tail_command_exists(runner):
    result = runner.invoke(exo.cli, ["confirmations", "tail"])

    assert result.exit_code == 0, (
        "exo confirmations tail should stream confirmations for debugging,"
        " yet the subcommand has not been wired"
    )
