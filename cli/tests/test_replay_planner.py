import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from replay_planner import ReplayPlanner


def _sample_event():
    return {
        "event_id": "01ABCDEF",
        "source": "fs-watcher",
        "event_type": "file.created",
        "host": "localhost",
        "payload": {"path": "/tmp/file"},
        "ts_orig": "2024-10-31T12:00:00+00:00",
    }


def test_replay_planner_database_target_errors(monkeypatch):
    planner = ReplayPlanner(db_url="postgresql://ignored")

    def _forbid_connection(*args, **kwargs):
        pytest.fail("Replay planner should not attempt a direct database write when exercising the staging harness")

    monkeypatch.setattr(planner, "get_connection", _forbid_connection)
    planner._replay_event(_sample_event(), "database")


def test_replay_planner_nats_target_publishes():
    planner = ReplayPlanner(db_url="postgresql://ignored")

    planner._replay_event(_sample_event(), "nats")
    pytest.fail("Replay planner's NATS target is still a stub; expected it to publish events")
