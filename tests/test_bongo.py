import argparse
import subprocess
from pathlib import Path

import pytest

from devtools import bongo


def test_check_connect_flag() -> None:
    args = bongo.parse_args(["check", "--connect"])

    assert args.command == "check"
    assert args.connect is True


def test_check_cluster_connection_redacts_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    uri = "mongodb://alice:secret@example.test"
    result = subprocess.CompletedProcess(
        [],
        1,
        "",
        f"cannot connect to {uri}",
    )
    monkeypatch.setattr(bongo.subprocess, "run", lambda *args, **kwargs: result)

    connected, detail = bongo._check_cluster_connection(uri)

    assert connected is False
    assert "secret" not in detail
    assert "alice:***" in detail


def test_check_cluster_connection_hints_at_atlas_network_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uri = "mongodb+srv://alice:secret@example.mongodb.net"
    result = subprocess.CompletedProcess([], 1, "", "MongoServerSelectionError: timed out")
    monkeypatch.setattr(bongo.subprocess, "run", lambda *args, **kwargs: result)

    connected, detail = bongo._check_cluster_connection(uri)

    assert connected is False
    assert "Atlas Network Access" in detail
    assert "IP access list" in detail


def test_check_cluster_connection_uses_short_default_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uri = "mongodb://localhost:27017"
    command: list[str] = []

    def run(args, **kwargs):
        command.extend(args)
        return subprocess.CompletedProcess([], 0, "1\n", "")

    monkeypatch.setattr(bongo.subprocess, "run", run)

    assert bongo._check_cluster_connection(uri) == (True, "")
    assert command[1] == "mongodb://localhost:27017?serverSelectionTimeoutMS=5000"


def test_check_cluster_connection_preserves_configured_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uri = "mongodb://localhost:27017?serverSelectionTimeoutMS=2000"
    command: list[str] = []

    def run(args, **kwargs):
        command.extend(args)
        return subprocess.CompletedProcess([], 0, "1\n", "")

    monkeypatch.setattr(bongo.subprocess, "run", run)

    assert bongo._check_cluster_connection(uri) == (True, "")
    assert command[1] == uri


def test_check_connect_attempts_every_cluster(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """\
default = "first"

[clusters.first]
uri = "mongodb://first.example.test"

[clusters.second]
uri = "mongodb://second.example.test"
"""
    )
    attempts: list[str] = []

    def check_connection(uri: str) -> tuple[bool, str]:
        attempts.append(uri)
        if "first" in uri:
            return False, "connection refused"
        return True, ""

    monkeypatch.setattr(bongo, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(bongo, "_require_tools", lambda *tools: None)
    monkeypatch.setattr(bongo, "_check_cluster_connection", check_connection)

    with pytest.raises(SystemExit) as error:
        bongo._cmd_check(argparse.Namespace(connect=True))

    captured = capsys.readouterr()
    assert error.value.code == 1
    assert attempts == [
        "mongodb://first.example.test",
        "mongodb://second.example.test",
    ]
    assert "second  OK" in captured.out
    assert "first  FAILED" in captured.err
