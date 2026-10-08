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


def test_cat_terms_build_query_and_words() -> None:
    conditions, words, _ = bongo._parse_cat_terms(
        ["Axel", "role=admin", "age>30", "name~^ax", "zip!=12345", "address.city=Stockholm"]
    )

    assert words == ["axel"]
    assert conditions == [
        {"role": {"$in": ["admin"]}},
        {"age": {"$gt": 30}},
        {"name": {"$regex": "^ax", "$options": "i"}},
        {"zip": {"$nin": [12345, "12345"]}},
        {"address.city": {"$in": ["Stockholm"]}},
    ]


def test_cat_terms_infer_ids_dates_and_literals() -> None:
    oid = "65f1c0ffee65f1c0ffee65f1"
    conditions, words, _ = bongo._parse_cat_terms(
        [oid, f"owner={oid}", "active=true", "createdAt>=2026-01-01", "seen<2026-01-01T12:30"]
    )

    assert words == []
    assert conditions == [
        {"_id": {"$in": [{"$oid": oid}, oid]}},
        {"owner": {"$in": [{"$oid": oid}, oid]}},
        {"active": {"$in": [True]}},
        {"createdAt": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}},
        {"seen": {"$lt": {"$date": "2026-01-01T12:30:00Z"}}},
    ]


def test_cat_plain_words_and_projection() -> None:
    doc = bongo._plain(
        {
            "_id": {"$oid": "65f1c0ffee65f1c0ffee65f1"},
            "name": "Axel Eklöf",
            "created": {"$date": "2026-01-01T00:00:00Z"},
            "address": {"city": "Stockholm", "zip": 12345},
            "tags": ["Admin"],
        }
    )

    assert doc["_id"] == "65f1c0ffee65f1c0ffee65f1"
    assert doc["created"] == "2026-01-01T00:00:00Z"
    assert bongo._matches_words(doc, ["axel", "admin", "1234"])
    assert not bongo._matches_words(doc, ["axel", "name"])  # keys are not searched
    assert bongo._project(doc, ["address.city", "missing"]) == {
        "_id": "65f1c0ffee65f1c0ffee65f1",
        "address": {"city": "Stockholm"},
    }


def test_cat_args_allow_flags_anywhere() -> None:
    args = bongo.parse_args(["cat", "main", "users", "-n", "all", "axel", "-r", "role=admin"])

    assert args.command == "cat"
    assert (args.database, args.collection) == ("main", "users")
    assert args.terms == ["axel", "role=admin"]
    assert args.limit == "all"
    assert args.reverse is True
    assert bongo.parse_args(["cat", "main", "users", "-n", "3"]).limit == 3


class _FakeExport:
    def __init__(self, lines: list[str]) -> None:
        self.stdout = iter(lines)

    def wait(self) -> int:
        return 0

    def kill(self) -> None:
        pass


def _run_cat(monkeypatch: pytest.MonkeyPatch, docs: int, argv: list[str]) -> list[str]:
    commands: list[list[str]] = []

    def popen(cmd, **kwargs):
        commands.append(cmd)
        return _FakeExport([f'{{"_id": {{"$oid": "{i:024x}"}}, "name": "user{i}"}}\n' for i in range(docs)])

    monkeypatch.setattr(bongo.subprocess, "Popen", popen)
    config = {"default": "local", "clusters": {"local": {"uri": "mongodb://localhost:27017"}}}
    bongo._cmd_cat(config, bongo.parse_args(["cat", *argv]))
    return commands[0]


def test_cat_limits_output_and_warns_on_stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    command = _run_cat(monkeypatch, 25, ["main", "users"])

    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 10
    assert captured.out.splitlines()[0] == '{"_id": "000000000000000000000000", "name": "user0"}'
    assert "first 10 documents" in captured.err
    assert "--limit=11" in command


def test_cat_all_prints_everything_without_warning(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    command = _run_cat(monkeypatch, 25, ["main", "users", "-n", "all"])

    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 25
    assert captured.err == ""
    assert not any(arg.startswith("--limit") for arg in command)


def test_cat_bare_word_filters_client_side(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    command = _run_cat(monkeypatch, 25, ["main", "users", "USER2", "-c"])

    assert capsys.readouterr().out.strip() == "6"  # user2, user20..user24
    assert not any(arg.startswith(("--limit", "--fields")) for arg in command)


def test_cat_explicit_limit_does_not_warn(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_cat(monkeypatch, 25, ["main", "users", "-n", "3"])

    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 3
    assert captured.err == ""


def test_cat_exclude_and_depth() -> None:
    doc = {
        "_id": "1",
        "name": "Axel",
        "logins": [{"ip": "1.1.1.1", "at": "x"}, {"ip": "2.2.2.2", "at": "y"}],
        "address": {"city": "Stockholm", "geo": {"lat": 1, "lon": 2}},
        "empty": [],
    }

    bongo._exclude(doc, ["logins", "ip"])
    bongo._exclude(doc, ["name"])
    bongo._exclude(doc, ["missing", "deeper"])
    assert doc["logins"] == [{"at": "x"}, {"at": "y"}]
    assert "name" not in doc

    assert bongo._collapse(doc, 1) == {
        "_id": "1",
        "logins": "[… 2 items]",
        "address": "{… 2 fields}",
        "empty": [],
    }
    assert bongo._collapse(doc, 2)["address"] == {"city": "Stockholm", "geo": "{… 2 fields}"}
    assert bongo._render_json(bongo._collapse(doc, 1)["logins"]) == "[… 2 items]"


def test_cat_exclude_and_depth_flags(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_cat(monkeypatch, 1, ["main", "users", "-x", "_id,name"])
    assert capsys.readouterr().out.strip() == "{}"

    with pytest.raises(SystemExit):
        bongo.parse_args(["cat", "main", "users", "-d", "0"])


def test_cat_highlights_search_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bongo, "_c", lambda code, text: f"<{text}>" if code == "30;43" else text)
    _, _, highlights = bongo._parse_cat_terms(["axel", "email~^AX", "note~(unclosed", "role=admin"])
    doc = {
        "name": "Axel \"axe\" Axelsson",
        "email": "axel@x.com",
        "role": "admin",
        "friends": [{"email": "max@x.com"}],
        "age": 23,
    }

    rendered = bongo._render_json(doc, highlights=highlights)

    assert '"name": "<Axel> \\"axe\\" <Axel>sson"' in rendered
    assert '"email": "<axel>@x.com"' in rendered  # bare word and ^AX overlap into one span
    assert '"email": "max@x.com"' in rendered  # the regex only applies to the top-level email field
    assert '"role": "admin"' in rendered
    assert bongo._render_json({"age": 23}, highlights=bongo._parse_cat_terms(["23"])[2]) == '{\n  "age": <23>\n}'


def test_ls_database_lists_collections(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = {
        "default": "local",
        "clusters": {"local": {"uri": "mongodb://localhost:27017", "protected": ["main"]}},
    }
    stats = {"users": {"count": 1234}, "audit": {"count": 0}}
    monkeypatch.setattr(bongo, "_collection_stats", lambda uri, db: stats if db == "main" else {})

    for target in ("local:main", "main"):  # a bare name that is not a cluster is a db
        bongo._cmd_ls(config, bongo.parse_args(["ls", target]))
        lines = capsys.readouterr().out.splitlines()
        assert lines[0] == "local:main  [protected]"
        assert [line.split() for line in lines[1:]] == [["audit", "0", "docs"], ["users", "1,234", "docs"]]

    with pytest.raises(SystemExit, match="'nope' not found"):
        bongo._cmd_ls(config, bongo.parse_args(["ls", "local:nope"]))


def test_ls_cluster_still_lists_databases(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = {"default": "local", "clusters": {"local": {"uri": "mongodb://localhost:27017"}}}
    monkeypatch.setattr(bongo, "_list_databases", lambda uri: [{"name": "main", "size": 2048}])

    for argv in (["ls"], ["ls", "local"]):
        bongo._cmd_ls(config, bongo.parse_args(argv))
        assert "main" in capsys.readouterr().out
