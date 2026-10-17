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
    assert ".limit(11)" in command[-1]


def test_cat_all_prints_everything_without_warning(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    command = _run_cat(monkeypatch, 25, ["main", "users", "-n", "all"])

    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 25
    assert captured.err == ""
    assert ".limit(" not in command[-1]


def test_cat_bare_word_filters_client_side(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    command = _run_cat(monkeypatch, 25, ["main", "users", "USER2", "-c"])

    assert capsys.readouterr().out.strip() == "6"  # user2, user20..user24
    assert ".limit(" not in command[-1] and "{_id: 1}" not in command[-1]


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


_OID = {"$oid": "65f1c0ffee65f1c0ffee65f1"}


def test_edit_diff_recurses_into_subdocuments_and_keeps_types_strict() -> None:
    old = {
        "_id": _OID,
        "role": "user",
        "score": 1,
        "settings": {"theme": "light", "lang": "sv"},
        "tags": ["a"],
        "created": {"$date": "2026-01-01T00:00:00Z"},
        "legacy": "x",
    }
    new = {
        "_id": _OID,
        "role": "admin",
        "score": 1.0,
        "settings": {"theme": "dark", "lang": "sv", "beta": True},
        "tags": ["a", "b"],
        "created": "2026-01-01T00:00:00Z",
    }

    ops = bongo._edit_changes(old, old, new)

    assert [(path, old_v, new_v) for path, old_v, new_v in ops] == [
        ("role", "user", "admin"),
        ("score", 1, 1.0),
        ("settings.theme", "light", "dark"),
        ("settings.beta", bongo._MISSING, True),
        ("tags", ["a"], ["a", "b"]),
        ("created", {"$date": "2026-01-01T00:00:00Z"}, "2026-01-01T00:00:00Z"),
        ("legacy", "x", bongo._MISSING),
    ]
    query, update = bongo._edit_update(_OID, ops)
    assert query == {
        "_id": _OID,
        "role": "user",
        "score": 1,
        "settings.theme": "light",
        "settings.beta": {"$exists": False},
        "tags": ["a"],
        "created": {"$date": "2026-01-01T00:00:00Z"},
        "legacy": "x",
    }
    assert update == {
        "$set": {
            "role": "admin", "score": 1.0, "settings.theme": "dark", "settings.beta": True,
            "tags": ["a", "b"], "created": "2026-01-01T00:00:00Z",
        },
        "$unset": {"legacy": ""},
    }
    assert bongo._edit_changes(old, old, dict(old)) == []


def test_edit_rejects_bad_edits() -> None:
    doc = {"_id": _OID, "name": "x"}

    for edited, message in [
        ([], "JSON object"),
        ({"name": "x"}, "_id cannot be changed"),
        ({"_id": {"$oid": "000000000000000000000000"}, "name": "x"}, "_id cannot be changed"),
        ({"_id": _OID, "a.b": 1}, "dotted"),
        ({"_id": _OID, "$bad": 1}, "dotted or \\$-prefixed"),
    ]:
        with pytest.raises(bongo._EditError, match=message):
            bongo._edit_changes(doc, doc, edited)


def test_edit_view_with_fields_and_hidden_content() -> None:
    doc = {
        "_id": _OID,
        "name": "Axel",
        "settings": {"theme": "light", "lang": "sv"},
        "logins": [{"ip": "1.1.1.1", "at": "x"}],
    }

    view = bongo._edit_view(doc, ["settings.theme"], [])
    assert view == {"_id": _OID, "settings": {"theme": "light"}}
    # Editing a shown field is fine; adding a field that exists but was hidden overwrites it, visibly.
    ops = bongo._edit_changes(doc, view, {"_id": _OID, "settings": {"theme": "dark"}, "name": "Bob"})
    assert ops == [("settings.theme", "light", "dark"), ("name", "Axel", "Bob")]
    # Removing a subdocument would also drop its hidden fields.
    with pytest.raises(bongo._EditError, match="'settings' contains fields hidden"):
        bongo._edit_changes(doc, view, {"_id": _OID})

    view = bongo._edit_view(doc, [], [["_id"], ["logins", "ip"]])
    assert view == {"_id": _OID, "name": "Axel", "settings": doc["settings"], "logins": [{"at": "x"}]}
    assert doc["logins"] == [{"ip": "1.1.1.1", "at": "x"}]  # the original is untouched
    with pytest.raises(bongo._EditError, match="'logins' contains fields hidden"):
        bongo._edit_changes(doc, view, {**view, "logins": []})
    assert bongo._edit_changes(doc, view, {**view, "name": "Bob"}) == [("name", "Axel", "Bob")]


def test_edit_keeps_bson_types_exact() -> None:
    canonical = {
        "i": {"$numberInt": "2"},
        "d": {"$numberDouble": "2.0"},
        "nan": {"$numberDouble": "NaN"},
        "long": {"$numberLong": "9007199254740993"},
        "at": {"$date": {"$numberLong": "1767225600500"}},
        "old": {"$date": {"$numberLong": "-1000"}},
        "nested": [{"n": {"$numberInt": "1"}}],
    }

    doc = bongo._friendly(canonical)

    assert doc == {
        "i": 2, "d": 2.0, "nan": {"$numberDouble": "NaN"}, "long": {"$numberLong": "9007199254740993"},
        "at": {"$date": "2026-01-01T00:00:00.500Z"}, "old": {"$date": {"$numberLong": "-1000"}}, "nested": [{"n": 1}],
    }
    assert bongo.json.dumps(doc["d"]) == "2.0"  # the double stays recognisable in the editor
    assert bongo._exact(doc) == {**canonical, "at": {"$date": "2026-01-01T00:00:00.500Z"}}
    assert bongo._exact([True, 2**40, float("inf")]) == [True, {"$numberLong": str(2**40)}, {"$numberDouble": "Infinity"}]

    script = bongo._update_script("main", "users", {"_id": _OID, "n": 2, "m": {"$exists": False}}, {"$set": {"n": 2.0}})
    assert '\\"n\\": {\\"$numberInt\\": \\"2\\"}' in script
    assert '\\"m\\": {\\"$exists\\": false}' in script
    assert '{\\"$set\\": {\\"n\\": {\\"$numberDouble\\": \\"2.0\\"}}}' in script
    assert script.count("{relaxed: false}") == 2


def test_edit_shell_literal() -> None:
    query, update = bongo._edit_update(
        _OID, [("at", {"$date": "2026-01-01T00:00:00Z"}, bongo._MISSING), ("n", bongo._MISSING, {"$numberLong": "9007199254740993"})]
    )

    assert bongo._shell_literal(query) == (
        '{"_id": ObjectId("65f1c0ffee65f1c0ffee65f1"), "at": ISODate("2026-01-01T00:00:00Z"), "n": {"$exists": false}}'
    )
    assert bongo._shell_literal(update) == '{"$set": {"n": NumberLong("9007199254740993")}, "$unset": {"at": ""}}'
    assert bongo._shell_literal({"$binary": {"base64": "", "subType": "00"}}).startswith("EJSON.parse(")
    assert bongo._shell_literal([2, 2.0, 2.5, 2**40]) == '[2, Double(2), 2.5, NumberLong("1099511627776")]'


def test_edit_args() -> None:
    args = bongo.parse_args(["edit", "main", "users", "axel", "-v", "-f", "settings"])

    assert args.command == "edit"
    assert (args.database, args.collection, args.terms) == ("main", "users", ["axel"])
    assert args.verbose is True and args.fields == "settings"
    with pytest.raises(SystemExit):
        bongo.parse_args(["edit", "main", "users", "-n", "3"])


_EDIT_DOCS = [
    {"_id": _OID, "name": "Axel", "role": "user"},
    {"_id": {"$oid": "000000000000000000000001"}, "name": "Bob", "role": "user"},
]


def _run_edit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    argv: list[str],
    edits: list,
    answers: list[str],
    matched: int = 1,
    picked: int | None = None,
) -> dict:
    """Run `bongo edit` with fake mongosh, editor, prompts and picker; returns what was observed."""
    seen: dict = {"commands": [], "updates": [], "buffers": []}

    def popen(cmd, **kwargs):
        seen["commands"].append(cmd)
        return _FakeExport([bongo.json.dumps(doc) + "\n" for doc in _EDIT_DOCS])

    def editor(path: Path) -> int:
        seen["buffers"].append(path.read_text())
        edit = edits.pop(0)
        if isinstance(edit, int):
            return edit
        path.write_text(edit if isinstance(edit, str) else bongo.json.dumps(edit))
        return 0

    def mongosh_json(uri: str, expression: str, timeout: float = 30):
        seen["updates"].append(expression)
        return {"matched": matched, "modified": matched}

    def pick(items: list[str], prompt: str, previews=None):
        seen["picker"] = items
        seen["previews"] = previews
        return items[picked] if picked is not None else None

    monkeypatch.setattr(bongo.subprocess, "Popen", popen)
    monkeypatch.setattr(bongo, "_run_editor", editor)
    monkeypatch.setattr(bongo, "_mongosh_json", mongosh_json)
    monkeypatch.setattr(bongo, "_pick", pick)
    monkeypatch.setattr("builtins.input", lambda prompt: answers.pop(0))
    monkeypatch.setattr(bongo, "_EDIT_LOG_PATH", tmp_path / "edits.jsonl")
    config = {"default": "local", "clusters": {"local": {"uri": "mongodb://localhost:27017", "protected": ["main"]}}}
    try:
        bongo._cmd_edit(config, bongo.parse_args(["edit", *argv]))
    except SystemExit as e:
        seen["exit"] = e.code
    return seen


def test_edit_applies_reviewed_changes_and_logs_them(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    seen = _run_edit(
        monkeypatch, tmp_path, ["main", "users", "axel", "-v"],
        edits=[{"_id": _OID, "name": "Axel", "role": "admin"}], answers=["y"],
    )

    out = capsys.readouterr().out
    assert "exit" not in seen
    assert [bongo.json.loads(b) for b in seen["buffers"]] == [_EDIT_DOCS[0]]  # bare word 'axel' filtered client-side to one doc
    assert ".limit(" not in seen["commands"][0][-1]
    assert "'local:main' is protected" in out
    assert '~ role  "user" → "admin"' in out
    assert 'updateOne(\n  {"_id": ObjectId("65f1c0ffee65f1c0ffee65f1"), "role": "user"},\n  {"$set": {"role": "admin"}}\n)' in out
    assert "Updated 65f1c0ffee65f1c0ffee65f1 (1 field;" in out
    assert '\\"$set\\":{\\"role\\":\\"admin\\"}' in seen["updates"][0].replace(" ", "")
    entry = bongo.json.loads((tmp_path / "edits.jsonl").read_text())
    assert entry["target"] == "local:main" and entry["before"] == _EDIT_DOCS[0]
    assert entry["update"] == {"$set": {"role": "admin"}}


def test_edit_picks_among_matches_and_reopens_on_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bob = _EDIT_DOCS[1]
    seen = _run_edit(
        monkeypatch, tmp_path, ["main", "users"],
        edits=["{not json", {**bob, "_id": "x"}, {**bob, "role": "admin"}, {**bob, "role": "owner"}],
        answers=["", "y", "e", "n"],
        picked=1,
    )

    assert seen["picker"][1] == '000000000000000000000001  {"name": "Bob", "role": "user"}'
    assert bongo.json.loads(seen["previews"][1]) == bob  # pretty-printed as it opens in the editor
    assert len(seen["previews"][1].splitlines()) == 7  # multi-line, not one compact line
    assert ".limit(201)" in seen["commands"][0][-1]
    # The buffer opens with the document, and each reopen keeps what was typed so far.
    assert [bongo.json.loads(seen["buffers"][0])] == [bob]
    assert seen["buffers"][1:] == ["{not json", bongo.json.dumps({**bob, "_id": "x"}), bongo.json.dumps({**bob, "role": "admin"})]
    assert seen["exit"] == "bongo: aborted"
    assert seen["updates"] == []
    err = capsys.readouterr().err
    assert "Expecting property name" in err and "_id cannot be changed" in err


def test_pick_with_previews_shows_them_in_an_fzf_pane(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict = {}

    def run(cmd, input, **kwargs):
        seen["cmd"], seen["input"] = cmd, input
        workdir = cmd[cmd.index("--preview") + 1].split()[1].rsplit("/", 1)[0]
        seen["preview_1"] = Path(workdir, "1").read_text()
        return subprocess.CompletedProcess(cmd, 0, "1\tsecond item\n", "")

    monkeypatch.setattr(bongo.shutil, "which", lambda tool: "/usr/bin/fzf")
    monkeypatch.setattr(bongo.subprocess, "run", run)

    assert bongo._pick(["first item", "second item"], "pick:", ["one\n1", "two\n2"]) == "second item"
    assert seen["input"] == "0\tfirst item\n1\tsecond item"
    assert seen["preview_1"] == "two\n2"
    assert seen["cmd"][seen["cmd"].index("--with-nth") + 1] == "2.."
    assert seen["cmd"][seen["cmd"].index("--preview-window") + 1].startswith("right:")


def test_edit_without_changes_or_with_editor_failure_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    seen = _run_edit(monkeypatch, tmp_path, ["main", "users", "axel"], edits=[_EDIT_DOCS[0]], answers=[])
    assert "No changes" in capsys.readouterr().out
    assert seen["updates"] == []

    seen = _run_edit(monkeypatch, tmp_path, ["main", "users", "axel"], edits=[1], answers=[])
    assert seen["exit"] == "bongo: editor exited with code 1 — nothing written"

    seen = _run_edit(monkeypatch, tmp_path, ["main", "users"], edits=[], answers=[])
    assert seen["exit"] == "bongo: aborted"  # picker dismissed


def test_edit_conflict_keeps_the_edited_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen = _run_edit(
        monkeypatch, tmp_path, ["main", "users", "axel"],
        edits=[{**_EDIT_DOCS[0], "role": "admin"}], answers=["y"], matched=0,
    )

    assert "changed since it was opened" in seen["exit"]
    kept = Path(seen["exit"].rsplit(" ", 1)[1].rstrip(")"))
    assert bongo.json.loads(kept.read_text())["role"] == "admin"
    assert not (tmp_path / "edits.jsonl").exists()
    bongo.shutil.rmtree(kept.parent)


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


_COMPLETE_CONFIG = {
    "default": "local",
    "clusters": {"local": {"uri": "mongodb://localhost:27017"}, "dev": {"uri": "mongodb://dev.test"}},
    "scripts": {"adduser": "scripts/adduser.js"},
}


@pytest.fixture
def complete_names(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[str]:
    """Stub the cluster lookups behind completion; returns the keys that were looked up."""
    names = {"local": ["admin", "main", "pr-1"], "dev": ["staging"], "dev:staging": ["users", "system.views", "alarms"]}
    looked_up: list[str] = []

    def lookup(key: str, uri: str, expression: str) -> list[str]:
        looked_up.append(key)
        return names.get(key, [])

    monkeypatch.setattr(bongo, "_load_config", lambda: _COMPLETE_CONFIG)
    monkeypatch.setattr(bongo, "_complete_names", lookup)
    monkeypatch.setattr(bongo, "_SNAPSHOT_DIR", tmp_path)
    return looked_up


def test_complete_subcommands_and_flags(complete_names: list[str]) -> None:
    assert "cat\tprint documents from a collection, with simple filters" in bongo._complete(["c"])
    assert bongo._complete(["nope", ""]) == []

    flags = bongo._complete(["cp", "main", "-"])
    assert "--force\tallow overwriting a protected database" in flags
    assert not any(flag.startswith("-h") for flag in flags)
    assert bongo._complete(["cp", "main", "pr-1", ""]) == flags  # nothing positional left to offer
    assert complete_names == []


def test_complete_databases(complete_names: list[str]) -> None:
    # No cluster typed yet: cluster prefixes plus the default cluster's databases, minus system ones.
    assert bongo._complete(["cat", ""]) == ["dev:", "local:", "main", "pr-1"]
    assert bongo._complete(["cp", "main", "dev:st"]) == ["dev:staging"]
    assert bongo._complete(["ls", "nope:"]) == []
    assert bongo._complete(["prune", ""]) == ["dev", "local"]


def test_complete_collections_skip_flags_and_their_values(complete_names: list[str]) -> None:
    assert bongo._complete(["cat", "dev:staging", ""]) == ["alarms", "users"]
    assert bongo._complete(["cat", "-n", "5", "-r", "dev:staging", "us"]) == ["alarms", "users"]
    assert bongo._complete(["cat", "dev:staging", "-n", ""]) == []  # the value of -n
    assert bongo._complete(["cat", "bad name", ""]) == []
    assert "dev:bad name" not in complete_names


def test_complete_collections_of_branch_database(
    complete_names: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bongo, "_git_branch", lambda: "feature/x")

    bongo._complete(["cat", ".", ""])

    assert complete_names == ["local:feature-x"]


def test_complete_scripts_and_snapshots(complete_names: list[str], tmp_path: Path) -> None:
    for name in ("local__main__20260101-000000", "local__main__20260102-000000", "dev__staging__20260101-000000"):
        (tmp_path / f"{name}.archive.gz").touch()

    assert bongo._complete(["run", ""]) == ["adduser", ":files"]
    assert bongo._complete(["run", "adduser", "main", "--", ""]) == []
    assert bongo._complete(["restore", ""]) == ["dev:", "local:", "main"]
    assert bongo._complete(["restore", "dev:"]) == ["dev:staging"]
    assert bongo._complete(["restore", "--file", ""]) == [":files"]
    assert complete_names == []  # snapshots are local files


def test_complete_names_caches_results_and_failures(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[str] = []

    def mongosh_json(uri: str, expression: str, timeout: float = 30):
        calls.append(uri)
        if uri == "down":
            raise SystemExit("bongo: mongosh timed out")
        return ["main", "pr-1"]

    now = [1000.0]
    monkeypatch.setattr(bongo, "_COMPLETE_CACHE_PATH", tmp_path / "cache" / "completion.json")
    monkeypatch.setattr(bongo, "_mongosh_json", mongosh_json)
    monkeypatch.setattr(bongo.time, "time", lambda: now[0])

    assert bongo._complete_names("local", "up", "x") == ["main", "pr-1"]
    assert bongo._complete_names("dev", "down", "x") == []
    assert bongo._complete_names("local", "up", "x") == ["main", "pr-1"]
    assert bongo._complete_names("dev", "down", "x") == []
    assert calls == ["up", "down"]

    now[0] += bongo._COMPLETE_FAILURE_TTL_SECONDS + 1  # failures are retried sooner
    bongo._complete_names("local", "up", "x")
    bongo._complete_names("dev", "down", "x")
    assert calls == ["up", "down", "down"]

    bongo._clear_complete_cache()
    bongo._complete_names("local", "up", "x")
    assert calls == ["up", "down", "down", "up"]


def test_complete_command_prints_candidates(complete_names: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    bongo.main(["__complete", "cat", "dev:"])
    assert capsys.readouterr().out == "dev:staging\n"

    bongo.main(["completion", "zsh"])
    assert "compdef _bongo bongo" in capsys.readouterr().out
