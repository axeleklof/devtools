import subprocess

import pytest

from devtools import oneshot


def test_copy_to_clipboard_uses_pbcopy_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation: dict = {}
    monkeypatch.setattr(
        oneshot.shutil,
        "which",
        lambda tool: "/usr/bin/pbcopy" if tool == "pbcopy" else None,
    )

    def run(cmd, **kwargs):
        invocation["cmd"] = cmd
        invocation.update(kwargs)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(oneshot.subprocess, "run", run)

    assert oneshot._copy_to_clipboard("ls -la") is True
    assert invocation["cmd"] == ["pbcopy"]
    assert invocation["input"] == "ls -la"


def test_copy_to_clipboard_uses_xclip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(
        oneshot.shutil,
        "which",
        lambda tool: "/usr/bin/xclip" if tool == "xclip" else None,
    )
    monkeypatch.setattr(
        oneshot.subprocess,
        "run",
        lambda cmd, **kwargs: commands.append(cmd)
        or subprocess.CompletedProcess(cmd, 0, "", ""),
    )

    assert oneshot._copy_to_clipboard("ls -la") is True
    assert commands == [["xclip", "-selection", "clipboard"]]


def test_copy_to_clipboard_returns_false_without_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(oneshot.shutil, "which", lambda tool: None)

    assert oneshot._copy_to_clipboard("ls -la") is False
