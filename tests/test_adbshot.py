import subprocess
from pathlib import Path

import pytest

from devtools import adbshot


def test_resize_uses_imagemagick_on_linux(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    commands: list[list[str]] = []

    monkeypatch.setattr(
        adbshot.shutil,
        "which",
        lambda tool: "/usr/bin/magick" if tool == "magick" else None,
    )
    monkeypatch.setattr(
        adbshot.subprocess,
        "run",
        lambda cmd, **kwargs: commands.append(cmd)
        or subprocess.CompletedProcess(cmd, 0, b"", b""),
    )

    path = tmp_path / "shot.png"
    assert adbshot._resize_png(path, 1000) is True
    assert commands == [["magick", str(path), "-resize", "x1000", str(path)]]


def test_copy_png_uses_wayland_clipboard(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    invocation: dict = {}

    monkeypatch.setattr(
        adbshot.shutil,
        "which",
        lambda tool: "/usr/bin/wl-copy" if tool == "wl-copy" else None,
    )

    def run(cmd, **kwargs):
        invocation["cmd"] = cmd
        invocation.update(kwargs)
        return subprocess.CompletedProcess(cmd, 0, b"", b"")

    monkeypatch.setattr(adbshot.subprocess, "run", run)
    path = tmp_path / "shot.png"
    path.write_bytes(b"png data")

    assert adbshot._copy_png_to_clipboard(path) is True
    assert invocation["cmd"] == ["wl-copy", "--type", "image/png"]
    assert invocation["input"] == b"png data"


def test_copy_png_reports_missing_clipboard_tool(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(adbshot.shutil, "which", lambda tool: None)

    assert adbshot._copy_png_to_clipboard(tmp_path / "shot.png") is False
