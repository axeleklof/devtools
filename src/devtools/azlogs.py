from __future__ import annotations

import argparse
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path

_BLOB_PATTERN = re.compile(r"^.+\.(\d{4}-\d{2}-\d{2})\.log$")

# Matches start-of-line level prefix: [ INFO ] or [ INFO - Service ] with optional colon
_PREFIX_RE = re.compile(rb"^\[ ([A-Z][A-Z_]*)(?:\s+-\s+[^\]]+)? \]:?")
# Matches inline field markers: | [ QUERY ]: or | [ USER ]:
_INLINE_KEY_RE = re.compile(rb"\| \[ ([A-Z][A-Z_]*) \]:")
# Matches timestamp: [2026-04-16 08:33:03]
_TIMESTAMP_RE = re.compile(rb" ?\[(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})\]")

_RESET = b"\033[0m"
_TIMESTAMP_COLOR = b"\033[2;33m"  # dim yellow

_LEVEL_COLORS: dict[bytes, bytes] = {
    b"ERROR": b"\033[91m",  # bright red
    b"WARN": b"\033[93m",  # bright yellow
    b"WARNING": b"\033[93m",  # bright yellow
    b"INFO": b"\033[96m",  # bright cyan
    b"DEBUG": b"\033[2m",  # dim
    b"TRACE": b"\033[2m",  # dim
}
_KEY_COLORS: dict[bytes, bytes] = {
    b"QUERY": b"\033[94m",  # bright blue
    b"USER": b"\033[95m",  # bright magenta
}
_DEFAULT_LEVEL_COLOR = b"\033[36m"  # cyan for unrecognized levels
_DEFAULT_KEY_COLOR = b"\033[33m"  # yellow for unrecognized keys
_POLL_INTERVAL = 5.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="azlogs",
        description="Browse and view Azure Blob Storage log files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  azlogs                  # pick customer and date interactively\n"
            "  azlogs river            # open today's log for 'riverview'\n"
            "  azlogs river -f         # follow today's log, poll every 5s\n"
            "  azlogs river -f 10      # follow today's log, poll every 10s\n"
            "\n"
            "Navigation (less):\n"
            "  j/k or ↑/↓             scroll up/down\n"
            "  G/g                    jump to end/start\n"
            "  /pattern               search forward\n"
            "  n/N                    next/previous match\n"
            "  ←/→                    scroll horizontally (chopped lines)\n"
            "  F                      toggle follow mode (in -f)\n"
            "  q                      quit\n"
            "\n"
            "Environment variables:\n"
            "  AZURE_BLOB_BASE_URL     base URL, e.g. https://example.blob.core.windows.net/\n"
            "  AZURE_SAS_TOKEN         SAS query string, e.g. sv=2021-...&sig=...\n"
        ),
    )
    parser.add_argument(
        "customer",
        nargs="?",
        metavar="CUSTOMER",
        help="fuzzy customer name — skips both pickers and opens today's log",
    )
    parser.add_argument(
        "--mouse",
        action="store_true",
        help="enable mouse scrolling in less (disables text selection without holding Option)",
    )
    parser.add_argument(
        "-f",
        "--follow",
        nargs="?",
        const=_POLL_INTERVAL,
        default=None,
        type=float,
        metavar="SECONDS",
        help=f"follow mode: poll every N seconds (default: {_POLL_INTERVAL:.0f}); scroll up to pause, F to resume",
    )
    return parser.parse_args(argv)


def _check_deps() -> None:
    missing = [tool for tool in ("fzf",) if not shutil.which(tool)]
    if missing:
        for tool in missing:
            print(
                f"Error: '{tool}' not found. Install it and try again.", file=sys.stderr
            )
        sys.exit(1)


def _azure_get(url: str) -> str:
    try:
        with urllib.request.urlopen(url) as resp:
            if resp.status != 200:
                print(f"Error: Azure returned HTTP {resp.status}", file=sys.stderr)
                sys.exit(1)
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        print(f"Error: Azure returned HTTP {exc.code}: {exc.reason}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(f"Error: Could not reach Azure: {exc.reason}", file=sys.stderr)
        sys.exit(1)


def _list_containers(base_url: str, sas_token: str) -> list[str]:
    url = f"{base_url}?comp=list&{sas_token}"
    xml_text = _azure_get(url)
    root = ET.fromstring(xml_text)
    return [el.text for el in root.iter("Name") if el.text and "logs" in el.text]


def _list_blobs(base_url: str, sas_token: str, container: str) -> list[str]:
    url = f"{base_url}/{container}?restype=container&comp=list&{sas_token}"
    xml_text = _azure_get(url)
    root = ET.fromstring(xml_text)

    cutoff = date.today() - timedelta(days=14)
    blobs: list[tuple[date, str]] = []
    for el in root.iter("Name"):
        if not el.text:
            continue
        m = _BLOB_PATTERN.match(el.text)
        if not m:
            continue
        blob_date = date.fromisoformat(m.group(1))
        if blob_date >= cutoff:
            blobs.append((blob_date, el.text))

    blobs.sort(key=lambda t: t[0], reverse=True)
    return [name for _, name in blobs]


def _strip_logs(name: str) -> str:
    return name[:-4] if name.endswith("logs") else name


def _fuzzy_match(query: str, names: list[str]) -> str | None:
    q = query.lower()

    substring_matches = [n for n in names if q in n.lower()]
    if substring_matches:
        substring_matches.sort(key=lambda n: n.lower().index(q))
        return substring_matches[0]

    def is_subsequence(needle: str, haystack: str) -> bool:
        it = iter(haystack)
        return all(c in it for c in needle)

    subseq_matches = [n for n in names if is_subsequence(q, n.lower())]
    return subseq_matches[0] if subseq_matches else None


def _fzf_select(items: list[str], prompt: str, query: str = "") -> str:
    cmd = ["fzf", "--prompt", prompt, "--height", "40%", "--layout", "reverse"]
    if query:
        cmd += ["--query", query]
    result = subprocess.run(
        cmd, input="\n".join(items), text=True, stdout=subprocess.PIPE
    )
    if result.returncode != 0:
        sys.exit(0)
    return result.stdout.strip()


def _banner_line(customer: str, date_str: str, label: str = "") -> bytes:
    text = f" {customer} · {date_str}"
    if label:
        text += f" · {label}"
    text += " "
    width = shutil.get_terminal_size().columns
    fill = max(0, width - len(text))
    left = max(fill // 2 - 20, 0)
    line = "─" * left + text + "─" * (fill - left)
    return b"\033[2;37m" + line.encode() + b"\033[0m\n"


def _colorize_line(line: bytes) -> bytes:
    # Extract and strip timestamp, keep only HH:MM:SS for the prefix
    ts_prefix = b""
    ts_match = _TIMESTAMP_RE.search(line)
    if ts_match:
        ts_prefix = _TIMESTAMP_COLOR + b"[" + ts_match.group(2) + b"]" + _RESET + b" "
        line = (line[: ts_match.start()] + line[ts_match.end() :]).rstrip()

    # Colorize start-of-line level prefix
    m = _PREFIX_RE.match(line)
    if m:
        color = _LEVEL_COLORS.get(m.group(1), _DEFAULT_LEVEL_COLOR)
        line = color + m.group(0) + _RESET + line[m.end() :]

    # Colorize inline | [ KEY ]: markers
    def _replace_key(km: re.Match) -> bytes:
        c = _KEY_COLORS.get(km.group(1), _DEFAULT_KEY_COLOR)
        return b"| " + c + b"[ " + km.group(1) + b" ]:" + _RESET

    line = _INLINE_KEY_RE.sub(_replace_key, line)

    return ts_prefix + line


def _stream_to_dest(url: str, dest) -> int:
    """Stream colorized log from url into dest (binary writable). Returns raw byte count."""
    raw_bytes = 0
    try:
        with urllib.request.urlopen(url) as resp:
            if resp.status != 200:
                print(f"Error: Azure returned HTTP {resp.status}", file=sys.stderr)
                sys.exit(1)
            remainder = b""
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                raw_bytes += len(chunk)
                data = remainder + chunk
                lines = data.split(b"\n")
                remainder = lines[-1]
                for line in lines[:-1]:
                    dest.write(_colorize_line(line) + b"\n")
            if remainder:
                dest.write(_colorize_line(remainder))
    except BrokenPipeError:
        pass  # viewer exited before download finished
    except urllib.error.HTTPError as exc:
        print(f"Error: Azure returned HTTP {exc.code}: {exc.reason}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(f"Error: Could not reach Azure: {exc.reason}", file=sys.stderr)
        sys.exit(1)
    return raw_bytes


def _poll_log(
    url: str, tmp: Path, offset: list[int], stop: threading.Event, interval: float
) -> None:
    """Background thread: append new bytes from Azure to tmp every interval seconds."""
    while not stop.wait(interval):
        try:
            req = urllib.request.Request(url, headers={"Range": f"bytes={offset[0]}-"})
            with urllib.request.urlopen(req) as resp:
                if resp.status == 206:
                    new_bytes = resp.read()
                    if new_bytes:
                        colorized = b"\n".join(
                            _colorize_line(line) for line in new_bytes.split(b"\n")
                        )
                        with tmp.open("ab") as f:
                            f.write(colorized)
                        offset[0] += len(new_bytes)
        except urllib.error.HTTPError as exc:
            if exc.code != 416:  # 416 = no new content, expected
                pass
        except Exception:
            pass  # never crash the background thread


def _open_log(url: str, customer: str, date_str: str, mouse: bool = False) -> None:
    fetched = datetime.now().strftime("%H:%M:%S")
    banner = _banner_line(customer, date_str, f"fetched {fetched}")
    less_cmd = ["less", "-RINSs", "--incsearch", "-M", "-j.5", "+G"]
    if mouse:
        less_cmd.append("--mouse")
    less_proc = subprocess.Popen(less_cmd, stdin=subprocess.PIPE)
    try:
        less_proc.stdin.write(banner)  # type: ignore[union-attr]
        _stream_to_dest(url, less_proc.stdin)
        less_proc.stdin.write(b"\n" + banner)  # type: ignore[union-attr]
    finally:
        try:
            if less_proc.stdin:
                less_proc.stdin.close()
        except BrokenPipeError:
            pass
    less_proc.wait()


def _follow_log(url: str, customer: str, date_str: str, interval: float, mouse: bool = False) -> None:
    tmp = Path(tempfile.mktemp(prefix="azlogs_", suffix=".log"))
    stop = threading.Event()

    try:
        banner = _banner_line(customer, date_str, "follow")
        with tmp.open("wb") as f:
            f.write(banner)
            raw_size = _stream_to_dest(url, f)

        offset = [raw_size]
        thread = threading.Thread(
            target=_poll_log,
            args=(url, tmp, offset, stop, interval),
            daemon=True,
        )
        thread.start()

        less_cmd = ["less", "-RINSs", "--incsearch", "-M", "-j.5", "+GF", str(tmp)]
        if mouse:
            less_cmd.insert(1, "--mouse")
        # Ignore SIGINT in Python so Ctrl+C only reaches less (exits follow mode)
        # rather than killing this process. User can then scroll freely and press
        # F to resume follow mode, or q to quit.
        old_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            subprocess.run(less_cmd)
        finally:
            signal.signal(signal.SIGINT, old_sigint)
    finally:
        stop.set()
        tmp.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    _check_deps()

    base_url = os.environ.get("AZURE_BLOB_BASE_URL", "").rstrip("/")
    sas_token = os.environ.get("AZURE_SAS_TOKEN", "")
    if not base_url or not sas_token:
        for v, val in (
            ("AZURE_BLOB_BASE_URL", base_url),
            ("AZURE_SAS_TOKEN", sas_token),
        ):
            if not val:
                print(f"Error: ${v} is not set.", file=sys.stderr)
        sys.exit(1)

    containers = _list_containers(base_url, sas_token)
    if not containers:
        print("Error: No log containers found.", file=sys.stderr)
        sys.exit(1)

    customer_names = [_strip_logs(c) for c in containers]

    if args.customer:
        match = _fuzzy_match(args.customer, customer_names)
        if not match:
            print(f"Error: No container matching '{args.customer}'.", file=sys.stderr)
            sys.exit(1)
        container = match + "logs"
        blobs = _list_blobs(base_url, sas_token, container)
        today = date.today().isoformat()
        today_blobs = [b for b in blobs if today in b]
        if not today_blobs:
            print(
                f"Error: No log for today ({today}) found in '{container}'.",
                file=sys.stderr,
            )
            sys.exit(1)
        blob_name = today_blobs[0]
        m = _BLOB_PATTERN.match(blob_name)
        date_str = m.group(1) if m else today
    else:
        customer = _fzf_select(customer_names, "customer> ")
        container = customer + "logs"
        blobs = _list_blobs(base_url, sas_token, container)
        if not blobs:
            print(
                f"Error: No log files found in '{container}' for the past 14 days.",
                file=sys.stderr,
            )
            sys.exit(1)
        today = date.today().isoformat()
        blob_name = _fzf_select(blobs, "log> ", query=today)
        m = _BLOB_PATTERN.match(blob_name)
        date_str = m.group(1) if m else blob_name

    url = f"{base_url}/{container}/{blob_name}?{sas_token}"
    customer_display = _strip_logs(container)

    if args.follow is not None:
        _follow_log(url, customer_display, date_str, interval=args.follow, mouse=args.mouse)
    else:
        _open_log(url, customer_display, date_str, mouse=args.mouse)
