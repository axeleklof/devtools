"""One-shot LLM query — get a shell command or brief explanation without leaving the terminal."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import urllib.request
from datetime import date
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]


_CONFIG_PATH = Path.home() / ".config" / "oneshot" / "config.toml"
_STDIN_LIMIT = 32 * 1024  # 32 KB

# Tools that are useful when available but should not override standard choices.
_POWER_TOOLS = [
    "fd", "rg", "fzf", "broot",
    "jq", "yq", "bat", "delta", "xsv",
    "http", "curlie",
    "gh", "lazygit",
    "glow", "gum", "fx",
]

_RUNTIMES = ["python3", "node", "go", "ruby", "bun"]


# ---------------------------------------------------------------------------
# Context detection
# ---------------------------------------------------------------------------

def _detect_tools() -> str:
    found = [t for t in _POWER_TOOLS if shutil.which(t)]
    return ", ".join(found) if found else ""


def _detect_runtimes() -> str:
    parts = []
    for rt in _RUNTIMES:
        if not shutil.which(rt):
            continue
        try:
            out = subprocess.check_output(
                [rt, "--version"], stderr=subprocess.STDOUT, timeout=2, text=True
            ).strip().splitlines()[0]
            tokens = out.split()
            version = tokens[-1] if tokens else ""
            parts_v = version.lstrip("v").split(".")
            short = ".".join(parts_v[:2]) if len(parts_v) >= 2 else version
            parts.append(f"{rt.replace('python3', 'python')} {short}")
        except Exception:
            parts.append(rt)
    return ", ".join(parts)


def _in_git_repo() -> bool:
    try:
        subprocess.check_output(
            ["git", "rev-parse", "--is-inside-work-tree"],
            stderr=subprocess.DEVNULL, timeout=2,
        )
        return True
    except Exception:
        return False


def _build_context() -> str:
    lines = []

    os_ver = platform.mac_ver()[0]
    os_str = f"macOS {os_ver}" if os_ver else platform.system()
    shell = os.environ.get("SHELL", "zsh")
    shell_name = os.path.basename(shell)

    try:
        shell_ver = subprocess.check_output(
            [shell, "--version"], stderr=subprocess.STDOUT, timeout=2, text=True
        ).strip().splitlines()[0]
        shell_str = " ".join(shell_ver.split()[:2])
    except Exception:
        shell_str = shell_name

    cwd_name = os.path.basename(os.getcwd()) or os.getcwd()
    git = ", in git repo" if _in_git_repo() else ""
    today = date.today().isoformat()

    lines.append(f"System: {os_str}, {shell_str}{git}, cwd: {cwd_name}, date: {today}")

    tools = _detect_tools()
    if tools:
        lines.append(
            f"Prefer standard POSIX tools (find, grep, awk, sed, curl). "
            f"These are also installed if they offer a clear advantage: {tools}"
        )

    runtimes = _detect_runtimes()
    if runtimes:
        lines.append(
            f"Shell is preferred. If a task is impractical in shell, "
            f"these runtimes are available: {runtimes}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Config / API resolution
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    if not _CONFIG_PATH.exists():
        return {}
    try:
        with open(_CONFIG_PATH, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        print(f"oneshot: warning: could not read config: {e}", file=sys.stderr)
        return {}


def _resolve_api_config(profile: str | None) -> tuple[str, str, str]:
    """Return (api_key, api_url, model) by applying priority order."""
    config = _load_config()

    # Determine which profile to use
    if profile is None:
        profile = config.get("default", {}).get("profile")

    profile_data: dict = {}
    if profile:
        profile_data = config.get("profiles", {}).get(profile, {})
        if not profile_data and profile:
            print(f"oneshot: warning: profile '{profile}' not found in config", file=sys.stderr)

    def _from_profile(key: str) -> str | None:
        env_var = profile_data.get(f"{key}_env")
        if env_var:
            return os.environ.get(env_var)
        return profile_data.get(key)

    # Priority: env vars > profile > hardcoded defaults
    api_key = (
        os.environ.get("ONESHOT_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
        or _from_profile("api_key")
    )
    api_url = (
        os.environ.get("ONESHOT_API_URL")
        or _from_profile("api_url")
        or "https://api.openai.com/v1/chat/completions"
    )
    model = (
        os.environ.get("ONESHOT_MODEL")
        or _from_profile("model")
        or "gpt-4o-mini"
    )

    if not api_key:
        sys.exit("oneshot: no API key found — set ONESHOT_API_KEY or configure a profile in ~/.config/oneshot/config.toml")

    return api_key, api_url, model


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

_SYSTEM_CMD = """\
You are a terminal assistant. The user will ask a question about how to do something.
Reply with a single shell command that solves it.

Rules:
- Output ONLY a fenced ```bash code block containing the command. Nothing before or after it.
- No explanation, no prose, no alternatives, no markdown outside the code block.
- Prefer the simplest correct solution. Use standard tools unless a specialist tool is clearly better.
- This is a one-shot query. No follow-up will be sent. Do not ask clarifying questions.

{context}
"""

_SYSTEM_CMD_VERBOSE = """\
You are a terminal assistant. The user will ask a question about how to do something.
Reply with a brief explanation followed by the command.

Rules:
- Structure your response as: one to three sentences of explanation, then a single fenced ```bash code block.
- The code block must contain exactly one command. No comments inside the code block.
- Nothing after the code block.
- If there are multiple variants (e.g. for a file vs a string), explain the difference in prose and put the most common variant in the code block.
- Prefer the simplest correct solution. Use standard tools unless a specialist tool is clearly better.
- This is a one-shot query. No follow-up will be sent. Do not ask clarifying questions.

{context}
"""

_SYSTEM_EXPLAIN = """\
You are a technical assistant. The user will ask about a concept, tool, or topic.
Give a brief, direct explanation in markdown.

Rules:
- Be concise. No filler ("great question", "I hope this helps", "in summary", "feel free to ask").
- No trailing offers to elaborate or follow up.
- This is a one-shot query. No follow-up will be sent.

{context}
"""


def _get_system_prompt(mode: str, verbose: bool) -> str:
    context = _build_context()
    if mode == "explain":
        return _SYSTEM_EXPLAIN.format(context=context)
    if verbose:
        return _SYSTEM_CMD_VERBOSE.format(context=context)
    return _SYSTEM_CMD.format(context=context)


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def _call_api(system: str, user: str, api_key: str, api_url: str, model: str) -> str:
    payload = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.2,
    }).encode()

    req = urllib.request.Request(
        api_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        sys.exit(f"oneshot: API error {e.code}: {body}")
    except Exception as e:
        sys.exit(f"oneshot: request failed: {e}")

    return data["choices"][0]["message"]["content"].strip()


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _extract_code_block(text: str) -> str | None:
    """Return the content of the first fenced code block, or None."""
    lines = text.splitlines()
    inside = False
    collected: list[str] = []
    for line in lines:
        if not inside and line.startswith("```"):
            inside = True
            continue
        if inside:
            if line.startswith("```"):
                break
            collected.append(line)
    return "\n".join(collected).strip() if collected else None


def _copy_to_clipboard(text: str) -> None:
    try:
        proc = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE)
        proc.communicate(text.encode())
    except Exception:
        pass  # clipboard failure is non-fatal


def _read_stdin() -> str | None:
    """Read piped stdin, capped at _STDIN_LIMIT. Returns None if stdin is a TTY."""
    if sys.stdin.isatty():
        return None
    raw = sys.stdin.buffer.read(_STDIN_LIMIT + 1)
    if len(raw) > _STDIN_LIMIT:
        print(
            f"oneshot: warning: stdin truncated to {_STDIN_LIMIT // 1024} KB",
            file=sys.stderr,
        )
        raw = raw[:_STDIN_LIMIT]
    return raw.decode(errors="replace")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="oneshot",
        description="One-shot LLM query from the terminal.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Modes:\n"
            "  default / -c   Return a shell command (copied to clipboard)\n"
            "  -x             Return a markdown explanation\n\n"
            "Config file: ~/.config/oneshot/config.toml\n\n"
            "Environment variables (override config):\n"
            "  ONESHOT_API_KEY   API key (falls back to OPENAI_API_KEY)\n"
            "  ONESHOT_API_URL   API endpoint\n"
            "  ONESHOT_MODEL     Model name\n\n"
            "Examples:\n"
            "  oneshot \"find all files modified in the last 24 hours\"\n"
            "  oneshot -v \"find all files modified in the last 24 hours\"\n"
            "  oneshot -x \"what is a semaphore\" | glow\n"
            "  oneshot -p local \"list processes on port 8080\"\n"
            "  cat error.log | oneshot \"what's wrong here\"\n"
            "  git diff | oneshot -v \"summarise these changes\"\n"
        ),
    )
    parser.add_argument("query", help="Your question")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "-c", "--cmd",
        action="store_true",
        help="Command mode — return a shell command (default)",
    )
    mode.add_argument(
        "-x", "--explain",
        action="store_true",
        help="Explain mode — return a markdown explanation",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="(Command mode) include a brief explanation before the command",
    )
    parser.add_argument(
        "-p", "--profile",
        metavar="NAME",
        help="Use a named profile from ~/.config/oneshot/config.toml",
    )
    parser.add_argument(
        "-m", "--md",
        action="store_true",
        help="Markdown-safe output — hints go to stderr, command wrapped in a code block",
    )
    parser.add_argument(
        "--no-clip",
        action="store_true",
        help="Do not copy command to clipboard",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    stdin_content = _read_stdin()
    user_message = args.query
    if stdin_content:
        user_message = f"{args.query}\n\n---\n{stdin_content}"

    mode = "explain" if args.explain else "cmd"
    system = _get_system_prompt(mode, verbose=args.verbose)
    api_key, api_url, model = _resolve_api_config(args.profile)
    response = _call_api(system, user_message, api_key, api_url, model)

    if mode == "cmd":
        command = _extract_code_block(response)
        if command:
            if not args.no_clip:
                _copy_to_clipboard(command)
            hint = "(command copied to clipboard)" if args.verbose else "(copied to clipboard)"
            if args.verbose:
                print(response)
            elif args.md:
                print(f"```bash\n{command}\n```")
            else:
                print(command)
            if args.md:
                print(hint, file=sys.stderr)
            elif args.verbose:
                print(f"\n\x1b[2m{hint}\x1b[0m")
            else:
                print(f"\x1b[2m{hint}\x1b[0m", file=sys.stderr)
        else:
            print(response)
    else:
        print(response)
