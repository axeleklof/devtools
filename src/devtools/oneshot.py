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


# Tools that are useful when available but should not override standard choices.
_POWER_TOOLS = [
    "fd", "rg", "fzf", "broot",
    "jq", "yq", "bat", "delta", "xsv",
    "http", "curlie",
    "gh", "lazygit",
    "glow", "gum", "fx",
]

_RUNTIMES = ["python3", "node", "go", "ruby", "bun"]


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
            # Extract just the version number — e.g. "Python 3.13.0" → "python 3.13"
            tokens = out.split()
            version = tokens[-1] if tokens else ""
            # Trim to major.minor
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
        # e.g. "zsh 5.9 (x86_64-apple-darwin23.0)" → "zsh 5.9"
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


def _call_api(system: str, user: str) -> str:
    api_key = os.environ.get("ONESHOT_API_KEY") or os.environ.get("OPENAI_API_KEY")
    api_url = os.environ.get("ONESHOT_API_URL", "https://api.openai.com/v1/chat/completions")
    model = os.environ.get("ONESHOT_MODEL", "gpt-4o-mini")

    if not api_key:
        sys.exit("oneshot: ONESHOT_API_KEY (or OPENAI_API_KEY) is not set")

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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="oneshot",
        description="One-shot LLM query from the terminal.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Modes:\n"
            "  default / -c   Return a shell command (copied to clipboard)\n"
            "  -x             Return a markdown explanation\n\n"
            "Environment variables:\n"
            "  ONESHOT_API_KEY   API key (falls back to OPENAI_API_KEY)\n"
            "  ONESHOT_API_URL   API endpoint (default: OpenAI)\n"
            "  ONESHOT_MODEL     Model name (default: gpt-4o-mini)\n\n"
            "Examples:\n"
            "  oneshot \"find all files modified in the last 24 hours\"\n"
            "  oneshot -v \"find all files modified in the last 24 hours\"\n"
            "  oneshot -x \"what is a semaphore\"\n"
            "  oneshot -x \"what is a semaphore\" | glow\n"
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
        "--no-clip",
        action="store_true",
        help="Do not copy command to clipboard",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    mode = "explain" if args.explain else "cmd"
    system = _get_system_prompt(mode, verbose=args.verbose)
    response = _call_api(system, args.query)

    if mode == "cmd":
        command = _extract_code_block(response)
        if command:
            if not args.no_clip:
                _copy_to_clipboard(command)
            if args.verbose:
                # Print full response but highlight that the command is in clipboard
                print(response)
                print("\n\x1b[2m(command copied to clipboard)\x1b[0m")
            else:
                print(command)
                print("\x1b[2m(copied to clipboard)\x1b[0m", file=sys.stderr)
        else:
            # Model didn't return a code block — print raw and don't claim clipboard
            print(response)
    else:
        print(response)
