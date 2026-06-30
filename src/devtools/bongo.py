"""Copy, list and drop MongoDB databases across configured clusters — a thin wrapper over mongodump/mongorestore."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
from collections import deque
from datetime import datetime
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

_CONFIG_PATH = Path.home() / ".config" / "bongo" / "config.toml"
_CONFIG_DIR = _CONFIG_PATH.parent
_STATE_PATH = Path.home() / ".config" / "bongo" / "state.json"
_SNAPSHOT_DIR = Path.home() / ".local" / "share" / "bongo" / "snapshots"
_TIMESTAMP_FMT = "%Y%m%d-%H%M%S"

_DB_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")

_EXAMPLE_CONFIG = """\
# bongo configuration — clusters addressable as <name>:<db> on the command line.
# The cluster marked as default is used for bare db names without a prefix.

default = "local"

[clusters.local]
uri = "mongodb://localhost:27017"
# Databases that bongo refuses to drop or overwrite without --force:
protected = ["main"]

# [clusters.atlas-dev]
# uri = "mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net"
# protected = []

# Optional script labels for `bongo run <label> <cluster>:<db>`.
# Relative paths resolve from this config directory.
# [scripts]
# adduser = "scripts/adduser.js"
"""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_KNOWN_TOP_LEVEL = {"default", "clusters", "scripts"}
_KNOWN_CLUSTER_KEYS = {"uri", "protected"}


def _validate_config(config: dict) -> list[str]:
    """Return a list of human-readable problems with the parsed config (empty = valid)."""
    errors: list[str] = []

    for key in config:
        if key in _KNOWN_TOP_LEVEL:
            continue
        if key == "cluster":
            errors.append(
                "found a [cluster.*] table (singular) — clusters live under "
                "[clusters.<name>] (plural); rename your [cluster.x] headers to [clusters.x]"
            )
        else:
            errors.append(f"unknown top-level key '{key}' (expected: default, clusters, scripts)")

    clusters = config.get("clusters")
    if not isinstance(clusters, dict) or not clusters:
        errors.append("no clusters defined — add at least one [clusters.<name>] with a uri")
        clusters = {}

    for name, cluster in clusters.items():
        if not isinstance(cluster, dict):
            errors.append(f"cluster '{name}' must be a table ([clusters.{name}])")
            continue
        uri = cluster.get("uri")
        if not isinstance(uri, str) or not uri:
            errors.append(f"cluster '{name}' is missing a non-empty 'uri'")
        protected = cluster.get("protected", [])
        if not isinstance(protected, list) or not all(isinstance(p, str) for p in protected):
            errors.append(f"cluster '{name}': 'protected' must be a list of database names")
        unknown = set(cluster) - _KNOWN_CLUSTER_KEYS
        if unknown:
            keys = ", ".join(sorted(unknown))
            errors.append(f"cluster '{name}': unknown key(s) {keys} (expected: uri, protected)")

    default = config.get("default")
    if default is not None:
        if not isinstance(default, str):
            errors.append("'default' must be a cluster name (string)")
        elif default not in clusters:
            errors.append(f"default cluster '{default}' is not defined under [clusters.{default}]")

    scripts = config.get("scripts", {})
    if not isinstance(scripts, dict):
        errors.append("'scripts' must be a table ([scripts])")
    else:
        for label, script_path in scripts.items():
            if not label.strip():
                errors.append("script labels must be non-empty")
            if not isinstance(script_path, str) or not script_path.strip():
                errors.append(f"script '{label}' must point to a non-empty file path")

    return errors


def _load_config() -> dict:
    if not _CONFIG_PATH.exists():
        sys.exit(f"bongo: no config found — run 'bongo init' to create {_CONFIG_PATH}")
    try:
        with open(_CONFIG_PATH, "rb") as f:
            config = tomllib.load(f)
    except Exception as e:
        sys.exit(f"bongo: could not read config: {e}")
    errors = _validate_config(config)
    if errors:
        detail = "\n".join(f"  - {e}" for e in errors)
        sys.exit(f"bongo: invalid config at {_CONFIG_PATH}:\n{detail}\n(run 'bongo check' to re-validate)")
    return config


def _get_cluster(config: dict, name: str) -> dict:
    cluster = config["clusters"].get(name)
    if cluster is None:
        known = ", ".join(sorted(config["clusters"]))
        sys.exit(f"bongo: unknown cluster '{name}' (configured: {known})")
    if not cluster.get("uri"):
        sys.exit(f"bongo: cluster '{name}' has no uri in config")
    return cluster


def _git_branch_db() -> str:
    """Return the current git branch name sanitized into a valid db name."""
    try:
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL, text=True, timeout=5,
        ).strip()
    except Exception:
        sys.exit("bongo: '.' requires being inside a git repository")
    if branch == "HEAD":
        sys.exit("bongo: '.' cannot resolve a branch name (detached HEAD)")
    db = re.sub(r"[^A-Za-z0-9_-]", "-", branch)[:63]
    print(f"Resolved '.' -> '{db}' (branch {branch})")
    return db


def _resolve_address(config: dict, address: str) -> tuple[str, str]:
    """Split 'cluster:db' (or bare 'db', using the default cluster) into (cluster, db).

    A db of '.' resolves to the current git branch name, sanitized.
    """
    if ":" in address:
        cluster_name, db = address.split(":", 1)
    else:
        cluster_name, db = config.get("default", ""), address
        if not cluster_name:
            sys.exit(f"bongo: '{address}' has no cluster prefix and no default cluster is set in config")
    if db == ".":
        db = _git_branch_db()
    if not _DB_NAME_RE.match(db):
        sys.exit(f"bongo: invalid database name '{db}' (allowed: letters, digits, _ and -)")
    _get_cluster(config, cluster_name)
    return cluster_name, db


def _is_protected(config: dict, cluster_name: str, db: str) -> bool:
    return db in config["clusters"][cluster_name].get("protected", [])


def _path_from(value: str, base: Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return base / path


def _resolve_script(config: dict, script: str) -> tuple[str | None, Path]:
    scripts = config.get("scripts", {})
    if script in scripts:
        path = _path_from(scripts[script], _CONFIG_DIR)
        label: str | None = script
    else:
        path = _path_from(script, Path.cwd())
        label = None

    if not path.exists():
        if label:
            sys.exit(f"bongo: script '{label}' points to missing file: {path}")
        sys.exit(f"bongo: script '{script}' is not configured and no file exists at {path}")
    if not path.is_file():
        sys.exit(f"bongo: script path is not a file: {path}")
    if not os.access(path, os.R_OK):
        sys.exit(f"bongo: script path is not readable: {path}")
    return label, path


# ---------------------------------------------------------------------------
# State — manifest of databases bongo has created, used by prune
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if not _STATE_PATH.exists():
        return {"created": {}}
    try:
        with open(_STATE_PATH) as f:
            state = json.load(f)
    except Exception:
        return {"created": {}}
    state.setdefault("created", {})
    return state


def _save_state(state: dict) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STATE_PATH.write_text(json.dumps(state, indent=2) + "\n")


def _record_created(cluster_name: str, db: str, source: str) -> None:
    state = _load_state()
    state["created"][f"{cluster_name}:{db}"] = {
        "source": source,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    _save_state(state)


def _forget_created(cluster_name: str, db: str) -> None:
    state = _load_state()
    if state["created"].pop(f"{cluster_name}:{db}", None) is not None:
        _save_state(state)


# ---------------------------------------------------------------------------
# Mongo helpers
# ---------------------------------------------------------------------------

def _require_tools(*tools: str) -> None:
    missing = [t for t in tools if not shutil.which(t)]
    if missing:
        hints = {
            "mongosh": "brew install mongosh",
            "mongodump": "brew install mongodb-database-tools",
            "mongorestore": "brew install mongodb-database-tools",
        }
        lines = [f"bongo: missing required tools: {', '.join(missing)}"]
        for hint in sorted({hints[t] for t in missing}):
            lines.append(f"  {hint}")
        sys.exit("\n".join(lines))


def _mongosh_json(uri: str, expression: str):
    """Evaluate a JS expression via mongosh and return its JSON-parsed result."""
    try:
        out = subprocess.check_output(
            ["mongosh", uri, "--quiet", "--eval", f"JSON.stringify({expression})"],
            stderr=subprocess.PIPE, text=True, timeout=30,
        )
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or "").strip() or (e.output or "").strip()
        sys.exit(f"bongo: mongosh failed: {detail}")
    except subprocess.TimeoutExpired:
        sys.exit("bongo: mongosh timed out — is the cluster reachable?")
    try:
        return json.loads(out.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        sys.exit(f"bongo: unexpected mongosh output: {out.strip()}")


def _list_databases(uri: str) -> list[dict]:
    return _mongosh_json(
        uri,
        "db.adminCommand({listDatabases: 1}).databases"
        ".map(d => ({name: d.name, size: Number(d.sizeOnDisk)}))",
    )


def _db_exists(uri: str, db: str) -> bool:
    return any(d["name"] == db for d in _list_databases(uri))


def _drop_database(uri: str, db: str) -> None:
    result = _mongosh_json(uri, f"db.getSiblingDB({json.dumps(db)}).dropDatabase()")
    if not result.get("ok"):
        sys.exit(f"bongo: drop failed: {result}")


def _redact_uri(uri: str) -> str:
    return re.sub(r"://([^:/@]+):[^@]+@", r"://\1:***@", uri)


def _format_size(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _confirm(prompt: str) -> bool:
    try:
        return input(f"{prompt} [y/N] ").strip().lower() in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        print()
        return False


def _pick(items: list[str], prompt: str) -> str | None:
    """Let the user pick one item — via fzf when available, else a numbered list."""
    if not items:
        return None
    if shutil.which("fzf"):
        proc = subprocess.run(
            ["fzf", "--prompt", f"{prompt} ", "--height", "40%", "--reverse"],
            input="\n".join(items), stdout=subprocess.PIPE, text=True,
        )
        choice = proc.stdout.strip()
        return choice or None
    for i, item in enumerate(items, 1):
        print(f"{i:3}) {item}")
    try:
        raw = input(f"{prompt} [1-{len(items)}] ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return None
    if raw.isdigit() and 1 <= int(raw) <= len(items):
        return items[int(raw) - 1]
    return None


def _uri_with_db(uri: str, db: str) -> str:
    """Insert a default database into a connection string, preserving query options."""
    base, _, query = uri.partition("?")
    scheme, _, rest = base.partition("://")
    host = rest.split("/", 1)[0]
    return f"{scheme}://{host}/{db}" + (f"?{query}" if query else "")


# ---------------------------------------------------------------------------
# Progress rendering — turns raw mongodump/mongorestore stderr into clean lines
# ---------------------------------------------------------------------------

# `2026-06-12T13:59:59+0200\tfinished restoring db.coll (1234 documents, 0 failures)`
_TOOL_DONE_RE = re.compile(
    r"\t(?:done dumping|finished restoring) `?([^\s`]+)`? \((\d+) documents?(?:, (\d+) failures?)?\)"
)
# `[#######.........]  db.coll  12000/50000  (24.0%)`
_TOOL_PROGRESS_RE = re.compile(r"\[[#.]*\]\s+(\S+)\s+\d+/\d+\s+\(([\d.]+)%\)")


def _c(code: str, text: str) -> str:
    if sys.stdout.isatty() and not os.environ.get("NO_COLOR"):
        return f"\x1b[{code}m{text}\x1b[0m"
    return text


def _collection_of(namespace: str) -> str:
    return namespace.split(".", 1)[1] if "." in namespace else namespace


def _stream_mongo_progress(stream) -> dict:
    """Consume a mongo tool's stderr, printing one line per finished collection
    and an in-place progress bar for the collection currently in flight."""
    stats = {"collections": 0, "docs": 0, "failures": 0, "tail": deque(maxlen=15)}
    interactive = sys.stdout.isatty()
    bar_active = False

    def clear_bar() -> None:
        nonlocal bar_active
        if bar_active:
            sys.stdout.write("\r\x1b[2K")
            bar_active = False

    for line in stream:
        stats["tail"].append(line)
        done = _TOOL_DONE_RE.search(line)
        if done:
            docs, failures = int(done.group(2)), int(done.group(3) or 0)
            stats["collections"] += 1
            stats["docs"] += docs
            stats["failures"] += failures
            clear_bar()
            note = "  " + _c("31", f"{failures:,} failed!") if failures else ""
            print(f"  {_c('32', '✓')} {_collection_of(done.group(1)):<34} {docs:>12,} docs{note}")
            continue
        progress = _TOOL_PROGRESS_RE.search(line)
        if progress and interactive:
            pct = float(progress.group(2))
            filled = int(pct / 100 * 22)
            bar = "=" * filled + ">" + " " * (22 - filled)
            sys.stdout.write(
                f"\r\x1b[2K  {_c('36', '▸')} {_collection_of(progress.group(1)):<34} [{bar}] {pct:5.1f}%"
            )
            sys.stdout.flush()
            bar_active = True
    clear_bar()
    return stats


def _summary(stats: dict) -> str:
    return f"{stats['collections']} collections, {stats['docs']:,} docs"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def _cmd_ls(config: dict, args: argparse.Namespace) -> None:
    cluster_name = args.cluster or config.get("default")
    if not cluster_name:
        sys.exit("bongo: no cluster given and no default cluster is set in config")
    cluster = _get_cluster(config, cluster_name)

    databases = _list_databases(cluster["uri"])
    print(f"{cluster_name} ({_redact_uri(cluster['uri'])})")
    hidden = 0
    for db in sorted(databases, key=lambda d: d["name"]):
        if db["name"] in _SYSTEM_DBS and not args.all:
            hidden += 1
            continue
        tag = "  [protected]" if _is_protected(config, cluster_name, db["name"]) else ""
        print(f"  {db['name']:<30} {_format_size(db['size']):>10}{tag}")
    if hidden:
        print(_c("2", f"  ({hidden} system databases hidden — use -a to show)"))


_SYSTEM_DBS = {"admin", "config", "local"}


def _cmd_rm(config: dict, args: argparse.Namespace) -> None:
    if args.database is None:
        cluster_name = config.get("default")
        if not cluster_name:
            sys.exit("bongo: no database given and no default cluster is set in config")
        uri = _get_cluster(config, cluster_name)["uri"]
        choices = [
            d["name"] for d in sorted(_list_databases(uri), key=lambda d: d["name"])
            if d["name"] not in _SYSTEM_DBS and not _is_protected(config, cluster_name, d["name"])
        ]
        picked = _pick(choices, f"drop from {cluster_name}:")
        if picked is None:
            sys.exit("bongo: aborted")
        args.database = f"{cluster_name}:{picked}"
    cluster_name, db = _resolve_address(config, args.database)
    cluster = config["clusters"][cluster_name]

    if _is_protected(config, cluster_name, db) and not args.force:
        sys.exit(f"bongo: '{cluster_name}:{db}' is protected — use --force to drop it")
    if not _db_exists(cluster["uri"], db):
        sys.exit(f"bongo: database '{db}' not found on cluster '{cluster_name}'")
    if not args.yes and not _confirm(f"Drop '{cluster_name}:{db}'?"):
        sys.exit("bongo: aborted")

    _drop_database(cluster["uri"], db)
    _forget_created(cluster_name, db)
    print(f"Dropped '{cluster_name}:{db}'")


def _cmd_cp(config: dict, args: argparse.Namespace) -> None:
    src_cluster_name, src_db = _resolve_address(config, args.source)
    dst_cluster_name, dst_db = _resolve_address(config, args.target)
    src_uri = config["clusters"][src_cluster_name]["uri"]
    dst_uri = config["clusters"][dst_cluster_name]["uri"]

    if (src_cluster_name, src_db) == (dst_cluster_name, dst_db):
        sys.exit("bongo: source and target are the same database")
    if not _db_exists(src_uri, src_db):
        sys.exit(f"bongo: database '{src_db}' not found on cluster '{src_cluster_name}'")

    overwrite = _db_exists(dst_uri, dst_db)
    if overwrite:
        if _is_protected(config, dst_cluster_name, dst_db) and not args.force:
            sys.exit(f"bongo: '{dst_cluster_name}:{dst_db}' is protected — use --force to overwrite it")
        if not args.yes and not _confirm(
            f"Target '{dst_cluster_name}:{dst_db}' already exists. Overwrite?"
        ):
            sys.exit("bongo: aborted")

    quiet = not args.verbose
    print(f"Copying '{src_cluster_name}:{src_db}' -> '{dst_cluster_name}:{dst_db}' ...")
    dump = subprocess.Popen(
        ["mongodump", f"--uri={src_uri}", f"--db={src_db}", "--archive"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE if quiet else None,
    )
    restore_cmd = [
        "mongorestore", f"--uri={dst_uri}", "--archive",
        f"--nsFrom={src_db}.*", f"--nsTo={dst_db}.*",
    ]
    if overwrite:
        restore_cmd.append("--drop")
    restore = subprocess.Popen(
        restore_cmd, stdin=dump.stdout,
        stderr=subprocess.PIPE if quiet else None, text=quiet or None,
    )
    dump.stdout.close()  # let mongodump receive SIGPIPE if mongorestore dies

    stats = None
    dump_tail: deque[bytes] = deque(maxlen=15)
    if quiet:
        # dump's stderr must be drained in parallel or its pipe buffer can fill and stall it
        drainer = threading.Thread(target=lambda: dump_tail.extend(dump.stderr), daemon=True)
        drainer.start()
        stats = _stream_mongo_progress(restore.stderr)

    restore_rc = restore.wait()
    dump_rc = dump.wait()
    if dump_rc != 0:
        if dump_tail:
            sys.stderr.write(b"".join(dump_tail).decode(errors="replace"))
        sys.exit(f"bongo: mongodump exited with code {dump_rc}")
    if restore_rc != 0:
        if stats:
            sys.stderr.writelines(stats["tail"])
        sys.exit(f"bongo: mongorestore exited with code {restore_rc}")
    _record_created(dst_cluster_name, dst_db, f"{src_cluster_name}:{src_db}")
    suffix = f" ({_summary(stats)})" if stats else ""
    print(f"Done — '{dst_cluster_name}:{dst_db}' is ready{suffix}")
    if stats and stats["failures"]:
        sys.exit(f"bongo: warning: {stats['failures']:,} documents failed to restore")


def _cmd_prune(config: dict, args: argparse.Namespace) -> None:
    cluster_name = args.cluster or config.get("default")
    if not cluster_name:
        sys.exit("bongo: no cluster given and no default cluster is set in config")
    cluster = _get_cluster(config, cluster_name)

    state = _load_state()
    existing = {d["name"] for d in _list_databases(cluster["uri"])}

    candidates: list[tuple[str, dict]] = []
    for key, meta in list(state["created"].items()):
        entry_cluster, _, db = key.partition(":")
        if entry_cluster != cluster_name:
            continue
        if db not in existing:
            del state["created"][key]  # dropped outside bongo — clean up the manifest
            continue
        if _is_protected(config, cluster_name, db):
            continue
        age_days = (datetime.now() - datetime.fromisoformat(meta["created_at"])).days
        if args.days is not None and age_days < args.days:
            continue
        candidates.append((db, {**meta, "age_days": age_days}))
    _save_state(state)

    if not candidates:
        print(f"Nothing to prune on '{cluster_name}'")
        return
    for db, meta in sorted(candidates, key=lambda c: c[0]):
        age = f"{meta['age_days']}d old" if meta["age_days"] else "today"
        label = f"'{cluster_name}:{db}' (from {meta['source']}, {age})"
        if args.yes or _confirm(f"Drop {label}?"):
            _drop_database(cluster["uri"], db)
            _forget_created(cluster_name, db)
            print(f"Dropped {label}")


def _snapshot_files(cluster_name: str | None = None, db: str | None = None) -> list[Path]:
    pattern = f"{cluster_name or '*'}__{db or '*'}__*.archive.gz"
    return sorted(_SNAPSHOT_DIR.glob(pattern))


def _cmd_snapshot(config: dict, args: argparse.Namespace) -> None:
    if args.database is None:
        snapshots = _snapshot_files()
        if not snapshots:
            print(f"No snapshots in {_SNAPSHOT_DIR}")
            return
        for path in snapshots:
            size = _format_size(path.stat().st_size)
            print(f"  {path.name:<55} {size:>10}")
        return

    cluster_name, db = _resolve_address(config, args.database)
    uri = config["clusters"][cluster_name]["uri"]
    if not _db_exists(uri, db):
        sys.exit(f"bongo: database '{db}' not found on cluster '{cluster_name}'")

    _SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime(_TIMESTAMP_FMT)
    path = _SNAPSHOT_DIR / f"{cluster_name}__{db}__{timestamp}.archive.gz"

    quiet = not args.verbose
    print(f"Snapshotting '{cluster_name}:{db}' ...")
    dump = subprocess.Popen(
        ["mongodump", f"--uri={uri}", f"--db={db}", "--gzip", f"--archive={path}"],
        stderr=subprocess.PIPE if quiet else None, text=quiet or None,
    )
    stats = _stream_mongo_progress(dump.stderr) if quiet else None
    if dump.wait() != 0:
        if stats:
            sys.stderr.writelines(stats["tail"])
        path.unlink(missing_ok=True)
        sys.exit(f"bongo: mongodump exited with code {dump.returncode}")
    suffix = f", {_summary(stats)}" if stats else ""
    print(f"Done — {path} ({_format_size(path.stat().st_size)}{suffix})")


def _parse_snapshot_filename(path: Path) -> tuple[str, str]:
    parts = path.name.removesuffix(".archive.gz").split("__")
    if len(parts) != 3:
        sys.exit(f"bongo: cannot parse snapshot filename '{path.name}' (expected <cluster>__<db>__<timestamp>.archive.gz)")
    return parts[0], parts[1]


def _cmd_restore(config: dict, args: argparse.Namespace) -> None:
    if args.database is None and not args.file:
        names = [p.name for p in reversed(_snapshot_files())]
        if not names:
            sys.exit(f"bongo: no snapshots in {_SNAPSHOT_DIR}")
        picked = _pick(names, "restore snapshot:")
        if picked is None:
            sys.exit("bongo: aborted")
        args.file = str(_SNAPSHOT_DIR / picked)
    if args.file:
        path = Path(args.file).expanduser()
        if not path.exists():
            sys.exit(f"bongo: snapshot file not found: {path}")
        src_cluster_name, src_db = _parse_snapshot_filename(path)
    else:
        src_cluster_name, src_db = _resolve_address(config, args.database)
        snapshots = _snapshot_files(src_cluster_name, src_db)
        if not snapshots:
            sys.exit(f"bongo: no snapshots found for '{src_cluster_name}:{src_db}' — create one with 'bongo snapshot'")
        path = snapshots[-1]  # timestamped filenames sort chronologically

    dst_cluster_name, dst_db = _resolve_address(config, args.target or f"{src_cluster_name}:{src_db}")
    dst_uri = config["clusters"][dst_cluster_name]["uri"]

    overwrite = _db_exists(dst_uri, dst_db)
    if overwrite:
        if _is_protected(config, dst_cluster_name, dst_db) and not args.force:
            sys.exit(f"bongo: '{dst_cluster_name}:{dst_db}' is protected — use --force to overwrite it")
        if not args.yes and not _confirm(
            f"Restore '{path.name}' over existing '{dst_cluster_name}:{dst_db}'?"
        ):
            sys.exit("bongo: aborted")

    quiet = not args.verbose
    print(f"Restoring '{path.name}' -> '{dst_cluster_name}:{dst_db}' ...")
    restore_cmd = [
        "mongorestore", f"--uri={dst_uri}", "--gzip", f"--archive={path}",
        f"--nsFrom={src_db}.*", f"--nsTo={dst_db}.*",
    ]
    if overwrite:
        restore_cmd.append("--drop")
    restore = subprocess.Popen(
        restore_cmd, stderr=subprocess.PIPE if quiet else None, text=quiet or None,
    )
    stats = _stream_mongo_progress(restore.stderr) if quiet else None
    if restore.wait() != 0:
        if stats:
            sys.stderr.writelines(stats["tail"])
        sys.exit(f"bongo: mongorestore exited with code {restore.returncode}")
    if not overwrite:
        _record_created(dst_cluster_name, dst_db, str(path.name))
    suffix = f" ({_summary(stats)})" if stats else ""
    print(f"Done — '{dst_cluster_name}:{dst_db}' is ready{suffix}")
    if stats and stats["failures"]:
        sys.exit(f"bongo: warning: {stats['failures']:,} documents failed to restore")


def _cmd_sh(config: dict, args: argparse.Namespace) -> None:
    target = args.target
    cluster_name, db = None, None
    if target is None:
        cluster_name = config.get("default")
        if not cluster_name:
            sys.exit("bongo: no target given and no default cluster is set in config")
    elif ":" in target:
        cluster_name, db = _resolve_address(config, target)
    elif target in config["clusters"]:
        cluster_name = target
    else:
        cluster_name, db = _resolve_address(config, target)

    uri = _get_cluster(config, cluster_name)["uri"]
    if db:
        uri = _uri_with_db(uri, db)
    print(f"Connecting to {cluster_name}" + (f":{db}" if db else "") + " ...")
    os.execvp("mongosh", ["mongosh", uri])


def _cmd_run(config: dict, args: argparse.Namespace) -> None:
    label, path = _resolve_script(config, args.script)
    cluster_name, db = _resolve_address(config, args.target)
    uri = _uri_with_db(config["clusters"][cluster_name]["uri"], db)
    name = label or str(path)
    context = {
        "cluster": cluster_name,
        "database": db,
        "target": f"{cluster_name}:{db}",
        "dryRun": args.dry_run,
        "args": args.script_args,
    }
    prelude = (
        f"globalThis.bongo = {json.dumps(context)};\n"
        "globalThis.dryRun = globalThis.bongo.dryRun;\n"
        f"load({json.dumps(str(path))});\n"
        "undefined;"
    )

    dry = " (dryRun=true)" if args.dry_run else ""
    print(f"Running '{name}' on '{cluster_name}:{db}'{dry} ...")
    result = subprocess.run(["mongosh", uri, "--quiet", "--eval", prelude])
    if result.returncode != 0:
        sys.exit(f"bongo: script exited with code {result.returncode}")
    print("Done")


def _collection_stats(uri: str, db: str) -> dict[str, dict]:
    """Return {collection: {count, indexes}} for a database."""
    collections = _mongosh_json(
        uri,
        f"(() => {{ const d = db.getSiblingDB({json.dumps(db)});"
        " return d.getCollectionNames().filter(c => !c.startsWith('system.'))"
        ".map(c => ({name: c, count: d.getCollection(c).estimatedDocumentCount(),"
        " indexes: d.getCollection(c).getIndexes().map(i => i.name).sort()})); })()",
    )
    return {c["name"]: c for c in collections}


def _cmd_diff(config: dict, args: argparse.Namespace) -> None:
    a_cluster, a_db = _resolve_address(config, args.a)
    b_cluster, b_db = _resolve_address(config, args.b)
    a_label, b_label = f"{a_cluster}:{a_db}", f"{b_cluster}:{b_db}"
    a_uri = config["clusters"][a_cluster]["uri"]
    b_uri = config["clusters"][b_cluster]["uri"]

    for uri, cluster, db, label in ((a_uri, a_cluster, a_db, a_label), (b_uri, b_cluster, b_db, b_label)):
        if not _db_exists(uri, db):
            sys.exit(f"bongo: database '{db}' not found on cluster '{cluster}'")
    a_stats = _collection_stats(a_uri, a_db)
    b_stats = _collection_stats(b_uri, b_db)

    only_a = sorted(set(a_stats) - set(b_stats))
    only_b = sorted(set(b_stats) - set(a_stats))
    changed: list[str] = []
    identical = 0
    for name in sorted(set(a_stats) & set(b_stats)):
        a_c, b_c = a_stats[name], b_stats[name]
        notes = []
        if a_c["count"] != b_c["count"]:
            notes.append(f"{a_c['count']:,} -> {b_c['count']:,} docs")
        added = sorted(set(b_c["indexes"]) - set(a_c["indexes"]))
        removed = sorted(set(a_c["indexes"]) - set(b_c["indexes"]))
        if added:
            notes.append("indexes +" + " +".join(added))
        if removed:
            notes.append("indexes -" + " -".join(removed))
        if notes:
            changed.append(f"  {name:<30} {', '.join(notes)}")
        else:
            identical += 1

    print(f"{a_label} <-> {b_label}")
    if not (only_a or only_b or changed):
        print(f"No differences ({identical} collections compared)")
        return
    if only_a:
        print(f"only in {a_label}:")
        for name in only_a:
            print(f"  {name:<30} {a_stats[name]['count']:,} docs")
    if only_b:
        print(f"only in {b_label}:")
        for name in only_b:
            print(f"  {name:<30} {b_stats[name]['count']:,} docs")
    if changed:
        print("changed:")
        for line in changed:
            print(line)
    if identical:
        print(f"{identical} collections identical")


def _cmd_check(args: argparse.Namespace) -> None:
    if not _CONFIG_PATH.exists():
        sys.exit(f"bongo: no config found — run 'bongo init' to create {_CONFIG_PATH}")
    try:
        with open(_CONFIG_PATH, "rb") as f:
            config = tomllib.load(f)
    except Exception as e:
        sys.exit(f"bongo: could not parse {_CONFIG_PATH}: {e}")

    errors = _validate_config(config)
    if errors:
        print(f"{_CONFIG_PATH}: invalid", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)

    clusters = config["clusters"]
    default = config.get("default")
    print(f"{_CONFIG_PATH}: OK — {len(clusters)} cluster(s)")
    for name in sorted(clusters):
        tags = ["default"] if name == default else []
        protected = clusters[name].get("protected", [])
        if protected:
            tags.append(f"protected: {', '.join(protected)}")
        suffix = f"  [{'; '.join(tags)}]" if tags else ""
        print(f"  {name}  {_redact_uri(clusters[name]['uri'])}{suffix}")
    scripts = config.get("scripts", {})
    if scripts:
        print("scripts")
        for label in sorted(scripts):
            path = _path_from(scripts[label], _CONFIG_DIR)
            suffix = "" if path.is_file() else "  [missing]"
            print(f"  {label}  {path}{suffix}")


def _cmd_init(args: argparse.Namespace) -> None:
    if _CONFIG_PATH.exists():
        sys.exit(f"bongo: config already exists at {_CONFIG_PATH}")
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CONFIG_PATH.write_text(_EXAMPLE_CONFIG)
    _CONFIG_PATH.chmod(0o600)
    print(f"Created {_CONFIG_PATH} — edit it to add your clusters")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_run_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="bongo run", description="Run a mongosh JavaScript file on a database.")
    parser.add_argument("script", help="script label from [scripts] or a .js file path")
    parser.add_argument("target", help="target database (<cluster>:<db> or <db>)")
    parser.add_argument("--dry-run", action="store_true", help="set globalThis.bongo.dryRun and globalThis.dryRun")
    parser.add_argument("script_args", nargs="*", metavar="ARG", help="arguments exposed as globalThis.bongo.args")

    args_argv = list(argv)
    script_args: list[str] = []
    if "--" in args_argv:
        sep = args_argv.index("--")
        script_args = args_argv[sep + 1:]
        args_argv = args_argv[:sep]

    args, unknown = parser.parse_known_args(args_argv)
    if unknown:
        parser.error(f"unrecognized arguments: {' '.join(unknown)} (put script arguments after --)")
    args.command = "run"
    args.script_args += script_args
    return args


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    raw_argv = sys.argv[1:] if argv is None else argv
    if raw_argv[:1] == ["run"]:
        return _parse_run_args(raw_argv[1:])

    parser = argparse.ArgumentParser(
        prog="bongo",
        description="Copy, list and drop MongoDB databases across configured clusters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Databases are addressed as <cluster>:<db>; a bare <db> uses the\n"
            "default cluster from ~/.config/bongo/config.toml.\n\n"
            "Examples:\n"
            "  bongo cp main pr-539              # copy within the default cluster\n"
            "  bongo cp main .                   # '.' = current git branch name, sanitized\n"
            "  bongo cp atlas-dev:staging local:main\n"
            "  bongo sh                          # mongosh shell on the default cluster\n"
            "  bongo run adduser pr-539          # run a configured script label on a database\n"
            "  bongo run ./fix.js atlas-dev:main # run a one-off script file\n"
            "  bongo diff main pr-539            # collection/count/index differences\n"
            "  bongo ls                          # databases on the default cluster\n"
            "  bongo ls atlas-dev\n"
            "  bongo rm pr-539\n"
            "  bongo prune --days 7              # offer to drop bongo-created dbs older than a week\n"
            "  bongo snapshot main               # archive to ~/.local/share/bongo/snapshots\n"
            "  bongo snapshot                    # list snapshots\n"
            "  bongo restore main                # restore latest snapshot of main in place\n"
            "  bongo restore main main-redo      # ...or into a different db\n"
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    cp = sub.add_parser("cp", help="copy a database (within or across clusters)")
    cp.add_argument("source", help="source database (<cluster>:<db> or <db>)")
    cp.add_argument("target", help="target database (<cluster>:<db> or <db>)")
    cp.add_argument("-y", "--yes", action="store_true", help="skip the overwrite confirmation")
    cp.add_argument("--force", action="store_true", help="allow overwriting a protected database")
    cp.add_argument("-v", "--verbose", action="store_true", help="show raw mongodump/mongorestore output")

    rm = sub.add_parser("rm", help="drop a database (no arg: pick interactively)")
    rm.add_argument("database", nargs="?", help="database to drop (<cluster>:<db> or <db>)")
    rm.add_argument("-y", "--yes", action="store_true", help="skip the confirmation prompt")
    rm.add_argument("--force", action="store_true", help="allow dropping a protected database")

    ls = sub.add_parser("ls", help="list databases on a cluster")
    ls.add_argument("cluster", nargs="?", help="cluster name (defaults to the default cluster)")
    ls.add_argument("-a", "--all", action="store_true", help="include system databases (admin, config, local)")

    prune = sub.add_parser("prune", help="interactively drop databases created by bongo")
    prune.add_argument("cluster", nargs="?", help="cluster name (defaults to the default cluster)")
    prune.add_argument("--days", type=int, metavar="N", help="only consider databases older than N days")
    prune.add_argument("-y", "--yes", action="store_true", help="drop all candidates without prompting")

    snapshot = sub.add_parser("snapshot", help="dump a database to a local archive (no arg: list snapshots)")
    snapshot.add_argument("database", nargs="?", help="database to snapshot (<cluster>:<db> or <db>)")
    snapshot.add_argument("-v", "--verbose", action="store_true", help="show raw mongodump output")

    restore = sub.add_parser("restore", help="restore a database from its latest snapshot (no arg: pick interactively)")
    restore.add_argument("database", nargs="?", help="database whose snapshot to restore (<cluster>:<db> or <db>)")
    restore.add_argument("target", nargs="?", help="target database (defaults to restoring in place)")
    restore.add_argument("--file", metavar="PATH", help="restore a specific snapshot file instead of the latest")
    restore.add_argument("-y", "--yes", action="store_true", help="skip the overwrite confirmation")
    restore.add_argument("--force", action="store_true", help="allow overwriting a protected database")
    restore.add_argument("-v", "--verbose", action="store_true", help="show raw mongorestore output")

    sh = sub.add_parser("sh", help="open a mongosh shell on a configured cluster")
    sh.add_argument("target", nargs="?", help="<cluster>, <cluster>:<db> or <db> (defaults to the default cluster)")

    run = sub.add_parser("run", help="run a mongosh JavaScript file on a database")
    run.add_argument("script", help="script label from [scripts] or a .js file path")
    run.add_argument("target", help="target database (<cluster>:<db> or <db>)")
    run.add_argument("--dry-run", action="store_true", help="set globalThis.bongo.dryRun and globalThis.dryRun")
    run.add_argument("script_args", nargs="*", metavar="ARG", help="arguments exposed as globalThis.bongo.args")

    diff = sub.add_parser("diff", help="compare two databases (collections, doc counts, indexes)")
    diff.add_argument("a", help="first database (<cluster>:<db> or <db>)")
    diff.add_argument("b", help="second database (<cluster>:<db> or <db>)")

    sub.add_parser("init", help="create a starter config file")
    sub.add_parser("check", help="validate the config file and list configured clusters")

    return parser.parse_args(raw_argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.command == "init":
        _cmd_init(args)
        return

    if args.command == "check":
        _cmd_check(args)
        return

    config = _load_config()
    if args.command == "ls":
        _require_tools("mongosh")
        _cmd_ls(config, args)
    elif args.command == "rm":
        _require_tools("mongosh")
        _cmd_rm(config, args)
    elif args.command == "cp":
        _require_tools("mongosh", "mongodump", "mongorestore")
        _cmd_cp(config, args)
    elif args.command == "prune":
        _require_tools("mongosh")
        _cmd_prune(config, args)
    elif args.command == "snapshot":
        if args.database is not None:
            _require_tools("mongosh", "mongodump")
        _cmd_snapshot(config, args)
    elif args.command == "restore":
        _require_tools("mongosh", "mongorestore")
        _cmd_restore(config, args)
    elif args.command == "sh":
        _require_tools("mongosh")
        _cmd_sh(config, args)
    elif args.command == "run":
        _require_tools("mongosh")
        _cmd_run(config, args)
    elif args.command == "diff":
        _require_tools("mongosh")
        _cmd_diff(config, args)
