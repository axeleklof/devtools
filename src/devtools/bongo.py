"""Copy, list and drop MongoDB databases across configured clusters — a thin wrapper over mongodump/mongorestore."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

_CONFIG_PATH = Path.home() / ".config" / "bongo" / "config.toml"
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
"""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    if not _CONFIG_PATH.exists():
        sys.exit(f"bongo: no config found — run 'bongo init' to create {_CONFIG_PATH}")
    try:
        with open(_CONFIG_PATH, "rb") as f:
            config = tomllib.load(f)
    except Exception as e:
        sys.exit(f"bongo: could not read config: {e}")
    if not config.get("clusters"):
        sys.exit(f"bongo: no clusters defined in {_CONFIG_PATH}")
    return config


def _get_cluster(config: dict, name: str) -> dict:
    cluster = config["clusters"].get(name)
    if cluster is None:
        known = ", ".join(sorted(config["clusters"]))
        sys.exit(f"bongo: unknown cluster '{name}' (configured: {known})")
    if not cluster.get("uri"):
        sys.exit(f"bongo: cluster '{name}' has no uri in config")
    return cluster


def _resolve_address(config: dict, address: str) -> tuple[str, str]:
    """Split 'cluster:db' (or bare 'db', using the default cluster) into (cluster, db)."""
    if ":" in address:
        cluster_name, db = address.split(":", 1)
    else:
        cluster_name, db = config.get("default", ""), address
        if not cluster_name:
            sys.exit(f"bongo: '{address}' has no cluster prefix and no default cluster is set in config")
    if not _DB_NAME_RE.match(db):
        sys.exit(f"bongo: invalid database name '{db}' (allowed: letters, digits, _ and -)")
    _get_cluster(config, cluster_name)
    return cluster_name, db


def _is_protected(config: dict, cluster_name: str, db: str) -> bool:
    return db in config["clusters"][cluster_name].get("protected", [])


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
    for db in sorted(databases, key=lambda d: d["name"]):
        tag = "  [protected]" if _is_protected(config, cluster_name, db["name"]) else ""
        print(f"  {db['name']:<30} {_format_size(db['size']):>10}{tag}")


def _cmd_rm(config: dict, args: argparse.Namespace) -> None:
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

    print(f"Copying '{src_cluster_name}:{src_db}' -> '{dst_cluster_name}:{dst_db}' ...")
    dump = subprocess.Popen(
        ["mongodump", f"--uri={src_uri}", f"--db={src_db}", "--archive"],
        stdout=subprocess.PIPE,
    )
    restore_cmd = [
        "mongorestore", f"--uri={dst_uri}", "--archive",
        f"--nsFrom={src_db}.*", f"--nsTo={dst_db}.*",
    ]
    if overwrite:
        restore_cmd.append("--drop")
    restore = subprocess.Popen(restore_cmd, stdin=dump.stdout)
    dump.stdout.close()  # let mongodump receive SIGPIPE if mongorestore dies

    restore_rc = restore.wait()
    dump_rc = dump.wait()
    if dump_rc != 0:
        sys.exit(f"bongo: mongodump exited with code {dump_rc}")
    if restore_rc != 0:
        sys.exit(f"bongo: mongorestore exited with code {restore_rc}")
    _record_created(dst_cluster_name, dst_db, f"{src_cluster_name}:{src_db}")
    print(f"Done — '{dst_cluster_name}:{dst_db}' is ready")


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

    print(f"Snapshotting '{cluster_name}:{db}' ...")
    result = subprocess.run(
        ["mongodump", f"--uri={uri}", f"--db={db}", "--gzip", f"--archive={path}"],
    )
    if result.returncode != 0:
        path.unlink(missing_ok=True)
        sys.exit(f"bongo: mongodump exited with code {result.returncode}")
    print(f"Done — {path} ({_format_size(path.stat().st_size)})")


def _parse_snapshot_filename(path: Path) -> tuple[str, str]:
    parts = path.name.removesuffix(".archive.gz").split("__")
    if len(parts) != 3:
        sys.exit(f"bongo: cannot parse snapshot filename '{path.name}' (expected <cluster>__<db>__<timestamp>.archive.gz)")
    return parts[0], parts[1]


def _cmd_restore(config: dict, args: argparse.Namespace) -> None:
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

    print(f"Restoring '{path.name}' -> '{dst_cluster_name}:{dst_db}' ...")
    restore_cmd = [
        "mongorestore", f"--uri={dst_uri}", "--gzip", f"--archive={path}",
        f"--nsFrom={src_db}.*", f"--nsTo={dst_db}.*",
    ]
    if overwrite:
        restore_cmd.append("--drop")
    result = subprocess.run(restore_cmd)
    if result.returncode != 0:
        sys.exit(f"bongo: mongorestore exited with code {result.returncode}")
    if not overwrite:
        _record_created(dst_cluster_name, dst_db, str(path.name))
    print(f"Done — '{dst_cluster_name}:{dst_db}' is ready")


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

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bongo",
        description="Copy, list and drop MongoDB databases across configured clusters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Databases are addressed as <cluster>:<db>; a bare <db> uses the\n"
            "default cluster from ~/.config/bongo/config.toml.\n\n"
            "Examples:\n"
            "  bongo cp main pr-539              # copy within the default cluster\n"
            "  bongo cp atlas-dev:staging local:main\n"
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

    rm = sub.add_parser("rm", help="drop a database")
    rm.add_argument("database", help="database to drop (<cluster>:<db> or <db>)")
    rm.add_argument("-y", "--yes", action="store_true", help="skip the confirmation prompt")
    rm.add_argument("--force", action="store_true", help="allow dropping a protected database")

    ls = sub.add_parser("ls", help="list databases on a cluster")
    ls.add_argument("cluster", nargs="?", help="cluster name (defaults to the default cluster)")

    prune = sub.add_parser("prune", help="interactively drop databases created by bongo")
    prune.add_argument("cluster", nargs="?", help="cluster name (defaults to the default cluster)")
    prune.add_argument("--days", type=int, metavar="N", help="only consider databases older than N days")
    prune.add_argument("-y", "--yes", action="store_true", help="drop all candidates without prompting")

    snapshot = sub.add_parser("snapshot", help="dump a database to a local archive (no arg: list snapshots)")
    snapshot.add_argument("database", nargs="?", help="database to snapshot (<cluster>:<db> or <db>)")

    restore = sub.add_parser("restore", help="restore a database from its latest snapshot")
    restore.add_argument("database", nargs="?", help="database whose snapshot to restore (<cluster>:<db> or <db>)")
    restore.add_argument("target", nargs="?", help="target database (defaults to restoring in place)")
    restore.add_argument("--file", metavar="PATH", help="restore a specific snapshot file instead of the latest")
    restore.add_argument("-y", "--yes", action="store_true", help="skip the overwrite confirmation")
    restore.add_argument("--force", action="store_true", help="allow overwriting a protected database")

    sub.add_parser("init", help="create a starter config file")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.command == "init":
        _cmd_init(args)
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
        if args.database is None and not args.file:
            sys.exit("bongo: restore needs a database or --file")
        _require_tools("mongosh", "mongorestore")
        _cmd_restore(config, args)
