# devtools

Collection of CLI tools I use to make my life easier. macOS only.

## Install

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/).

### Global install (no clone needed)

```bash
uv tool install git+https://github.com/axeleklof/devtools.git
```

This makes `adbshot`, `adbw`, `azlogs`, and `oneshot` available globally on your PATH. To update later:

```bash
uv tool upgrade devtools
```

### Try without installing

```bash
uvx --from git+https://github.com/axeleklof/devtools.git adbshot
```

### Uninstall

```bash
uv tool uninstall devtools
```

### From a local clone

```bash
git clone https://github.com/axeleklof/devtools.git
cd devtools
uv sync
```

Run tools with `uv run <tool>` or activate the venv first.

## Tools

### adbshot

Capture screenshots from a connected Android device via `adb`.

```bash
adbshot                        # copy to clipboard at full resolution
adbshot -l                     # copy to clipboard resized to 1000px tall
adbshot -l -H 1200 -f ~/shot  # save to file at 1200px tall
adbshot -f ~/shot.png -c       # save to file and copy to clipboard
```

### adbw

Set up wireless ADB debugging. Handles device selection, IP discovery, and optional reverse port forwarding.

```bash
adbw                          # basic wireless setup
adbw -p 5556                  # custom port
adbw -r 3000,8080             # with reverse port forwarding
adbw --ip 192.168.1.42        # reconnect without USB
```

### azlogs

Browse and view Azure Blob Storage log files. Uses `fzf` for interactive selection and `less` for viewing.

Assumes:
- Containers are named `{customer}logs` — only containers whose name contains `logs` are listed
- Log blobs follow the pattern `{prefix}.YYYY-MM-DD.log`
- The SAS token has container list (enumeration) and blob read permissions

```bash
azlogs                        # pick customer and date interactively
azlogs river                  # open today's log for the best-matching customer
azlogs river -f               # follow mode: poll for new lines every 5s
azlogs river -f 10            # follow mode with 10s poll interval
```

Required environment variables (e.g. in `.zshrc.local`):
```bash
export AZURE_BLOB_BASE_URL=https://example.blob.core.windows.net/
export AZURE_SAS_TOKEN=sv=2021-...&sig=...
```

Navigation: `j`/`k` to scroll, `/` to search, `←`/`→` for long lines, `Ctrl+C` to pause follow mode, `F` to resume, `q` to quit.

### bongo

Copy, list and drop MongoDB databases across configured clusters — a thin wrapper over `mongodump`/`mongorestore`. Handy for cloning a base database before testing a PR with destructive migrations.

Requires `mongosh` and the MongoDB database tools (`brew install mongosh mongodb-database-tools`).

```bash
bongo init                            # create a starter config
bongo cp main pr-539                  # copy within the default cluster
bongo cp main .                       # '.' = current git branch name, sanitized (user/axel/fix-1 -> user-axel-fix-1)
bongo cp atlas-dev:staging local:main # copy across clusters (streamed, no temp files)
bongo sh                              # mongosh shell on the default cluster (or: bongo sh atlas-dev:somedb)
bongo diff main pr-539                # compare collections, doc counts and indexes
bongo ls                              # list databases on the default cluster (with sizes)
bongo ls atlas-dev
bongo rm pr-539                       # drop a database (asks for confirmation)
bongo prune --days 7                  # offer to drop bongo-created dbs older than a week
bongo snapshot main                   # gzipped archive in ~/.local/share/bongo/snapshots
bongo snapshot                        # list snapshots
bongo restore main                    # restore latest snapshot of main in place
bongo restore main main-redo          # ...or into a different db (--file picks a specific snapshot)
```

`cp`, `snapshot` and `restore` render one ✓ line per collection with doc counts (plus a live progress bar for the collection in flight, when the output is a terminal). Pass `-v` for the raw mongodump/mongorestore output. Colors respect `NO_COLOR`.

`bongo rm` and `bongo restore` with no arguments open an interactive picker (fzf when installed, a numbered list otherwise). The `rm` picker hides system and protected databases.

bongo keeps a manifest (`~/.config/bongo/state.json`) of databases it created, so `prune` only ever offers to drop those — never databases it didn't make. Snapshots are handy before running a destructive migration: `bongo snapshot main`, run the script, and `bongo restore main` rolls it back.

Databases are addressed as `<cluster>:<db>`; a bare `<db>` uses the default cluster. Clusters are defined in `~/.config/bongo/config.toml`:

```toml
default = "local"

[clusters.local]
uri = "mongodb://localhost:27017"
protected = ["main"]

[clusters.atlas-dev]
uri = "mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net"
protected = []
```

Databases listed in `protected` cannot be dropped or overwritten without `--force`. Copying onto an existing database prompts before replacing it (`-y` skips the prompt).

### oneshot

One-shot LLM query from the terminal — get a shell command or a quick explanation without leaving your workflow.

```bash
oneshot "find files modified in the last 24 hours"       # command mode (default)
oneshot -v "find files modified in the last 24 hours"    # command + brief explanation
oneshot -x "what is a semaphore"                         # explanation as markdown
oneshot -x "what is a semaphore" | glow                  # render with glow
oneshot -p local "list processes on port 8080"           # use a named profile
cat error.log | oneshot "what's wrong here"              # pipe content as context
git diff | oneshot -v "summarise these changes"
```

In command mode the command is automatically copied to your clipboard. Useful aliases:

```bash
alias osc='oneshot'
ose() { oneshot -x "$@" | glow -; }     # explain, rendered
osv() { oneshot -v -m "$@" | glow -; }  # command + explanation, rendered
```

#### Configuration

Create `~/.config/oneshot/config.toml` to define named profiles. The `[default]` section sets which profile is active when no `-p` flag is given.

```toml
[default]
profile = "deepseek"

[profiles.deepseek]
api_url = "https://api.deepseek.com/v1/chat/completions"
model = "deepseek-chat"
api_key = "sk-..."

[profiles.local]
api_url = "http://localhost:11434/v1/chat/completions"
model = "llama3.2"
api_key = "ollama"
```

If you'd rather not store keys in the config file, use `api_key_env` to reference an environment variable instead:

```toml
[profiles.openai]
api_url = "https://api.openai.com/v1/chat/completions"
model = "gpt-4o-mini"
api_key_env = "OPENAI_API_KEY"
```

**Priority order** (highest wins): `-p` CLI flag → `ONESHOT_API_KEY` / `ONESHOT_API_URL` / `ONESHOT_MODEL` env vars → config profile → hardcoded defaults (OpenAI, `gpt-4o-mini`).

#### What is sent to the API

Each request includes a system prompt with the following context collected at invocation time:

| Field | Value | Example |
|---|---|---|
| OS | macOS version | `macOS 15.4` |
| Shell | Name and version | `zsh 5.9` |
| Working directory | Basename only (not full path) | `devtools` |
| Git context | Whether cwd is inside a git repo | `in git repo` |
| Date | Today's date | `2026-06-01` |
| Installed tools | Presence check of ~15 common CLI tools | `fd, rg, jq, bat` |
| Runtimes | Name and major.minor version | `python 3.13, node 22.1` |

If you pipe content into `oneshot`, that content is included in the user message sent to the API. It is capped at 32 KB and a warning is printed if truncated.

No shell history, environment variables, file contents, or full paths are ever sent.
