# devtools

Collection of CLI tools I use to make my life easier. Supports macOS and Linux.

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

Clipboard output uses the native macOS clipboard, `wl-copy` on Wayland, or
`xclip` on X11. Low-resolution output requires ImageMagick on Linux.

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

Logs are colorized on the fly (log level, timestamps, and inline `[ KEY ]` markers) and the status line is kept minimal.

```bash
azlogs                        # pick customer and date interactively
azlogs river                  # open today's log for the best-matching customer
azlogs river 1                # yesterday's log (N = days ago, so 1 = yesterday)
azlogs river mon              # most recent Monday (weekday names: mon, tue, ...)
azlogs river 06-20            # most recent June 20 (MM-DD)
azlogs river 2026-06-18       # an explicit date (YYYY-MM-DD)
azlogs river --days 3         # preload the last 3 days into one scrollable buffer
azlogs river -w               # wrap long lines instead of chopping them
azlogs river -f               # follow mode: poll for new lines every 5s
azlogs river -f 10            # follow mode with 10s poll interval
```

The optional `WHEN` argument picks the day to open. If the requested date has no
log, you drop into the date picker pre-seeded with that date instead of an error.

Required environment variables (e.g. in `.zshrc.local`):
```bash
export AZURE_BLOB_BASE_URL=https://example.blob.core.windows.net/
export AZURE_SAS_TOKEN=sv=2021-...&sig=...
```

Only blobs from the last 14 days are listed, which is also how far the inline `[` navigation can reach back.

Navigation: `j`/`k` to scroll, `[`/`]` to load the previous/next day's log inline (merged seamlessly into the same scroll buffer), `/` to search, `←`/`→` for long lines (or run with `-w` to wrap instead), `q` or `Ctrl+C` to quit.

In follow mode (`-f`), `Ctrl+C` pauses following so you can scroll back, `F` resumes, and `q` quits.

### bongo

Copy, list and drop MongoDB databases across configured clusters — a thin wrapper over `mongodump`/`mongorestore`. Handy for cloning a base database before testing a PR with destructive migrations.

Requires `mongosh` and the [MongoDB Database Tools](https://www.mongodb.com/docs/database-tools/installation/).

```bash
bongo init                            # create a starter config
bongo cp main pr-539                  # copy within the default cluster
bongo cp main .                       # '.' = current git branch name, sanitized (user/axel/fix-1 -> user-axel-fix-1)
bongo cp atlas-dev:staging local:main # copy across clusters (streamed, no temp files)
bongo sh                              # mongosh shell on the default cluster (or: bongo sh atlas-dev:somedb)
bongo run adduser pr-539              # run a configured JavaScript script on a database
bongo run ./fix.js atlas-dev:staging  # run a one-off script file
bongo diff main pr-539                # compare collections, doc counts and indexes
bongo cat main users                  # print documents (first 10; -n all for everything)
bongo cat main users axel role=admin  # ...filtered: 'axel' anywhere, and role equal to admin
bongo ls                              # list databases on the default cluster (with sizes)
bongo ls atlas-dev
bongo ls atlas-dev:main                # list collections in a database (with doc counts)
bongo rm pr-539                       # drop a database (asks for confirmation)
bongo prune --days 7                  # offer to drop bongo-created dbs older than a week
bongo snapshot main                   # gzipped archive in ~/.local/share/bongo/snapshots
bongo snapshot                        # list snapshots
bongo restore main                    # restore latest snapshot of main in place
bongo restore main main-redo          # ...or into a different db (--file picks a specific snapshot)
bongo check --connect                 # validate config and ping every configured cluster
```

`cp`, `snapshot` and `restore` render one ✓ line per collection with doc counts (plus a live progress bar for the collection in flight, when the output is a terminal). Pass `-v` for the raw mongodump/mongorestore output. Colors respect `NO_COLOR`.

`bongo rm` and `bongo restore` with no arguments open an interactive picker (fzf when installed, a numbered list otherwise). The `rm` picker hides system and protected databases.

bongo keeps a manifest (`~/.config/bongo/state.json`) of databases it created, so `prune` only ever offers to drop those — never databases it didn't make. Snapshots are handy before running a destructive migration: `bongo snapshot main`, run the script, and `bongo restore main` rolls it back.

`bongo run SCRIPT TARGET` runs a mongosh JavaScript file against `TARGET`. `SCRIPT` is first resolved as a label from `[scripts]`, then as a direct file path. Scripts get the normal mongosh `db` global plus bongo context:

```js
globalThis.bongo = {
  cluster: "local",
  database: "main",
  target: "local:main",
  dryRun: false,
  args: [],
};
globalThis.dryRun = globalThis.bongo.dryRun;
```

`--dry-run` only sets those flags; scripts must opt in to avoid writes.

For local helper scripts, put files under `~/.config/bongo/scripts` and add labels in `[scripts]`:

```bash
bongo run adduser main
bongo run adduser main -- --script-specific-option value
```

`bongo cat DATABASE COLLECTION [TERM...]` prints documents, filtered by terms that are ANDed together:

```bash
bongo cat main users axel                 # bare word: case-insensitive match in any value
bongo cat main users name~axel            # field matches a case-insensitive regex
bongo cat main users role=admin           # equals (also !=)
bongo cat main users 'age>30'             # > >= < <= (quote them for the shell)
bongo cat main users 'createdAt>=2026-01-01'
bongo cat main users 65f1c0ffee65f1c0ffee65f1   # bare 24-hex word: _id lookup
bongo cat main users address.city=Stockholm -f name,email
bongo cat main users -r -n 1              # newest document
bongo cat main users -x loginHistory,address.zip   # everything but these fields
bongo cat main users -d 1                 # collapse nested values: "tags": [… 2 items]
bongo cat dev:main users -n all | jq .email
```

Values are typed for you: `true`/`false`/`null`, ObjectIds, ISO dates, and numbers (`zip=12345` matches both `12345` and `"12345"`). Flags: `-n N|all` (default 10, with a notice on stderr when more match), `-s FIELD` sort, `-r` descending (by `_id` without `-s`), `-f a,b` fields, `-x a,b` exclude fields, `-d N` collapse anything nested deeper than N levels into a placeholder (a string like `"{… 3 fields}"` when piped), `-c` count, `-q JSON` raw query. Output is pretty-printed and colored on a terminal, and one JSON document per line when piped; ObjectIds and dates are printed as plain strings. Bare words are matched client-side, so on a large remote collection prefer `field~word`. Requires `mongoexport` from the Database Tools.

Databases are addressed as `<cluster>:<db>`; a bare `<db>` uses the default cluster. Clusters are defined in `~/.config/bongo/config.toml`:

```toml
default = "local"

[clusters.local]
uri = "mongodb://localhost:27017"
protected = ["main"]

[clusters.atlas-dev]
uri = "mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net"
protected = []

[scripts]
adduser = "scripts/adduser.js" # relative to ~/.config/bongo
```

`bongo check` validates the config structure and lists its clusters. Add `--connect` to use each configured URI with `mongosh` and ping every cluster; all clusters are checked, and the command exits nonzero if any connection fails. Checks use a five-second server-selection timeout unless the URI already specifies `serverSelectionTimeoutMS`. Timeout-style failures for Atlas clusters include a hint to check the project's Network Access IP access list.

Databases listed in `protected` cannot be dropped or overwritten without `--force`. Copying onto an existing database prompts before replacing it (`-y` skips the prompt).

Tab completion for zsh covers subcommands, flags, clusters, databases, collections, script labels and snapshots. Add this to `~/.zshrc`, after `compinit` (or after oh-my-zsh is sourced):

```sh
source <(bongo completion zsh)
```

Completing a database or collection name connects to the cluster with `mongosh` (three-second timeout); the names are cached for 60 seconds in `~/.cache/bongo/completion.json` and the cache is cleared whenever `cp`, `rm`, `prune`, `restore` or `run` finishes.

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

In command mode the command is automatically copied to your clipboard when a
supported clipboard tool is available. Useful aliases:

On Linux, clipboard copying requires `wl-copy` from `wl-clipboard` on Wayland or
`xclip` on X11.

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
| OS | Operating system and version when available | `macOS 15.4` or `Linux` |
| Shell | Name and version | `zsh 5.9` |
| Working directory | Basename only (not full path) | `devtools` |
| Git context | Whether cwd is inside a git repo | `in git repo` |
| Date | Today's date | `2026-06-01` |
| Installed tools | Presence check of ~15 common CLI tools | `fd, rg, jq, bat` |
| Runtimes | Name and major.minor version | `python 3.13, node 22.1` |

If you pipe content into `oneshot`, that content is included in the user message sent to the API. It is capped at 32 KB and a warning is printed if truncated.

No shell history, environment variables, file contents, or full paths are ever sent.
