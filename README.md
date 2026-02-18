# devtools

Collection of CLI tools I use to make my life easier. macOS only.

## Install

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/).

### Global install (no clone needed)

```bash
uv tool install git+https://github.com/axeleklof/devtools.git
```

This makes `adbshot` and `adbw` available globally on your PATH. To update later:

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
