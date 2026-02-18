"""Take screenshots from Android devices via adb."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="adbshot",
        description="Capture a screenshot from a connected Android device.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  adbshot                        # clipboard @ full-res\n"
            "  adbshot -l                     # clipboard @ 1000 px tall\n"
            "  adbshot -l -H 1200 -f ~/shot   # save ~/shot.png @ 1200 px tall\n"
            "  adbshot -f ~/shot.png -c       # save to file AND copy to clipboard"
        ),
    )
    parser.add_argument(
        "-l", "--lowres",
        action="store_true",
        help="Resize to target height (default 1000 px).",
    )
    parser.add_argument(
        "-H", "--height",
        type=int,
        default=1000,
        metavar="N",
        help="Target height in px when -l is used (default 1000).",
    )
    parser.add_argument(
        "-f", "--file",
        nargs="?",
        const="__AUTO__",
        default=None,
        metavar="PATH",
        help=(
            "Save PNG to PATH (folder auto-created). "
            "If PATH has no .png extension, it will be appended."
        ),
    )
    parser.add_argument(
        "-c", "--clipboard",
        action="store_true",
        help=(
            "Copy PNG to clipboard. If neither -f nor -c is given, "
            "clipboard is the default."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    outfile: str | None = args.file
    do_clipboard: bool = args.clipboard

    # Default behaviour: clipboard when no -f given
    if outfile is None and not do_clipboard:
        do_clipboard = True

    # Resolve auto-generated filename
    if outfile == "__AUTO__":
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        outfile = f"./screenshot-{ts}.png"

    # Capture screenshot to a temp file
    tmp_png = Path(tempfile.mktemp(prefix="android_screenshot.", suffix=".png"))
    try:
        with open(tmp_png, "wb") as f:
            result = subprocess.run(
                ["adb", "exec-out", "screencap", "-p"],
                stdout=f,
                stderr=subprocess.PIPE,
            )
        if result.returncode != 0:
            stderr = result.stderr.decode(errors="replace").strip()
            if "no devices" in stderr or "no emulators" in stderr:
                print("No adb devices connected", file=sys.stderr)
            else:
                print(f"adb screencap failed: {stderr}", file=sys.stderr)
            sys.exit(1)

        # Optional resize
        if args.lowres:
            result = subprocess.run(
                [
                    "sips", "--resampleHeight", str(args.height),
                    str(tmp_png), "--out", str(tmp_png),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if result.returncode != 0:
                print("sips resize failed", file=sys.stderr)
                sys.exit(1)

        # Save to file
        saved_msg = ""
        if outfile is not None:
            out = Path(outfile).expanduser()
            if out.suffix != ".png":
                out = out.with_suffix(out.suffix + ".png")
            try:
                out.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                print(f"Failed to create directory: {out.parent}", file=sys.stderr)
                sys.exit(1)
            try:
                shutil.copy2(tmp_png, out)
            except OSError:
                print(f"Failed to save file: {out}", file=sys.stderr)
                sys.exit(1)
            saved_msg = f"Saved to {out}"

        # Copy to clipboard
        clip_msg = ""
        if do_clipboard:
            script = (
                'set the clipboard to (read file POSIX file "'
                + str(tmp_png)
                + '" as «class PNGf»)'
            )
            result = subprocess.run(
                ["osascript", "-e", script],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if result.returncode != 0:
                print("Failed to copy to clipboard", file=sys.stderr)
                sys.exit(1)
            clip_msg = "Copied to clipboard"

    finally:
        tmp_png.unlink(missing_ok=True)

    # Status message
    if clip_msg and saved_msg:
        msg = f"{clip_msg} and {saved_msg}"
    elif saved_msg:
        msg = saved_msg
    else:
        msg = clip_msg
    if args.lowres:
        msg += f" ({args.height}px tall)"
    print(msg)


if __name__ == "__main__":
    main()
