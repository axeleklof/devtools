"""Wireless ADB connection setup with device selection and reverse port forwarding."""

from __future__ import annotations

import argparse
import ipaddress
import re
import shutil
import subprocess
import sys
import time


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="adbw",
        description="Set up wireless ADB debugging with device selection.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  adbw                          # basic wireless setup\n"
            "  adbw -p 5556                  # custom port\n"
            "  adbw -r 3000,8080             # with reverse port forwarding\n"
            "  adbw --ip 192.168.1.42        # reconnect without USB"
        ),
    )
    parser.add_argument(
        "-p", "--port",
        type=int,
        default=5555,
        metavar="PORT",
        help="ADB port (default: 5555).",
    )
    parser.add_argument(
        "-r", "--reverse",
        metavar="PORTS",
        help="Comma-separated ports for reverse forwarding (e.g. 3000,8080 or 4000).",
    )
    parser.add_argument(
        "--ip",
        metavar="IP",
        help="Device IP for direct reconnection (skips USB discovery).",
    )
    return parser.parse_args(argv)


def _run_adb(*args: str, serial: str | None = None, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    cmd = ["adb"]
    if serial:
        cmd += ["-s", serial]
    cmd += list(args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _get_device_name(serial: str) -> str:
    model = "Unknown"
    brand = ""
    try:
        r = _run_adb("shell", "getprop", "ro.product.model", serial=serial, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            model = r.stdout.strip()
    except subprocess.TimeoutExpired:
        pass
    try:
        r = _run_adb("shell", "getprop", "ro.product.brand", serial=serial, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            brand = r.stdout.strip()
    except subprocess.TimeoutExpired:
        pass
    return f"{brand} {model}" if brand else model


def _parse_devices() -> tuple[list[str], set[str]]:
    """Parse adb devices output. Returns (usb_serials, wireless_targets)."""
    result = _run_adb("devices")
    if result.returncode != 0:
        print("Error: Failed to list adb devices.", file=sys.stderr)
        sys.exit(1)
    usb: list[str] = []
    wireless: set[str] = set()
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == "device":
            if ":" in parts[0]:
                wireless.add(parts[0])
            else:
                usb.append(parts[0])
    return usb, wireless


def _select_device(devices: list[str]) -> tuple[str, str]:
    """Select a device and return (serial, display_name)."""
    names: dict[str, str] = {}
    for d in devices:
        names[d] = _get_device_name(d)

    if len(devices) == 1:
        serial = devices[0]
        return serial, names[serial]

    print("Multiple devices found:\n")
    for i, d in enumerate(devices, 1):
        print(f"  {i}. {names[d]} ({d})")
    print()

    while True:
        try:
            choice = input(f"Select device (1-{len(devices)}): ")
        except (EOFError, KeyboardInterrupt):
            print(file=sys.stderr)
            sys.exit(1)
        if choice.isdigit() and 1 <= int(choice) <= len(devices):
            serial = devices[int(choice) - 1]
            return serial, names[serial]
        print(f"Invalid selection. Enter a number between 1 and {len(devices)}.")


def _get_device_ip(serial: str) -> str:
    """Get the wireless IP address of the device."""
    # Try ip addr show on common interfaces
    for iface in ("wlan0", "wlan1", "wifi0"):
        try:
            r = _run_adb("shell", "ip", "addr", "show", iface, serial=serial, timeout=5)
            if r.returncode == 0:
                for line in r.stdout.splitlines():
                    m = re.search(r"inet (\d+\.\d+\.\d+\.\d+)/", line)
                    if m:
                        ip = m.group(1)
                        try:
                            ipaddress.IPv4Address(ip)
                            return ip
                        except ValueError:
                            continue
        except subprocess.TimeoutExpired:
            continue

    # Fallback: ip route, look for 'src' keyword
    try:
        r = _run_adb("shell", "ip", "route", serial=serial, timeout=5)
        if r.returncode == 0:
            for line in r.stdout.splitlines():
                tokens = line.split()
                for i, tok in enumerate(tokens):
                    if tok == "src" and i + 1 < len(tokens):
                        ip = tokens[i + 1]
                        try:
                            ipaddress.IPv4Address(ip)
                            return ip
                        except ValueError:
                            continue
    except subprocess.TimeoutExpired:
        pass

    print("Error: Could not determine device IP address.", file=sys.stderr)
    sys.exit(1)


def _connect(ip: str, port: int) -> None:
    """Connect to device wirelessly with retries."""
    target = f"{ip}:{port}"
    print(f"Connecting to {target}...")

    deadline = time.monotonic() + 5.0
    last_output = ""
    while True:
        try:
            r = _run_adb("connect", target, timeout=3)
            last_output = (r.stdout + r.stderr).strip()
            if "connected to" in last_output or "already connected" in last_output:
                return
        except subprocess.TimeoutExpired:
            last_output = "connection timed out"

        if time.monotonic() >= deadline:
            break
        time.sleep(0.5)

    print(f"Error: Failed to connect to {target}: {last_output}", file=sys.stderr)
    sys.exit(1)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Validate port
    if not 1 <= args.port <= 65535:
        print(f"Error: Port must be between 1 and 65535, got {args.port}.", file=sys.stderr)
        sys.exit(1)

    # Parse reverse ports — each entry is "port" (same on both sides)
    # or "device:host" for different ports. Single port like -r 4000 is fine.
    reverse_ports: list[tuple[int, int]] = []
    if args.reverse:
        for tok in args.reverse.split(","):
            tok = tok.strip()
            if ":" in tok:
                parts = tok.split(":", 1)
                if not all(p.isdigit() for p in parts):
                    print(f"Error: Invalid reverse port '{tok}'. Must be port or device:host.", file=sys.stderr)
                    sys.exit(1)
                device_port, host_port = int(parts[0]), int(parts[1])
            else:
                if not tok.isdigit():
                    print(f"Error: Invalid reverse port '{tok}'. Must be 1-65535.", file=sys.stderr)
                    sys.exit(1)
                device_port = host_port = int(tok)
            for p in (device_port, host_port):
                if not 1 <= p <= 65535:
                    print(f"Error: Port {p} out of range. Must be 1-65535.", file=sys.stderr)
                    sys.exit(1)
            reverse_ports.append((device_port, host_port))

    # Check adb available
    if not shutil.which("adb"):
        print("Error: adb not found. Install Android SDK platform-tools.", file=sys.stderr)
        sys.exit(1)

    # Validate --ip if given
    if args.ip:
        try:
            ipaddress.IPv4Address(args.ip)
        except ValueError:
            print(f"Error: Invalid IP address '{args.ip}'.", file=sys.stderr)
            sys.exit(1)

    device_name = ""
    already_connected = False

    if args.ip:
        ip = args.ip
        target = f"{ip}:{args.port}"
    else:
        usb_devices, wireless_targets = _parse_devices()

        if usb_devices:
            serial, device_name = _select_device(usb_devices)

            # Get IP before switching to tcpip mode
            ip = _get_device_ip(serial)
            target = f"{ip}:{args.port}"

            # Check if this device is already connected wirelessly
            if target in wireless_targets:
                already_connected = True
        elif wireless_targets:
            # No USB but already connected wirelessly — pick the wireless target
            targets = sorted(wireless_targets)
            if len(targets) == 1:
                target = targets[0]
                device_name = _get_device_name(target)
            else:
                names: dict[str, str] = {}
                for t in targets:
                    names[t] = _get_device_name(t)
                print("Multiple wireless devices found:\n")
                for i, t in enumerate(targets, 1):
                    print(f"  {i}. {names[t]} ({t})")
                print()
                while True:
                    try:
                        choice = input(f"Select device (1-{len(targets)}): ")
                    except (EOFError, KeyboardInterrupt):
                        print(file=sys.stderr)
                        sys.exit(1)
                    if choice.isdigit() and 1 <= int(choice) <= len(targets):
                        target = targets[int(choice) - 1]
                        device_name = names[target]
                        break
                    print(f"Invalid selection. Enter a number between 1 and {len(targets)}.")
            already_connected = True
        else:
            print("Error: No devices found. Connect a device and enable USB debugging.", file=sys.stderr)
            sys.exit(1)

    if already_connected:
        # Only print status when running bare (no action flags)
        if not reverse_ports:
            if device_name:
                print(f"Already connected to {device_name} at {target}")
            else:
                print(f"Already connected at {target}")
    else:
        if device_name:
            print(f"Setting up wireless ADB on {device_name} port {args.port}")

        # Enable TCP/IP mode (only when coming from USB)
        if not args.ip:
            r = _run_adb("tcpip", str(args.port), serial=serial)
            output = (r.stdout + r.stderr).strip()
            if r.returncode != 0 or "error" in output.lower():
                print(f"Error: Failed to enable TCP/IP mode: {output}", file=sys.stderr)
                sys.exit(1)

        # Connect wirelessly
        _connect(ip, args.port)

        if device_name:
            print(f"Connected to {device_name} at {target}")
        else:
            print(f"Connected to {target}")

    # Reverse port forwarding
    if reverse_ports:
        port_strs = [str(dp) if dp == hp else f"{dp}:{hp}" for dp, hp in reverse_ports]
        print(f"Setting up reverse forwarding on ports {', '.join(port_strs)}")
        for device_port, host_port in reverse_ports:
            r = _run_adb("reverse", f"tcp:{device_port}", f"tcp:{host_port}", serial=target)
            if r.returncode != 0:
                print(f"Warning: Failed to reverse port {device_port}.", file=sys.stderr)

    if not already_connected and not args.ip:
        print("You can now disconnect the USB cable.")


if __name__ == "__main__":
    main()
