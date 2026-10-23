#!/usr/bin/env python3
"""Start/stop only this experiment's Spring process, with PID/command verification."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / "runtime"
STATE = RUNTIME / "app-process.json"
PORT = 28080


def owned_process(strict=True):
    if not STATE.exists():
        return None
    state = json.loads(STATE.read_text())
    if Path(state["jar"]).resolve().parent != (ROOT / "spring/build/libs").resolve():
        if strict:
            raise RuntimeError("Recorded JAR belongs to another checkout; refusing to manage its process")
        return None
    query = subprocess.run(["ps", "-p", str(state["pid"]), "-o", "stat=", "-o", "command="], capture_output=True, text=True)
    if query.returncode:
        return None
    fields = query.stdout.strip().split(None, 1)
    # A killed JVM may remain briefly as a zombie before its parent reaps it.
    # Its command becomes <defunct>; it is already stopped and must not be signalled.
    if not fields or fields[0].startswith("Z"):
        return None
    process_command = fields[1] if len(fields) > 1 else ""
    if state["jar"] not in process_command or "java" not in process_command:
        if strict:
            raise RuntimeError("Recorded PID belongs to a different command; refusing to signal it")
        # After signalling a verified process, a changed command means the original
        # JVM has exited or is exiting. Never send another signal to this PID.
        return None
    return state


def stop(crash=False):
    state = owned_process()
    if state:
        os.kill(state["pid"], signal.SIGKILL if crash else signal.SIGTERM)
        deadline = time.monotonic() + 40
        while owned_process(strict=False) and time.monotonic() < deadline:
            time.sleep(.2)
        if owned_process(strict=False):
            raise RuntimeError("App did not exit; inspect runtime/app.log")
    STATE.unlink(missing_ok=True)
    print(json.dumps({"action": "crash" if crash else "stop", "pid": None if state is None else state["pid"]}))


def start(build=False):
    if owned_process():
        print(json.dumps({"action": "already-running", **owned_process()}))
        return
    # A healthy server on this port could belong to another checkout. Refuse
    # before launching a JVM whose readiness could otherwise be misidentified.
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", PORT))
        except OSError as error:
            raise RuntimeError(f"Port {PORT} is occupied; no JVM was started") from error
    RUNTIME.mkdir(parents=True, exist_ok=True)
    if build:
        with (RUNTIME / "build.log").open("w") as log:
            code = subprocess.call(["./gradlew", "test", "bootJar", "--no-daemon"], cwd=ROOT / "spring", stdout=log, stderr=subprocess.STDOUT)
        if code:
            raise RuntimeError("Build failed; inspect runtime/build.log")
    jars = [p for p in (ROOT / "spring/build/libs").glob("*.jar") if not p.name.endswith("-plain.jar")]
    if len(jars) != 1:
        raise RuntimeError("Expected one bootJar; run app.py start --build")
    with (RUNTIME / "app.log").open("a") as log:
        proc = subprocess.Popen(["java", "-Xms128m", "-Xmx512m", "-jar", str(jars[0])], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    state = {"pid": proc.pid, "jar": str(jars[0]), "startedAt": time.time()}
    STATE.write_text(json.dumps(state) + "\n")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"App exited {proc.returncode}; inspect runtime/app.log")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/lab/health", timeout=2) as response:
                if response.status == 200 and json.load(response).get("status") == "UP":
                    print(json.dumps({"action": "start", **state}))
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(.3)
    raise RuntimeError("App did not become healthy in60s; inspect runtime/app.log")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["start", "stop", "restart", "crash", "status"])
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()
    if args.action in ("stop", "restart", "crash"):
        stop(args.action == "crash")
    if args.action in ("start", "restart"):
        start(args.build)
    if args.action == "status":
        print(json.dumps(owned_process()))


if __name__ == "__main__":
    main()
