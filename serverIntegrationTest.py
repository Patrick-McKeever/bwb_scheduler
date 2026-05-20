#!/usr/bin/env python3
"""
Starts the workers and HTTP server as background processes, submits a workflow
from test_workflows/salmon_v1_req.json, and polls /workflow_status until done.
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.error

SERVER_ADDR = "http://localhost:8080"
POLL_INTERVAL_SECS = 5
TERMINAL_STATUSES = {"FINISHED", "FAILED", "CANCELED", "TERMINATED", "TIMED_OUT"}
WORKFLOW_REQ_PATH = "test_workflows/salmon_v1_req.json"


def start_background(cmd: list[str]) -> subprocess.Popen:
    print(f"[+] Starting: {' '.join(cmd)}")
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_for_server(timeout_secs: int = 30) -> None:
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{SERVER_ADDR}/workflow_status", timeout=1)
        except urllib.error.HTTPError:
            # Any HTTP response (even 4xx) means the server is up.
            return
        except (urllib.error.URLError, ConnectionRefusedError):
            time.sleep(0.5)
    print("[!] Timed out waiting for server to become available.", file=sys.stderr)
    sys.exit(1)


def post_json(path: str, payload: dict) -> dict:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{SERVER_ADDR}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def shutdown(procs: list[subprocess.Popen]) -> None:
    print("[+] Shutting down background processes...")
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def main() -> None:
    procs: list[subprocess.Popen] = []

    def on_signal(signum, frame):
        shutdown(procs)
        sys.exit(1)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    # Start workers
    procs.append(start_background([
        "go", "run", "main.go", "workers",
        "--workerName", "worker-queue",
        "--ram", "8200MB",
        "--cpus", "4",
        "--gpus", "0",
    ]))

    # Start HTTP server
    procs.append(start_background(["go", "run", "main.go", "serve"]))

    print(f"[+] Waiting for server at {SERVER_ADDR}...")
    wait_for_server(timeout_secs=60)
    print("[+] Server is up.")

    # Load and submit workflow request
    if not os.path.exists(WORKFLOW_REQ_PATH):
        print(f"[!] Request file not found: {WORKFLOW_REQ_PATH}", file=sys.stderr)
        shutdown(procs)
        sys.exit(1)

    with open(WORKFLOW_REQ_PATH) as f:
        workflow_req = json.load(f)

    print(f"[+] Submitting workflow from {WORKFLOW_REQ_PATH}...")
    try:
        start_resp = post_json("/start_workflow", workflow_req)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"[!] /start_workflow returned {e.code}: {body}", file=sys.stderr)
        shutdown(procs)
        sys.exit(1)

    workflow_id = start_resp["workflow_id"]
    run_id = start_resp.get("run_id", "")
    print(f"[+] Workflow started — id={workflow_id}  run_id={run_id}")

    # Poll until terminal status
    status_req = {"workflow_id": workflow_id, "run_id": run_id}
    final_status = "Unknown"
    while True:
        time.sleep(POLL_INTERVAL_SECS)
        try:
            status_resp = post_json("/workflow_status", status_req)
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"[!] /workflow_status returned {e.code}: {body}", file=sys.stderr)
            shutdown(procs)
            sys.exit(1)

        final_status = status_resp.get("workflow_status", "Unknown")
        node_statuses = status_resp.get("node_statuses", {})
        print(f"[~] status={final_status}  nodes={node_statuses}")

        if final_status in TERMINAL_STATUSES:
            print(f"[+] Workflow reached terminal status: {final_status}")
            break

    shutdown(procs)
    sys.exit(0 if final_status == "FINISHED" else 1)


if __name__ == "__main__":
    main()
