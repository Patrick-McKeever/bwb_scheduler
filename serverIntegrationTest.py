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
import threading
import time
import urllib.request
import urllib.error

serverAddr = "http://localhost:5444"
pollIntervalSecs = 5
terminalStatuses = {"FINISHED", "FAILED", "CANCELED", "TERMINATED", "TIMED_OUT"}
workflowReqPath = "test_workflows/salmon_v1_req.json"


class ManagedProcess:
    # Wraps a Popen and continuously drains its output into a buffer so it
    # can be printed later without blocking the process or losing history.
    def __init__(self, name: str, cmd: list[str]):
        self.name = name
        self.cmd = cmd
        self.outputLines: list[str] = []
        self.lock = threading.Lock()
        print(f"[+] Starting: {' '.join(cmd)}")
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.readerThread = threading.Thread(target=self._readOutput, daemon=True)
        self.readerThread.start()

    def _readOutput(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            with self.lock:
                self.outputLines.append(line.rstrip("\n"))

    def dumpOutput(self) -> None:
        with self.lock:
            lines = list(self.outputLines)
        print(f"[+] Output from '{self.name}' ({' '.join(self.cmd)}):")
        if not lines:
            print("    (no output captured)")
        for line in lines:
            print(f"    {line}")

    def terminate(self) -> None:
        self.proc.terminate()

    def wait(self, timeout: float) -> None:
        self.proc.wait(timeout=timeout)

    def kill(self) -> None:
        self.proc.kill()


def waitForServer(timeout_secs: int = 30) -> None:
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{serverAddr}/workflow_status", timeout=1)
        # Any HTTP response (even 4xx) means the server is up.
        except urllib.error.HTTPError:
            return
        except (urllib.error.URLError, ConnectionRefusedError):
            time.sleep(0.5)
    print("[!] Timed out waiting for server to become available.", file=sys.stderr)
    sys.exit(1)


def postJson(path: str, payload: dict) -> dict:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{serverAddr}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def shutdown(procs: list[ManagedProcess], dumpOutputOnFailure: bool = False) -> None:
    print("[+] Shutting down background processes...")
    if dumpOutputOnFailure:
        for p in procs:
            p.dumpOutput()
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def main() -> None:
    procs: list[ManagedProcess] = []

    def onSignal(signum, frame):
        shutdown(procs)
        sys.exit(1)

    signal.signal(signal.SIGINT, onSignal)
    signal.signal(signal.SIGTERM, onSignal)

    procs.append(ManagedProcess("workers", [
        "go", "run", "main.go", "workers",
        "--workerName", "worker-queue",
        "--ram", "8200MB",
        "--cpus", "4",
        "--gpus", "0",
    ]))

    procs.append(ManagedProcess("server", ["go", "run", "main.go", "serve", "--addr", ":5444"]))

    print(f"[+] Waiting for server at {serverAddr}...")
    waitForServer(timeout_secs=60)
    print("[+] Server is up.")

    if not os.path.exists(workflowReqPath):
        print(f"[!] Request file not found: {workflowReqPath}", file=sys.stderr)
        shutdown(procs, dumpOutputOnFailure=True)
        sys.exit(1)

    with open(workflowReqPath) as f:
        workflowReq = json.load(f)

    print(f"[+] Submitting workflow from {workflowReqPath}...")
    try:
        startResp = postJson("/start_workflow", workflowReq)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"[!] /start_workflow returned {e.code}: {body}", file=sys.stderr)
        shutdown(procs, dumpOutputOnFailure=True)
        sys.exit(1)

    workflowId = startResp["workflow_id"]
    runId = startResp.get("run_id", "")
    print(f"[+] Workflow started — id={workflowId}  run_id={runId}")

    statusReq = {"workflow_id": workflowId, "run_id": runId}
    finalStatus = "Unknown"
    while True:
        time.sleep(pollIntervalSecs)
        try:
            statusResp = postJson("/workflow_status", statusReq)
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"[!] /workflow_status returned {e.code}: {body}", file=sys.stderr)
            shutdown(procs, dumpOutputOnFailure=True)
            sys.exit(1)

        finalStatus = statusResp.get("workflow_status", "Unknown")
        nodeStatuses = statusResp.get("node_statuses", {})
        print(f"[~] status={finalStatus}  nodes={nodeStatuses}")

        if finalStatus in terminalStatuses:
            print(f"[+] Workflow reached terminal status: {finalStatus}")
            break

    pipelineFailed = finalStatus != "FINISHED"
    shutdown(procs, dumpOutputOnFailure=pipelineFailed)
    sys.exit(0 if not pipelineFailed else 1)


if __name__ == "__main__":
    if not "BWB_SCHED_DIR" in os.environ:
        print("Please set env var BWB_SCHED_DIR before running")
        sys.exit(1)
    main()
