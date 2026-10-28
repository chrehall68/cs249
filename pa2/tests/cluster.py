"""Helpers shared by the tests.

Run every test from the pa2 directory, e.g. `python3 tests/test_replication.py`.
Each test starts its own servers on the ports in servers.json, so stop run.sh first.
Server logs are written to logs/tests/<case>-<id>.log.
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

SERVER_BINARY = "target/debug/server"
LOG_DIR = "logs/tests"

with open("servers.json") as f:
    HTTP_PORTS = {server["id"]: server["http_port"] for server in json.load(f)["servers"]}


# ==============================
# HTTP requests
# ==============================
def request(server, method, path, body=None, user=None, timeout=20):
    """Sends a request to server "A", "B" or "C".

    Returns (status, json body), or (None, None) if the server didn't answer in time
    or dropped the connection.
    """
    headers = {}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if user is not None:
        headers["X-User"] = user
    url = f"http://127.0.0.1:{HTTP_PORTS[server]}{path}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        # 4xx and 5xx responses still have a json body
        return error.code, json.load(error)
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return None, None


def get(server, path, timeout=20):
    return request(server, "GET", path, timeout=timeout)


def post(server, path, body=None, user=None, timeout=20):
    return request(server, "POST", path, body=body, user=user, timeout=timeout)


def wait_until(condition, timeout=15):
    """Checks condition() every half second.

    Returns how many seconds it took to become true, or None if it never did.
    """
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return round(time.time() - start, 1)
        time.sleep(0.5)
    return None


# ==============================
# Starting and stopping servers
# ==============================
class Cluster:
    """Starts servers A, B and C; use it in a `with` block so they always get stopped."""

    def __init__(self, name, config="servers.json"):
        self.name = name
        for server in HTTP_PORTS:
            if get(server, "/health", timeout=1)[0] is not None:
                sys.exit(f"server {server} is already running; stop it first so the ports are free")

        subprocess.run(["cargo", "build", "--quiet"], check=True)
        os.makedirs(LOG_DIR, exist_ok=True)
        self.processes = {}
        for server in ["A", "B", "C"]:
            log = open(self.log_path(server), "w")
            self.processes[server] = subprocess.Popen(
                [SERVER_BINARY, "--id", server, "--config", config],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            # small gap so each server's peers are already listening when it connects
            time.sleep(0.5)
        time.sleep(1.5)

    def log_path(self, server):
        return f"{LOG_DIR}/{self.name}-{server}.log"

    def read_log(self, server):
        with open(self.log_path(server)) as f:
            return f.read()

    def kill(self, server):
        """Terminates the server; its sockets close."""
        self.processes[server].send_signal(signal.SIGKILL)

    def freeze(self, server):
        """Pauses the server; its sockets stay open but it never replies."""
        self.processes[server].send_signal(signal.SIGSTOP)

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        # SIGKILL also stops frozen processes
        for process in self.processes.values():
            process.kill()
            process.wait()


# ==============================
# Running test cases
# ==============================
def run_cases(cases):
    """Runs each test case function; a case fails if it raises an AssertionError."""
    failures = 0
    for case in cases:
        print(f"=== {case.__name__} ===")
        try:
            case()
            print("PASS")
        except AssertionError as error:
            print(f"FAIL: {error}")
            failures += 1
        print()
    sys.exit(1 if failures else 0)
