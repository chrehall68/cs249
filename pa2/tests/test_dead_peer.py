"""Checks that requests involving a dead peer get an answer instead of hanging.

Each case sends a message for #cs249 (primary A, backup B) while one of them is down.
A request counts as hung if there is no response within 20 seconds; heartbeats should
mark the dead server failed after about 6-8 seconds.

Run from the pa2 directory: python3 tests/test_dead_peer.py
"""

import time

from cluster import Cluster, post, run_cases


def setup_alice():
    post("A", "/users", {"username": "alice"})
    post("A", "/channels/%23cs249/members", user="alice")


def send_and_time(server):
    start = time.time()
    status, body = post(server, "/channels/%23cs249/messages", {"text": "hello"}, user="alice")
    print(f"status {status} after {time.time() - start:.1f}s: {body}")
    return status


def freeze_primary():
    """A is frozen (sockets stay open, no replies); C forwards to it, then must fall back."""
    with Cluster("freeze_primary") as cluster:
        setup_alice()
        cluster.freeze("A")
        status = send_and_time("C")
        assert status is not None, "no response; the request hung or the connection was dropped"


def kill_primary():
    """A is killed; C forwards to it, then must fall back."""
    with Cluster("kill_primary") as cluster:
        setup_alice()
        cluster.kill("A")
        status = send_and_time("C")
        assert status is not None, "no response; the request hung or the connection was dropped"


def freeze_backup():
    """B is frozen while A waits for its replication ack; A is healthy, so it must answer 201."""
    with Cluster("freeze_backup") as cluster:
        setup_alice()
        cluster.freeze("B")
        status = send_and_time("A")
        assert status is not None, "no response; the request hung or the connection was dropped"
        assert status == 201, f"expected 201 from the primary while its backup is down, got {status}"


run_cases([freeze_primary, kill_primary, freeze_backup])
