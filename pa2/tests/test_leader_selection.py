"""Checks that the surviving servers agree on the right leader after a failure.

Servers may disagree while failure detection is in progress, so each survivor's
GET /leader is polled until it reports the expected leader (or 15 seconds pass).

Run from the pa2 directory: python3 tests/test_leader_selection.py
"""

from cluster import Cluster, get, run_cases, wait_until


def leader_of(server):
    status, body = get(server, "/leader", timeout=2)
    if status != 200:
        return None
    return body["leader"], body["alive"]


def check_initial_leader():
    for server in ["A", "B", "C"]:
        assert leader_of(server) == ("C", True), f"initial leader on {server} is {leader_of(server)}, not C"


def check_failover(survivors, expected_leader):
    seconds = wait_until(lambda: all(leader_of(s) == (expected_leader, True) for s in survivors))
    assert seconds is not None, (
        f"survivors did not agree on {expected_leader}: "
        + ", ".join(f"{s} reports {leader_of(s)}" for s in survivors)
    )
    print(f"{' and '.join(survivors)} agreed on leader {expected_leader} after {seconds}s")

    for server in survivors:
        assert get(server, "/health", timeout=2)[0] == 200, f"{server} stopped answering /health"


def check_b_announced(cluster):
    announcements = [
        line
        for line in cluster.read_log("B").splitlines()
        if "Sending message to A" in line
        and '"type":"LEADER_ANNOUNCEMENT"' in line
        and '"leader":"B"' in line
    ]
    assert announcements, "B's log shows no LEADER_ANNOUNCEMENT for B sent to A"


def kill_leader():
    """C is killed; A and B must both report B, and B must announce itself to A."""
    with Cluster("kill_leader") as cluster:
        check_initial_leader()
        cluster.kill("C")
        check_failover(survivors=["A", "B"], expected_leader="B")
        check_b_announced(cluster)


def freeze_leader():
    """Like kill_leader, but C is frozen, so only heartbeats can detect it."""
    with Cluster("freeze_leader") as cluster:
        check_initial_leader()
        cluster.freeze("C")
        check_failover(survivors=["A", "B"], expected_leader="B")
        check_b_announced(cluster)


def kill_non_leader():
    """A is killed; the leader must stay C."""
    with Cluster("kill_non_leader") as cluster:
        check_initial_leader()
        cluster.kill("A")
        check_failover(survivors=["B", "C"], expected_leader="C")


run_cases([kill_leader, freeze_leader, kill_non_leader])
