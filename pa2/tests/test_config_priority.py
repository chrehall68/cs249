"""Checks that leader priorities are read from the config file, not hard-coded.

The servers get a config with shuffled priorities (A=2, B=3, C=1) but the usual
ports. Ordering by id in either direction gives a different answer than ordering
by priority, so:
  1. at startup, every server must report leader B (highest priority)
  2. after B is killed, A and C must both report leader A

Run from the pa2 directory: python3 tests/test_config_priority.py
"""

import json
import os

from cluster import LOG_DIR, Cluster, get, run_cases, wait_until

PRIORITIES = {"A": 2, "B": 3, "C": 1}


def write_shuffled_config():
    with open("servers.json") as f:
        config = json.load(f)
    for server in config["servers"]:
        server["priority"] = PRIORITIES[server["id"]]
    os.makedirs(LOG_DIR, exist_ok=True)
    path = f"{LOG_DIR}/shuffled_priorities.json"
    with open(path, "w") as f:
        json.dump(config, f, indent=2)
    return path


def leader_of(server):
    status, body = get(server, "/leader", timeout=2)
    return body["leader"] if status == 200 else None


def leader_follows_config_priorities():
    with Cluster("config_priority", config=write_shuffled_config()) as cluster:
        for server in ["A", "B", "C"]:
            assert leader_of(server) == "B", f"{server} reports {leader_of(server)} at startup, not B"

        cluster.kill("B")
        seconds = wait_until(lambda: leader_of("A") == "A" and leader_of("C") == "A")
        assert seconds is not None, (
            f"after killing B, A reports {leader_of('A')} and C reports {leader_of('C')}, not A"
        )
        print(f"A and C agreed on leader A after {seconds}s")


run_cases([leader_follows_config_priorities])
