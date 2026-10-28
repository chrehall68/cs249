"""Checks that backups store the same messages, under the same ids, as their primaries.

For each channel, 40 messages are sent at the same time, spread across all three
servers, so most get forwarded and replications can arrive out of order. Then each
server's local copy is read through /debug/... (which never forwards).

Run from the pa2 directory: python3 tests/test_replication.py
"""

from concurrent.futures import ThreadPoolExecutor

from cluster import Cluster, get, post, run_cases

COUNT = 40


def local_messages(server, channel):
    """Returns {message_id: (username, text)} as stored on that server."""
    status, body = get(server, f"/debug/channels/{channel}/messages?limit=1000")
    assert status == 200, f"reading {channel} on {server} returned {status}: {body}"
    return {m["message_id"]: (m["username"], m["text"]) for m in body["messages"]}


def check_channel(channel, primary, backup):
    post("A", f"/channels/{channel}/members", user="alice")

    def send(i):
        server = ["A", "B", "C"][i % 3]
        return post(server, f"/channels/{channel}/messages", {"text": f"m{i}"}, user="alice")

    with ThreadPoolExecutor(COUNT) as pool:
        responses = list(pool.map(send, range(COUNT)))

    # what the clients were told
    sent = {}
    for status, body in responses:
        assert status == 201, f"a send returned {status}: {body}"
        assert body["message_id"] not in sent, f"{body['message_id']} was returned twice"
        sent[body["message_id"]] = (body["username"], body["text"])

    on_primary = local_messages(primary, channel)
    on_backup = local_messages(backup, channel)
    print(f"{channel}: clients got {len(sent)} ids, primary {primary} stored {len(on_primary)}, "
          f"backup {backup} stored {len(on_backup)}")
    assert on_primary == sent, f"primary {primary} stored different messages than the clients were told"
    for message_id in sorted(on_primary):
        assert on_backup.get(message_id) == on_primary[message_id], (
            f"{message_id}: primary has {on_primary[message_id]}, backup has {on_backup.get(message_id)}"
        )
    assert len(on_backup) == len(on_primary), "backup has extra messages"


def all_channels_replicate():
    with Cluster("replication"):
        post("A", "/users", {"username": "alice"})
        # %23 is the url encoding of #
        check_channel("%23cs249", primary="A", backup="B")
        check_channel("%23distributed", primary="B", backup="C")
        check_channel("%23systems", primary="C", backup="A")


run_cases([all_channels_replicate])
