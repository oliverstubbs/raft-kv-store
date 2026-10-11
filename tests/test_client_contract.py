"""What a client sees: 421 from a non-leader, 503 from a write that never applies.

Section 8: a follower rejects the request and names the leader instead of
forwarding it, and the leader answers a write only once the entry is *applied*,
not merely committed. These are the paths client.py is written against.
"""

import asyncio
import time

import pytest
from fastapi import HTTPException

from models import NodeState
from helpers import entry


async def as_leader(node, term: int = 1):
    await node.storage.set_term_and_vote(term, node.node_id)
    node.state = NodeState.LEADER
    node.next_index = {peer: node.get_last_index() + 1 for peer in node.peers}
    node.match_index = {peer: 0 for peer in node.peers}


async def leader_with_an_applied_write(node):
    """A leader holding a committed, applied current-term entry: a = 1."""
    await as_leader(node, term=1)
    await node.storage.append_log([entry(1, 1, "PUT", "a", "1")])
    node.commit_index = 1
    await node.apply_log()


def acked_by_the_cluster(node):
    # timestamps in the future, so they count as acks arriving after the read did
    node.last_ack = {peer: time.monotonic() + 1 for peer in node.peers}


# 421 on a non-leader


async def test_a_follower_refuses_a_write(node):
    node.state = NodeState.FOLLOWER
    node.leader_id = "3"

    with pytest.raises(HTTPException) as raised:
        await node.set_value("a", "1")

    assert raised.value.status_code == 421
    assert raised.value.detail == {"error": "not leader", "leader_id": "3"}
    assert node.get_log() == []         # refused, not written locally


async def test_a_follower_refuses_a_delete(node):
    node.state = NodeState.FOLLOWER
    node.leader_id = "3"

    with pytest.raises(HTTPException) as raised:
        await node.delete_value("a")

    assert raised.value.status_code == 421


async def test_a_follower_refuses_a_read(node):
    # reads go through the leader too, or a deposed node serves stale data
    node.state = NodeState.FOLLOWER
    node.leader_id = "3"

    with pytest.raises(HTTPException) as raised:
        await node.get_value("a")

    assert raised.value.status_code == 421
    assert raised.value.detail["leader_id"] == "3"


async def test_mid_election_the_421_names_no_leader(node):
    node.state = NodeState.CANDIDATE
    node.leader_id = None

    with pytest.raises(HTTPException) as raised:
        await node.set_value("a", "1")

    # client.py has to switch node rather than trust this, and cap its retries
    assert raised.value.detail["leader_id"] is None


# the leader's write path


async def test_a_write_returns_only_once_applied(node):
    await as_leader(node)
    write = asyncio.create_task(node.set_value("a", "1"))

    while node.get_last_index() == 0:
        await asyncio.sleep(0.005)

    # appended, but applying happens on the next pass of run_node
    assert write.done() is False
    assert node.storage.get_value("a") is None

    node.commit_index = node.get_last_index()
    await node.apply_log()

    assert await write is None          # no previous value
    assert node.storage.get_value("a") == "1"


async def test_a_write_returns_the_previous_value(node):
    await leader_with_an_applied_write(node)
    write = asyncio.create_task(node.set_value("a", "2"))

    while node.get_last_index() == 1:
        await asyncio.sleep(0.005)
    node.commit_index = node.get_last_index()
    await node.apply_log()

    assert await write == "1"


async def test_a_write_that_never_applies_returns_503(node):
    await as_leader(node)
    node.apply_timeout = 0.05

    with pytest.raises(HTTPException) as raised:
        await node.set_value("a", "1")

    assert raised.value.status_code == 503
    # the entry is still in the log, and a later leader may yet commit it - a 503
    # does not mean the write did not happen
    assert node.get_last_index() == 1


async def test_a_write_fails_rather_than_hanging_if_the_leader_steps_down(node):
    await as_leader(node)
    node.apply_timeout = 5
    write = asyncio.create_task(node.set_value("a", "1"))

    while node.get_last_index() == 0:
        await asyncio.sleep(0.005)
    node.state = NodeState.FOLLOWER
    node.leader_id = "3"

    with pytest.raises(HTTPException) as raised:
        await write

    assert raised.value.status_code == 421


# the leader's read path


async def test_the_leader_serves_a_read_once_a_majority_is_following(node):
    await leader_with_an_applied_write(node)
    acked_by_the_cluster(node)

    assert await node.get_value("a") == "1"
    assert await node.get_value("missing", "default") == "default"


async def test_a_read_is_refused_without_a_majority_acking(node):
    # a deposed leader that has not noticed yet must not answer
    await leader_with_an_applied_write(node)
    node.election_time_max = 0.05
    node.last_ack = {}

    with pytest.raises(HTTPException) as raised:
        await node.get_value("a")

    assert raised.value.status_code == 421


async def test_a_read_is_refused_if_the_node_is_deposed_while_waiting(node):
    await leader_with_an_applied_write(node)
    node.last_ack = {}
    read = asyncio.create_task(node.get_value("a"))
    await asyncio.sleep(0.02)

    node.state = NodeState.FOLLOWER
    node.leader_id = "3"

    with pytest.raises(HTTPException) as raised:
        await read

    assert raised.value.status_code == 421


async def test_a_read_waits_for_this_term_to_be_committed(node):
    # a fresh leader whose NOOP has not committed yet holds the read instead of
    # answering from a state machine that may be missing entries
    await as_leader(node, term=2)
    node.storage.set_value("a", "stale")
    await node.storage.set_last_applied(0)
    acked_by_the_cluster(node)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(node.get_value("a"), 0.1)
