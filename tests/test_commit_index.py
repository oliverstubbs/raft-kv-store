"""update_commit_index and has_majority: the majority arithmetic on the leader.

A five-node cluster throughout, so a majority is 3 - this node plus two peers.
"""

import pytest

from models import NodeState
from helpers import entries


def as_leader(node, match_index: dict[str, int]):
    node.state = NodeState.LEADER
    node.match_index = {peer: match_index.get(peer, 0) for peer in node.peers}
    return node


@pytest.mark.parametrize(
    "peer_acks, expected",
    [(0, False), (1, False), (2, True), (3, True), (4, True)],
)
def test_has_majority_counts_this_node(node, peer_acks, expected):
    assert node.has_majority(peer_acks) is expected


async def test_commits_when_a_majority_holds_the_entry(node):
    await node.storage.set_term_and_vote(1, "1")
    await node.storage.append_log(entries(1, [1, 1, 1]))
    as_leader(node, {"node2:8000": 3, "node3:8000": 3})

    node.update_commit_index()

    assert node.commit_index == 3


async def test_does_not_commit_short_of_a_majority(node):
    await node.storage.set_term_and_vote(1, "1")
    await node.storage.append_log(entries(1, [1, 1, 1]))
    as_leader(node, {"node2:8000": 3})      # leader + one peer is only 2 of 5

    node.update_commit_index()

    assert node.commit_index == 0


async def test_commits_the_highest_index_a_majority_holds(node):
    await node.storage.set_term_and_vote(1, "1")
    await node.storage.append_log(entries(1, [1, 1, 1]))
    as_leader(node, {"node2:8000": 3, "node3:8000": 2, "node4:8000": 1})

    node.update_commit_index()

    assert node.commit_index == 2           # node2 and the leader have 3, which is not a majority


async def test_does_not_commit_an_entry_from_an_earlier_term(node):
    # the 5.4.2 rule: a replicated-but-uncommitted entry from an old term
    # must not be committed by counting replicas
    await node.storage.set_term_and_vote(2, "1")
    await node.storage.append_log(entries(1, [1, 1]))
    as_leader(node, {"node2:8000": 2, "node3:8000": 2, "node4:8000": 2})

    node.update_commit_index()

    assert node.commit_index == 0


async def test_a_current_term_entry_carries_the_prefix_with_it(node):
    # which is what the NOOP on election is for: index 3 is this term's entry,
    # and committing it commits 1 and 2 behind it
    await node.storage.set_term_and_vote(2, "1")
    await node.storage.append_log(entries(1, [1, 1, 2]))
    as_leader(node, {"node2:8000": 3, "node3:8000": 3})

    node.update_commit_index()

    assert node.commit_index == 3


async def test_commit_index_never_goes_backwards(node):
    await node.storage.set_term_and_vote(1, "1")
    await node.storage.append_log(entries(1, [1, 1, 1]))
    as_leader(node, {"node2:8000": 1, "node3:8000": 1})
    node.commit_index = 3

    # a restarted follower reporting a lower match_index must not un-commit anything
    node.update_commit_index()

    assert node.commit_index == 3
