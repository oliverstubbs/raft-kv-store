"""handle_append_entries: term handling, the consistency check, truncation and commit_index.

These are the invariants the faster-backtracking change is about to touch, so the
entries-and-indices cases are the point of the file. Nothing here goes over the
network - the handler is called directly.
"""

import time

from models import NodeState
from helpers import append_entries, entries, indices_of, terms_of


# term handling


async def test_rejects_a_stale_term(node):
    await node.storage.set_term_and_vote(5, None)

    res = await node.handle_append_entries(append_entries(term=4, leader_id="2"))

    assert res.success is False
    assert res.term == 5
    # not a valid leader, so nothing about it is recorded
    assert node.leader_id is None
    assert node.heard_from_leader() is False


async def test_adopts_a_higher_term_and_steps_down(node):
    await node.storage.set_term_and_vote(1, "1")
    node.state = NodeState.LEADER

    res = await node.handle_append_entries(append_entries(term=3, leader_id="2"))

    assert res.success is True
    assert node.storage.get_current_term() == 3
    assert node.storage.get_voted_for() is None     # new term, no vote cast in it yet
    assert node.state == NodeState.FOLLOWER
    assert node.leader_id == "2"


async def test_candidate_steps_down_on_an_equal_term(node):
    await node.storage.set_term_and_vote(2, "1")    # voted for itself
    node.state = NodeState.CANDIDATE

    res = await node.handle_append_entries(append_entries(term=2, leader_id="2"))

    assert res.success is True
    assert node.state == NodeState.FOLLOWER
    assert node.leader_id == "2"
    # the term did not change, so the vote in it still stands
    assert node.storage.get_current_term() == 2
    assert node.storage.get_voted_for() == "1"


async def test_a_valid_append_resets_the_election_timer(node):
    assert node.election_time_limit == 0

    await node.handle_append_entries(append_entries(term=1, leader_id="2"))

    assert node.election_time_limit > time.monotonic()
    assert node.heard_from_leader() is True


# consistency check


async def test_rejects_when_the_log_is_too_short(node):
    res = await node.handle_append_entries(
        append_entries(prev_log_index=3, prev_log_term=1, entries=entries(4, [1]))
    )

    assert res.success is False
    assert node.get_log() == []


async def test_rejects_on_a_term_mismatch_and_appends_nothing(node):
    await node.storage.append_log(entries(1, [1, 1]))

    res = await node.handle_append_entries(
        append_entries(term=2, prev_log_index=2, prev_log_term=2, entries=entries(3, [2]))
    )

    assert res.success is False
    assert terms_of(node) == [1, 1]


async def test_a_rejected_append_does_not_advance_commit_index(node):
    res = await node.handle_append_entries(
        append_entries(prev_log_index=9, prev_log_term=1, leader_commit=9)
    )

    assert res.success is False
    assert node.commit_index == 0


async def test_appends_onto_an_empty_log(node):
    new = entries(1, [1, 1])

    res = await node.handle_append_entries(
        append_entries(prev_log_index=0, prev_log_term=0, entries=new)
    )

    assert res.success is True
    assert node.get_log() == new


# truncation - only on a real conflict, never on overlap


async def test_identical_resend_changes_nothing(node):
    existing = entries(1, [1, 1, 1])
    await node.storage.append_log(existing)

    # the leader resends from next_index every round, so overlap is normal
    res = await node.handle_append_entries(
        append_entries(prev_log_index=1, prev_log_term=1, entries=existing[1:])
    )

    assert res.success is True
    assert node.get_log() == existing


async def test_overlapping_resend_appends_only_the_new_entry(node):
    await node.storage.append_log(entries(1, [1, 1, 1]))

    res = await node.handle_append_entries(
        append_entries(prev_log_index=1, prev_log_term=1, entries=entries(2, [1, 1, 1]))
    )

    assert res.success is True
    assert terms_of(node) == [1, 1, 1, 1]
    assert indices_of(node) == [1, 2, 3, 4]         # the log must stay dense


async def test_a_delayed_duplicate_does_not_chop_later_entries(node):
    await node.storage.append_log(entries(1, [1, 1, 1, 1]))

    # a round that overtook its successor: it only knows about indices 2 and 3,
    # but index 4 is already here and may already be committed
    res = await node.handle_append_entries(
        append_entries(prev_log_index=1, prev_log_term=1, entries=entries(2, [1, 1]))
    )

    assert res.success is True
    assert indices_of(node) == [1, 2, 3, 4]


async def test_a_real_conflict_truncates_the_suffix(node):
    await node.storage.append_log(entries(1, [1, 1, 1]))

    # a new leader in term 2 overwrites indices 2 and 3
    res = await node.handle_append_entries(
        append_entries(term=2, prev_log_index=1, prev_log_term=1, entries=entries(2, [2, 2]))
    )

    assert res.success is True
    assert terms_of(node) == [1, 2, 2]              # index 1 matched, so it survives
    assert indices_of(node) == [1, 2, 3]


async def test_truncation_survives_a_reload_from_disk(node):
    await node.storage.append_log(entries(1, [1, 1, 1]))
    await node.handle_append_entries(
        append_entries(term=2, prev_log_index=1, prev_log_term=1, entries=entries(2, [2]))
    )

    # the in-memory log and the log table must agree
    node.storage.load_log()

    assert terms_of(node) == [1, 2]
    assert indices_of(node) == [1, 2]


# commit_index


async def test_commit_index_is_capped_at_the_entries_received(node):
    res = await node.handle_append_entries(
        append_entries(prev_log_index=0, prev_log_term=0, entries=entries(1, [1, 1, 1]), leader_commit=5)
    )

    assert res.success is True
    assert node.commit_index == 3       # never past what this node actually holds


async def test_heartbeat_advances_commit_index(node):
    await node.storage.append_log(entries(1, [1, 1, 1]))

    await node.handle_append_entries(
        append_entries(prev_log_index=3, prev_log_term=1, leader_commit=2)
    )

    assert node.commit_index == 2


async def test_commit_index_is_never_lowered(node):
    await node.storage.append_log(entries(1, [1, 1, 1]))
    heartbeat = append_entries(prev_log_index=3, prev_log_term=1, leader_commit=3)
    await node.handle_append_entries(heartbeat)
    assert node.commit_index == 3

    # a stale heartbeat carrying an older leader_commit must not walk it back
    await node.handle_append_entries(
        append_entries(prev_log_index=3, prev_log_term=1, leader_commit=1)
    )

    assert node.commit_index == 3
