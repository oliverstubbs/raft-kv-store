"""handle_vote_request: one vote per term, the log-up-to-date rule (5.4.1) and pre-vote.

The pre-vote cases are the ones to keep: its grant condition is subtle and getting
it wrong cost ~7s of failover time once already.
"""

import time

from models import NodeState, RequestVoteRequest
from helpers import entries


def vote(
    term: int = 1,
    candidate_id: str = "2",
    last_log_index: int = 0,
    last_log_term: int = 0,
    pre_vote: bool = False,
) -> RequestVoteRequest:
    return RequestVoteRequest(
        term=term,
        candidate_id=candidate_id,
        last_log_index=last_log_index,
        last_log_term=last_log_term,
        pre_vote=pre_vote,
    )


# term handling and voting once


async def test_rejects_a_stale_term(node):
    await node.storage.set_term_and_vote(5, None)

    res = await node.handle_vote_request(vote(term=4))

    assert res.vote_granted is False
    assert res.term == 5
    assert node.storage.get_voted_for() is None


async def test_grants_a_vote_and_records_it(node):
    res = await node.handle_vote_request(vote(term=1, candidate_id="2"))

    assert res.vote_granted is True
    assert res.term == 1
    assert node.storage.get_current_term() == 1
    assert node.storage.get_voted_for() == "2"
    # granting means a leader may be forming, so don't campaign immediately
    assert node.election_time_limit > time.monotonic()


async def test_votes_only_once_per_term(node):
    await node.handle_vote_request(vote(term=1, candidate_id="2"))

    res = await node.handle_vote_request(vote(term=1, candidate_id="3"))

    assert res.vote_granted is False
    assert node.storage.get_voted_for() == "2"


async def test_a_repeat_request_from_the_same_candidate_is_granted(node):
    await node.handle_vote_request(vote(term=1, candidate_id="2"))

    # a retried RPC must get the same answer, not a denial
    res = await node.handle_vote_request(vote(term=1, candidate_id="2"))

    assert res.vote_granted is True
    assert node.storage.get_voted_for() == "2"


async def test_a_higher_term_frees_the_vote(node):
    await node.handle_vote_request(vote(term=1, candidate_id="2"))

    res = await node.handle_vote_request(vote(term=2, candidate_id="3"))

    assert res.vote_granted is True
    assert node.storage.get_current_term() == 2
    assert node.storage.get_voted_for() == "3"


async def test_a_denial_does_not_reset_the_election_timer(node):
    await node.storage.set_term_and_vote(5, None)

    await node.handle_vote_request(vote(term=4))

    assert node.election_time_limit == 0


async def test_the_term_and_vote_reach_disk_together(node, reopen):
    await node.handle_vote_request(vote(term=3, candidate_id="2"))

    # a term on disk next to an older term's vote would let this node vote
    # twice in term 3 after a restart
    restarted = reopen(node.storage)

    assert restarted.get_current_term() == 3
    assert restarted.get_voted_for() == "2"


# log up to date (5.4.1)


async def test_denies_a_candidate_with_a_shorter_log(node):
    await node.storage.append_log(entries(1, [1, 1]))

    res = await node.handle_vote_request(vote(term=2, last_log_term=1, last_log_index=1))

    assert res.vote_granted is False


async def test_grants_to_an_equally_long_log(node):
    await node.storage.append_log(entries(1, [1, 1]))

    res = await node.handle_vote_request(vote(term=2, last_log_term=1, last_log_index=2))

    assert res.vote_granted is True


async def test_a_higher_last_term_wins_over_a_longer_log(node):
    await node.storage.append_log(entries(1, [1, 1, 1]))

    # the candidate holds less, but what it holds is from a later term
    res = await node.handle_vote_request(vote(term=3, last_log_term=2, last_log_index=1))

    assert res.vote_granted is True


async def test_a_longer_log_does_not_beat_a_higher_last_term(node):
    await node.storage.append_log(entries(1, [2]))

    res = await node.handle_vote_request(vote(term=3, last_log_term=1, last_log_index=9))

    assert res.vote_granted is False


# pre-vote (thesis 9.6)


async def test_pre_vote_grants_without_changing_any_state(node):
    res = await node.handle_vote_request(vote(term=1, pre_vote=True))

    assert res.vote_granted is True
    # the whole point: answering costs nothing, so a partitioned node asking
    # cannot drag the cluster's term up
    assert node.storage.get_current_term() == 0
    assert node.storage.get_voted_for() is None
    assert node.election_time_limit == 0


async def test_pre_vote_is_denied_while_a_leader_is_live(node):
    node.last_leader_contact = time.monotonic()

    res = await node.handle_vote_request(vote(term=1, pre_vote=True))

    assert res.vote_granted is False


async def test_a_leader_denies_a_pre_vote(node):
    node.state = NodeState.LEADER

    res = await node.handle_vote_request(vote(term=1, pre_vote=True))

    assert res.vote_granted is False


async def test_a_candidate_still_grants_a_pre_vote(node):
    # run_election resets its own election timer before pre-voting, so keying the
    # grant off that timer instead of last_leader_contact made two candidates deny
    # each other - and with one node down the other candidate is the only granter
    node.state = NodeState.CANDIDATE
    node.reset_election_timer()
    assert node.election_timed_out() is False

    res = await node.handle_vote_request(vote(term=1, pre_vote=True))

    assert res.vote_granted is True


async def test_pre_vote_applies_the_log_check(node):
    await node.storage.append_log(entries(1, [1, 1]))

    res = await node.handle_vote_request(vote(term=2, pre_vote=True, last_log_term=1, last_log_index=1))

    assert res.vote_granted is False


async def test_pre_vote_with_a_stale_term_is_rejected(node):
    await node.storage.set_term_and_vote(5, None)

    res = await node.handle_vote_request(vote(term=4, pre_vote=True))

    assert res.vote_granted is False
    assert res.term == 5
