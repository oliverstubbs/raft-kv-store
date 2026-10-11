"""Storage: log density, write-through, and the two commit-boundary invariants.

The log must stay dense (position i holds index i + 1) because get_term_at guards
with an index value and then subscripts positionally - a gap makes it return a
different entry's term and the consistency check starts accepting what it should
reject. Snapshots (roadmap step 3) are what break this assumption on purpose.
"""

from helpers import committed_state, entries


def terms_in(storage) -> list[int]:
    return [entry.term for entry in storage.get_log()]


def indices_in(storage) -> list[int]:
    return [entry.index for entry in storage.get_log()]


# log density and write-through


async def test_appends_keep_the_log_dense(storage):
    await storage.append_log(entries(1, [1, 1]))
    await storage.append_entry(entries(3, [2])[0])

    assert indices_in(storage) == [1, 2, 3]
    assert terms_in(storage) == [1, 1, 2]


async def test_truncate_removes_from_the_index_onwards(storage):
    await storage.append_log(entries(1, [1, 1, 1, 1, 1]))

    await storage.truncate_log(3)

    assert indices_in(storage) == [1, 2]


async def test_truncate_then_append_stays_dense(storage):
    await storage.append_log(entries(1, [1, 1, 1, 1, 1]))

    await storage.truncate_log(3)
    await storage.append_log(entries(3, [2, 2]))

    assert indices_in(storage) == [1, 2, 3, 4]
    assert terms_in(storage) == [1, 1, 2, 2]


async def test_the_log_table_matches_the_log_in_memory(storage, reopen):
    # a past bug truncated self.log before its DB write failed, and the
    # divergence spread from there
    await storage.append_log(entries(1, [1, 1, 1]))
    await storage.truncate_log(2)
    await storage.append_log(entries(2, [2]))

    restarted = reopen(storage)

    assert restarted.get_log() == storage.get_log()


async def test_truncating_the_whole_log_empties_it(storage):
    await storage.append_log(entries(1, [1, 1]))

    await storage.truncate_log(1)

    assert storage.get_log() == []
    assert storage.get_last_index() == 0


async def test_get_log_returns_a_copy(storage):
    await storage.append_log(entries(1, [1]))

    storage.get_log().clear()

    assert len(storage.get_log()) == 1


# index and term lookups


def test_an_empty_log_reports_index_and_term_zero(storage):
    assert storage.get_last_index() == 0
    assert storage.get_last_term() == 0


def test_get_term_at_zero_is_the_empty_prefix(storage):
    # index 0 means "before the first entry", which every log agrees on -
    # it is what makes the first consistency check pass
    assert storage.get_term_at(0) == 0


async def test_get_term_at_within_the_log(storage):
    await storage.append_log(entries(1, [1, 2, 2]))

    assert storage.get_term_at(1) == 1
    assert storage.get_term_at(2) == 2
    assert storage.get_term_at(3) == 2


async def test_get_term_at_past_the_end_is_none(storage):
    await storage.append_log(entries(1, [1]))

    # handle_append_entries compares this against prev_log_term, so None has to
    # mean "not a match" rather than raising
    assert storage.get_term_at(2) is None
    assert storage.get_term_at(99) is None


# commit boundaries


async def test_set_last_applied_commits_the_applies_with_it(storage, reopen):
    # set_value deliberately leaves the transaction open so a batch of applies
    # and the new last_applied land in one commit
    storage.set_value("a", "1")
    storage.set_value("b", "2")
    await storage.set_last_applied(2)

    restarted = reopen(storage)

    assert restarted.get_all_values() == {"a": "1", "b": "2"}
    assert restarted.get_last_applied() == 2


async def test_applies_are_not_durable_before_that_commit(storage):
    storage.set_value("a", "1")

    # a crash here must not leave the data ahead of last_applied
    data, last_applied = committed_state(storage)

    assert data == {}
    assert last_applied == 0


async def test_deletes_are_part_of_the_same_batch(storage, reopen):
    storage.set_value("a", "1")
    await storage.set_last_applied(1)

    storage.delete_value("a")
    await storage.set_last_applied(2)

    restarted = reopen(storage)

    assert restarted.get_all_values() == {}
    assert restarted.get_last_applied() == 2


async def test_term_and_vote_are_one_commit(storage, reopen):
    await storage.set_term_and_vote(4, "3")

    restarted = reopen(storage)

    assert (restarted.get_current_term(), restarted.get_voted_for()) == (4, "3")


def test_a_fresh_node_starts_at_term_zero_with_no_vote(storage):
    assert storage.get_current_term() == 0
    assert storage.get_voted_for() is None
    assert storage.get_last_applied() == 0
