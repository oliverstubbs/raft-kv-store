"""apply_log: committed entries reaching the data table, once, in order.

The storage tests cover the commit boundary at the Storage level; this covers the
caller that depends on it - a batch of applies and the new last_applied have to
land in one commit, or a crash leaves the data disagreeing with last_applied.
"""

from helpers import count_saves, entries, entry


async def apply_up_to(node, commit_index: int):
    node.commit_index = commit_index
    await node.apply_log()


async def test_applies_a_put(node):
    await node.storage.append_log([entry(1, 1, "PUT", "a", "1")])

    await apply_up_to(node, 1)

    assert node.storage.get_all_values() == {"a": "1"}
    assert node.storage.get_last_applied() == 1


async def test_applies_a_delete(node):
    await node.storage.append_log([entry(1, 1, "PUT", "a", "1"), entry(2, 1, "DELETE", "a")])

    await apply_up_to(node, 2)

    assert node.storage.get_all_values() == {}
    assert node.storage.get_last_applied() == 2


async def test_a_noop_advances_last_applied_without_touching_the_data(node):
    # the election no-op: it exists to make the commit rule fire, and must be
    # applied like anything else or last_applied stalls behind commit_index
    await node.storage.append_log([entry(1, 1, "NOOP")])

    await apply_up_to(node, 1)

    assert node.storage.get_all_values() == {}
    assert node.storage.get_last_applied() == 1


async def test_applies_in_log_order(node):
    await node.storage.append_log([
        entry(1, 1, "PUT", "a", "first"),
        entry(2, 1, "PUT", "a", "second"),
        entry(3, 1, "DELETE", "b"),
    ])

    await apply_up_to(node, 3)

    assert node.storage.get_all_values() == {"a": "second"}


async def test_stops_at_commit_index(node):
    await node.storage.append_log(entries(1, [1, 1, 1]))

    await apply_up_to(node, 2)

    # index 3 is in the log but not committed, so it must not be applied
    assert sorted(node.storage.get_all_values()) == ["k1", "k2"]
    assert node.storage.get_last_applied() == 2


async def test_a_batch_is_one_commit(node):
    await node.storage.append_log(entries(1, [1] * 50))
    saves = count_saves(node.storage)

    await apply_up_to(node, 50)

    assert len(node.storage.get_all_values()) == 50
    assert len(saves) == 1       # not one per entry


async def test_does_nothing_when_there_is_nothing_committed(node):
    await node.storage.append_log(entries(1, [1, 1]))
    await apply_up_to(node, 2)
    saves = count_saves(node.storage)

    await node.apply_log()

    assert saves == []
    assert node.storage.get_last_applied() == 2


async def test_resumes_from_last_applied(node):
    await node.storage.append_log(entries(1, [1, 1, 1]))
    await apply_up_to(node, 1)

    await apply_up_to(node, 3)

    assert sorted(node.storage.get_all_values()) == ["k1", "k2", "k3"]
    assert node.storage.get_last_applied() == 3


async def test_the_data_and_last_applied_land_together(node, reopen):
    await node.storage.append_log(entries(1, [1, 1]))

    await apply_up_to(node, 2)
    restarted = reopen(node.storage)

    # a restart must never see applied data without the matching last_applied,
    # or those entries get applied twice
    assert len(restarted.get_all_values()) == 2
    assert restarted.get_last_applied() == 2
