import sqlite3

from models import AppendEntriesRequest, LogEntry, Operation


def entries(start_index: int, terms: list[int]) -> list[LogEntry]:
    """PUT entries at consecutive indices, one per term in `terms`."""
    return [
        LogEntry(
            index=start_index + offset,
            term=term,
            operation=Operation(type="PUT", key=f"k{start_index + offset}", value="v"),
        )
        for offset, term in enumerate(terms)
    ]


def append_entries(
    term: int = 1,
    leader_id: str = "2",
    prev_log_index: int = 0,
    prev_log_term: int = 0,
    entries: list[LogEntry] | None = None,
    leader_commit: int = 0,
) -> AppendEntriesRequest:
    return AppendEntriesRequest(
        term=term,
        leader_id=leader_id,
        prev_log_index=prev_log_index,
        prev_log_term=prev_log_term,
        entries=entries or [],
        leader_commit=leader_commit,
    )


def terms_of(node) -> list[int]:
    return [entry.term for entry in node.get_log()]


def indices_of(node) -> list[int]:
    return [entry.index for entry in node.get_log()]


def committed_state(storage) -> tuple[dict, int]:
    """The committed data rows and last_applied, read through a separate connection.

    Read-only on purpose: a second Storage() would run create_tables, and that
    write blocks while an uncommitted apply batch is still open.
    """
    conn = sqlite3.connect(f"file:{storage.db_path}?mode=ro", uri=True)
    try:
        data = dict(conn.execute("SELECT key, value FROM data"))
        last_applied = conn.execute("SELECT last_applied FROM raft_state WHERE id = 1").fetchone()[0]
        return data, last_applied
    finally:
        conn.close()


def entry(index: int, term: int, type: str = "PUT", key: str | None = None, value: str | None = None) -> LogEntry:
    """One entry, for when the op matters - entries() only builds PUTs."""
    return LogEntry(index=index, term=term, operation=Operation(type=type, key=key, value=value))


def count_saves(storage) -> list:
    """Replace storage.save with a counting wrapper; the returned list grows per commit."""
    saves = []
    real_save = storage.save

    async def counting_save():
        saves.append(1)
        await real_save()

    storage.save = counting_save
    return saves
