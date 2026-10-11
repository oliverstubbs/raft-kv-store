import pytest

from raft_node import RaftNode
from storage import Storage

# Five-node cluster, same shape as docker-compose: this node plus four peers,
# so a majority is 3 and has_majority() needs 2 peer acks.
PEERS = "node2:8000,node3:8000,node4:8000,node5:8000"


@pytest.fixture
def node(monkeypatch, tmp_path):
    """A RaftNode on a throwaway SQLite file.

    RaftNode.__init__ reads PEERS and DB_PATH from the environment and builds its
    own Storage, so the env has to be set before it is constructed.
    """
    monkeypatch.setenv("PEERS", PEERS)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "raft.db"))

    node = RaftNode(node_id="1")
    yield node
    node.storage.close_db_connection()


@pytest.fixture
def storage(tmp_path):
    storage = Storage(db_path=str(tmp_path / "raft.db"))
    yield storage
    storage.close_db_connection()


@pytest.fixture
def reopen():
    """Open a second Storage on the same file, to read back what was committed.

    Stands in for a restart: anything not committed is not there.
    """
    opened = []

    def reopen(storage: Storage) -> Storage:
        fresh = Storage(db_path=storage.db_path)
        opened.append(fresh)
        return fresh

    yield reopen
    for storage in opened:
        storage.close_db_connection()
