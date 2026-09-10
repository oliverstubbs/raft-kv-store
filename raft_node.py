import os

from models import NodeState, Operation, LogEntry
from storage import Storage

class RaftNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.state = NodeState.FOLLOWER

        # cluster membership
        self.peers = [p for p in os.getenv("PEERS", "").split(",") if p]
        print(f"node {self.node_id} peers: {self.peers}") # temp to check works
        
        # persistent state 
        self.storage = Storage(data_filepath='/data/storage.json', state_filepath='/data/raft_state.json')
        
        # volatile state (on all servers)
        self.commit_index: int = 0
        self.last_applied: int = 0
        
        # volatile state (on leaders)
        self.next_index = None
        self.match_index = None

    # data store operations
    
    def get_value(self, key: str, default=None):
        return self.storage.get_value(key, default)

    def set_value(self, key: str, value: str):
        # append to log
        self.append_log(Operation(type="PUT", key=key, value=value))

        # update data store
        old_val = self.storage.get_value(key)
        self.storage.set_value(key, value)
        return old_val

    def delete_value(self, key: str):
        # append to log
        self.append_log(Operation(type="DELETE", key=key))

        # update data store
        old_val = self.storage.delete_value(key)
        return old_val

    def get_all_values(self):
        return self.storage.get_all_values()

    # log operations
    def get_log(self):
        return self.storage.get_log()

    def append_log(self, op: Operation):
        self.storage.append_entry(LogEntry(
                index = self.get_last_index() + 1, 
                term = self.storage.get_current_term(), 
                operation = op
            ))

    def get_last_index(self):
        return self.storage.get_last_index()