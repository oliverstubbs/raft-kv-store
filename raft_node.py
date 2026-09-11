import os
import random
import time
import asyncio

from models import NodeState, Operation, LogEntry
from storage import Storage

class RaftNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.state = NodeState.FOLLOWER

        # cluster membership
        self.peers = [p for p in os.getenv("PEERS", "").split(",") if p]
        
        # persistent state 
        self.storage = Storage(data_filepath='/data/storage.json', state_filepath='/data/raft_state.json')
        
        # volatile state (on all servers)
        self.commit_index: int = 0
        self.last_applied: int = 0
        
        # volatile state (on leaders)
        self.next_index = None
        self.match_index = None
        
        # timers for election
        self.election_time_limit = 0
        self.election_time_min = 0.15
        self.election_time_max = 0.3
        

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
    
    # election operations
    
    def reset_election_timer(self):
        self.election_time_limit = time.monotonic() + random.uniform(self.election_time_min, self.election_time_max)
        
    async def run_node(self):
        self.reset_election_timer()
        
        while True:
            # handle transition to CANDIDATE state
            if(time.monotonic() > self.election_time_limit):
                print(f"{self.node_id}: transition to candidate")
                self.state = NodeState.CANDIDATE
                self.storage.increment_current_term()
                self.storage.set_voted_for(self.node_id)
                self.reset_election_timer()
                # send vote requests to all other servers
                
            await asyncio.sleep(0.01)