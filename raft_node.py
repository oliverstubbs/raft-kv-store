import os
import random
import time
import asyncio
import httpx

from models import AppendEntriesRequest, NodeState, Operation, LogEntry, RequestVoteRequest, RequestVoteResponse
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
        self.heartbeat_interval = 0.05
        

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

    def step_down(self, term: int):
        # adopt a newer term seen from a peer and revert to follower
        self.storage.set_current_term(term)
        self.storage.set_voted_for(None)
        self.state = NodeState.FOLLOWER
        self.next_index = None
        self.match_index = None

    def become_leader(self):
        self.state = NodeState.LEADER
        print(f"node {self.node_id}: transition to LEADER. I AM LEADER!!!!")
        # re-initialise volatile state on leaders
        self.next_index = {peer: self.get_last_index() + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}

    def election_timed_out(self) -> bool:
        return time.monotonic() > self.election_time_limit

    async def run_election(self):
        print(f"node {self.node_id}: transition to candidate")
        self.state = NodeState.CANDIDATE
        self.storage.increment_current_term()
        self.storage.set_voted_for(self.node_id)
        self.reset_election_timer()

        # send vote requests to all other servers
        responses = await self.send_vote_requests()

        # check if any peer is on a higher term, if so then we are behind and should step down
        highest_term = max((res.term for res in responses), default=0)
        if highest_term > self.storage.get_current_term():
            print(f"node {self.node_id}: saw term {highest_term} in a vote response - stepping down")
            self.step_down(highest_term)
            return

        # check if have maj of votes to become leader
        votes = sum(1 for res in responses if res.vote_granted)
        if votes + 1 > (len(self.peers) + 1) // 2: # +1 for this node's vote for itself
            self.become_leader()

    async def run_node(self):
        self.reset_election_timer()

        while True:
            # if leader send heartbeats
            if self.state == NodeState.LEADER:
                await self.send_heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
                continue

            # handle transition to CANDIDATE state
            if self.election_timed_out():
                await self.run_election()

            await asyncio.sleep(0.01)

    async def send_heartbeat(self):
        empty_RPC = AppendEntriesRequest(
            term = self.storage.get_current_term(),
            leader_id = self.node_id,
            prev_log_index = self.storage.get_last_index(),
            prev_log_term = self.storage.get_last_term(),
            entries = [],
            leader_commit = self.commit_index,
        )
        json = empty_RPC.model_dump()
        
        async with httpx.AsyncClient() as client:
            requests = [
                client.post(f"http://{peer}/append_entries", json=json)
                for peer in self.peers 
            ]
            await asyncio.gather(*requests, return_exceptions=True)
        
       
    def handle_vote_request(self, req: RequestVoteRequest) -> RequestVoteResponse:
        current_term = self.storage.get_current_term()
        if(req.term < current_term):
            print(f"node {self.node_id}: term is greater than {req.candidate_id}, vote request rejected")
            return RequestVoteResponse(term=current_term, vote_granted=False)

        # behind - update term and become follower
        if(req.term > current_term):
            current_term = req.term
            self.step_down(current_term)
            print(f"node {self.node_id}: term is behind - transitioning to FOLLOWER")
        
        if self.can_vote_for(req) and self.log_up_to_date(req):
            self.storage.set_voted_for(req.candidate_id)
            self.reset_election_timer()
            print(f"node {self.node_id}: voted for {req.candidate_id}")
            return RequestVoteResponse(term=current_term, vote_granted=True)
        else:
            print(f"node {self.node_id}: did not vote for {req.candidate_id}")
            return RequestVoteResponse(term=current_term, vote_granted=False)
    
    def can_vote_for(self, req: RequestVoteRequest) -> bool:
        voted_for = self.storage.get_voted_for()
        return voted_for is None or voted_for == req.candidate_id
    
    def log_up_to_date(self, req: RequestVoteRequest) -> bool:
        last_log_term = self.storage.get_last_term()
        last_log_index = self.storage.get_last_index()
        return (req.last_log_term > last_log_term or (req.last_log_term == last_log_term and req.last_log_index >= last_log_index))
    
    async def send_vote_requests(self) -> list[RequestVoteResponse]:
        req = RequestVoteRequest(
            term = self.storage.get_current_term(),
            candidate_id = self.node_id,
            last_log_index = self.storage.get_last_index(),
            last_log_term = self.storage.get_last_term()
        )
        json = req.model_dump()
        
        async with httpx.AsyncClient() as client:
            requests = [
                client.post(f"http://{peer}/request_vote", json=json)
                for peer in self.peers 
            ]
            responses = await asyncio.gather(*requests, return_exceptions=True)
            
            # filter out peers that don't respond 
            votes = []
            for res in responses:
                if isinstance(res, BaseException):
                    continue
                votes.append(RequestVoteResponse(**res.raise_for_status().json()))
            return votes    