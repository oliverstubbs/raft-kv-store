import os
import random
import time
import asyncio
from fastapi import HTTPException
import httpx

from models import AppendEntriesRequest, AppendEntriesResponse, NodeState, Operation, LogEntry, RequestVoteRequest, RequestVoteResponse
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

        # last leader
        self.leader_id: str | None = None
        
        # volatile state (on leaders)
        self.next_index = None
        self.match_index = None
        
        # timers for election
        self.election_time_limit = 0
        self.election_time_min = 0.15
        self.election_time_max = 0.3
        self.heartbeat_interval = 0.05
        self.rpc_timeout = 0.1
        

    # data store operations
    
    async def get_value(self, key: str, default=None):
        if self.state != NodeState.LEADER:
            raise HTTPException(status_code=421, detail="not a leader")
        
        # wait for to be up to date 
        while self.storage.get_term_at(self.commit_index) != self.storage.get_current_term():
            self.check_still_leader()
            await asyncio.sleep(0.01)
        temp = self.commit_index
        
        acks = await self.send_heartbeat()
        if self.state != NodeState.LEADER or acks + 1 <= (len(self.peers) + 1) // 2:
            raise HTTPException(status_code=421, detail="lost leadership")
        
        # ensure we are caught up
        while self.last_applied < temp:
            self.check_still_leader()
            await asyncio.sleep(0.01)
        
        return self.storage.get_value(key, default)

    async def set_value(self, key: str, value: str):
        await self.perform_request(Operation(type="PUT", key=key, value=value))

    async def delete_value(self, key: str):
        await self.perform_request(Operation(type="DELETE", key=key))
            
    async def perform_request(self, op: Operation):
        self.check_still_leader()
        index = self.append_log(op)

        # wait for entry to be applied to the state machine
        while self.last_applied < index:
            self.check_still_leader()
            await asyncio.sleep(0.01)

    def get_all_values(self):
        return self.storage.get_all_values()
    
    def check_still_leader(self):
        if self.state != NodeState.LEADER:
            raise HTTPException(status_code=421, detail="lost leadership")

    # log operations
    
    def get_log(self):
        return self.storage.get_log()

    def append_log(self, op: Operation) -> int:
        index = self.get_last_index() + 1
        self.storage.append_entry(LogEntry(
                index = index,
                term = self.storage.get_current_term(),
                operation = op
            ))
        return index

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
        self.leader_id = None
        self.next_index = None
        self.match_index = None

    def become_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        print(f"node {self.node_id}: transition to LEADER. I AM LEADER!!!!")
        
        # re-initialise volatile state on leaders
        self.next_index = {peer: self.get_last_index() + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        
        # add NOOP entry to log
        self.append_log(Operation(type="NOOP"))

    def election_timed_out(self) -> bool:
        return time.monotonic() > self.election_time_limit

    async def run_election(self):
        print(f"node {self.node_id}: transition to candidate")
        self.state = NodeState.CANDIDATE
        self.storage.increment_current_term()
        self.storage.set_voted_for(self.node_id)
        self.reset_election_timer()
        term = self.storage.get_current_term()

        # send vote requests to all other servers
        responses = await self.send_vote_requests()

        # check still valid to become leader
        if self.state != NodeState.CANDIDATE or self.storage.get_current_term() != term:
            print(f"node {self.node_id}: no longer a candidate for term {term} - abandoning election")
            return

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
            # check if need to apply log
            while self.commit_index > self.last_applied:
                self.apply_log()
            
            # if leader send heartbeats
            if self.state == NodeState.LEADER:
                await self.send_heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
                continue

            # handle transition to CANDIDATE state
            if self.election_timed_out():
                await self.run_election()

            await asyncio.sleep(0.01)

    def build_append_entries(self, peer: str) -> AppendEntriesRequest:
        log = self.storage.get_log()
        prev_log_index = self.next_index[peer] - 1
        prev_log_term = self.storage.get_term_at(prev_log_index)

        return AppendEntriesRequest(
            term = self.storage.get_current_term(),
            leader_id = self.node_id,
            prev_log_index = prev_log_index,
            prev_log_term = prev_log_term,
            entries = log[prev_log_index:],         # empty when up to date
            leader_commit = self.commit_index,
        )

    async def send_heartbeat(self) -> int:
        async with httpx.AsyncClient(timeout=self.rpc_timeout) as client:
            acks = await asyncio.gather(*(self.send_log(peer, client) for peer in self.peers))
            
        # double check still a leader
        if self.state != NodeState.LEADER:
            return 0
        self.update_commit_index()
        return sum(acks)
                
    async def send_log(self, peer: str, client: httpx.AsyncClient) -> bool:
        filled_RPC = self.build_append_entries(peer)
        
        try:
            res = await client.post(f"http://{peer}/append_entries", json=filled_RPC.model_dump())
            data = AppendEntriesResponse(**res.raise_for_status().json())
        except httpx.HTTPError:
            return False
        
        if self.step_down_if_behind(data.term) or self.state != NodeState.LEADER:
            return False
        
        # if successful then update next_index and match_index
        if data.success:
            last_sent = filled_RPC.prev_log_index + len(filled_RPC.entries)
            self.next_index[peer] = last_sent + 1
            self.match_index[peer] = last_sent
            return True
        # if fails, decrement next_index and try again
        else: 
            self.next_index[peer] = max(1, self.next_index[peer] -1)
            return False
        
    def update_commit_index(self):
        # find highest possible N 
        indices = sorted([self.get_last_index(), *self.match_index.values()], reverse=True)
        n = indices[(len(self.peers) + 1) // 2]
            
        # loop through max N to min N to see if can update commit_index
        current_term = self.storage.get_current_term()
        for i in range(n, self.commit_index, -1):
            if self.storage.get_term_at(i) == current_term:
                self.commit_index = i
                break
    
    def step_down_if_behind(self, req_term: int) -> bool:
        if(req_term > self.storage.get_current_term()):
            self.step_down(req_term)
            print(f"node {self.node_id}: term is behind - transitioning to FOLLOWER")
            return True
        return False
       
    def handle_vote_request(self, req: RequestVoteRequest) -> RequestVoteResponse:
        current_term = self.storage.get_current_term()
        if(req.term < current_term):
            print(f"node {self.node_id}: term is greater than {req.candidate_id}, vote request rejected")
            return RequestVoteResponse(term=current_term, vote_granted=False)

        # check if behind - update term and become follower
        self.step_down_if_behind(req.term)
        
        if self.can_vote_for(req) and self.log_up_to_date(req):
            self.storage.set_voted_for(req.candidate_id)
            self.reset_election_timer()
            print(f"node {self.node_id}: voted for {req.candidate_id}")
            return RequestVoteResponse(term=self.storage.get_current_term(), vote_granted=True)
        else:
            print(f"node {self.node_id}: did not vote for {req.candidate_id}")
            return RequestVoteResponse(term=self.storage.get_current_term(), vote_granted=False)
    
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
        
        async with httpx.AsyncClient(timeout=self.rpc_timeout) as client:
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
        
    def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        if req.entries:
            print(f"node {self.node_id}: got {len(req.entries)} entries from {req.leader_id}")
        
        # check if term is behind
        current_term = self.storage.get_current_term()
        if req.term < current_term:
            return AppendEntriesResponse(term=current_term, success=False)
        
        # heard from a valid leader, so follow it whatever state we were in
        self.step_down_if_behind(req.term)
        self.state = NodeState.FOLLOWER
        self.leader_id = req.leader_id
        self.reset_election_timer()

        # check if contains entry at prev log index
        if self.storage.get_term_at(req.prev_log_index) != req.prev_log_term:
            return AppendEntriesResponse(term=current_term, success=False)

        # delete any conflicting entries and append the new entries
        log = self.get_log()
        index = req.prev_log_index + 1
        for entry in req.entries:
            if index <= len(log):
                # check if an existing entry conflicts with a new one
                if log[index - 1].term != entry.term:
                    # delete existing entry and all that follow it
                    log = log[: index - 1]
                    log.append(entry)
            # otherwise past the end, so can just add to end of log
            else:
                log.append(entry)
            index += 1
        self.storage.set_log(log)     
        
        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, index-1)

        return AppendEntriesResponse(term=self.storage.get_current_term(), success=True)

    def apply_log(self):
        op = self.get_log()[self.last_applied].operation
        match op.type:
            case "PUT":
                self.storage.set_value(op.key, op.value)
            case "DELETE":
                self.storage.delete_value(op.key)
            case "NOOP":
                pass
                
        self.last_applied += 1