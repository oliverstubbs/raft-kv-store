import os
import random
import time
import asyncio
import traceback
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
        self.storage = Storage(db_path=os.getenv("DB_PATH", "raft.db"))
        
        # volatile state (on all servers)
        self.commit_index: int = 0

        # held by anything that changes term, vote, log, data or state
        self.state_lock = asyncio.Lock()

        # last leader
        self.leader_id: str | None = None
        
        # volatile state (on leaders)
        self.next_index = None
        self.match_index = None

        # references for heartbeats
        self.rounds = set()
        self.in_flight = {}
        self.last_ack = {}

        # timers for election
        self.last_leader_contact = 0
        self.election_time_limit = 0
        self.election_time_min = 0.15
        self.election_time_max = 0.3
        
        # other timers
        self.heartbeat_interval = 0.05
        self.rpc_timeout = 0.1
        self.apply_timeout = 10

    # data store operations
    
    async def get_value(self, key: str, default=None):
        self.check_still_leader()
        
        # wait for to be up to date 
        while self.storage.get_term_at(self.commit_index) != self.storage.get_current_term():
            self.check_still_leader()
            await asyncio.sleep(0.01)
        temp = self.commit_index
        
        await self.wait_for_majority_ack()

        # ensure we are caught up
        await self.wait_until_applied(temp)

        return self.storage.get_value(key, default)

    async def set_value(self, key: str, value: str):
        return await self.perform_request(Operation(type="PUT", key=key, value=value))

    async def delete_value(self, key: str):
        return await self.perform_request(Operation(type="DELETE", key=key))

    async def perform_request(self, op: Operation):
        self.check_still_leader()
        old_value = self.storage.get_value(op.key)
        async with self.state_lock:
            self.check_still_leader()
            index = await self.append_log(op)

        # wait for entry to be applied to the state machine
        await self.wait_until_applied(index)

        return old_value

    def get_all_values(self):
        return self.storage.get_all_values()
    
    def check_still_leader(self):
        if self.state != NodeState.LEADER:
            raise HTTPException(status_code=421, detail="lost leadership")

    def has_majority(self, peer_count: int) -> bool:
        # peer_count excludes this node, which always counts itself
        return peer_count + 1 > (len(self.peers) + 1) // 2

    async def wait_for_majority_ack(self):
        asked_at = time.monotonic()
        deadline = asked_at + self.election_time_max

        while time.monotonic() < deadline:
            self.check_still_leader()
            if self.has_majority(sum(1 for at in self.last_ack.values() if at > asked_at)):
                return
            await asyncio.sleep(0.01)

        raise HTTPException(status_code=421, detail="lost leadership")

    async def wait_until_applied(self, index: int):
        deadline = time.monotonic() + self.apply_timeout
        while self.storage.get_last_applied() < index:
            self.check_still_leader()
            if time.monotonic() > deadline:
                raise HTTPException(status_code=503, detail="entry not applied in time")
            await asyncio.sleep(0.01)

    # log operations

    def get_log(self):
        return self.storage.get_log()

    async def append_log(self, op: Operation) -> int:
        index = self.get_last_index() + 1
        await self.storage.append_entry(LogEntry(
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

    # adopt a newer term seen from a peer and revert to follower
    async def step_down(self, term: int):
        # update volatile state then save updated
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        self.next_index = None
        self.match_index = None
        await self.storage.set_term_and_vote(term, None)

    async def become_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        print(f"node {self.node_id}: transition to LEADER. I AM LEADER!!!!")
        
        # re-initialise volatile state on leaders
        self.next_index = {peer: self.get_last_index() + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        
        # add NOOP entry to log
        await self.append_log(Operation(type="NOOP"))

    def election_timed_out(self) -> bool:
        return time.monotonic() > self.election_time_limit
    
    # true if recently heard from leader
    def heard_from_leader(self) -> bool:
        return time.monotonic() - self.last_leader_contact < self.election_time_min

    async def run_election(self):
        print(f"node {self.node_id}: transition to candidate")
        self.state = NodeState.CANDIDATE
        self.reset_election_timer()

        # pre-vote: only bump the term if a majority would vote for us
        responses = await self.send_vote_requests(pre_vote=True)
        votes = sum(1 for res in responses if res.vote_granted)
        if not self.has_majority(votes):
            print(f"node {self.node_id}: pre-vote failed - staying follower")
            self.state = NodeState.FOLLOWER
            return

        # update term and vote
        async with self.state_lock:
            if self.state != NodeState.CANDIDATE:
                print(f"node {self.node_id}: no longer a candidate - abandoning election")
                return
            term = self.storage.get_current_term() + 1
            await self.storage.set_term_and_vote(term, self.node_id)

        # send vote requests to all other servers
        responses = await self.send_vote_requests()

        async with self.state_lock:
            # check still valid to become leader
            if self.state != NodeState.CANDIDATE or self.storage.get_current_term() != term:
                print(f"node {self.node_id}: no longer a candidate for term {term} - abandoning election")
                return

            # check if any peer is on a higher term, if so then we are behind and should step down
            highest_term = max((res.term for res in responses), default=0)
            if highest_term > term:
                print(f"node {self.node_id}: saw term {highest_term} in a vote response - stepping down")
                await self.step_down(highest_term)
                return

            # check if have maj of votes to become leader
            votes = sum(1 for res in responses if res.vote_granted)
            if self.has_majority(votes):
                await self.become_leader()

    async def run_node(self):
        self.reset_election_timer()
        heartbeat = asyncio.create_task(self.heartbeat_loop())
        
        try:
            while True:
                # check if need to apply log
                try:
                    await self.apply_log()
                except Exception:
                    print(f"node {self.node_id}: error in run_node applying log")
                    traceback.print_exc()

                # handle transition to CANDIDATE state
                try:
                    if self.state != NodeState.LEADER and self.election_timed_out():
                        await self.run_election()
                except Exception:
                    print(f"node {self.node_id}: error in run_node running election")
                    traceback.print_exc()
                    
                await asyncio.sleep(0.01)
        finally:
            heartbeat.cancel()

    async def heartbeat_loop(self):
        while True:
            try:
                if self.state == NodeState.LEADER:
                    round = asyncio.create_task(self.send_heartbeat())
                    self.rounds.add(round)
                    round.add_done_callback(self.rounds.discard)
            except Exception:
                print(f"node {self.node_id}: error in heartbeat_loop")
                traceback.print_exc()
            finally:
                await asyncio.sleep(self.heartbeat_interval)

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
        if self.state != NodeState.LEADER:
            return False
        
        # check if still waiting response from last one
        if self.in_flight.get(peer):
            return False
        filled_RPC = self.build_append_entries(peer)

        self.in_flight[peer] = True
        try:
            res = await client.post(f"http://{peer}/append_entries", json=filled_RPC.model_dump())
            data = AppendEntriesResponse(**res.raise_for_status().json())
        except (httpx.HTTPError, ValueError):
            return False
        finally:
            self.in_flight[peer] = False

        if data.term > self.storage.get_current_term():
            async with self.state_lock:
                if await self.step_down_if_behind(data.term):
                    return False
        if self.state != NodeState.LEADER:
            return False

        # if successful then update next_index and match_index
        if data.success:
            last_sent = filled_RPC.prev_log_index + len(filled_RPC.entries)
            self.next_index[peer] = last_sent + 1
            self.match_index[peer] = last_sent
            self.last_ack[peer] = time.monotonic()
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
    
    async def step_down_if_behind(self, req_term: int) -> bool:
        if(req_term > self.storage.get_current_term()):
            await self.step_down(req_term)
            print(f"node {self.node_id}: term is behind - transitioning to FOLLOWER")
            return True
        return False
       
    async def handle_vote_request(self, req: RequestVoteRequest) -> RequestVoteResponse:
        async with self.state_lock:
            current_term = self.storage.get_current_term()
            if(req.term < current_term):
                print(f"node {self.node_id}: term is greater than {req.candidate_id}, vote request rejected")
                return RequestVoteResponse(term=current_term, vote_granted=False)

            # pre-vote: answer as if it were real, but change nothing
            if req.pre_vote:
                granted = self.state != NodeState.LEADER and not self.heard_from_leader() and self.log_up_to_date(req)
                return RequestVoteResponse(term=current_term, vote_granted=granted)

            # check if behind - update term and become follower
            await self.step_down_if_behind(req.term)
            current_term = self.storage.get_current_term()

            if self.can_vote_for(req) and self.log_up_to_date(req):
                await self.storage.set_voted_for(req.candidate_id)
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
    
    async def send_vote_request(self, peer: str, client: httpx.Client, json: dict) -> RequestVoteResponse:
        try:
            res = await client.post(f"http://{peer}/request_vote", json=json)
            return RequestVoteResponse(**res.raise_for_status().json())
        except (httpx.HTTPError, ValueError):
            return None
    
    async def send_vote_requests(self, pre_vote: bool = False) -> list[RequestVoteResponse]:
        # send next term if prevote
        term = self.storage.get_current_term() + (1 if pre_vote else 0)
            
        req = RequestVoteRequest(
            term = term,
            candidate_id = self.node_id,
            last_log_index = self.storage.get_last_index(),
            last_log_term = self.storage.get_last_term(),
            pre_vote = pre_vote
        )
        json = req.model_dump()
        
        async with httpx.AsyncClient(timeout=self.rpc_timeout) as client:
            results = await asyncio.gather(
                *(self.send_vote_request(peer, client, json) for peer in self.peers)
            )
            return [r for r in results if r is not None] # filters out missing responses
        
    def get_first_new_entry(self, log, entries, start) -> int:
        for offset, entry in enumerate(entries):
            index = start + offset
            # check if an existing entry conflicts with a new one
            if index > len(log) or log[index -1].term != entry.term:
                return offset
        return len(entries)
    
    async def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        async with self.state_lock:
            if req.entries:
                print(f"node {self.node_id}: got {len(req.entries)} entries from {req.leader_id}")
        
            # check if term is behind
            current_term = self.storage.get_current_term()
            if req.term < current_term:
                return AppendEntriesResponse(term=current_term, success=False)
        
            # heard from a valid leader, so follow it whatever state we were in
            await self.step_down_if_behind(req.term)
            self.state = NodeState.FOLLOWER
            self.leader_id = req.leader_id
            self.reset_election_timer()
            self.last_leader_contact = time.monotonic()

            # check if contains entry at prev log index
            if self.storage.get_term_at(req.prev_log_index) != req.prev_log_term:
                return AppendEntriesResponse(term=current_term, success=False)

            # delete any conflicting entries and append the new entries
            if req.entries:
                start = req.prev_log_index + 1
                log = self.get_log()
                new_from = self.get_first_new_entry(log, req.entries, start)
                
                if new_from < len(req.entries):
                    index = start + new_from
                    if index <= len(log):
                        # delete existing entry and all that follow it
                        await self.storage.truncate_log(index)
                    # add the new ones
                    await self.storage.append_log(req.entries[new_from:])

            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, req.prev_log_index + len(req.entries))

            return AppendEntriesResponse(term=self.storage.get_current_term(), success=True)

    async def apply_log(self):
        async with self.state_lock:
            last_applied = self.storage.get_last_applied()

            if self.commit_index <= last_applied:
                return

            # loop through all logs and write them to disk
            log = self.get_log()
            while self.commit_index > last_applied:
                op = log[last_applied].operation
                print(f"node {self.node_id}: applying {last_applied + 1} ({op.type})")
                match op.type:
                    case "PUT":
                        self.storage.set_value(op.key, op.value)
                    case "DELETE":
                        self.storage.delete_value(op.key)
                    case "NOOP":
                        pass
                last_applied += 1

            # actually save it
            await self.storage.set_last_applied(last_applied)