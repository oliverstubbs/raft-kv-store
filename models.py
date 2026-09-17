from enum import Enum
from typing import List, Literal
from pydantic import BaseModel

class Operation(BaseModel):
    type: Literal["PUT", "DELETE", "NOOP"] 
    key: str | None = None
    value: str | None = None

class LogEntry(BaseModel):
    index: int
    term: int
    operation: Operation
   
class RaftState(BaseModel):
    current_term: int = 0
    voted_for: str | None = None
    log: List[LogEntry] = []
    
class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    
# Invoked by leader to replicate log entries & used as a heartbeat
class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry] = []
    leader_commit: int

class AppendEntriesResponse(BaseModel):
    term: int
    success: bool

# Invoked by candidates to gather votes
class RequestVoteRequest(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    pre_vote: bool = False
    
class RequestVoteResponse(BaseModel):
    term: int
    vote_granted: bool