import os
import asyncio

from fastapi import FastAPI
from contextlib import asynccontextmanager

from raft_node import RaftNode
from models import AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse

node_id = os.getenv("NODE_ID", "node")
raft_node = RaftNode(node_id=node_id)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # on startup start raft node loop
    node_loop = asyncio.create_task(raft_node.run_node())
    yield
    # on shutdown try stop the loop
    node_loop.cancel()
    try:
        await node_loop
    except asyncio.CancelledError:
        print("Shutdown complete.")
    
server = FastAPI(lifespan=lifespan)

@server.get("/")
def read_root():
    return {}

@server.get("/value/{key}")
async def get_value(key: str):
    val = await raft_node.get_value(key)
    return {"key": key, "value": val}

@server.put("/value/{key}")
async def set_value(key: str, value: str):
    old_val = await raft_node.set_value(key, value)
    return {"key": key, "new_value": value, "old_value": old_val}

@server.delete("/value/{key}")
async def delete_value(key: str):
    old_val = await raft_node.delete_value(key)
    return {"key": key, "old_value": old_val}

@server.get("/entrySet")
async def get_entrySet():
    return raft_node.get_all_values()

@server.get("/temp/log")
async def get_log():
    return raft_node.get_log()

@server.post("/request_vote")
async def request_vote(request: RequestVoteRequest) -> RequestVoteResponse:
    return raft_node.handle_vote_request(request)

@server.post("/append_entries")
async def append_entries(request: AppendEntriesRequest) -> AppendEntriesResponse:
    return raft_node.handle_append_entries(request)