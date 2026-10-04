import os
import asyncio
import hmac

from fastapi import Depends, FastAPI, Header, HTTPException
from contextlib import asynccontextmanager

from raft_node import RaftNode, CLUSTER_SECRET
from models import AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse

node_id = os.getenv("NODE_ID", "node")
raft_node = RaftNode(node_id=node_id)

def verify_peer(secret: str | None = Header(default=None, alias="X-Cluster-Secret")):
    if not CLUSTER_SECRET:
        return
    if secret is None or not hmac.compare_digest(secret, CLUSTER_SECRET):
        raise HTTPException(status_code=403, detail="bad or missing cluster secret")

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
    raft_node.storage.close_db_connection()
    
server = FastAPI(lifespan=lifespan)

@server.get("/")
async def read_root():
    return {
        "node_id": raft_node.node_id,
        "state": raft_node.state.value,
        "leader_id": raft_node.leader_id,
    }

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

# debugging routes
if os.getenv("DEBUG_ENDPOINTS"):
    @server.get("/entrySet")
    async def get_entrySet():
        return raft_node.get_all_values()

    @server.get("/temp/log")
    async def get_log():
        return raft_node.get_log()

@server.post("/request_vote", dependencies=[Depends(verify_peer)])
async def request_vote(request: RequestVoteRequest) -> RequestVoteResponse:
    return await raft_node.handle_vote_request(request)

@server.post("/append_entries", dependencies=[Depends(verify_peer)])
async def append_entries(request: AppendEntriesRequest) -> AppendEntriesResponse:
    return await raft_node.handle_append_entries(request)