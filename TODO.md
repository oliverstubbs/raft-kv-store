# To Do List :)

#### Done
- Move storage to use DB (SQLite)
- Apply entries and updte last_applied in one go

#### Next
- Update file writes to use asyncio (save in a thread) so doesn't block heartbeats or RPC handling
- in send_heartbeat don't wait 150ms for full responses

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop
- add client.py example
- close db connection 