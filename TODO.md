# To Do List :)

#### Done
- Update file writes to use asyncio (save in a thread) so doesn't block heartbeats or RPC handling

#### Next
- in send_heartbeat don't wait 150ms for full responses

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop
- add client.py example
- close db connection 