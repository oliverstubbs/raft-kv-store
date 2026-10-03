# To Do List :)

#### Done
- state_lock now covers the election path, and apply_log commits inside it

#### Next
- in send_heartbeat don't wait 150ms for full responses
- fix set_log - currently rewrites the whole log table

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop
- add client.py example
- close db connection 