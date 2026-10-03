# To Do List :)

#### Done
- fix set_log - currently rewrites the whole log table
- fixes prevote not giving vote bug
- each vote request's response is handled independently

#### Next
- in send_heartbeat don't wait 150ms for full responses

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop
- close db connection 
- add client.py example