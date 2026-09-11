# To Do List :)

#### Done
- Leader election logic:
  - Send votes to peers
  - Vote counting
  - Transition to leader logic
  - Dont time out while LEADER
- Heartbeat timers:
  - Update to send (leader)
  - Update to receive (follower)

#### Next
- Log replication:
  - Actual handle the /append_entries route
  - Leader steps down when a reply carries a higher term
  - Commit index + applying entries to the store
- Route writes to the leader (followers redirect or reject)

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add timeout to http peer requests
- Add try/except for main run node loop
- Candidate should step down when a vote response has a higher term