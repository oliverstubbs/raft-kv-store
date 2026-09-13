# To Do List :)

#### Done
- Actually handle the /append_entries route
- Leader steps down when a reply carries a higher term
- Candidate steps down when a vote response has a higher term
- Candidate steps down when it hears from a leader on the same term
- Track leader_id so can redirect requests
- Add timeout to http peer requests

#### Next
- Log replication:
  - Commit index + applying entries to the store
- Route writes to the leader (followers redirect or reject)

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop