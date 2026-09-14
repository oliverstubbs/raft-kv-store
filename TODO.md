# To Do List :)

#### Done
- send_heartbead now updates next_index & match_index from follower replies
- Leaders will retry sending updated log from further back if failed 

#### Next
- Log replication:
  - Update commit_index on the leader once a majority has an entry
  - Apply committed entries to the store
  - PUT/DELETE waits for commit before replying

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop