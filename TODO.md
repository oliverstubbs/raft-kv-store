# To Do List :)

#### Done
- Leader now appends to its log only and leaves its data store (for now)
- Heartbeats carry real entries from next_index
- Followers will update its own log from the entries from the heartbeats 

#### Next
- Log replication:
  - Update stored data on replies from followers
  - Retry replication on failure
  - Apply committed entries to the store
  - PUT/DELETE waits for commit before replying

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop