# To Do List :)

#### Done
- Update commit_index on the leader once a majority has an entry
- Apply committed entries to the store
- PUT/DELETE waits for commit before replying

#### Next
- Followers don't reject request and forward to leader instead

#### Later
- New leaders append a no-op entry on election so entries from earlier terms get committed straight away instead of waiting for the next client write
- Persist last_applied in storage.json (same save as the data) so a restart doesn't re-apply the whole log
- Batch the applies - apply all pending entries then save once (instead of a file write per entry)
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop
- Pre votes