# To Do List :)

#### Done
- New leaders append a no-op entry on election so entries from earlier terms get committed straight away instead of waiting for the next client write
- Route GETs through the leader and have the leader confirm it's still leader before answering

#### Next
- Pre votes

#### Later
- Persist last_applied in storage.json (same save as the data) so a restart doesn't re-apply the whole log
- Batch the applies - apply all pending entries then save once (instead of a file write per entry)
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop