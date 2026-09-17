# To Do List :)

#### Done
- Pre-votes

#### Next
- Persist last_applied in storage.json (same save as the data) so a restart doesn't re-apply the whole log
- - Batch the applies - apply all pending entries then save once (instead of a file write per entry)

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints
- http API requests don't handle exceptions
- Add try/except for main run node loop