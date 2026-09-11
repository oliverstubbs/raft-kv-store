# To Do List :)

#### Done
- Added request_vote route
- Added basic voting logic:
  - term adoption
  - one vote per term
  - check log up to date

#### Next
- Leader election logic:
  - Send votes to peers
  - Vote counting
  - Transition to leader logic
  - Dont time out while LEADER
- Heartbeat timers:
  - Update to send (leader)
  - Update to receive (follower)

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints