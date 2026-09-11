# To Do List :)

#### Done
- Added election timer
- Added basic transition to candidate 
- Added main background loop

#### Next
- Leader election logic:
  - Add route in api to handle requests
  - Voting logic
  - Send votes to peers
  - Vote counting
  - Transition to leader logic
- Heartbeat timers:
  - Update to send (leader)
  - Update to receive (follower)

#### Later
- Turn uvicorn auto reload off in the docker containers
- Remove temp endpoints