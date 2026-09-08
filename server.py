from fastapi import FastAPI
from storage import Storage
from models import Operation, LogEntry

server = FastAPI()
storage = Storage(data_filepath='/data/storage.json', state_filepath='/data/raft_state.json')
term = 1 # temp

def appendLog(op: Operation):
    storage.append_log(LogEntry(index=storage.get_lastIndex() + 1, term=term, operation=op))

@server.get("/")
def read_root():
    return {}

@server.get("/value/{key}")
def get_value(key: str):
    val = storage.get_value(key)
    return {"key": key, "value": val}

@server.put("/value/{key}")
def set_value(key: str, value: str):
    # append log
    appendLog(Operation(type="PUT", key=key, value=value))

    # update data store
    old_val = storage.get_value(key)
    storage.set_value(key, value)
    return {"key": key, "new_value": value, "old_value": old_val}

@server.delete("/value/{key}")
def delete_value(key: str):
    # append log
    appendLog(Operation(type="DELETE", key=key))
        
    old_val = storage.delete_value(key)
    return {"key": key, "old_value": old_val}

@server.get("/entrySet")
def get_entrySet():
    return storage.get_all_values()

@server.get("/temp/log")
def get_log():
    return storage.get_log()