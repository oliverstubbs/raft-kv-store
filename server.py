from fastapi import FastAPI
import json

server = FastAPI()
storage = {}

@server.get("/")
def read_root():
    return {}

@server.get("/value/{key}")
def get_value(key: str):
    val = None
    if key in storage:
        val = storage[key]
    return {"key": key, "value": val}

@server.put("/value/{key}")
def set_value(key: str, value: str):
    old_val = storage.get(key)
    storage[key] = value
    writeToDisk()
    return {"key": key, "new_value": value, "old_value": old_val}

@server.delete("/value/{key}")
def delete_value(key: str):
    old_val = storage.pop(key)
    writeToDisk()
    return {"key": key, "old_value": old_val}

@server.get("/entrySet")
def get_entrySet():
    return storage


def readFromDisk():
    global storage
    with open('storage.json', 'r') as f:
        storage = json.load(f)

def writeToDisk():
    with open ('storage.json', 'w') as f:
        json.dump(storage, f)
        
readFromDisk()