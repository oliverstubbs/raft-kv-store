import json
from typing import List
from models import LogEntry


class Storage:
    def __init__(self, data_filepath='storage.json', state_filepath='raft_state.json'):
        self.data_filepath = data_filepath
        self.state_filepath = state_filepath
        self.data = {}
        self.log: List[LogEntry] = []
        self.lastIndex = 0  # index starts at 1
        self.load()

    def load(self):
        try:
            with open(self.data_filepath, 'r') as f:
                self.data = json.load(f)
        except FileNotFoundError:
            self.data = {}
        try:
            with open(self.state_filepath, 'r') as f:
                state = json.load(f)
                self.log = [LogEntry.model_validate(entry) for entry in state["log"]]
                self.lastIndex = self.log[-1].index if self.log else 0
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            self.log = []
            self.lastIndex = 0
            # will later include current term and votes


    def save_data(self):
        with open(self.data_filepath, 'w') as f:
            json.dump(self.data, f)

    def save_state(self):
        with open(self.state_filepath, 'w') as f:
            log_data = [entry.model_dump() for entry in self.log]
            json.dump({"log": log_data}, f)
    
    # update data store

    def get_value(self, key, default=None):
        return self.data.get(key, default)

    def set_value(self, key, value):
        self.data[key] = value
        self.save_data()

    def delete_value(self, key):
        value = self.data.pop(key)
        self.save_data()
        return value

    def get_all_values(self):
        return self.data

    def __contains__(self, key):
        return key in self.data

    # state store methods

    def get_log(self):
        return self.log
    
    def get_lastIndex(self):
        return self.lastIndex

    def append_log(self, entry: LogEntry):
        self.log.append(entry)
        self.lastIndex = self.lastIndex + 1
        self.save_state()