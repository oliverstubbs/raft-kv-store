import json
from models import RaftState, LogEntry


class Storage:
    def __init__(self, data_filepath='storage.json', state_filepath='raft_state.json'):
        self.data_filepath = data_filepath
        self.state_filepath = state_filepath
        self.data = {}
        
        # Persistent state
        self.state = RaftState()
        self.load()

    def load(self):
        try:
            with open(self.data_filepath, 'r') as f:
                self.data = json.load(f)
        except FileNotFoundError:
            self.data = {}
        try:
            with open(self.state_filepath, 'r') as f:
                state_data = json.load(f)
                self.state = RaftState.model_validate(state_data)
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            self.state = RaftState()


    def save_data(self):
        with open(self.data_filepath, 'w') as f:
            json.dump(self.data, f)

    def save_state(self):
        with open(self.state_filepath, 'w') as f:
            json.dump(self.state.model_dump(), f)
    
    # update data store

    def get_value(self, key, default=None):
        return self.data.get(key, default)

    def set_value(self, key, value):
        self.data[key] = value
        self.save_data()

    def delete_value(self, key):
        value = self.data.pop(key, None)
        self.save_data()
        return value

    def get_all_values(self):
        return self.data

    def __contains__(self, key):
        return key in self.data

    # state store methods

    def get_log(self):
        return self.state.log
    
    def set_log(self, log: list[LogEntry]):
        self.state.log = log
        self.save_state()

    def get_last_index(self):
        return self.state.log[-1].index if self.state.log else 0
    
    def get_last_term(self):
        return self.state.log[-1].term if self.state.log else 0

    def get_term_at(self, index: int) -> int | None:
        # index 0 = no term yet
        if index == 0:
            return 0
        # index not stored
        if index > self.get_last_index():
            return None
        return self.state.log[index - 1].term

    def append_entry(self, entry: LogEntry):
        self.state.log.append(entry)
        self.save_state()

    def get_current_term(self):
        return self.state.current_term

    def set_current_term(self, term: int):
        self.state.current_term = term
        self.save_state()
        
    def increment_current_term(self):
        self.state.current_term += 1
        self.save_state()

    def get_voted_for(self):
        return self.state.voted_for

    def set_voted_for(self, candidate_id: str | None):
        self.state.voted_for = candidate_id
        self.save_state()