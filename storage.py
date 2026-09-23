import json
import sqlite3
from models import RaftState, LogEntry

class Storage:
    def __init__(self, db_path='raft.db'):
        self.db_path = db_path
        self.db_conn = sqlite3.connect(self.db_path)
        self.create_tables()

    def save(self):
        self.db_conn.commit()
    
    # update data store

    def get_value(self, key, default=None):
        row = self.db_conn.execute("SELECT value FROM data WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

    def set_value(self, key, value):
        self.db_conn.execute("INSERT OR REPLACE INTO data VALUES (?, ?)", (key, value))
        return "NEED TO CHANGE"

    def delete_value(self, key):
        self.db_conn.execute("DELETE FROM data WHERE key = ?", (key,))
        return "NEED TO CHANGE"

    def get_all_values(self):
        return self.data

    def __contains__(self, key):
        return key in self.data
    
    def get_last_applied(self, default=0):
        row = self.db_conn.execute("SELECT last_applied FROM raft_state WHERE id = 1").fetchone()
        return row[0] if row else default
    
    def set_last_applied(self, val):
        self.db_conn.execute("UPDATE raft_state SET last_applied = ?", (val,))
        self.save()

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
        
    def create_tables(self):
        
        self.db_conn.executescript("""
            CREATE TABLE IF NOT EXISTS data (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            
            CREATE TABLE IF NOT EXISTS log (
                idx INTEGER PRIMARY KEY,
                term INTEGER NOT NULL, 
                operation TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS raft_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                current_term INTEGER NOT NULL DEFAULT 0,
                voted_for TEXT,
                last_applied INTEGER NOT NULL DEFAULT 0
            );
            
            INSERT OR IGNORE INTO raft_state (id, voted_for) VALUES (1, NULL);
        """)
                  
        self.db_conn.commit()