import asyncio
import sqlite3
from models import LogEntry, Operation

class Storage:
    def __init__(self, db_path='raft.db'):
        self.db_path = db_path
        self.db_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.create_tables()
        self.load_log()

    async def save(self):
        await asyncio.to_thread(self.db_conn.commit)

    # data table operations

    def get_value(self, key, default=None):
        row = self.db_conn.execute("SELECT value FROM data WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

    def set_value(self, key, value):
        self.db_conn.execute("INSERT OR REPLACE INTO data VALUES (?, ?)", (key, value))

    def delete_value(self, key):
        self.db_conn.execute("DELETE FROM data WHERE key = ?", (key,))

    def get_all_values(self):
        return dict(self.db_conn.execute("SELECT key, value FROM data"))
    
    # log table operations
    
    def load_log(self):
        rows = self.db_conn.execute("SELECT idx, term, op_type, key, value FROM log ORDER BY idx").fetchall()
        self.log = [
            LogEntry(
                index = idx,
                term = term,
                operation = Operation(type=op_type, key=key, value=value)
            )
            for idx, term, op_type, key, value in rows
        ]
    
    def get_log(self) -> list[LogEntry]:
        return list(self.log) # send copy
    
    async def set_log(self, log: list[LogEntry]):
        self.log = log
        self.db_conn.execute("DELETE FROM log")
        self.db_conn.executemany(
            "INSERT INTO log VALUES (?, ?, ?, ?, ?)",
            [(e.index, e.term, e.operation.type, e.operation.key, e.operation.value) for e in log],
        )
        await self.save()

    def get_last_index(self):
        return self.log[-1].index if self.log else 0
    
    def get_last_term(self):
        return self.log[-1].term if self.log else 0

    def get_term_at(self, index: int, default=None) -> int | None:
        # index 0 = no term yet
        if index == 0:
            return 0
        # index not stored
        if index > self.get_last_index():
            return None
        return self.log[index - 1].term

    async def append_entry(self, entry: LogEntry):
        self.log.append(entry)
        self.db_conn.execute("INSERT OR REPLACE INTO log VALUES (?, ?, ?, ?, ?)", (entry.index, entry.term, entry.operation.type, entry.operation.key, entry.operation.value))
        await self.save()
    
    # raft_state operations
    
    def get_current_term(self):
        return self.db_conn.execute("SELECT current_term FROM raft_state WHERE id = 1").fetchone()[0]

    async def set_current_term(self, term: int):
        self.db_conn.execute("UPDATE raft_state SET current_term = ? WHERE id = 1", (term,))
        await self.save()

    async def increment_current_term(self):
        self.db_conn.execute("UPDATE raft_state SET current_term = current_term + 1 WHERE id = 1")
        await self.save()

    def get_voted_for(self):
        return self.db_conn.execute("SELECT voted_for FROM raft_state WHERE id = 1").fetchone()[0]

    async def set_voted_for(self, candidate_id: str | None):
        self.db_conn.execute("UPDATE raft_state SET voted_for = ? WHERE id = 1", (candidate_id,))
        await self.save()
    
    def get_last_applied(self, default=0):
        row = self.db_conn.execute("SELECT last_applied FROM raft_state WHERE id = 1").fetchone()
        return row[0] if row else default
    
    async def set_last_applied(self, val):
        self.db_conn.execute("UPDATE raft_state SET last_applied = ? WHERE id = 1", (val,))
        await self.save() # saves, applying all changes not commited in the data table operations
        
    # method to be called on start up
        
    def create_tables(self):
        
        self.db_conn.executescript("""
            CREATE TABLE IF NOT EXISTS data (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            
            CREATE TABLE IF NOT EXISTS log (
                idx INTEGER PRIMARY KEY,
                term INTEGER NOT NULL, 
                op_type TEXT NOT NULL CHECK (op_type IN ('PUT', 'DELETE', 'NOOP')),
                key TEXT,
                value TEXT
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