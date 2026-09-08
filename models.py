from pydantic import BaseModel


class Operation(BaseModel):
    type: str
    key: str
    value: str | None = None


class LogEntry(BaseModel):
    index: int
    term: int
    operation: Operation