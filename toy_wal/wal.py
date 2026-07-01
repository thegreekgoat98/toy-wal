"""
toywal/wal.py

Write-Ahead Log implementation.
Every change is recorded here BEFORE touching the data store.
"""

import json
import os
import time
import threading
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional


class OpType(str, Enum):
    BEGIN = "BEGIN"
    SET = "SET"
    DELETE = "DELETE"
    COMMIT = "COMMIT"
    ABORT = "ABORT"
    CHECKPOINT = "CHECKPOINT"


@dataclass
class LogRecord:
    lsn: int                        # Log Sequence Number — monotonically increasing
    txn_id: int                     # Which transaction this belongs to
    op: OpType
    key: Optional[str] = None
    old_value: Optional[str] = None # For undo during recovery
    new_value: Optional[str] = None # For redo during recovery
    timestamp: float = 0.0

    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()

    def serialize(self) -> str:
        return json.dumps(asdict(self)) + "\n"

    @classmethod
    def deserialize(cls, line: str) -> "LogRecord":
        d = json.loads(line.strip())
        d["op"] = OpType(d["op"])
        return cls(**d)


class WAL:
    """
    Append-only Write-Ahead Log.

    Contract:
        1. A log record is fsynced to disk BEFORE the data page is modified.
        2. A transaction is only durable once its COMMIT record is on disk.
        3. On crash, replay forward from the last checkpoint.
    """

    def __init__(self, log_path: str = "wal.log"):
        self.log_path = log_path
        self._lsn = 0
        self._lock = threading.Lock()
        self._load_lsn()

    def _load_lsn(self):
        """Resume LSN from where we left off."""
        if not os.path.exists(self.log_path):
            return
        last = None
        with open(self.log_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    last = line
        if last:
            record = LogRecord.deserialize(last)
            self._lsn = record.lsn + 1

    def _next_lsn(self) -> int:
        lsn = self._lsn
        self._lsn += 1
        return lsn

    def append(self, record: LogRecord) -> int:
        """
        Append a record to the WAL and fsync.
        Returns the LSN assigned to this record.
        """
        with self._lock:
            record.lsn = self._next_lsn()
            with open(self.log_path, "a") as f:
                f.write(record.serialize())
                f.flush()
                os.fsync(f.fileno())   # <-- the key guarantee: log hits disk
            return record.lsn

    def read_from(self, start_lsn: int = 0):
        """Iterate log records from a given LSN (for recovery)."""
        if not os.path.exists(self.log_path):
            return
        with open(self.log_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = LogRecord.deserialize(line)
                if record.lsn >= start_lsn:
                    yield record

    def truncate_before(self, lsn: int):
        """
        Remove log records before `lsn` (post-checkpoint cleanup).
        In production this is done by recycling WAL segment files.
        """
        if not os.path.exists(self.log_path):
            return
        kept = []
        with open(self.log_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = LogRecord.deserialize(line)
                if record.lsn >= lsn:
                    kept.append(record.serialize())
        with open(self.log_path, "w") as f:
            f.writelines(kept)
            f.flush()
            os.fsync(f.fileno())