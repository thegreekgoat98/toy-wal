"""
toy_wal/transaction.py

Transaction: a unit of work. All changes go through here.
The WAL is written BEFORE the buffer pool is touched.
"""

import threading
from typing import Optional
from .wal import WAL, LogRecord, OpType


class Transaction:
    def __init__(self, txn_id: int, wal: WAL, buffer_pool: dict, lock: threading.Lock):
        self._txn_id = txn_id
        self._wal = wal
        self._buffer = buffer_pool      # shared in-memory store
        self._lock = lock
        self._active = True
        self._wal.append(LogRecord(lsn=0, txn_id=txn_id, op=OpType.BEGIN))

    @property
    def txn_id(self):
        return self._txn_id

    def set(self, key: str, value: str):
        """Write key=value. Log first, then update buffer."""
        self._assert_active()
        with self._lock:
            old = self._buffer.get(key)
            # 1. WAL record hits disk
            self._wal.append(LogRecord(
                lsn=0,
                txn_id=self._txn_id,
                op=OpType.SET,
                key=key,
                old_value=old,
                new_value=value,
            ))
            # 2. Now safe to update the buffer pool
            self._buffer[key] = value

    def delete(self, key: str):
        """Delete a key. Log the old value so recovery can undo."""
        self._assert_active()
        with self._lock:
            old = self._buffer.get(key)
            self._wal.append(LogRecord(
                lsn=0,
                txn_id=self._txn_id,
                op=OpType.DELETE,
                key=key,
                old_value=old,
            ))
            self._buffer.pop(key, None)

    def get(self, key: str) -> Optional[str]:
        """Read from the buffer pool (your own writes are visible)."""
        self._assert_active()
        return self._buffer.get(key)

    def commit(self):
        """
        Flush the COMMIT record. At this point the transaction is durable.
        Even if the process crashes right after, recovery will replay it.
        """
        self._assert_active()
        self._wal.append(LogRecord(lsn=0, txn_id=self._txn_id, op=OpType.COMMIT))
        self._active = False

    def abort(self):
        """
        Undo all changes made by this transaction, then log ABORT.
        In a real DB, undo uses the old_value from WAL records directly.
        """
        self._assert_active()
        # Replay this txn's log records in reverse to undo
        my_records = [
            r for r in self._wal.read_from(0)
            if r.txn_id == self._txn_id and r.op in (OpType.SET, OpType.DELETE)
        ]
        with self._lock:
            for record in reversed(my_records):
                if record.op == OpType.SET:
                    if record.old_value is None:
                        self._buffer.pop(record.key, None)
                    else:
                        self._buffer[record.key] = record.old_value
                elif record.op == OpType.DELETE:
                    if record.old_value is not None:
                        self._buffer[record.key] = record.old_value

        self._wal.append(LogRecord(lsn=0, txn_id=self._txn_id, op=OpType.ABORT))
        self._active = False

    def _assert_active(self):
        if not self._active:
            raise RuntimeError(f"Transaction {self._txn_id} is no longer active.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._active:
            if exc_type:
                self.abort()
            else:
                self.commit()
        return False