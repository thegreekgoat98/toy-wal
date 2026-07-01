"""
toywal/db.py

The database engine. Owns the buffer pool, WAL, and checkpoint logic.
On startup, it runs crash recovery before accepting any transactions.
"""

import json
import os
import threading
from .wal import WAL, LogRecord, OpType
from .transaction import Transaction


CHECKPOINT_FILE = "checkpoint.json"


class Database:
    """
    A toy key-value store backed by a Write-Ahead Log.

    Startup sequence (mirrors PostgreSQL):
        1. Load last checkpoint (the stable data snapshot)
        2. Replay WAL from checkpoint LSN forward (REDO phase)
        3. Undo any transactions that never committed (UNDO phase)
        4. Ready.
    """

    def __init__(self, wal_path: str = "wal.log", checkpoint_path: str = CHECKPOINT_FILE):
        self._wal = WAL(log_path=wal_path)
        self._checkpoint_path = checkpoint_path
        self._buffer: dict = {}         # in-memory "buffer pool"
        self._lock = threading.Lock()
        self._txn_counter = 0
        self._last_checkpoint_lsn = 0

        self._recover()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def begin(self) -> Transaction:
        """Start a new transaction."""
        with self._lock:
            self._txn_counter += 1
            txn_id = self._txn_counter
        return Transaction(txn_id, self._wal, self._buffer, self._lock)

    def get(self, key: str):
        """Read directly (outside a transaction). Reads committed state."""
        return self._buffer.get(key)

    def checkpoint(self):
        """
        Flush current buffer state to disk and record checkpoint LSN.
        WAL records before this LSN are no longer needed for recovery.
        """
        with self._lock:
            snapshot = dict(self._buffer)
            # Write a CHECKPOINT record to WAL
            rec = LogRecord(lsn=0, txn_id=0, op=OpType.CHECKPOINT)
            checkpoint_lsn = self._wal.append(rec)

        # Persist the data snapshot
        with open(self._checkpoint_path, "w") as f:
            json.dump({"lsn": checkpoint_lsn, "data": snapshot}, f, indent=2)

        self._last_checkpoint_lsn = checkpoint_lsn
        self._wal.truncate_before(checkpoint_lsn)
        print(f"[CHECKPOINT] LSN={checkpoint_lsn}, keys={list(snapshot.keys())}")

    def dump(self):
        """Show current in-memory state."""
        return dict(self._buffer)

    # ------------------------------------------------------------------ #
    # Recovery                                                             #
    # ------------------------------------------------------------------ #

    def _recover(self):
        """
        ARIES-style recovery:
            1. Load checkpoint (Analysis)
            2. REDO: replay all log records after checkpoint LSN
            3. UNDO: roll back any transaction without a COMMIT
        """
        checkpoint_lsn = self._load_checkpoint()
        print(f"[RECOVERY] Starting from checkpoint LSN={checkpoint_lsn}")

        committed_txns = set()
        aborted_txns = set()

        # --- REDO pass ---
        for record in self._wal.read_from(start_lsn=checkpoint_lsn):
            if record.op == OpType.SET and record.key:
                self._buffer[record.key] = record.new_value
            elif record.op == OpType.DELETE and record.key:
                self._buffer.pop(record.key, None)
            elif record.op == OpType.COMMIT:
                committed_txns.add(record.txn_id)
            elif record.op == OpType.ABORT:
                aborted_txns.add(record.txn_id)

        # --- Determine losers (started but never committed) ---
        started_txns = set()
        for record in self._wal.read_from(start_lsn=checkpoint_lsn):
            if record.op == OpType.BEGIN:
                started_txns.add(record.txn_id)

        losers = started_txns - committed_txns - aborted_txns

        # --- UNDO pass: reverse loser transactions ---
        if losers:
            print(f"[RECOVERY] Undoing incomplete transactions: {losers}")
            undo_records = [
                r for r in self._wal.read_from(0)
                if r.txn_id in losers and r.op in (OpType.SET, OpType.DELETE)
            ]
            for record in reversed(undo_records):
                if record.op == OpType.SET:
                    if record.old_value is None:
                        self._buffer.pop(record.key, None)
                    else:
                        self._buffer[record.key] = record.old_value
                elif record.op == OpType.DELETE:
                    if record.old_value is not None:
                        self._buffer[record.key] = record.old_value

        print(f"[RECOVERY] Done. State: {self._buffer}")

    def _load_checkpoint(self) -> int:
        """Load the last stable snapshot into the buffer pool."""
        if not os.path.exists(self._checkpoint_path):
            return 0
        with open(self._checkpoint_path, "r") as f:
            data = json.load(f)
        self._buffer = data.get("data", {})
        lsn = data.get("lsn", 0)
        self._last_checkpoint_lsn = lsn
        return lsn