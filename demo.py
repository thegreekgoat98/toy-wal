"""
demo.py — Showcase all WAL features end-to-end.

Run this twice to see crash recovery in action:
    python demo.py          # First run: writes data, simulates crash
    python demo.py          # Second run: recovers from WAL, state is intact
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(__file__))
from toy_wal import Database

TEMP_DIR = tempfile.gettempdir()   # C:\Users\...\AppData\Local\Temp on Windows
WAL_PATH = os.path.join(TEMP_DIR, "toywal_demo.log")
CHECKPOINT_PATH = os.path.join(TEMP_DIR, "toywal_checkpoint.json")


def separator(title: str):
    print(f"\n{'─' * 50}")
    print(f"  {title}")
    print(f"{'─' * 50}")


# ------------------------------------------------------------------ #
# 1. Normal committed transactions                                     #
# ------------------------------------------------------------------ #
separator("1. Committed Transactions")

db = Database(wal_path=WAL_PATH, checkpoint_path=CHECKPOINT_PATH)

with db.begin() as txn:
    txn.set("user:1", "Alice")
    txn.set("user:2", "Bob")
    print(f"  SET user:1=Alice, user:2=Bob  (txn={txn.txn_id})")
# context manager auto-commits

with db.begin() as txn:
    txn.set("balance:alice", "1000")
    txn.set("balance:bob", "500")
    print(f"  SET balance:alice=1000, balance:bob=500  (txn={txn.txn_id})")

print(f"\n  DB state: {db.dump()}")


# ------------------------------------------------------------------ #
# 2. Abort / rollback                                                  #
# ------------------------------------------------------------------ #
separator("2. Aborted Transaction (Rollback)")

with db.begin() as txn:
    txn.set("balance:alice", "0")       # simulate a bad transfer
    print(f"  Temporarily set balance:alice=0")
    print(f"  (mid-transaction read): {txn.get('balance:alice')}")
    txn.abort()                          # something went wrong — roll back
    print(f"  Transaction aborted.")

print(f"\n  balance:alice after abort: {db.get('balance:alice')}")  # should be 1000


# ------------------------------------------------------------------ #
# 3. Checkpoint                                                        #
# ------------------------------------------------------------------ #
separator("3. Checkpoint")
db.checkpoint()


# ------------------------------------------------------------------ #
# 4. Simulate crash mid-transaction                                    #
# ------------------------------------------------------------------ #
separator("4. Simulated Crash (incomplete transaction)")

# Start a transaction but DO NOT commit — simulate a crash
txn = db.begin()
txn.set("user:3", "Charlie")
txn.set("balance:charlie", "9999")
print(f"  SET user:3=Charlie, balance:charlie=9999 (txn={txn.txn_id}) — NOT committed")
print(f"  >>> Simulating crash... process dies here <<<")
# No commit. No abort. WAL has the BEGIN + SET records but no COMMIT.


# ------------------------------------------------------------------ #
# 5. Restart and recover                                               #
# ------------------------------------------------------------------ #
separator("5. Recovery After Crash")

db2 = Database(wal_path=WAL_PATH, checkpoint_path=CHECKPOINT_PATH)
state = db2.dump()
print(f"\n  Recovered state: {state}")
print(f"\n  user:3 = {db2.get('user:3')}           ← should be None (rolled back)")
print(f"  balance:charlie = {db2.get('balance:charlie')}  ← should be None (rolled back)")
print(f"  user:1 = {db2.get('user:1')}         ← should be Alice (committed before crash)")
print(f"  balance:alice = {db2.get('balance:alice')}      ← should be 1000 (committed)")


# ------------------------------------------------------------------ #
# Cleanup                                                              #
# ------------------------------------------------------------------ #
separator("Done")
for path in [WAL_PATH, CHECKPOINT_PATH]:
    if os.path.exists(path):
        os.remove(path)
print("  Cleaned up demo files.")