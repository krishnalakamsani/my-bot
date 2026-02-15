import os
import sys

# Ensure workspace root is on sys.path so local stub modules (e.g., psycopg2.py) are found
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
SERVERB_DIR = os.path.abspath(os.path.join(ROOT, 'serverB'))
if SERVERB_DIR not in sys.path:
    sys.path.insert(0, SERVERB_DIR)
import time
import threading
import importlib
import sys

# Ensure simulate mode
os.environ['SIMULATE'] = 'true'

# Simple fake DB connection used by execution.record_trade
class FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._last_returned = None

    def execute(self, query, params=None):
        if isinstance(query, str) and 'RETURNING id' in query:
            with self._conn._lock:
                self._conn._last_id += 1
                self._last_returned = self._conn._last_id

    def fetchone(self):
        return (self._last_returned,)

    def close(self):
        return


class FakeConn:
    def __init__(self):
        self._lock = threading.Lock()
        self._last_id = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        return

    def close(self):
        return


def fake_connect(*a, **k):
    return FakeConn()

# Patch psycopg2 before importing modules
import psycopg2
psycopg2.connect = fake_connect

# Import modules (load top-level modules so single instances are shared)
import event_bus as event_bus
import position_manager as pm

# Reload execution to ensure it uses the patched psycopg2
execution = importlib.reload(importlib.import_module('serverB.execution'))
pm = importlib.reload(pm)

from event_bus import publish
# Use the execution module's default_manager to ensure we reference the same instance
default_manager = execution.default_manager

print('Starting concurrency test...')

N = 8
pos_ids = [f"TEST_POS_{i}" for i in range(N)]
threads = []

for pid in pos_ids:
    payload = {
        'side': 'BUY',
        'pos_id': pid,
        'symbol': 'NIFTY',
        'quantity': 1,
        'price': 100.0,
        'security_id': f'SIM_{pid}',
    }
    t = threading.Thread(target=publish, args=('ENTRY_SIGNAL', payload))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

# Allow handler threads to complete
time.sleep(1)

positions = default_manager.list_positions()
print('Positions after entries:', positions)
if len(positions) != N:
    print(f'FAIL: expected {N} positions, found {len(positions)}')
    sys.exit(2)

# Now concurrently publish EXIT_SIGNAL for all positions
threads = []
for pid in pos_ids:
    payload = {'pos_id': pid, 'price': 50.0, 'security_id': f'SIM_{pid}'}
    t = threading.Thread(target=publish, args=('EXIT_SIGNAL', payload))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

time.sleep(1)
positions_after = default_manager.list_positions()
print('Positions after exits:', positions_after)
all_closed = all(bool(v.get('closed_ts')) and v.get('exit_price') is not None for v in positions_after.values())
if not all_closed:
    print(f'FAIL: not all positions marked closed after exits')
    sys.exit(3)

print('PASS')
sys.exit(0)
