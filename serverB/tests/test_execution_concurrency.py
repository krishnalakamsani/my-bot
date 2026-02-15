import os
import time
import threading
import importlib

import pytest


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._last_returned = None

    def execute(self, query, params=None):
        # Simulate INSERT ... RETURNING id
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


@pytest.mark.timeout(10)
def test_concurrent_entry_and_exit(monkeypatch):
    # Ensure execution uses SIMULATE mode so no real broker calls are attempted
    monkeypatch.setenv('SIMULATE', 'true')

    # Patch psycopg2.connect used by serverB.execution before importing it
    monkeypatch.setattr('psycopg2.connect', fake_connect, raising=False)

    # Reload modules to ensure handlers register with patched environment
    import serverB.event_bus as event_bus
    import serverB.position_manager as pm

    # Reload execution to pick up the patched psycopg2.connect
    execution = importlib.reload(importlib.import_module('serverB.execution'))

    # Ensure position manager is fresh
    pm = importlib.reload(pm)

    from serverB.event_bus import publish
    from serverB.position_manager import default_manager

    # Publish many ENTRY_SIGNAL concurrently
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
    assert len(positions) == N, f"Expected {N} positions, found {len(positions)}"

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
    assert len(positions_after) == 0, f"Expected 0 positions after exits, found {len(positions_after)}"
