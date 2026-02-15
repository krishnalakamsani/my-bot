"""Lightweight psycopg2 stub for tests (provides connect function).
This file exists only to allow test imports in the workspace test environment.
"""

def connect(*args, **kwargs):
    raise NotImplementedError("psycopg2.connect should be monkeypatched in tests")
