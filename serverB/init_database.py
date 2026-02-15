#!/usr/bin/env python3
"""
Database initialization script
Run this to create/reset the database schema
"""

import asyncio
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / 'backend'))

from database import init_db

async def main():
    print("Initializing database...")
    try:
        await init_db()
        print("✓ Database initialized successfully!")
        print("✓ Tables created: trades, daily_stats, config, candle_data")
    except Exception as e:
        print(f"✗ Error initializing database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
