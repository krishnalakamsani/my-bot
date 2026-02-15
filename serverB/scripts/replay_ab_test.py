#!/usr/bin/env python3
"""Simple replay A/B tester: run legacy vs tuned thresholds over historical candles

Usage: run from repo root:
  python3 scripts/replay_ab_test.py --date 2026-02-11 --interval 5 --index NIFTY

This script loads candles from backend DB and simulates entry/exit decisions
using the existing ScoreEngine + ScoreMdsRunner logic. It does NOT simulate
option prices; it counts exits by reason (Reversal/Neutral/Momentum/Force).
"""
import argparse
import asyncio
import statistics
from datetime import datetime, timedelta

import sys
sys.path.insert(0, '.')

from backend.database import get_candle_data_for_replay, init_db
from backend.score_engine import ScoreEngine, Candle
from backend.strategies.runner import ScoreMdsRunner
from backend import config as cfg


async def run_replay(date_ist, interval, index_name, legacy_mode):
    # Ensure DB exists
    await init_db()

    candles = await get_candle_data_for_replay(index_name, interval, date_ist, limit=20000)
    if not candles:
        print(f"No candles found for {index_name} {interval}s date={date_ist}")
        return None

    # Toggle legacy flag in runtime config
    cfg.config['use_legacy_thresholds'] = bool(legacy_mode)

    se = ScoreEngine(
        st_period=int(cfg.config.get('supertrend_period', 7)),
        st_multiplier=float(cfg.config.get('supertrend_multiplier', 4)),
        macd_fast=int(cfg.config.get('macd_fast', 12)),
        macd_slow=int(cfg.config.get('macd_slow', 26)),
        macd_signal=int(cfg.config.get('macd_signal', 9)),
        base_timeframe_seconds=int(cfg.config.get('candle_interval', interval) or interval),
        bonus_macd_triple=float(cfg.config.get('mds_bonus_macd_triple', 1.0)),
        bonus_macd_momentum=float(cfg.config.get('mds_bonus_macd_momentum', 0.5)),
        bonus_macd_cross=float(cfg.config.get('mds_bonus_macd_cross', 0.5)),
    )

    runner = ScoreMdsRunner()

    in_position = False
    position_type = None
    entry_candle_idx = None
    trades = []

    min_hold = int(cfg.config.get('min_hold_seconds', 0) or 0)

    for i, c in enumerate(candles):
        # Build Candle
        candle = Candle(high=float(c.get('high') or 0.0), low=float(c.get('low') or 0.0), close=float(c.get('close') or 0.0))
        snap = se.on_base_candle(candle)

        # compute slow_mom similar to trading_bot: use macd_score + hist_score from slow TF if available
        tf_scores = snap.tf_scores or {}
        slow_mom = 0.0
        try:
            slow_tf = max(int(k) for k in tf_scores.keys())
            slow = tf_scores.get(slow_tf)
            if slow:
                slow_mom = float(getattr(slow, 'macd_score', 0.0) or 0.0) + float(getattr(slow, 'hist_score', 0.0) or 0.0)
        except Exception:
            slow_mom = 0.0

        score = float(snap.score or 0.0)
        slope = float(snap.slope or 0.0)

        if in_position:
            # decide exit
            ed = runner.decide_exit(position_type=position_type, score=score, slope=slope, slow_mom=slow_mom)
            if ed.should_exit:
                trades.append({'entry_idx': entry_candle_idx, 'exit_idx': i, 'type': position_type, 'exit_reason': ed.reason})
                in_position = False
                position_type = None
                entry_candle_idx = None
                runner.on_entry_attempted()
            continue

        # not in position: decide entry
        ready = bool(snap.ready)
        is_choppy = bool(snap.is_choppy)
        direction = snap.direction or 'NONE'

        confirm_needed = 1  # use 1 for replay/analysis
        ed = runner.decide_entry(ready=ready, is_choppy=is_choppy, direction=direction, score=score, slope=slope, confirm_needed=confirm_needed)
        if ed.should_enter:
            # enforce min_hold as simple check (we'll record entry index)
            in_position = True
            position_type = 'CE' if ed.option_type == 'CE' else 'PE'
            entry_candle_idx = i
            runner.on_entry_attempted()

    # any open position at end -> mark Force Square-off
    if in_position:
        trades.append({'entry_idx': entry_candle_idx, 'exit_idx': len(candles)-1, 'type': position_type, 'exit_reason': 'Force Square-off'})

    # summarize
    summary = {}
    summary['total_trades'] = len(trades)
    by_reason = {}
    for t in trades:
        r = t['exit_reason'] or 'unknown'
        by_reason[r] = by_reason.get(r, 0) + 1
    summary['by_reason'] = by_reason
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=False, help='Replay IST date YYYY-MM-DD (optional, omit for latest)')
    parser.add_argument('--interval', type=int, default=5, help='Base timeframe seconds')
    parser.add_argument('--index', default='NIFTY', help='Index name')
    args = parser.parse_args()

    loop = asyncio.get_event_loop()

    print('Running legacy (pre-tuned) replay...')
    legacy = loop.run_until_complete(run_replay(args.date, args.interval, args.index, legacy_mode=True))
    print(legacy)

    print('\nRunning tuned replay...')
    tuned = loop.run_until_complete(run_replay(args.date, args.interval, args.index, legacy_mode=False))
    print(tuned)


if __name__ == '__main__':
    main()
