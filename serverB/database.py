# Database operations
import aiosqlite
from config import DB_PATH, config
import logging
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

async def init_db():
    """Initialize SQLite database"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT UNIQUE,
                entry_time TEXT,
                exit_time TEXT,
                option_type TEXT,
                strike INTEGER,
                expiry TEXT,
                entry_price REAL,
                exit_price REAL,
                qty INTEGER,
                pnl REAL,
                exit_reason TEXT,
                mode TEXT,
                index_name TEXT,
                created_at TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS daily_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                total_trades INTEGER,
                total_pnl REAL,
                max_drawdown REAL,
                daily_stop_triggered INTEGER,
                mode TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS candle_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                candle_number INTEGER,
                index_name TEXT,
                interval_seconds INTEGER,
                high REAL,
                low REAL,
                close REAL,
                supertrend_value REAL,
                macd_value REAL,
                signal_status TEXT,
                created_at TEXT
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS tick_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                index_name TEXT,
                index_ltp REAL,
                option_security_id TEXT,
                option_ltp REAL,
                created_at TEXT
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                config_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                applied_at TEXT
            )
        ''')
        await db.commit()

        # Migration: add applied_at if table existed before
        try:
            cursor = await db.execute("PRAGMA table_info(strategies)")
            columns = [row[1] for row in await cursor.fetchall()]
            if 'applied_at' not in columns:
                await db.execute("ALTER TABLE strategies ADD COLUMN applied_at TEXT")
                await db.commit()
                logger.info("[DB] Added applied_at column to strategies table")
        except Exception as e:
            logger.error(f"[DB] Strategies migration error: {e}")
        
        # Migration: Add index_name column if it doesn't exist
        try:
            cursor = await db.execute("PRAGMA table_info(trades)")
            columns = [row[1] for row in await cursor.fetchall()]
            if 'index_name' not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN index_name TEXT DEFAULT 'NIFTY'")
                await db.commit()
                logger.info("[DB] Added index_name column to trades table")
        except Exception as e:
            logger.error(f"[DB] Migration error: {e}")

        # Migration: add interval_seconds to candle_data if table existed before
        try:
            cursor = await db.execute("PRAGMA table_info(candle_data)")
            columns = [row[1] for row in await cursor.fetchall()]
            if 'interval_seconds' not in columns:
                await db.execute("ALTER TABLE candle_data ADD COLUMN interval_seconds INTEGER")
                await db.commit()
                logger.info("[DB] Added interval_seconds column to candle_data table")
        except Exception as e:
            logger.error(f"[DB] Candle_data migration error: {e}")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def upsert_strategy(name: str, strategy_config: dict) -> dict:
    """Create/update a saved strategy.

    Stores config as JSON. Name is unique.
    """
    if not name or not str(name).strip():
        raise ValueError("Strategy name is required")

    name = str(name).strip()
    payload = json.dumps(strategy_config or {}, separators=(",", ":"), ensure_ascii=False)
    now = _utc_now_iso()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            '''
            INSERT INTO strategies (name, config_json, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                config_json=excluded.config_json,
                updated_at=excluded.updated_at
            ''',
            (name, payload, now, now),
        )
        await db.commit()

        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, created_at, updated_at, applied_at FROM strategies WHERE name = ?",
            (name,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else {"name": name}


async def list_strategies() -> list:
    """List saved strategies (metadata only)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, created_at, updated_at, applied_at FROM strategies ORDER BY updated_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_strategy(strategy_id: int) -> dict | None:
    """Get a strategy including its JSON config."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, config_json, created_at, updated_at, applied_at FROM strategies WHERE id = ?",
            (int(strategy_id),),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            data = dict(row)
            try:
                data["config"] = json.loads(data.get("config_json") or "{}")
            except Exception:
                data["config"] = {}
            return data


async def delete_strategy(strategy_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM strategies WHERE id = ?", (int(strategy_id),))
        await db.commit()
        return cur.rowcount > 0


async def rename_strategy(strategy_id: int, new_name: str) -> dict:
    if not new_name or not str(new_name).strip():
        raise ValueError("New name is required")
    new_name = str(new_name).strip()
    now = _utc_now_iso()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE strategies SET name = ?, updated_at = ? WHERE id = ?",
            (new_name, now, int(strategy_id)),
        )
        await db.commit()
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, created_at, updated_at, applied_at FROM strategies WHERE id = ?",
            (int(strategy_id),),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                raise ValueError("Strategy not found")
            return dict(row)


async def duplicate_strategy(strategy_id: int, new_name: str) -> dict:
    original = await get_strategy(strategy_id)
    if not original:
        raise ValueError("Strategy not found")
    return await upsert_strategy(new_name, original.get("config") or {})


async def mark_strategy_applied(strategy_id: int) -> None:
    now = _utc_now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE strategies SET applied_at = ? WHERE id = ?",
            (now, int(strategy_id)),
        )
        await db.commit()


async def export_strategies() -> list:
    """Export strategies as list of {name, config}."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT name, config_json FROM strategies ORDER BY updated_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
            exported = []
            for r in rows:
                cfg = {}
                try:
                    cfg = json.loads(r["config_json"] or "{}")
                except Exception:
                    cfg = {}
                exported.append({"name": r["name"], "config": cfg})
            return exported


async def import_strategies(items: list[dict]) -> dict:
    """Import strategies from a list. Upserts by name."""
    imported = 0
    for item in items or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        cfg = item.get("config")
        if not name or not isinstance(cfg, dict):
            continue
        await upsert_strategy(name, cfg)
        imported += 1
    return {"imported": imported}

async def load_config():
    """Load config from database"""
    try:
        import os

        env_lock = {
            "market_data_provider": "MARKET_DATA_PROVIDER",
            "mds_base_url": "MDS_BASE_URL",
            "mds_poll_seconds": "MDS_POLL_SECONDS",
            "store_tick_data": "STORE_TICK_DATA",
            "store_candle_data": "STORE_CANDLE_DATA",
            "prune_db_on_startup": "PRUNE_DB_ON_STARTUP",
            "max_candle_rows": "MAX_CANDLE_ROWS",
            "max_tick_rows": "MAX_TICK_ROWS",
            "vacuum_db_on_prune": "VACUUM_DB_ON_PRUNE",
            "enable_internal_market_data_service": "ENABLE_INTERNAL_MARKET_DATA_SERVICE",
        }

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute('SELECT key, value FROM config') as cursor:
                rows = await cursor.fetchall()
                logger.info(f"[DB] Loaded {len(rows)} config entries from database")
                for key, value in rows:
                    if key in config:
                        env_var = env_lock.get(key)
                        if env_var and os.getenv(env_var) is not None:
                            continue
                        int_keys = {
                            'order_qty',
                            'max_trades_per_day',
                            'candle_interval',
                            'supertrend_period',
                            'min_trade_gap',
                            'htf_filter_timeframe',
                            'min_hold_seconds',
                            'min_order_cooldown_seconds',
                            'macd_fast',
                            'macd_slow',
                            'macd_signal',
                            'max_candle_rows',
                            'max_tick_rows',
                        }
                        float_keys = {
                            'daily_max_loss',
                            'initial_stoploss',
                            'max_loss_per_trade',
                            'trail_start_profit',
                            'trail_step',
                            'target_points',
                            'risk_per_trade',
                            'supertrend_multiplier',
                            'market_data_poll_seconds',
                            'tick_persist_interval_seconds',
                            'paper_replay_speed',
                            'mds_poll_seconds',
                        }
                        bool_keys = {
                            'trade_only_on_flip',
                            'trading_enabled',
                            'htf_filter_enabled',
                            'macd_confirmation_enabled',
                            'bypass_market_hours',
                            'store_tick_data',
                            'store_candle_data',
                            'pause_market_data_when_closed',
                            'paper_replay_enabled',
                            'prune_db_on_startup',
                            'vacuum_db_on_prune',
                            'enable_internal_market_data_service',
                        }

                        if key in int_keys:
                            config[key] = int(value)
                        elif key in float_keys:
                            config[key] = float(value)
                        elif key in bool_keys:
                            config[key] = str(value).lower() in ('true', '1', 'yes', 'y', 'on')
                        else:
                            config[key] = value
    except Exception as e:
        logger.error(f"Error loading config: {e}")


async def _prune_table_to_last_n(db: aiosqlite.Connection, table: str, max_rows: int) -> int:
    """Keep only the last N rows by id; returns deleted row count."""
    max_rows = int(max_rows)
    if max_rows <= 0:
        return 0

    cur = await db.execute(f"SELECT COUNT(1) FROM {table}")
    count_row = await cur.fetchone()
    total = int(count_row[0] or 0) if count_row else 0
    if total <= max_rows:
        return 0

    # Delete everything older than the Nth latest id.
    cur = await db.execute(
        f"SELECT id FROM {table} ORDER BY id DESC LIMIT 1 OFFSET ?",
        (max_rows - 1,),
    )
    row = await cur.fetchone()
    if not row:
        return 0
    cutoff_id = int(row[0])
    cur = await db.execute(f"DELETE FROM {table} WHERE id < ?", (cutoff_id,))
    return int(cur.rowcount or 0)


async def prune_backend_market_data(*, vacuum: bool | None = None) -> dict:
    """Prune backend SQLite tick/candle tables to keep DB small.

    Behavior:
    - If store_* is False and max_*_rows == 0: delete all rows from that table.
    - If store_* is True and max_*_rows > 0: keep last N rows.
    """
    vacuum = config.get('vacuum_db_on_prune', True) if vacuum is None else bool(vacuum)
    deleted_candles = 0
    deleted_ticks = 0
    pruned = False

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            store_candles = bool(config.get('store_candle_data', False))
            store_ticks = bool(config.get('store_tick_data', False))

            max_candles = int(config.get('max_candle_rows', 0) or 0)
            max_ticks = int(config.get('max_tick_rows', 0) or 0)

            if not store_candles and max_candles == 0:
                cur = await db.execute("DELETE FROM candle_data")
                deleted_candles = int(cur.rowcount or 0)
                pruned = pruned or deleted_candles > 0
            elif store_candles and max_candles > 0:
                deleted_candles = await _prune_table_to_last_n(db, "candle_data", max_candles)
                pruned = pruned or deleted_candles > 0

            if not store_ticks and max_ticks == 0:
                cur = await db.execute("DELETE FROM tick_data")
                deleted_ticks = int(cur.rowcount or 0)
                pruned = pruned or deleted_ticks > 0
            elif store_ticks and max_ticks > 0:
                deleted_ticks = await _prune_table_to_last_n(db, "tick_data", max_ticks)
                pruned = pruned or deleted_ticks > 0

            await db.commit()

            if pruned and vacuum:
                try:
                    await db.execute("VACUUM")
                    await db.commit()
                except Exception as e:
                    logger.warning(f"[DB] VACUUM failed: {e}")

    except Exception as e:
        logger.error(f"[DB] Prune error: {e}")

    return {
        "deleted_candles": deleted_candles,
        "deleted_ticks": deleted_ticks,
        "vacuum": bool(vacuum),
    }

async def save_config():
    """Save config to database"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            for key, value in config.items():
                await db.execute(
                    'INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)',
                    (key, str(value))
                )
            await db.commit()
    except Exception as e:
        logger.error(f"Error saving config: {e}")

async def save_trade(trade_data: dict):
    """Save trade to database"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('''
                INSERT INTO trades (trade_id, entry_time, option_type, strike, expiry, entry_price, qty, mode, index_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade_data['trade_id'],
                trade_data['entry_time'],
                trade_data['option_type'],
                trade_data['strike'],
                trade_data['expiry'],
                trade_data['entry_price'],
                trade_data['qty'],
                trade_data['mode'],
                trade_data.get('index_name', 'NIFTY'),
                trade_data['created_at']
            ))
            await db.commit()
            logger.info(f"[DB] Trade saved: {trade_data['trade_id']}")
    except Exception as e:
        logger.error(f"[DB] Error saving trade: {e}")

async def update_trade_exit(trade_id: str, exit_time: str, exit_price: float, pnl: float, exit_reason: str):
    """Update trade with exit details"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('''
                UPDATE trades 
                SET exit_time = ?, exit_price = ?, pnl = ?, exit_reason = ?
                WHERE trade_id = ?
            ''', (exit_time, exit_price, pnl, exit_reason, trade_id))
            await db.commit()
            logger.info(f"[DB] Trade exit updated: {trade_id}, PnL: {pnl:.2f}")
    except Exception as e:
        logger.error(f"[DB] Error updating trade exit: {e}")


async def update_trade_qty(trade_id: str, qty: int):
    """Update trade quantity (used when scaling out partially)."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                'UPDATE trades SET qty = ? WHERE trade_id = ?',
                (int(qty), str(trade_id)),
            )
            await db.commit()
    except Exception as e:
        logger.error(f"[DB] Error updating trade qty: {e}")

async def get_trades(limit: int = None) -> list:
    """Get trade history
    
    Args:
        limit: Number of trades to fetch. None = all trades
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if limit:
            async with db.execute(
                'SELECT * FROM trades ORDER BY created_at DESC LIMIT ?',
                (limit,)
            ) as cursor:
                rows = await cursor.fetchall()
        else:
            # Fetch all trades
            async with db.execute(
                'SELECT * FROM trades ORDER BY created_at DESC'
            ) as cursor:
                rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_trade_analytics() -> dict:
    """Get comprehensive trade analytics with advanced metrics"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        # Fetch all completed trades
        async with db.execute(
            'SELECT * FROM trades WHERE pnl IS NOT NULL ORDER BY created_at DESC'
        ) as cursor:
            trades = await cursor.fetchall()
        
        trades = [dict(row) for row in trades]
        
        if not trades:
            return {
                'total_trades': 0,
                'total_pnl': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'avg_win': 0,
                'avg_loss': 0,
                'profit_factor': 0,
                'max_profit': 0,
                'max_loss': 0,
                'avg_trade_pnl': 0,
                'trades_by_type': {},
                'trades_by_index': {},
                'trades_by_exit_reason': {},
                'drawdown_analysis': {},
                'trades': []
            }
        
        total_trades = len(trades)
        total_pnl = sum(t['pnl'] for t in trades)
        winning_trades = [t for t in trades if t['pnl'] > 0]
        losing_trades = [t for t in trades if t['pnl'] < 0]
        
        win_count = len(winning_trades)
        loss_count = len(losing_trades)
        total_profit = sum(t['pnl'] for t in winning_trades)
        total_loss = abs(sum(t['pnl'] for t in losing_trades))
        
        # Calculate statistics
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
        avg_win = total_profit / win_count if win_count > 0 else 0
        avg_loss = total_loss / loss_count if loss_count > 0 else 0
        profit_factor = total_profit / total_loss if total_loss > 0 else (total_profit if total_profit > 0 else 0)
        avg_trade_pnl = total_pnl / total_trades if total_trades > 0 else 0
        
        # Calculate standard deviation and Sharpe ratio
        if len(trades) > 1:
            pnls = [t['pnl'] for t in trades]
            mean_pnl = sum(pnls) / len(pnls)
            variance = sum((x - mean_pnl) ** 2 for x in pnls) / len(pnls)
            std_dev = variance ** 0.5
            sharpe_ratio = (avg_trade_pnl / std_dev) if std_dev > 0 else 0
        else:
            std_dev = 0
            sharpe_ratio = 0
        
        # Calculate consecutive wins/losses
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        
        for trade in trades:
            if trade['pnl'] > 0:
                current_wins += 1
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
                current_losses = 0
            else:
                current_losses += 1
                max_consecutive_losses = max(max_consecutive_losses, current_losses)
                current_wins = 0
        
        # Calculate running maximum and drawdown
        running_pnl = 0
        running_max = 0
        max_drawdown = 0
        drawdown_values = []
        
        for trade in reversed(trades):  # Process in chronological order
            running_pnl += trade['pnl']
            if running_pnl > running_max:
                running_max = running_pnl
            current_drawdown = running_max - running_pnl
            max_drawdown = max(max_drawdown, current_drawdown)
            drawdown_values.append(current_drawdown)
        
        # Trades by option type
        trades_by_type = {}
        for trade in trades:
            opt_type = trade['option_type']
            if opt_type not in trades_by_type:
                trades_by_type[opt_type] = []
            trades_by_type[opt_type].append(trade)
        
        # Trades by index
        trades_by_index = {}
        for trade in trades:
            index = trade.get('index_name', 'UNKNOWN')
            if index not in trades_by_index:
                trades_by_index[index] = []
            trades_by_index[index].append(trade)
        
        # Trades by exit reason
        trades_by_exit_reason = {}
        for trade in trades:
            reason = trade.get('exit_reason', 'Unknown')
            if reason not in trades_by_exit_reason:
                trades_by_exit_reason[reason] = []
            trades_by_exit_reason[reason].append(trade)
        
        # Group by day for daily stats
        daily_stats = {}
        for trade in trades:
            entry_date = trade['entry_time'].split('T')[0] if trade['entry_time'] else 'Unknown'
            if entry_date not in daily_stats:
                daily_stats[entry_date] = {'trades': 0, 'pnl': 0, 'wins': 0}
            daily_stats[entry_date]['trades'] += 1
            daily_stats[entry_date]['pnl'] += trade['pnl']
            if trade['pnl'] > 0:
                daily_stats[entry_date]['wins'] += 1
        
        return {
            'total_trades': total_trades,
            'total_pnl': round(total_pnl, 2),
            'winning_trades': win_count,
            'losing_trades': loss_count,
            'win_rate': round(win_rate, 2),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'profit_factor': round(profit_factor, 2),
            'max_profit': round(max((t['pnl'] for t in trades), default=0), 2),
            'max_loss': round(min((t['pnl'] for t in trades), default=0), 2),
            'avg_trade_pnl': round(avg_trade_pnl, 2),
            'std_dev': round(std_dev, 2),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'max_consecutive_wins': max_consecutive_wins,
            'max_consecutive_losses': max_consecutive_losses,
            'max_drawdown': round(max_drawdown, 2),
            'avg_drawdown': round(sum(drawdown_values) / len(drawdown_values), 2) if drawdown_values else 0,
            'trading_days': len(daily_stats),
            'avg_trades_per_day': round(total_trades / len(daily_stats), 2) if daily_stats else 0,
            'trades_by_type': {
                opt_type: {
                    'count': len(type_trades),
                    'pnl': round(sum(t['pnl'] for t in type_trades), 2),
                    'win_rate': round(len([t for t in type_trades if t['pnl'] > 0]) / len(type_trades) * 100, 2) if type_trades else 0
                }
                for opt_type, type_trades in trades_by_type.items()
            },
            'trades_by_index': {
                index: {
                    'count': len(index_trades),
                    'pnl': round(sum(t['pnl'] for t in index_trades), 2),
                    'win_rate': round(len([t for t in index_trades if t['pnl'] > 0]) / len(index_trades) * 100, 2) if index_trades else 0
                }
                for index, index_trades in trades_by_index.items()
            },
            'trades_by_exit_reason': {
                reason: {
                    'count': len(reason_trades),
                    'pnl': round(sum(t['pnl'] for t in reason_trades), 2),
                    'win_rate': round(len([t for t in reason_trades if t['pnl'] > 0]) / len(reason_trades) * 100, 2) if reason_trades else 0
                }
                for reason, reason_trades in trades_by_exit_reason.items()
            },
            'daily_stats': daily_stats,
            'trades': trades
        }
async def save_candle_data(
    candle_number: int,
    index_name: str,
    high: float,
    low: float,
    close: float,
    supertrend_value: float,
    macd_value: float,
    signal_status: str,
    interval_seconds: int | None = None,
):
    """Save candle data for analysis"""
    try:
        if not bool(config.get('store_candle_data', False)):
            return
        from datetime import datetime, timezone
        timestamp = datetime.now(timezone.utc).isoformat()
        
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                '''INSERT INTO candle_data 
                   (timestamp, candle_number, index_name, interval_seconds, high, low, close, supertrend_value, macd_value, signal_status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    timestamp,
                    candle_number,
                    index_name,
                    int(interval_seconds) if interval_seconds is not None else None,
                    high,
                    low,
                    close,
                    supertrend_value,
                    macd_value,
                    signal_status,
                    timestamp,
                )
            )
            await db.commit()
            # Optional retention
            max_rows = int(config.get('max_candle_rows', 0) or 0)
            if max_rows > 0:
                try:
                    await _prune_table_to_last_n(db, "candle_data", max_rows)
                    await db.commit()
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"[DB] Error saving candle data: {e}")


async def save_tick_data(
    index_name: str,
    index_ltp: float,
    option_security_id: str | None = None,
    option_ltp: float | None = None,
    timestamp: str | None = None,
):
    """Persist raw tick data (index + optional option LTP) for later analysis/replay."""
    try:
        if not bool(config.get('store_tick_data', False)):
            return
        ts = str(timestamp or _utc_now_iso())
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                '''INSERT INTO tick_data (timestamp, index_name, index_ltp, option_security_id, option_ltp, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)''',
                (
                    ts,
                    str(index_name),
                    float(index_ltp or 0.0),
                    str(option_security_id) if option_security_id else None,
                    float(option_ltp) if option_ltp is not None else None,
                    ts,
                ),
            )
            await db.commit()
            max_rows = int(config.get('max_tick_rows', 0) or 0)
            if max_rows > 0:
                try:
                    await _prune_table_to_last_n(db, "tick_data", max_rows)
                    await db.commit()
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"[DB] Error saving tick data: {e}")

async def get_candle_data(limit: int = 1000, index_name: str = None):
    """Retrieve candle data for analysis"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            
            if index_name:
                query = f'SELECT * FROM candle_data WHERE index_name = ? ORDER BY candle_number DESC LIMIT {limit}'
                async with db.execute(query, (index_name,)) as cursor:
                    rows = await cursor.fetchall()
            else:
                query = f'SELECT * FROM candle_data ORDER BY candle_number DESC LIMIT {limit}'
                async with db.execute(query) as cursor:
                    rows = await cursor.fetchall()
            
            return [dict(row) for row in reversed(rows)]  # Return in ascending order
    except Exception as e:
        logger.error(f"[DB] Error retrieving candle data: {e}")
        return []


async def get_candle_data_for_replay(index_name: str, interval_seconds: int, date_ist: str | None = None, limit: int = 20000) -> list:
    """Fetch candles for replay.

    Args:
        index_name: e.g. 'NIFTY'
        interval_seconds: e.g. 5/15/30/60/300/900
        date_ist: 'YYYY-MM-DD' in IST. If omitted, returns latest `limit` candles.

    Note: candle_data timestamps are stored in UTC ISO strings.
    We approximate IST date matching by shifting UTC timestamp by +5:30 within SQLite.
    """
    try:
        interval_seconds = int(interval_seconds)
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row

            if date_ist:
                query = f'''
                    SELECT *
                    FROM candle_data
                    WHERE index_name = ?
                      AND (interval_seconds = ? OR interval_seconds IS NULL)
                      AND date(datetime(timestamp), '+5 hours', '+30 minutes') = date(?)
                    ORDER BY id ASC
                    LIMIT {int(limit)}
                '''
                params = (str(index_name), int(interval_seconds), str(date_ist))
                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
            else:
                query = f'''
                    SELECT *
                    FROM candle_data
                    WHERE index_name = ?
                      AND (interval_seconds = ? OR interval_seconds IS NULL)
                    ORDER BY id DESC
                    LIMIT {int(limit)}
                '''
                async with db.execute(query, (str(index_name), int(interval_seconds))) as cursor:
                    rows = await cursor.fetchall()
                rows = list(reversed(rows))

            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"[DB] Error retrieving replay candles: {e}")
        return []