# Index configurations for trading
# Each index has different security ID, lot size, strike interval, etc.

INDICES = {
    "NIFTY": {
        "name": "NIFTY 50",
        "security_id": 13,
        "exchange_segment": "IDX_I",
        "fno_segment": "NSE_FNO",
        "lot_size": 65,
        "strike_interval": 50,  # Round to nearest 50
        "expiry_day": 1,  # Tuesday (0=Monday, 1=Tuesday, etc.)
        "expiry_type": "weekly",  # weekly expiry
        "trading_symbol": "NIFTY",
    },
    "BANKNIFTY": {
        "name": "BANK NIFTY",
        "security_id": 25,
        "exchange_segment": "IDX_I",
        "fno_segment": "NSE_FNO",
        "lot_size": 30,
        "strike_interval": 100,  # Round to nearest 100
        "expiry_day": 1,  # Last Tuesday of month
        "expiry_type": "monthly",  # monthly expiry (last Tuesday)
        "trading_symbol": "BANKNIFTY",
    },
    "SENSEX": {
        "name": "SENSEX",
        "security_id": 51,
        "exchange_segment": "IDX_I",  # Dhan uses IDX_I for BSE indices too
        "fno_segment": "BSE_FNO",
        "lot_size": 20,
        "strike_interval": 100,  # Round to nearest 100
        "expiry_day": 3,  # Thursday (0=Monday, 3=Thursday)
        "expiry_type": "weekly",  # weekly expiry
        "trading_symbol": "SENSEX",
    },
    "FINNIFTY": {
        "name": "FINNIFTY",
        "security_id": 27,
        "exchange_segment": "IDX_I",
        "fno_segment": "NSE_FNO",
        "lot_size": 60,
        "strike_interval": 50,
        "expiry_day": 1,  # Last Tuesday of month
        "expiry_type": "monthly",  # monthly expiry (last Tuesday)
        "trading_symbol": "FINNIFTY",
    },
}

def get_index_config(index_name: str) -> dict:
    """Get configuration for an index"""
    return INDICES.get(index_name.upper(), INDICES["NIFTY"])

def get_available_indices() -> list:
    """Get list of available indices"""
    return list(INDICES.keys())

def round_to_strike(price: float, index_name: str) -> int:
    """Round price to nearest strike for the given index"""
    config = get_index_config(index_name)
    interval = config["strike_interval"]
    return round(price / interval) * interval
