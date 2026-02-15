# Utility functions
from datetime import datetime, timezone, timedelta

def get_ist_time():
    """Get current IST time"""
    utc_now = datetime.now(timezone.utc)
    ist = utc_now + timedelta(hours=5, minutes=30)
    return ist

def is_market_open():
    """Check if market is open (9:15 AM - 3:30 PM IST, Monday-Friday)"""
    try:
        from config import config as _config
        if _config.get('bypass_market_hours', False):
            return True
    except Exception:
        pass

    ist = get_ist_time()
    
    # Check if it's a weekday (Monday=0 to Friday=4, Saturday=5, Sunday=6)
    if ist.weekday() >= 5:  # Saturday or Sunday
        return False
    
    market_open = ist.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = ist.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= ist <= market_close

def can_take_new_trade():
    """Check if new trades are allowed (before 3:10 PM IST, weekday only)"""
    try:
        from config import config as _config
        if _config.get('bypass_market_hours', False):
            return True
    except Exception:
        pass

    ist = get_ist_time()
    
    # Check if it's a weekday
    if ist.weekday() >= 5:  # Saturday or Sunday
        return False
    
    cutoff_time = ist.replace(hour=15, minute=10, second=0, microsecond=0)
    return ist < cutoff_time

def should_force_squareoff():
    """Check if it's time to force square off (3:25 PM IST, weekday only)"""
    try:
        from config import config as _config
        if _config.get('bypass_market_hours', False):
            return False
    except Exception:
        pass

    ist = get_ist_time()
    
    # Check if it's a weekday
    if ist.weekday() >= 5:  # Saturday or Sunday
        return False
    
    squareoff_time = ist.replace(hour=15, minute=25, second=0, microsecond=0)
    return ist >= squareoff_time

def get_expiry_date(expiry_day: int) -> str:
    """Calculate next expiry date based on expiry day of week"""
    ist = get_ist_time()
    days_until_expiry = (expiry_day - ist.weekday()) % 7
    if days_until_expiry == 0:
        if ist.hour >= 15 and ist.minute >= 30:
            days_until_expiry = 7
    expiry_date = ist + timedelta(days=days_until_expiry)
    return expiry_date.strftime("%Y-%m-%d")

def format_timeframe(seconds: int) -> str:
    """Format timeframe seconds to human readable string"""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes}m"
    else:
        hours = seconds // 3600
        return f"{hours}h"
