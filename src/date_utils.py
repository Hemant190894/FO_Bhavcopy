import pandas as pd
from datetime import datetime, timedelta
import config
import logging

def load_holidays():
    """Load NSE holidays from CSV file"""
    try:
        holidays_df = pd.read_csv(config.NSE_HOLIDAYS_FILE)
        holidays = [datetime.strptime(date, '%Y-%m-%d') for date in holidays_df['Date']]
        logging.info(f"Loaded {len(holidays)} holidays from {config.NSE_HOLIDAYS_FILE}")
        return holidays
    except Exception as e:
        logging.error(f"Error loading holidays: {e}")
        return []

# Load holidays when module is imported
NSE_HOLIDAYS = load_holidays()

def is_market_holiday(date):
    """Check if given date is a market holiday"""
    # Convert to datetime if date string is provided
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')
    
    # Check if it's weekend
    if date.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        return True
    
    # Check if it's a holiday
    return date.replace(hour=0, minute=0, second=0, microsecond=0) in NSE_HOLIDAYS

def get_next_trading_day(date):
    """Get the next trading day after the given date"""
    next_day = date + timedelta(days=1)
    while is_market_holiday(next_day):
        next_day += timedelta(days=1)
    return next_day

def get_valid_dates(target_date=None):
    """
    Get valid dates for different file types
    Returns:
    """
    if target_date is None:
        target_date = datetime.now()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d')
        
    # If target date is a holiday, move to next trading day
    while is_market_holiday(target_date):
        target_date = get_next_trading_day(target_date)
    
    # For secban, we need the next trading day
    secban_date = get_next_trading_day(target_date)
    
    return {
        'bhavcopy': target_date,
        'volatility': target_date,
        'secban': secban_date
    }
