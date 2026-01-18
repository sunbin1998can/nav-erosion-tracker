"""
Yahoo Finance data fetching for NAV Erosion Tracker.
Uses yfinance library to fetch price and distribution data.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


def fetch_etf_data(ticker, start_date=None, end_date=None):
    """
    Fetch price history for an ETF.

    Args:
        ticker: Yahoo Finance ticker (e.g., 'HMAX.TO')
        start_date: Start date string 'YYYY-MM-DD' (default: 1 year ago)
        end_date: End date string 'YYYY-MM-DD' (default: today)

    Returns:
        dict with 'success', 'data' (DataFrame), and 'error' keys
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    try:
        etf = yf.Ticker(ticker)
        hist = etf.history(start=start_date, end=end_date)

        if hist.empty:
            return {
                'success': False,
                'data': None,
                'error': f'No data found for ticker {ticker}'
            }

        # Reset index to have Date as a column
        hist = hist.reset_index()
        hist['Date'] = pd.to_datetime(hist['Date']).dt.strftime('%Y-%m-%d')

        return {
            'success': True,
            'data': hist,
            'error': None
        }

    except Exception as e:
        return {
            'success': False,
            'data': None,
            'error': str(e)
        }


def fetch_distributions(ticker, start_date=None, end_date=None):
    """
    Fetch dividend/distribution history for an ETF.

    Args:
        ticker: Yahoo Finance ticker
        start_date: Start date string 'YYYY-MM-DD'
        end_date: End date string 'YYYY-MM-DD'

    Returns:
        dict with 'success', 'data' (DataFrame), and 'error' keys
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    try:
        etf = yf.Ticker(ticker)
        dividends = etf.dividends

        if dividends.empty:
            return {
                'success': True,
                'data': pd.DataFrame(columns=['Date', 'Dividend']),
                'error': None
            }

        # Filter by date range
        dividends = dividends.loc[start_date:end_date]

        # Convert to DataFrame with Date column
        df = dividends.reset_index()
        df.columns = ['Date', 'Dividend']
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

        return {
            'success': True,
            'data': df,
            'error': None
        }

    except Exception as e:
        return {
            'success': False,
            'data': None,
            'error': str(e)
        }


def fetch_etf_info(ticker):
    """
    Fetch basic info about an ETF to validate the ticker.

    Returns:
        dict with 'success', 'name', 'currency', and 'error' keys
    """
    try:
        etf = yf.Ticker(ticker)
        info = etf.info

        # Check if we got valid data
        if not info or info.get('regularMarketPrice') is None:
            # Try to get history as a fallback check
            hist = etf.history(period='5d')
            if hist.empty:
                return {
                    'success': False,
                    'name': None,
                    'currency': None,
                    'error': f'Invalid ticker: {ticker}'
                }

        name = info.get('shortName') or info.get('longName') or ticker
        currency = info.get('currency', 'USD')

        return {
            'success': True,
            'name': name,
            'currency': currency,
            'error': None
        }

    except Exception as e:
        return {
            'success': False,
            'name': None,
            'currency': None,
            'error': str(e)
        }


def fetch_all_data(ticker, months=12):
    """
    Fetch both price and distribution data for an ETF.

    Args:
        ticker: Yahoo Finance ticker
        months: Number of months of history to fetch

    Returns:
        dict with price data, distribution data, and any errors
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 31)

    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    price_result = fetch_etf_data(ticker, start_str, end_str)
    dist_result = fetch_distributions(ticker, start_str, end_str)

    return {
        'prices': price_result,
        'distributions': dist_result,
        'ticker': ticker,
        'start_date': start_str,
        'end_date': end_str
    }


def get_monthly_data(ticker, months=12):
    """
    Get monthly closing prices and distributions.

    Returns data aggregated by month for simpler analysis.
    """
    result = fetch_all_data(ticker, months)

    if not result['prices']['success']:
        return {
            'success': False,
            'error': result['prices']['error'],
            'monthly_data': None
        }

    prices_df = result['prices']['data']

    # Convert to datetime for grouping
    prices_df['DateDT'] = pd.to_datetime(prices_df['Date'])
    prices_df['YearMonth'] = prices_df['DateDT'].dt.to_period('M')

    # Get last close price of each month
    monthly_prices = prices_df.groupby('YearMonth').agg({
        'Close': 'last',
        'Date': 'last'
    }).reset_index()

    # Process distributions if available
    monthly_dist = {}
    if result['distributions']['success'] and not result['distributions']['data'].empty:
        dist_df = result['distributions']['data']
        dist_df['DateDT'] = pd.to_datetime(dist_df['Date'])
        dist_df['YearMonth'] = dist_df['DateDT'].dt.to_period('M')

        dist_by_month = dist_df.groupby('YearMonth')['Dividend'].sum()
        monthly_dist = dist_by_month.to_dict()

    # Combine into monthly data
    monthly_data = []
    for _, row in monthly_prices.iterrows():
        ym = row['YearMonth']
        monthly_data.append({
            'year_month': str(ym),
            'date': row['Date'],
            'close_price': float(row['Close']),
            'distribution': float(monthly_dist.get(ym, 0))
        })

    return {
        'success': True,
        'error': None,
        'monthly_data': monthly_data,
        'raw_prices': result['prices']['data'],
        'raw_distributions': result['distributions']['data'] if result['distributions']['success'] else None
    }
