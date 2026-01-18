"""
NAV Erosion calculations for covered call ETFs.
Implements the core metrics: NAV erosion %, true return %, and flag determination.
"""

from datetime import datetime


def calculate_nav_erosion(start_price, end_price):
    """
    Calculate NAV erosion percentage.

    NAV erosion = (end_price - start_price) / start_price

    A negative value indicates erosion (price decline).
    """
    if start_price <= 0:
        return 0
    return (end_price - start_price) / start_price


def calculate_true_return(start_price, end_price, total_distributions):
    """
    Calculate true return (income-adjusted return).

    True return = ((end_price - start_price) + total_distributions) / start_price

    This accounts for distributions received, giving the actual return to investors.
    """
    if start_price <= 0:
        return 0
    return ((end_price - start_price) + total_distributions) / start_price


def get_flag(nav_erosion_pct, warn_threshold=-0.06, sell_threshold=-0.10):
    """
    Determine status flag based on NAV erosion.

    Args:
        nav_erosion_pct: NAV erosion as decimal (e.g., -0.08 for -8%)
        warn_threshold: Threshold for WARNING (default -6%)
        sell_threshold: Threshold for SELL (default -10%)

    Returns:
        'OK', 'WARNING', or 'SELL'
    """
    if nav_erosion_pct <= sell_threshold:
        return 'SELL'
    elif nav_erosion_pct <= warn_threshold:
        return 'WARNING'
    return 'OK'


def calculate_metrics(monthly_data, warn_threshold=-0.06, sell_threshold=-0.10):
    """
    Calculate all metrics from monthly data.

    Args:
        monthly_data: List of dicts with 'date', 'close_price', 'distribution'
        warn_threshold: Threshold for WARNING flag
        sell_threshold: Threshold for SELL flag

    Returns:
        dict with all calculated metrics
    """
    if not monthly_data or len(monthly_data) < 2:
        return None

    # Sort by date to ensure correct order
    sorted_data = sorted(monthly_data, key=lambda x: x['date'])

    start_price = sorted_data[0]['close_price']
    end_price = sorted_data[-1]['close_price']
    total_distributions = sum(d['distribution'] for d in sorted_data)

    nav_erosion_pct = calculate_nav_erosion(start_price, end_price)
    true_return_pct = calculate_true_return(start_price, end_price, total_distributions)
    flag = get_flag(nav_erosion_pct, warn_threshold, sell_threshold)

    return {
        'calc_date': datetime.now().strftime('%Y-%m-%d'),
        'window_start': sorted_data[0]['date'],
        'window_end': sorted_data[-1]['date'],
        'start_price': start_price,
        'end_price': end_price,
        'total_distributions': total_distributions,
        'nav_erosion_pct': nav_erosion_pct,
        'true_return_pct': true_return_pct,
        'flag': flag
    }


def generate_monthly_breakdown(monthly_data):
    """
    Generate a monthly breakdown table with cumulative erosion.

    Returns list of dicts with:
        - month
        - close_price
        - distribution
        - cumulative_erosion_pct
    """
    if not monthly_data:
        return []

    sorted_data = sorted(monthly_data, key=lambda x: x['date'])
    start_price = sorted_data[0]['close_price']

    breakdown = []
    for data in sorted_data:
        cumulative_erosion = calculate_nav_erosion(start_price, data['close_price'])
        breakdown.append({
            'month': data.get('year_month', data['date'][:7]),
            'date': data['date'],
            'close_price': data['close_price'],
            'distribution': data['distribution'],
            'cumulative_erosion_pct': cumulative_erosion
        })

    return breakdown


def calculate_distribution_yield(monthly_data):
    """
    Calculate annualized distribution yield.

    Uses average monthly distribution and latest price.
    """
    if not monthly_data:
        return 0

    sorted_data = sorted(monthly_data, key=lambda x: x['date'])
    total_distributions = sum(d['distribution'] for d in sorted_data)
    months = len(sorted_data)
    current_price = sorted_data[-1]['close_price']

    if current_price <= 0 or months == 0:
        return 0

    # Annualize the yield
    monthly_avg = total_distributions / months
    annual_distributions = monthly_avg * 12
    return annual_distributions / current_price


def format_percentage(value):
    """Format a decimal as a percentage string."""
    return f"{value * 100:.2f}%"


def get_flag_color(flag):
    """Get Bootstrap color class for a flag."""
    colors = {
        'OK': 'success',
        'WARNING': 'warning',
        'SELL': 'danger'
    }
    return colors.get(flag, 'secondary')


def summarize_metrics(metrics):
    """
    Create a human-readable summary of metrics.
    """
    if not metrics:
        return "No data available"

    return {
        'nav_erosion': format_percentage(metrics['nav_erosion_pct']),
        'true_return': format_percentage(metrics['true_return_pct']),
        'total_distributions': f"${metrics['total_distributions']:.2f}",
        'price_change': f"${metrics['end_price'] - metrics['start_price']:.2f}",
        'flag': metrics['flag'],
        'flag_color': get_flag_color(metrics['flag'])
    }
