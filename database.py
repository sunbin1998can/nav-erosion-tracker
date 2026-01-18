"""
Database operations for NAV Erosion Tracker.
Uses SQLite for simplicity - no installation required.
"""

import sqlite3
import os
from datetime import datetime
from contextlib import contextmanager

DATABASE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'tracker.db')


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Initialize database with required tables."""
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)

    with get_db() as conn:
        cursor = conn.cursor()

        # ETFs being tracked
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etfs (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                ticker TEXT NOT NULL UNIQUE,
                warn_threshold REAL DEFAULT -0.06,
                sell_threshold REAL DEFAULT -0.10,
                added_date TEXT NOT NULL,
                active INTEGER DEFAULT 1
            )
        ''')

        # Monthly snapshots
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY,
                etf_id INTEGER NOT NULL,
                snapshot_date TEXT NOT NULL,
                close_price REAL NOT NULL,
                distribution REAL DEFAULT 0,
                FOREIGN KEY (etf_id) REFERENCES etfs(id),
                UNIQUE(etf_id, snapshot_date)
            )
        ''')

        # Calculated metrics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY,
                etf_id INTEGER NOT NULL,
                calc_date TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                start_price REAL NOT NULL,
                end_price REAL NOT NULL,
                total_distributions REAL NOT NULL,
                nav_erosion_pct REAL NOT NULL,
                true_return_pct REAL NOT NULL,
                flag TEXT NOT NULL,
                FOREIGN KEY (etf_id) REFERENCES etfs(id)
            )
        ''')

        # Settings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')


def add_etf(name, ticker, warn_threshold=-0.06, sell_threshold=-0.10):
    """Add a new ETF to track."""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO etfs (name, ticker, warn_threshold, sell_threshold, added_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, ticker.upper(), warn_threshold, sell_threshold, datetime.now().strftime('%Y-%m-%d')))
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None  # Ticker already exists


def remove_etf(etf_id):
    """Remove an ETF from tracking (soft delete)."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE etfs SET active = 0 WHERE id = ?', (etf_id,))
        return cursor.rowcount > 0


def delete_etf(etf_id):
    """Permanently delete an ETF and all its data."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM metrics WHERE etf_id = ?', (etf_id,))
        cursor.execute('DELETE FROM snapshots WHERE etf_id = ?', (etf_id,))
        cursor.execute('DELETE FROM etfs WHERE id = ?', (etf_id,))
        return cursor.rowcount > 0


def get_all_etfs(active_only=True):
    """Get all tracked ETFs."""
    with get_db() as conn:
        cursor = conn.cursor()
        if active_only:
            cursor.execute('SELECT * FROM etfs WHERE active = 1 ORDER BY name')
        else:
            cursor.execute('SELECT * FROM etfs ORDER BY name')
        return [dict(row) for row in cursor.fetchall()]


def get_etf(etf_id):
    """Get a single ETF by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM etfs WHERE id = ?', (etf_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_etf_by_ticker(ticker):
    """Get a single ETF by ticker."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM etfs WHERE ticker = ?', (ticker.upper(),))
        row = cursor.fetchone()
        return dict(row) if row else None


def save_snapshot(etf_id, date, close_price, distribution=0):
    """Save a price/distribution snapshot."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO snapshots (etf_id, snapshot_date, close_price, distribution)
            VALUES (?, ?, ?, ?)
        ''', (etf_id, date, close_price, distribution))
        return cursor.lastrowid


def get_etf_snapshots(etf_id, limit=None):
    """Get snapshots for an ETF, ordered by date descending."""
    with get_db() as conn:
        cursor = conn.cursor()
        query = 'SELECT * FROM snapshots WHERE etf_id = ? ORDER BY snapshot_date DESC'
        if limit:
            query += f' LIMIT {limit}'
        cursor.execute(query, (etf_id,))
        return [dict(row) for row in cursor.fetchall()]


def get_etf_snapshots_range(etf_id, start_date, end_date):
    """Get snapshots for an ETF within a date range."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM snapshots
            WHERE etf_id = ? AND snapshot_date >= ? AND snapshot_date <= ?
            ORDER BY snapshot_date ASC
        ''', (etf_id, start_date, end_date))
        return [dict(row) for row in cursor.fetchall()]


def save_metrics(etf_id, metrics_dict):
    """Save calculated metrics."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO metrics (
                etf_id, calc_date, window_start, window_end,
                start_price, end_price, total_distributions,
                nav_erosion_pct, true_return_pct, flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            etf_id,
            metrics_dict['calc_date'],
            metrics_dict['window_start'],
            metrics_dict['window_end'],
            metrics_dict['start_price'],
            metrics_dict['end_price'],
            metrics_dict['total_distributions'],
            metrics_dict['nav_erosion_pct'],
            metrics_dict['true_return_pct'],
            metrics_dict['flag']
        ))
        return cursor.lastrowid


def get_latest_metrics(etf_id):
    """Get the most recent metrics for an ETF."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM metrics WHERE etf_id = ?
            ORDER BY calc_date DESC LIMIT 1
        ''', (etf_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_metrics_history(etf_id, limit=12):
    """Get metrics history for an ETF."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM metrics WHERE etf_id = ?
            ORDER BY calc_date DESC LIMIT ?
        ''', (etf_id, limit))
        return [dict(row) for row in cursor.fetchall()]


def get_all_latest_metrics():
    """Get latest metrics for all active ETFs."""
    etfs = get_all_etfs()
    results = []
    for etf in etfs:
        metrics = get_latest_metrics(etf['id'])
        results.append({
            'etf': etf,
            'metrics': metrics
        })
    return results


def update_etf_thresholds(etf_id, warn_threshold, sell_threshold):
    """Update thresholds for an ETF."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE etfs SET warn_threshold = ?, sell_threshold = ?
            WHERE id = ?
        ''', (warn_threshold, sell_threshold, etf_id))
        return cursor.rowcount > 0


def get_setting(key, default=None):
    """Get a setting value."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        row = cursor.fetchone()
        return row['value'] if row else default


def set_setting(key, value):
    """Set a setting value."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)
        ''', (key, str(value)))


def get_all_settings():
    """Get all settings as a dictionary."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT key, value FROM settings')
        return {row['key']: row['value'] for row in cursor.fetchall()}


def clear_etf_data(etf_id):
    """Clear all snapshots and metrics for an ETF (for re-fetching)."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM metrics WHERE etf_id = ?', (etf_id,))
        cursor.execute('DELETE FROM snapshots WHERE etf_id = ?', (etf_id,))
