# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip3 install -r requirements.txt

# Run the application (starts on port 5001)
python3 app.py

# Access at http://localhost:5001
```

There are no tests or linting configured for this project.

## Architecture

This is a Flask web application for tracking NAV (Net Asset Value) erosion in covered call ETFs. It fetches data from Yahoo Finance, calculates erosion metrics, and displays alerts.

### Module Responsibilities

- **app.py**: Flask routes and request handling. All routes defined here, uses the other modules as a data layer.
- **database.py**: SQLite operations via `get_db()` context manager. Tables: `etfs`, `snapshots`, `metrics`, `settings`. Database auto-creates at `data/tracker.db`.
- **fetcher.py**: Yahoo Finance integration using `yfinance`. Key function is `get_monthly_data(ticker)` which returns aggregated monthly prices and distributions.
- **calculator.py**: Core metric calculations. NAV erosion = `(end_price - start_price) / start_price`. True return includes distributions. Flags: OK (> -6%), WARNING (-6% to -10%), SELL (< -10%).

### Data Flow

1. User adds ETF ticker via `/add` route
2. `fetcher.get_monthly_data()` pulls 12 months of price/dividend history from Yahoo Finance
3. Data saved to `snapshots` table, `calculator.calculate_metrics()` computes erosion
4. Metrics saved to `metrics` table with flag determination
5. Dashboard reads from `get_all_latest_metrics()` to display scorecard

### Key Patterns

- All database functions return dicts (converted from sqlite3.Row)
- Fetcher functions return `{'success': bool, 'data': ..., 'error': ...}` pattern
- Thresholds stored as decimals (e.g., -0.06 for -6%) in database, converted to percentages in UI
- Template filters `|pct` and `|currency` for formatting in Jinja2 templates
