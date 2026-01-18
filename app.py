"""
NAV Erosion Tracker - Flask Web Application
Track covered call ETF NAV erosion with alerts.
"""

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, Response
import csv
import io
from datetime import datetime

import database as db
import fetcher
import calculator

app = Flask(__name__)
app.secret_key = 'nav-erosion-tracker-secret-key-change-in-production'


@app.before_request
def before_request():
    """Initialize database before first request."""
    db.init_db()


@app.route('/')
def dashboard():
    """Main dashboard with scorecard of all tracked ETFs."""
    etf_data = db.get_all_latest_metrics()

    # Check for alerts
    alerts = []
    for item in etf_data:
        if item['metrics'] and item['metrics']['flag'] != 'OK':
            alerts.append({
                'etf': item['etf'],
                'flag': item['metrics']['flag'],
                'nav_erosion': item['metrics']['nav_erosion_pct']
            })

    return render_template('dashboard.html',
                           etf_data=etf_data,
                           alerts=alerts,
                           calculator=calculator)


@app.route('/etf/<int:etf_id>')
def etf_detail(etf_id):
    """Detailed view for a single ETF."""
    etf = db.get_etf(etf_id)
    if not etf:
        flash('ETF not found', 'danger')
        return redirect(url_for('dashboard'))

    # Get metrics and snapshots
    metrics = db.get_latest_metrics(etf_id)
    metrics_history = db.get_metrics_history(etf_id, limit=12)
    snapshots = db.get_etf_snapshots(etf_id)

    # Generate monthly breakdown
    monthly_data = [
        {
            'date': s['snapshot_date'],
            'year_month': s['snapshot_date'][:7],
            'close_price': s['close_price'],
            'distribution': s['distribution']
        }
        for s in reversed(snapshots)  # Oldest first
    ]
    breakdown = calculator.generate_monthly_breakdown(monthly_data)

    # Prepare chart data
    chart_dates = [s['snapshot_date'] for s in reversed(snapshots)]
    chart_prices = [s['close_price'] for s in reversed(snapshots)]
    chart_distributions = [s['distribution'] for s in reversed(snapshots)]

    # Erosion trend from metrics history
    erosion_dates = [m['calc_date'] for m in reversed(metrics_history)]
    erosion_values = [m['nav_erosion_pct'] * 100 for m in reversed(metrics_history)]

    return render_template('etf_detail.html',
                           etf=etf,
                           metrics=metrics,
                           breakdown=breakdown,
                           chart_dates=chart_dates,
                           chart_prices=chart_prices,
                           chart_distributions=chart_distributions,
                           erosion_dates=erosion_dates,
                           erosion_values=erosion_values,
                           calculator=calculator)


@app.route('/add', methods=['GET', 'POST'])
def add_etf():
    """Add a new ETF to track."""
    if request.method == 'POST':
        ticker = request.form.get('ticker', '').strip().upper()
        name = request.form.get('name', '').strip()

        if not ticker:
            flash('Ticker symbol is required', 'danger')
            return render_template('add_etf.html')

        # Validate ticker with Yahoo Finance
        info = fetcher.fetch_etf_info(ticker)
        if not info['success']:
            flash(f'Invalid ticker: {info["error"]}', 'danger')
            return render_template('add_etf.html', ticker=ticker, name=name)

        # Use fetched name if not provided
        if not name:
            name = info['name']

        # Add to database
        etf_id = db.add_etf(name, ticker)
        if etf_id is None:
            flash(f'Ticker {ticker} is already being tracked', 'warning')
            return redirect(url_for('dashboard'))

        flash(f'Added {name} ({ticker}) to tracking', 'success')

        # Fetch initial data
        return redirect(url_for('refresh_etf', etf_id=etf_id))

    return render_template('add_etf.html')


@app.route('/delete/<int:etf_id>', methods=['POST'])
def delete_etf(etf_id):
    """Remove an ETF from tracking."""
    etf = db.get_etf(etf_id)
    if etf:
        db.delete_etf(etf_id)
        flash(f'Removed {etf["name"]} from tracking', 'success')
    return redirect(url_for('dashboard'))


@app.route('/refresh')
def refresh_all():
    """Refresh data for all ETFs."""
    etfs = db.get_all_etfs()
    success_count = 0
    error_count = 0

    for etf in etfs:
        result = _refresh_etf_data(etf['id'])
        if result['success']:
            success_count += 1
        else:
            error_count += 1

    if error_count > 0:
        flash(f'Refreshed {success_count} ETFs, {error_count} errors', 'warning')
    else:
        flash(f'Successfully refreshed {success_count} ETFs', 'success')

    return redirect(url_for('dashboard'))


@app.route('/refresh/<int:etf_id>')
def refresh_etf(etf_id):
    """Refresh data for a single ETF."""
    result = _refresh_etf_data(etf_id)

    if result['success']:
        flash(f'Data refreshed for {result["etf_name"]}', 'success')
    else:
        flash(f'Error refreshing data: {result["error"]}', 'danger')

    # Redirect back to referrer or dashboard
    referrer = request.referrer
    if referrer and 'etf/' in referrer:
        return redirect(url_for('etf_detail', etf_id=etf_id))
    return redirect(url_for('dashboard'))


def _refresh_etf_data(etf_id):
    """Internal function to refresh ETF data."""
    etf = db.get_etf(etf_id)
    if not etf:
        return {'success': False, 'error': 'ETF not found', 'etf_name': None}

    # Fetch data from Yahoo Finance
    data = fetcher.get_monthly_data(etf['ticker'])

    if not data['success']:
        return {'success': False, 'error': data['error'], 'etf_name': etf['name']}

    # Clear old data and save new snapshots
    db.clear_etf_data(etf_id)

    for month in data['monthly_data']:
        db.save_snapshot(
            etf_id,
            month['date'],
            month['close_price'],
            month['distribution']
        )

    # Calculate and save metrics
    metrics = calculator.calculate_metrics(
        data['monthly_data'],
        etf['warn_threshold'],
        etf['sell_threshold']
    )

    if metrics:
        db.save_metrics(etf_id, metrics)

    return {'success': True, 'error': None, 'etf_name': etf['name']}


@app.route('/settings', methods=['GET', 'POST'])
def settings():
    """Global settings and alert configuration."""
    if request.method == 'POST':
        # Save SMTP settings
        db.set_setting('smtp_server', request.form.get('smtp_server', ''))
        db.set_setting('smtp_port', request.form.get('smtp_port', '587'))
        db.set_setting('smtp_user', request.form.get('smtp_user', ''))
        db.set_setting('smtp_password', request.form.get('smtp_password', ''))
        db.set_setting('alert_email', request.form.get('alert_email', ''))
        db.set_setting('email_enabled', request.form.get('email_enabled', 'off'))

        # Save default thresholds
        db.set_setting('default_warn_threshold', request.form.get('default_warn_threshold', '-6'))
        db.set_setting('default_sell_threshold', request.form.get('default_sell_threshold', '-10'))

        flash('Settings saved', 'success')
        return redirect(url_for('settings'))

    current_settings = db.get_all_settings()
    etfs = db.get_all_etfs()

    return render_template('settings.html',
                           settings=current_settings,
                           etfs=etfs)


@app.route('/settings/etf/<int:etf_id>', methods=['POST'])
def update_etf_settings(etf_id):
    """Update thresholds for a specific ETF."""
    try:
        warn = float(request.form.get('warn_threshold', -6)) / 100
        sell = float(request.form.get('sell_threshold', -10)) / 100
        db.update_etf_thresholds(etf_id, warn, sell)
        flash('ETF thresholds updated', 'success')
    except ValueError:
        flash('Invalid threshold values', 'danger')

    return redirect(url_for('settings'))


@app.route('/export')
def export_csv():
    """Export all historical data to CSV."""
    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow([
        'ETF', 'Ticker', 'Date', 'Close Price', 'Distribution',
        'NAV Erosion %', 'True Return %', 'Flag'
    ])

    etfs = db.get_all_etfs()
    for etf in etfs:
        snapshots = db.get_etf_snapshots(etf['id'])
        metrics = db.get_latest_metrics(etf['id'])

        for snapshot in snapshots:
            writer.writerow([
                etf['name'],
                etf['ticker'],
                snapshot['snapshot_date'],
                f"{snapshot['close_price']:.2f}",
                f"{snapshot['distribution']:.4f}",
                f"{metrics['nav_erosion_pct'] * 100:.2f}" if metrics else '',
                f"{metrics['true_return_pct'] * 100:.2f}" if metrics else '',
                metrics['flag'] if metrics else ''
            ])

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment;filename=nav_erosion_export_{datetime.now().strftime("%Y%m%d")}.csv'}
    )


@app.route('/api/etf/<int:etf_id>/metrics')
def api_etf_metrics(etf_id):
    """API endpoint for ETF metrics (for AJAX updates)."""
    etf = db.get_etf(etf_id)
    if not etf:
        return jsonify({'error': 'ETF not found'}), 404

    metrics = db.get_latest_metrics(etf_id)
    return jsonify({
        'etf': etf,
        'metrics': metrics
    })


# Template filters
@app.template_filter('pct')
def format_percentage(value):
    """Format decimal as percentage."""
    if value is None:
        return 'N/A'
    return f"{value * 100:.2f}%"


@app.template_filter('currency')
def format_currency(value):
    """Format as currency."""
    if value is None:
        return 'N/A'
    return f"${value:.2f}"


if __name__ == '__main__':
    print("Starting NAV Erosion Tracker...")
    print("Open http://localhost:5001 in your browser")
    app.run(debug=True, host='0.0.0.0', port=5001)
