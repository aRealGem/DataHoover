#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${DB_PATH:-$ROOT_DIR/data/warehouse.duckdb}"

# Check if database exists
if [[ ! -f "$DB_PATH" ]]; then
    echo "❌ Database not found at: $DB_PATH"
    echo ""
    echo "Run ingestion first:"
    echo "  bash run-ingest.sh ingest-twelvedata --source twelvedata_watchlist_daily"
    exit 1
fi

# Check if table exists and has data
PYTHONPATH="${ROOT_DIR}/src" python3 << 'EOF'
import sys
import duckdb
from pathlib import Path

db_path = Path("data/warehouse.duckdb")
con = duckdb.connect(str(db_path))

try:
    # Check if table exists
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    
    if "twelvedata_time_series" not in table_names:
        print("❌ Table 'twelvedata_time_series' does not exist yet.")
        print()
        print("Run ingestion first:")
        print("  export TWELVEDATA_API_KEY='your_key'")
        print("  bash run-ingest.sh ingest-twelvedata --source twelvedata_watchlist_daily")
        sys.exit(1)
    
    # Check if table has data
    count = con.execute("SELECT COUNT(*) FROM twelvedata_time_series").fetchone()[0]
    if count == 0:
        print("⚠️  Table exists but is empty.")
        print()
        print("Run ingestion:")
        print("  bash run-ingest.sh ingest-twelvedata --source twelvedata_watchlist_daily")
        sys.exit(1)
    
    # Fetch latest values for each symbol
    result = con.execute('''
        SELECT 
            symbol,
            ts AS date,
            close,
            open,
            high,
            low,
            volume,
            currency,
            exchange
        FROM twelvedata_time_series
        WHERE (symbol, ts) IN (
            SELECT symbol, MAX(ts)
            FROM twelvedata_time_series
            GROUP BY symbol
        )
        ORDER BY symbol
    ''').fetchall()
    
    if not result:
        print("⚠️  No data found in table.")
        sys.exit(1)
    
    # Print table header
    print()
    print("📊 Latest Market Data")
    print("=" * 110)
    print(f"{'Symbol':<12} {'Date':<12} {'Close':>10} {'Open':>10} {'High':>10} {'Low':>10} {'Volume':>15} {'Currency':<8}")
    print("-" * 110)
    
    # Print data rows
    for row in result:
        symbol, date, close, open_val, high, low, volume, currency, exchange = row
        date_str = str(date)[:10] if date else "N/A"
        volume_str = f"{volume:,}" if volume else "N/A"
        currency_str = currency or "N/A"
        
        print(f"{symbol:<12} {date_str:<12} {close:>10.2f} {open_val:>10.2f} {high:>10.2f} {low:>10.2f} {volume_str:>15} {currency_str:<8}")
    
    print("-" * 110)
    print(f"Total symbols: {len(result)}")
    print()
    
    # Show summary stats
    con.execute('''
        SELECT 
            COUNT(DISTINCT symbol) as symbols,
            COUNT(*) as total_records,
            MIN(ts) as earliest_date,
            MAX(ts) as latest_date
        FROM twelvedata_time_series
    ''')
    stats = con.fetchone()
    print(f"Database stats: {stats[0]} symbols, {stats[1]} total records")
    print(f"Date range: {str(stats[2])[:10]} to {str(stats[3])[:10]}")
    print()

except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
finally:
    con.close()
EOF
