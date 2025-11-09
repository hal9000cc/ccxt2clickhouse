# CCXT2ClickHouse

Daemon for loading cryptocurrency quotes from exchanges via CCXT into ClickHouse, and Python library for accessing the data.

## Installation

### Install the package

```bash
pip install -e .
```

Or install from source:
```bash
git clone <repository-url>
cd ccxt2clickhouse
pip install -e .
```

### Configure settings

```bash
cp .env.example .env
# Edit .env file with your settings
```

### Install as systemd service

```bash
sudo ccxt2clickhouse-daemon install
```

### Start the service

```bash
sudo systemctl start ccxt2clickhouse
```

## Usage

### Service Management

```bash
# Install service
sudo ccxt2clickhouse-daemon install

# Uninstall service
sudo ccxt2clickhouse-daemon uninstall

# Manage via systemctl
sudo systemctl start ccxt2clickhouse
sudo systemctl stop ccxt2clickhouse
sudo systemctl restart ccxt2clickhouse
sudo systemctl status ccxt2clickhouse

# View logs
sudo journalctl -u ccxt2clickhouse -f
```

### Debug Mode

```bash
# Run in foreground (without daemonization)
ccxt2clickhouse-daemon

# Run as daemon manually
ccxt2clickhouse-daemon --daemon
```

### Using the Library

```python
import clickhouse_connect
import numpy as np
from datetime import datetime, timedelta

# Create client (uses environment variables or defaults)
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password=''
)

# Get quotes for a time range
end_time = datetime.now()
start_time = end_time - timedelta(days=7)
result = client.query(
    "SELECT time, open, high, low, close, volume FROM quotes "
    "WHERE source = %(source)s AND timeframe = %(tf)s "
    "AND time >= %(start)s AND time <= %(end)s "
    "ORDER BY time",
    parameters={
        'source': 'binance',
        'tf': '1m',
        'start': start_time,
        'end': end_time
    }
)

# Convert to numpy arrays
times = np.array([np.datetime64(row[0], 'ms') for row in result.result_rows])
prices = np.array([row[1:5] for row in result.result_rows])  # OHLC
volumes = np.array([row[5] for row in result.result_rows])
```

## Configuration

Configuration is set via `.env` file. Copy `.env.example` to `.env` and configure parameters:

- `CLICKHOUSE_*` - ClickHouse connection settings
- `EXCHANGE_*` - Exchange settings (CCXT)
- `TRADING_PAIRS` - Trading pairs to load
- `FETCH_INTERVAL` - Fetch interval in seconds

## Logging

Logging is configured in two channels:

1. **Full log** (all levels) is written to file `/var/log/ccxt2clickhouse.log`
   - Rotation: 10 MB per file, up to 5 files
   - View: `tail -f /var/log/ccxt2clickhouse.log`

2. **Critical errors** (ERROR, CRITICAL) are sent to systemd journal
   - View: `sudo journalctl -u ccxt2clickhouse -f`
   - Or errors only: `sudo journalctl -u ccxt2clickhouse -p err`

## Database Schema

The database schema is defined in `schema.sql`. To create the tables:

```bash
clickhouse-client < schema.sql
```

Or using Python:
```python
import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', port=8123)
with open('schema.sql', 'r') as f:
    client.command(f.read())
```

### Table Structure

- **quotes** - Main table for storing OHLCV data
  - `source` - Exchange name (LowCardinality(String))
  - `timeframe` - Timeframe string like '1m', '5m', '1h', '1d' (LowCardinality(String))
  - `time` - Timestamp with millisecond precision (DateTime64(3))
  - `open`, `high`, `low`, `close`, `volume` - Price and volume data (Float64)
  - Partitioned by: `(source, toYYYYMM(time))`
  - Ordered by: `(source, timeframe, time)`

- **quotes_view** - View for convenient data access

## Development

Quote loading logic will be implemented in the `_main_loop()` method of the `CCXT2ClickHouseDaemon` class.
