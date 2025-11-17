# CCXT2ClickHouse

Daemon for loading cryptocurrency quotes via CCXT library into ClickHouse.

## Description

CCXT2ClickHouse is a Python application (daemon) that automatically loads historical and real-time OHLCV (candlestick) data from cryptocurrency exchanges into ClickHouse database. The project uses the CCXT library to work with various exchanges.

## Features

- **Automatic historical data loading** from a specified date
- **Real-time loading** of new candles as they appear

## Installation

### Requirements

- Python 3.8 or higher
- ClickHouse server

### Package Installation

```bash
# Clone the repository
git clone <repository-url>
cd ccxt2clickhouse

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install the package (this will create the ccxt2clickhouse-daemon command)
pip install -e .
```

After installation, the `ccxt2clickhouse-daemon` command will be available in the system (if the virtual environment is activated or the package is installed globally).

## Configuration

Create a `.env` file in one of the following locations (in order of priority):

1. Current directory (where the daemon is run)
2. `~/.ccxt2clickhouse/.env`
3. `/etc/ccxt2clickhouse/.env`
4. Package directory

### Example `.env` file

```env
# ClickHouse settings
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=crypto_quotes
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Symbols to load
# Format: exchange:symbol:start_time
# Example: binance:BTC/USDT:2024-01-01,bybit:ETH/USDT:2024-01-01
SYMBOLS=binance:BTC/USDT:2024-01-01,binance:ETH/USDT:2024-01-01

# Timeframes
# Available: 1s, 1m, 3m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 1w
TIMEFRAMES=1m,5m,1h,1d

# Delay after bar time before request (in seconds)
# Required because the exchange may calculate the bar with a slight delay
BAR_REALTIME_DELAY_SECONDS=1
```

### Configuration Parameters

- **CLICKHOUSE_HOST** — ClickHouse server address (default: localhost)
- **CLICKHOUSE_PORT** — ClickHouse port (default: 8123)
- **CLICKHOUSE_DATABASE** — database name (default: crypto_quotes)
- **CLICKHOUSE_USER** — ClickHouse user (default: default)
- **CLICKHOUSE_PASSWORD** — ClickHouse password (default: empty)
- **SYMBOLS** — list of symbols to load in format `exchange:symbol:start_time`
  - `start_time` is optional and specifies the start date for historical loading (format: YYYY-MM-DD)
- **TIMEFRAMES** — comma-separated list of timeframes
- **BAR_REALTIME_DELAY_SECONDS** — delay in seconds after bar time before request (default: 1)

## Usage

### Command Line Commands

The daemon supports the following commands:

#### Running in Debug Mode (Foreground)

```bash
# Activate virtual environment
source venv/bin/activate

# Run daemon in foreground mode (for debugging)
ccxt2clickhouse-daemon

# Or with a path to the configuration file
ccxt2clickhouse-daemon --config /path/to/.env
```

The daemon runs in the console and outputs logs. Use `Ctrl+C` to stop.

#### Installing systemd Service

```bash
# Activate virtual environment
source venv/bin/activate

# Install service (requires root privileges)
# Use full path to command or sudo -E to preserve PATH
sudo venv/bin/ccxt2clickhouse-daemon install
# or
sudo -E env "PATH=$PATH" ccxt2clickhouse-daemon install
```

This command:
- Creates the `/etc/systemd/system/ccxt2clickhouse.service` file
- Reloads systemd
- Enables service autostart on system boot

After installation, start the service:

```bash
sudo systemctl start ccxt2clickhouse
```

#### Service Management

```bash
# Start service
sudo ccxt2clickhouse-daemon start
# or
sudo systemctl start ccxt2clickhouse

# Stop service
sudo ccxt2clickhouse-daemon stop
# or
sudo systemctl stop ccxt2clickhouse

# Restart service
sudo ccxt2clickhouse-daemon restart
# or
sudo systemctl restart ccxt2clickhouse

# Check status
sudo ccxt2clickhouse-daemon status
# or
sudo systemctl status ccxt2clickhouse
```

#### Removing systemd Service

```bash
# Activate virtual environment
source venv/bin/activate

# Remove service (requires root privileges)
# Use full path to command or sudo -E to preserve PATH
sudo venv/bin/ccxt2clickhouse-daemon uninstall
# or
sudo -E env "PATH=$PATH" ccxt2clickhouse-daemon uninstall
```

This command:
- Stops and disables the service
- Removes the service file
- Reloads systemd

### Viewing Logs

```bash
# View logs via systemd journal
sudo journalctl -u ccxt2clickhouse -f

# View last 100 lines
sudo journalctl -u ccxt2clickhouse -n 100

# View logs from file
tail -f /var/log/ccxt2clickhouse.log
# or
tail -f ~/.ccxt2clickhouse/ccxt2clickhouse.log
```

## Database Structure

The daemon automatically creates the necessary tables on first run.

## Logging

The daemon logs to the following locations:

1. **Log file**: `/var/log/ccxt2clickhouse.log` (or `~/.ccxt2clickhouse/ccxt2clickhouse.log` if `/var/log` is not accessible)
2. **Console**: INFO level and above
3. **Systemd journal**: critical errors (ERROR, CRITICAL)

Log format:
- **D** — DEBUG
- **I** — INFO
- **W** — WARNING
- **E** — ERROR
- **C** — CRITICAL

## Operation Details

Only one daemon can be used to load data into a single database.

### Historical Data Loading

On first run or when `start_time` is specified in the configuration, the daemon loads historical data from the specified date to the current moment. Data is loaded in batches of 1000.

### Real-time Loading

After loading historical data, the daemon switches to real-time loading mode.

## Supported Exchanges

The project supports all exchanges available through the CCXT library. A list of available exchanges can be found in the [CCXT documentation](https://docs.ccxt.com/#/README?id=exchanges).

## Supported Timeframes

- **1s** — 1 second
- **1m** — 1 minute
- **3m** — 3 minutes
- **5m** — 5 minutes
- **10m** — 10 minutes
- **15m** — 15 minutes
- **30m** — 30 minutes
- **1h** — 1 hour
- **2h** — 2 hours
- **4h** — 4 hours
- **6h** — 6 hours
- **8h** — 8 hours
- **12h** — 12 hours
- **1d** — 1 day
- **1w** — 1 week

### Dependencies

Main dependencies are listed in `requirements.txt`:

- `ccxt` — library for working with cryptocurrency exchanges
- `clickhouse-connect` — ClickHouse driver
- `python-dotenv` — loading environment variables from `.env`
- `numpy` — working with timestamps

## License

MIT License

## Author

hal9000cc (hal@hal9000.cc)

## Acknowledgments

- [CCXT](https://github.com/ccxt/ccxt) — universal library for working with cryptocurrency exchanges
- [ClickHouse](https://clickhouse.com/) — columnar database for analytics
