#!/usr/bin/env python3
"""
CCXT to ClickHouse daemon
Daemon for loading cryptocurrency quotes from exchanges via CCXT into ClickHouse
"""

import os
import sys
import signal
import argparse
import logging
import logging.handlers
import time
import asyncio
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime
from dotenv import load_dotenv
import clickhouse_connect
import numpy as np
import ccxt
from timeframe import Timeframe
from constants import TIME_TYPE_UNIT


def setup_logging(log_file: str = '/var/log/ccxt2clickhouse.log'):
    """
    Setup logging:
    - Full log (all levels) to file /var/log/ccxt2clickhouse.log
    - Console output (INFO and above)
    - Critical errors (ERROR, CRITICAL) to systemd journal
    
    Args:
        log_file: Path to log file
    """
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # Custom formatter to use short level names
    class ShortLevelFormatter(logging.Formatter):
        LEVEL_MAP = {
            'DEBUG': 'D',
            'INFO': 'I',
            'WARNING': 'W',
            'ERROR': 'E',
            'CRITICAL': 'C',
        }
        
        def format(self, record):
            record.levelname_short = self.LEVEL_MAP.get(record.levelname, record.levelname[0])
            return super().format(record)
    
    # Log format
    log_format = ShortLevelFormatter(
        '%(asctime)s - %(name)s - %(levelname_short)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console format (simpler)
    console_format = ShortLevelFormatter(
        '%(asctime)s - %(levelname_short)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 1. Console handler - INFO and above
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)
    
    # 2. File handler - full log (all levels)
    try:
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(log_format)
        root_logger.addHandler(file_handler)
    except PermissionError:
        # If no permission for /var/log, write to ~/.ccxt2clickhouse/
        log_dir = os.path.expanduser('~/.ccxt2clickhouse')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'ccxt2clickhouse.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(log_format)
        root_logger.addHandler(file_handler)
        logging.warning(f"No permission for /var/log, using {log_file}")
    except Exception as e:
        logging.warning(f"Failed to setup file logging: {e}")
    
    # 3. Syslog/journald handler - only critical errors
    try:
        # Use SysLogHandler for journald
        syslog_handler = logging.handlers.SysLogHandler(
            address='/dev/log',
            facility=logging.handlers.SysLogHandler.LOG_DAEMON
        )
        syslog_handler.setLevel(logging.ERROR)  # Only ERROR and CRITICAL
        # Simplified format for syslog
        syslog_format = logging.Formatter('%(name)s[%(process)d]: %(levelname)s - %(message)s')
        syslog_handler.setFormatter(syslog_format)
        root_logger.addHandler(syslog_handler)
    except Exception as e:
        logging.warning(f"Failed to setup syslog/journald logging: {e}")


# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)


def env_symbol_param(symbol_entry: str) -> Dict[str, any]:
    """
    Parse a single SYMBOLS entry: exchange:symbol[:start_date]
    
    Args:
        symbol_entry: Single symbol entry string (e.g., "binance:BTC/USDT:2024-01-01")
        
    Returns:
        Dictionary with keys: exchange, symbol, start_date
        
    Raises:
        ValueError: If entry format is invalid or required fields are empty
    """
    parts = symbol_entry.split(':')
    if len(parts) < 2:
        raise ValueError(f"Invalid SYMBOLS entry format '{symbol_entry}'. Expected format: exchange:symbol[:start_date]")
    
    exchange = parts[0].strip()
    symbol = parts[1].strip()
    
    if not exchange:
        raise ValueError(f"Empty exchange in SYMBOLS entry: '{symbol_entry}'")
    if not symbol:
        raise ValueError(f"Empty symbol in SYMBOLS entry: '{symbol_entry}'")
    
    # start_date is optional
    start_date = None
    if len(parts) >= 3:
        start_date = parts[2].strip()
        if start_date:
            # Validate start_date format (basic check)
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"Invalid start_date format '{start_date}' in SYMBOLS entry: '{symbol_entry}'. Expected YYYY-MM-DD")
    
    return {
        'exchange': exchange,
        'symbol': symbol,
        'start_date': start_date
    }


def env_symbol_params() -> Optional[List[Dict[str, any]]]:
    """
    Parse SYMBOLS and TIMEFRAMES environment variables and create symbol_params array.
    
    SYMBOLS format: exchange:symbol:start_date (comma-separated)
    Example: binance:BTC/USDT:2024-01-01,bybit:ETH/USDT:2024-01-01
    
    TIMEFRAMES format: comma-separated timeframe strings
    Example: 1m,5m,1h
    
    Returns:
        List of dictionaries, each with keys: exchange, symbol, timeframe
        One element for each combination of (exchange, symbol) from SYMBOLS and timeframe from TIMEFRAMES
        Returns None if parsing fails
    """
    try:
        symbols_str = os.getenv('SYMBOLS', '')
        timeframes_str = os.getenv('TIMEFRAMES', '')
        
        if not symbols_str:
            logger.error("SYMBOLS environment variable is not set or empty")
            return None
        
        if not timeframes_str:
            logger.error("TIMEFRAMES environment variable is not set or empty")
            return None
        
        # Parse symbols: exchange:symbol:start_date
        try:
            symbol_params_list = [env_symbol_param(s.strip()) for s in symbols_str.split(',') if s.strip()]
        except ValueError as e:
            logger.error(str(e))
            return None
        
        # Parse timeframes
        timeframes_str_list = [tf.strip() for tf in timeframes_str.split(',') if tf.strip()]
        if not timeframes_str_list:
            logger.error("No timeframes found in TIMEFRAMES")
            return None
        
        try:
            timeframes = [Timeframe.cast(tf_str) for tf_str in timeframes_str_list]
        except Exception as e:
            logger.error(f"Invalid timeframe: {e}")
            return None
        
        # Create result: one element for each combination of (exchange, symbol) and timeframe
        result = [
            {
                'exchange': sp['exchange'],
                'symbol': sp['symbol'],
                'timeframe': tf,
                'start_date': sp['start_date']
            }
            for sp in symbol_params_list
            for tf in timeframes
        ]
        
        logger.info(f"Parsed {len(result)} symbol/timeframe combinations from {len(symbol_params_list)} symbols and {len(timeframes)} timeframes")
        return result
        
    except Exception as e:
        logger.error(f"Error parsing SYMBOLS and TIMEFRAMES: {e}", exc_info=True)
        return None


class CCXT2ClickHouseDaemon:
    """Main daemon class"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize daemon
        
        Args:
            config_path: Path to .env configuration file
        """
        self.running = False
        self.config_path = config_path or self.find_config()
        self.clickhouse_client = None
        
        # Log config file being used
        if self.config_path:
            logger.info(f"Configuration file: {self.config_path}")
        else:
            logger.info("No configuration file specified, using environment variables and defaults")
        
        # Load configuration
        self.load_config()
        
        logger.info("CCXT2ClickHouse daemon initialized")
    
    def find_config(self) -> str:
        """Find .env configuration file"""
        # Search order:
        # 1. Current working directory (where command is executed)
        # 2. User config directory (~/.ccxt2clickhouse/.env)
        # 3. System config directory (/etc/ccxt2clickhouse/.env) if running as root
        # 4. Package directory (for development mode)
        possible_paths = [
            Path.cwd() / '.env',
            Path.home() / '.ccxt2clickhouse' / '.env',
        ]
        
        # Add system config if running as root
        if os.geteuid() == 0:
            possible_paths.append(Path('/etc/ccxt2clickhouse/.env'))
        
        # Add package directory for development (when running from source)
        package_dir = Path(__file__).parent
        possible_paths.append(package_dir / '.env')
        
        for path in possible_paths:
            if path.exists():
                logger.info(f"Using config file: {path}")
                return str(path)
        
        logger.warning("Config file .env not found, using defaults")
        return None
    
    def load_config(self):
        """Load configuration from .env file"""
        if not self.config_path or not os.path.exists(self.config_path):
            logger.warning("Config file not found, using environment variables or defaults")
            return
        
        load_dotenv(self.config_path)
        logger.info(f"Config loaded from {self.config_path}")
    
    def setup_signal_handlers(self):
        """Setup signal handlers"""
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGHUP, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        logger.info("Signal handlers configured")
    
    def signal_handler(self, signum, frame):
        """Signal handler for graceful shutdown"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def run(self):
        """
        Start daemon
        
        Note: When running under systemd, systemd manages the process.
              For manual debugging, run in foreground mode.
        """
        self.setup_signal_handlers()
        self.running = True
        
        logger.info("CCXT2ClickHouse daemon started")
        
        try:
            self.start_loops()
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
        finally:
            self.cleanup()
    
    def init_database(self):
        """
        Initialize database connection and schema
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            host = os.getenv('CLICKHOUSE_HOST', 'localhost')
            port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
            database = os.getenv('CLICKHOUSE_DATABASE', 'default')
            username = os.getenv('CLICKHOUSE_USER', 'default')
            password = os.getenv('CLICKHOUSE_PASSWORD', '')
            
            logger.info(f"Connecting to ClickHouse: {host}:{port}, database: {database}")
            
            self.clickhouse_client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database
            )
            
            # Create database if not exists
            try:
                self.clickhouse_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
                logger.info(f"Database '{database}' ready")
            except Exception as e:
                logger.warning(f"Could not create database (may already exist): {e}")
            
            try:
                result = self.clickhouse_client.query("SELECT version FROM db_quotes_version LIMIT 1")
                if result.result_rows:
                    version = result.result_rows[0][0]
                    logger.info(f"Database initialized, version: {version}")
                    return True
            except Exception as e:
                logger.info(f"Version table not found, initializing database schema: {e}")
            
            # Schema file is in the same directory as this script
            schema_file = Path(__file__).parent / 'schema.sql'
            if not schema_file.exists():
                logger.error(f"Schema file not found: {schema_file}")
                return False
            
            logger.info(f"Executing schema script: {schema_file}")
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Execute schema SQL statements
            # Split by semicolons and execute each statement
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            
            for statement in statements:
                if statement:
                    self.clickhouse_client.command(statement)
            
            # Schema script executed without errors, database is initialized
            logger.info("Database schema initialized successfully")
            return True
                
        except Exception as e:
            logger.error(f"Database initialization failed: {e}", exc_info=True)
            return False
    
    def start_loops(self):
        """Start main loops"""

        if not self.init_database():
            logger.error("Failed to initialize database, stopping daemon")
            self.running = False
            return
        
        bar_delay = int(os.getenv('BAR_REALTIME_DELAY_SECONDS', '1'))
        
        symbol_params = env_symbol_params()
        if symbol_params is None:
            logger.error("Failed to parse symbol parameters from environment, stopping daemon")
            self.running = False
            return
        
        if not symbol_params:
            logger.error("No symbol parameters configured, stopping daemon")
            self.running = False
            return
        
        logger.info(f"Configuration: {len(symbol_params)} symbol/timeframe combinations, delay={bar_delay}s")
        
        self.init_exchanges(symbol_params)

        try:
            self.fetch_bars_historical(symbol_params)
            self.fetch_bars_realtime(symbol_params, bar_delay)
        except Exception as e:
            logger.error(f"Error in main loop iteration: {e}", exc_info=True)
            if self.running:
                time.sleep(10)  # Pause before retry on error
    
    def init_exchanges(self, symbol_params: List[Dict[str, any]]):
        """
        Initialize CCXT exchange clients and add exchange_client reference to each symbol_param.
        Extracts unique exchanges from symbol_params, initializes them, and adds 'exchange_client' key
        to each element in symbol_params. If the same exchange appears in multiple elements,
        they will reference the same exchange instance.
        
        Args:
            symbol_params: List of dictionaries with keys: exchange, symbol, timeframe
            
        Raises:
            Exception: If exchange class not found or initialization fails
        """
        # Extract unique exchanges
        exchanges = {param['exchange'] for param in symbol_params}
        
        # Initialize exchange clients
        exchange_clients: Dict[str, ccxt.Exchange] = {}
        for exchange_name in exchanges:
            try:
                exchange_class = getattr(ccxt, exchange_name)
                exchange_clients[exchange_name] = exchange_class()
                logger.info(f"Initialized exchange: {exchange_name}")
            except AttributeError as e:
                logger.error(
                    f"Exchange class '{exchange_name}' not found in CCXT library. "
                    f"Available exchanges: {[name for name in dir(ccxt) if not name.startswith('_') and name[0].islower()][:10]}... "
                    f"Error: {e}",
                    exc_info=True
                )
                raise
            except Exception as e:
                logger.error(
                    f"Failed to initialize exchange '{exchange_name}': {type(e).__name__}: {e}",
                    exc_info=True
                )
                raise
        
        # Add exchange_client reference to each symbol_param
        for param in symbol_params:
            param['exchange_client'] = exchange_clients[param['exchange']]
    
    def wait_next_bar(self, timeframes: List[Timeframe], next_bar_times: Dict[Timeframe, int], bar_delay: int) -> List[Timeframe]:
        """
        Wait until the next bar time arrives for at least one timeframe.
        Updates next_bar_times dictionary with calculated bar times.
        
        Args:
            timeframes: List of Timeframe objects
            next_bar_times: Dictionary mapping timeframes to their next bar times (in milliseconds)
            bar_delay: Additional delay in seconds after bar time to wait for exchange to finalize the bar
            
        Returns:
            List of timeframes that are ready (their bar time has arrived)
        """
        now_dt = datetime.now()
        now_units = int(now_dt.timestamp() * 1000)
        now = np.datetime64(now_units, TIME_TYPE_UNIT)
        
        for tf in timeframes:
            if tf not in next_bar_times:
                current_bar_start = tf.begin_of_tf(now)
                current_bar_start_units = int(current_bar_start.astype(np.int64))
                bar_delay_units = bar_delay * 1000
                next_bar_start_units = current_bar_start_units + tf.value + bar_delay_units
                next_bar_times[tf] = next_bar_start_units
            else:
                if now_units >= next_bar_times[tf]:
                    current_bar_start = tf.begin_of_tf(now)
                    current_bar_start_units = int(current_bar_start.astype(np.int64))
                    bar_delay_units = bar_delay * 1000
                    next_bar_start_units = current_bar_start_units + tf.value + bar_delay_units
                    next_bar_times[tf] = next_bar_start_units
        
        min_next_time = min(next_bar_times.values())
        sleep_units = max(0, min_next_time - now_units)
        
        if sleep_units > 0:
            sleep_s = sleep_units / 1000.0
            logger.debug(f"Sleeping {sleep_s:.2f}s until next bar (timeframes: {[str(tf) for tf in timeframes]})")
            time.sleep(sleep_s)
        
        now_dt = datetime.now()
        now_units = int(now_dt.timestamp() * 1000)
        ready_timeframes = [
            tf for tf in timeframes
            if now_units >= next_bar_times[tf]
        ]
        
        return ready_timeframes
    
    async def fetch_candle_async(self, exchange: ccxt.Exchange, exchange_name: str, symbol: str, tf: Timeframe, since: datetime, until: Optional[datetime] = None, max_bars: int = 1000, retry_delay: int = 1) -> List[tuple]:
        """
        Asynchronously fetch candles from exchange
        
        Args:
            exchange: CCXT exchange instance
            exchange_name: Name of the exchange (for logging)
            symbol: Trading pair symbol
            tf: Timeframe object
            since: Start time as datetime
            until: End time as datetime (optional, for realtime - time of next incomplete bar)
            max_bars: Maximum number of bars per request
            retry_delay: Delay in seconds before retry if until bar is not found (default: 1)
            
        Returns:
            List of tuples (exchange_name, symbol, tf, candle) for successfully fetched candles
            candle is [timestamp, open, high, low, close, volume]
        """
        tf_str = str(tf)
        current_since = int(since.timestamp() * 1000)
        until_ms = int(until.timestamp() * 1000) if until else None
        all_candles = []
        
        while True:
            candles = await asyncio.to_thread(
                exchange.fetch_ohlcv,
                symbol=symbol,
                timeframe=tf_str,
                since=current_since,
                limit=max_bars
            )
            
            if not candles or len(candles) == 0:
                break
            
            fetched_candles = [(exchange_name, symbol, tf, candle) for candle in candles]
            
            if fetched_candles:
                self.save_bars(fetched_candles)
                all_candles.extend(fetched_candles)
            
            if until_ms:
                if not fetched_candles:
                    await asyncio.sleep(retry_delay)
                    continue
                last_candle_time = fetched_candles[-1][3][0]
                if last_candle_time < until_ms:
                    await asyncio.sleep(retry_delay)
                    continue
                if last_candle_time > until_ms:
                    raise ValueError(f"Unexpected bar time {last_candle_time} > until {until_ms} for {exchange_name}/{symbol}/{tf_str}")
            
            if len(candles) < max_bars:
                break
            
            last_candle_time = candles[-1][0]
            if last_candle_time <= current_since:
                break
            
            if until_ms and last_candle_time >= until_ms:
                break
            
            current_since = last_candle_time + 1
        
        return all_candles
    
    async def fetch_bars_batch(self, symbol_params: List[Dict[str, any]], ready_timeframes: List[Timeframe], now: np.datetime64) -> List[tuple]:
        """
        Asynchronously fetch candles for symbol_params that match ready_timeframes
        
        Args:
            symbol_params: List of dictionaries with keys: exchange, symbol, timeframe, exchange_client
            ready_timeframes: List of timeframes that are ready to fetch
            now: Current time as numpy datetime64
            
        Returns:
            List of tuples (exchange_name, symbol, tf, candle) for successfully fetched candles
        """
        tasks = []
        for param in symbol_params:
            if param['timeframe'] in ready_timeframes:
                tf = param['timeframe']
                current_bar_start = tf.begin_of_tf(now)
                previous_bar_start_units = int(current_bar_start.astype(np.int64)) - tf.value
                previous_bar_start_dt = datetime.fromtimestamp(previous_bar_start_units / 1000.0)
                next_bar_start_units = int(current_bar_start.astype(np.int64)) + tf.value
                next_bar_start_dt = datetime.fromtimestamp(next_bar_start_units / 1000.0)
                
                tasks.append(self.fetch_candle_async(
                    param['exchange_client'],
                    param['exchange'],
                    param['symbol'],
                    tf,
                    previous_bar_start_dt,
                    next_bar_start_dt,
                    2,
                    1
                ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        fetched_candles = []
        now_dt = datetime.now()
        now_units = int(now_dt.timestamp() * 1000)
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Exception in async fetch: {result}", exc_info=True)
            elif isinstance(result, list) and result:
                for candle_tuple in result:
                    exchange_name, symbol, tf, candle = candle_tuple
                    candle_time = candle[0]
                    current_bar_start = tf.begin_of_tf(np.datetime64(now_units, TIME_TYPE_UNIT))
                    current_bar_start_units = int(current_bar_start.astype(np.int64))
                    if candle_time < current_bar_start_units:
                        fetched_candles.append(candle_tuple)
        
        return fetched_candles
    
    def fetch_bars_historical(self, symbol_params: List[Dict[str, any]]):
        max_bars = 1000
        
        if not symbol_params:
            return
        
        self.get_last_bar_times(symbol_params)
        
        now_dt = datetime.now()
        now_units = int(now_dt.timestamp() * 1000)
        now = np.datetime64(now_units, TIME_TYPE_UNIT)
        
        tasks = []
        for param in symbol_params:
            last_time = param.get('last_time')
            
            if last_time is not None:
                last_time_ms = int(last_time.timestamp() * 1000) if isinstance(last_time, datetime) else last_time
                min_bar_start_dt = datetime.fromtimestamp((last_time_ms + param['timeframe'].value) / 1000.0)
            else:
                if param['start_date']:
                    start_date = datetime.strptime(param['start_date'], '%Y-%m-%d')
                    start_datetime64 = np.datetime64(int(start_date.timestamp() * 1000), TIME_TYPE_UNIT)
                    min_bar_start = param['timeframe'].begin_of_tf(start_datetime64)
                    min_bar_start_dt = datetime.fromtimestamp(int(min_bar_start.astype(np.int64)) / 1000.0)
                else:
                    continue
            
            max_bar_start = param['timeframe'].begin_of_tf(now)
            max_bar_start_dt = datetime.fromtimestamp(int(max_bar_start.astype(np.int64)) / 1000.0)
            
            if min_bar_start_dt >= max_bar_start_dt:
                continue
            
            task = self.fetch_candle_async(
                param['exchange_client'],
                param['exchange'],
                param['symbol'],
                param['timeframe'],
                min_bar_start_dt,
                None,
                max_bars
            )
            tasks.append(task)
        
        if not tasks:
            return
        
        async def fetch_all():
            return await asyncio.gather(*tasks, return_exceptions=True)
        
        all_fetched = asyncio.run(fetch_all())
        
        fetched_candles = []
        for result in all_fetched:
            if isinstance(result, Exception):
                logger.error(f"Exception in historical fetch: {result}", exc_info=True)
            elif isinstance(result, list) and result:
                if len(result) > 0:
                    fetched_candles.extend(result[:-1])
    
    def get_last_bar_times(self, symbol_params: List[Dict[str, any]]):
        if not symbol_params:
            return
        
        temp_table = 'temp_symbol_params'
        
        try:
            self.clickhouse_client.command(f"""
                CREATE TABLE IF NOT EXISTS {temp_table}
                (
                    source String,
                    symbol String,
                    timeframe String
                )
                ENGINE = Memory
            """)
            
            self.clickhouse_client.command(f"TRUNCATE TABLE IF EXISTS {temp_table}")
            
            data = [[param['exchange'], param['symbol'], str(param['timeframe'])] for param in symbol_params]
            self.clickhouse_client.insert(temp_table, data, column_names=['source', 'symbol', 'timeframe'])
            
            query = f"""
                SELECT t.source, t.symbol, t.timeframe, MAX(q.time) as last_time
                FROM {temp_table} t
                LEFT JOIN quotes q ON t.source = q.source AND t.symbol = q.symbol AND t.timeframe = q.timeframe
                GROUP BY t.source, t.symbol, t.timeframe
            """
            
            result = self.clickhouse_client.query(query)
            
            last_times_map = {}
            for row in result.result_rows:
                source, symbol, timeframe_str, last_time = row
                key = (source, symbol, timeframe_str)
                last_times_map[key] = last_time
            
            for param in symbol_params:
                key = (param['exchange'], param['symbol'], str(param['timeframe']))
                param['last_time'] = last_times_map.get(key)
                
        except Exception as e:
            logger.warning(f"Could not get last bar times from database: {e}")
            for param in symbol_params:
                param['last_time'] = None
    
    def save_bars(self, candles: List[tuple]):
        pass
    
    def save_candles_to_db(self, candles: List[tuple]):
        if not candles:
            return
        
        data = []
        for exchange_name, symbol, tf, candle in candles:
            data.append([
                exchange_name,
                symbol,
                str(tf),
                datetime.fromtimestamp(candle[0] / 1000.0),
                candle[1],
                candle[2],
                candle[3],
                candle[4],
                candle[5]
            ])
        
        try:
            self.clickhouse_client.insert(
                'quotes',
                data,
                column_names=['source', 'symbol', 'timeframe', 'time', 'open', 'high', 'low', 'close', 'volume']
            )
            logger.info(f"Saved {len(candles)} candles to database")
        except Exception as e:
            logger.error(f"Error saving candles to database: {e}", exc_info=True)

    def fetch_bars_realtime(self, symbol_params: List[Dict[str, any]], bar_delay: int):
        """
        Main work procedure - fetch quotes and write to ClickHouse
        
        Synchronizes requests to fetch candles exactly when new bars appear.
        For each timeframe, calculates the time of the next bar and waits until that moment.
        After bar time arrives, waits additional bar_delay seconds for exchange to finalize the bar.
        
        Args:
            symbol_params: List of dictionaries with keys: exchange, symbol, timeframe
            bar_delay: Additional delay in seconds after bar time to wait for exchange to finalize the bar
        """

        timeframes = list({param['timeframe'] for param in symbol_params})
        
        next_bar_times: Dict[Timeframe, int] = {}
        
        while self.running:
            try:
                ready_timeframes = self.wait_next_bar(timeframes, next_bar_times, bar_delay)
                
                if not ready_timeframes:
                    continue
                
                now_dt = datetime.now()
                now_units = int(now_dt.timestamp() * 1000)
                now = np.datetime64(now_units, TIME_TYPE_UNIT)
                
                fetched_candles = asyncio.run(
                    self.fetch_bars_batch(symbol_params, ready_timeframes, now)
                )
                
                for exchange_name, symbol, tf, candle in fetched_candles:
                    tf_str = str(tf)
                    logger.info(
                        f"Fetched {exchange_name}/{symbol}/{tf_str}: "
                        f"time={candle[0]}, close={candle[4]}, volume={candle[5]}"
                    )
                    # TODO: Save to ClickHouse
                
                for tf in ready_timeframes:
                    current_bar_start = tf.begin_of_tf(now)
                    current_bar_start_units = int(current_bar_start.astype(np.int64))
                    bar_delay_units = bar_delay * 1000
                    next_bar_start_units = current_bar_start_units + tf.value + bar_delay_units
                    next_bar_times[tf] = next_bar_start_units
                    
            except Exception as e:
                logger.error(f"Error in work loop: {e}", exc_info=True)
                if self.running:
                    time.sleep(1)
    
    def cleanup(self):
        """Cleanup resources on shutdown"""
        logger.info("Cleaning up resources...")
        
        if self.clickhouse_client:
            try:
                self.clickhouse_client.close()
                logger.info("ClickHouse connection closed")
            except Exception as e:
                logger.warning(f"Error closing ClickHouse connection: {e}")
        
        logger.info("CCXT2ClickHouse daemon stopped")


class SystemdServiceManager:
    """Systemd service management"""
    
    SERVICE_NAME = "ccxt2clickhouse"
    SERVICE_FILE = f"/etc/systemd/system/{SERVICE_NAME}.service"
    SCRIPT_PATH = Path(__file__).absolute()
    
    @classmethod
    def get_venv_python(cls) -> str:
        """Get Python path from venv"""
        # Find project root (go up from package to src to project root)
        package_dir = Path(__file__).parent
        project_root = package_dir.parent.parent
        venv_python = project_root / "venv" / "bin" / "python3"
        
        if venv_python.exists():
            return str(venv_python)
        
        # Fallback to system Python
        return sys.executable
    
    @classmethod
    def create_service_file(cls) -> str:
        """Create systemd unit file content"""
        python_path = cls.get_venv_python()
        # Use entry point command instead of direct script path
        script_path = "ccxt2clickhouse-daemon"
        # Find project root for working directory
        package_dir = Path(__file__).parent
        work_dir = package_dir.parent.parent
        
        # Determine service user
        # If running from root, use current user or create a dedicated one
        service_user = os.getenv('SUDO_USER') or os.getenv('USER', 'root')
        
        service_content = f"""[Unit]
Description=CCXT to ClickHouse daemon - loads cryptocurrency quotes to ClickHouse
After=network.target

[Service]
Type=simple
User={service_user}
WorkingDirectory={work_dir}
ExecStart={script_path}
ExecStop=/bin/kill -TERM $MAINPID
Restart=always
RestartSec=10
# Logging configured in code, don't redirect stdout/stderr
# Note: No --daemon flag needed, systemd manages the process

# Environment variables
Environment="PATH={work_dir}/venv/bin:/usr/local/bin:/usr/bin:/bin"

[Install]
WantedBy=multi-user.target
"""
        return service_content
    
    @classmethod
    def install(cls):
        """Install systemd service"""
        if os.geteuid() != 0:
            logger.error("Root privileges required for installation. Use: sudo python ccxt2clickhouse.py install")
            return False
        
        try:
            service_content = cls.create_service_file()
            
            with open(cls.SERVICE_FILE, 'w') as f:
                f.write(service_content)
            
            logger.info(f"Service file created: {cls.SERVICE_FILE}")
            
            # Reload systemd
            os.system("systemctl daemon-reload")
            logger.info("Systemd daemon reloaded")
            
            # Enable autostart
            os.system(f"systemctl enable {cls.SERVICE_NAME}")
            logger.info(f"Service {cls.SERVICE_NAME} enabled")
            
            logger.info(f"Service installed successfully. Use 'systemctl start {cls.SERVICE_NAME}' to start it.")
            return True
            
        except Exception as e:
            logger.error(f"Failed to install service: {e}")
            return False
    
    @classmethod
    def uninstall(cls):
        """Uninstall systemd service"""
        if os.geteuid() != 0:
            logger.error("Root privileges required for uninstallation. Use: sudo python ccxt2clickhouse.py uninstall")
            return False
        
        try:
            # Stop and disable service
            os.system(f"systemctl stop {cls.SERVICE_NAME}")
            os.system(f"systemctl disable {cls.SERVICE_NAME}")
            
            # Remove service file
            if os.path.exists(cls.SERVICE_FILE):
                os.remove(cls.SERVICE_FILE)
                logger.info(f"Service file removed: {cls.SERVICE_FILE}")
            
            # Reload systemd
            os.system("systemctl daemon-reload")
            logger.info("Systemd daemon reloaded")
            
            logger.info(f"Service {cls.SERVICE_NAME} uninstalled successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to uninstall service: {e}")
            return False
    
    @classmethod
    def service_command(cls, command: str):
        """Execute service management command"""
        if os.geteuid() != 0:
            logger.error(f"Root privileges required. Use: sudo systemctl {command} {cls.SERVICE_NAME}")
            return
        
        os.system(f"systemctl {command} {cls.SERVICE_NAME}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="CCXT to ClickHouse daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  ccxt2clickhouse-daemon                        # Run in foreground (debug mode)
  sudo ccxt2clickhouse-daemon install           # Install systemd service
  sudo ccxt2clickhouse-daemon uninstall         # Uninstall systemd service
  sudo systemctl start ccxt2clickhouse           # Start service
  sudo systemctl stop ccxt2clickhouse           # Stop service
  sudo journalctl -u ccxt2clickhouse -f         # View logs
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to .env configuration file'
    )
    
    # Service management commands
    parser.add_argument(
        'command',
        nargs='?',
        choices=['install', 'uninstall', 'start', 'stop', 'restart', 'status'],
        help='Service management command'
    )
    
    args = parser.parse_args()
    
    # Handle service management commands
    if args.command == 'install':
        SystemdServiceManager.install()
        return
    
    if args.command == 'uninstall':
        SystemdServiceManager.uninstall()
        return
    
    if args.command in ['start', 'stop', 'restart', 'status']:
        SystemdServiceManager.service_command(args.command)
        return
    
    # Start daemon (foreground mode for debugging)
    daemon = CCXT2ClickHouseDaemon(config_path=args.config)
    daemon.run()


if __name__ == '__main__':
    main()

