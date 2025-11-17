"""
Core CCXT2ClickHouse class - handles CCXT exchanges and ClickHouse database operations
"""
import os
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, UTC
import clickhouse_connect
import numpy as np
import ccxt
from .timeframe import Timeframe
from .constants import TIME_TYPE_UNIT, TIME_UNITS_IN_ONE_SECOND

# Import logger - will be set after logger is created
import logging
logger = logging.getLogger(__name__)

MAX_FETCH_BARS = 1000


class CCXT2ClickHouse:
    """Core class for fetching quotes from CCXT exchanges and saving to ClickHouse"""
    
    def __init__(self, clickhouse_params: Optional[Dict[str, Any]] = None):
        """
        Initialize CCXT2ClickHouse instance
        
        Args:
            clickhouse_params: Dictionary with ClickHouse connection parameters:
                - host: ClickHouse host (default: 'localhost')
                - port: ClickHouse port (default: 8123)
                - database: ClickHouse database name (default: 'default')
                - username: ClickHouse username (default: 'default')
                - password: ClickHouse password (default: '')
        """
        if clickhouse_params is None:
            clickhouse_params = {}
        
        self.clickhouse_host = clickhouse_params.get('host', 'localhost')
        self.clickhouse_port = clickhouse_params.get('port', 8123)
        self.clickhouse_database = clickhouse_params.get('database', 'default')
        self.clickhouse_username = clickhouse_params.get('username', 'default')
        self.clickhouse_password = clickhouse_params.get('password', '')
        self.clickhouse_client = None
        self.exchanges: Dict[str, ccxt.Exchange] = {}
    
    def init_database(self) -> bool:
        """
        Initialize database connection and schema
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            logger.info(f"Connecting to ClickHouse: {self.clickhouse_host}:{self.clickhouse_port}, database: {self.clickhouse_database}")
            
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=self.clickhouse_username,
                password=self.clickhouse_password,
                database=self.clickhouse_database
            )
            
            # Create database if not exists
            try:
                self.clickhouse_client.command(f"CREATE DATABASE IF NOT EXISTS {self.clickhouse_database}")
                logger.info(f"Database '{self.clickhouse_database}' ready")
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
            
            # Execute schema SQL
            for statement in schema_sql.split(';'):
                statement = statement.strip()
                if statement:
                    try:
                        self.clickhouse_client.command(statement)
                    except Exception as e:
                        logger.warning(f"Error executing schema statement: {e}")
            
            logger.info("Database schema initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}", exc_info=True)
            return False
    
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
        for exchange_name in exchanges:
            if exchange_name not in self.exchanges:
                try:
                    exchange_class = getattr(ccxt, exchange_name)
                    self.exchanges[exchange_name] = exchange_class({
                        'enableRateLimit': True,
                        'options': {
                            'defaultType': 'spot'
                        }
                    })
                    logger.info(f"Initialized exchange: {exchange_name}")
                except AttributeError:
                    raise Exception(f"Exchange '{exchange_name}' not found in CCXT library")
                except Exception as e:
                    raise Exception(f"Failed to initialize exchange '{exchange_name}': {e}")
        
        # Add exchange_client reference to each symbol_param
        for param in symbol_params:
            param['exchange_client'] = self.exchanges[param['exchange']]
    
    async def fetch_bar_async(self, exchange: ccxt.Exchange, exchange_name: str, symbol: str, tf: Timeframe, since: datetime, realtime: bool = False, max_bars: int = 1000, retry_delay: int = 1) -> tuple:
        """
        Asynchronously fetch bars from exchange
        
        Args:
            exchange: CCXT exchange instance
            exchange_name: Name of the exchange (for logging)
            symbol: Trading pair symbol
            tf: Timeframe object
            since: Start time as datetime
            realtime: If True, realtime mode - retry until we get a new bar
            max_bars: Maximum number of bars per request
            retry_delay: Delay in seconds before retry in realtime mode (default: 1)
            
        Returns:
            Tuple (exchange_name, symbol, tf, bars) where:
            - In historical mode: bars is empty list (bars are saved during fetch)
            - In realtime mode: bars contains only the last complete bar [timestamp, open, high, low, close, volume]
        """
        tf_str = str(tf)
        current_since = int(since.timestamp() * 1000)
        prev_bars = []
        
        while True:
            bars = await asyncio.to_thread(
                exchange.fetch_ohlcv,
                symbol=symbol,
                timeframe=tf_str,
                since=current_since,
                limit=max_bars
            )
            
            if not bars or len(bars) == 0:
                if realtime:
                    await asyncio.sleep(retry_delay)
                    continue

                if prev_bars:
                    del prev_bars[-1]
                    if prev_bars:
                        self.save_bars(exchange_name, symbol, tf, prev_bars)
                    else:
                        await asyncio.sleep(retry_delay)
                        continue

                break
            
            bars_count = len(bars)
            
            if bars_count == max_bars:
                if realtime:
                    raise ValueError(f"Unexpected: received max_bars ({max_bars}) bars in realtime mode for {exchange_name}/{symbol}/{tf_str}. This indicates too much data was fetched.")
                # Not the last batch - process prev_bars and save current batch to prev_bars
                if prev_bars:
                    self.save_bars(exchange_name, symbol, tf, prev_bars)
                prev_bars = bars
                # Continue to next batch
                last_bar_time = bars[-1][0]
                current_since = last_bar_time + 1
            else:
                # Last batch - add to prev_bars, remove last incomplete bar, process and return
                prev_bars.extend(bars)
                del prev_bars[-1]
                
                if prev_bars:
                    self.save_bars(exchange_name, symbol, tf, prev_bars)
                    
                    if realtime:
                        logger.debug(f"Realtime mode: returning {len(prev_bars)} complete bars for {exchange_name}/{symbol}/{tf_str}, last bar time: {datetime.fromtimestamp(prev_bars[-1][0] / 1000.0, UTC).strftime('%Y-%m-%d %H:%M:%S')}")
                        return exchange_name, symbol, tf, prev_bars
                
                # Historical mode or no bars - return empty
                return exchange_name, symbol, tf, []
        
        # Historical mode: all bars are saved, return empty list
        return exchange_name, symbol, tf, []
    
    async def fetch_bars_batch(self, symbol_params: List[Dict[str, any]], ready_timeframes: List[Timeframe], now: np.datetime64) -> List[tuple]:
        """
        Asynchronously fetch bars for symbol_params that match ready_timeframes
        
        Args:
            symbol_params: List of dictionaries with keys: exchange, symbol, timeframe, exchange_client
            ready_timeframes: List of timeframes that are ready to fetch
            now: Current time as numpy datetime64
            
        Returns:
            List of tuples (exchange_name, symbol, tf, bar) for successfully fetched bars
        """
        tasks = []
        for param in symbol_params:
            if param['timeframe'] in ready_timeframes:
                tf = param['timeframe']
                # Get last saved time and calculate next bar start
                last_time = param.get('last_time')
                if last_time is not None:
                    if isinstance(last_time, datetime):
                        last_time_units = int(np.datetime64(int(last_time.timestamp() * 1000), TIME_TYPE_UNIT).astype(np.int64))
                    else:
                        last_time_units = last_time
                    min_bar_start_units = last_time_units + tf.value
                    since_dt = datetime.fromtimestamp(min_bar_start_units / TIME_UNITS_IN_ONE_SECOND, UTC)
                else:
                    # No saved data, start from start_time or skip
                    if param.get('start_time'):
                        start_time = datetime.strptime(param['start_time'], '%Y-%m-%d').replace(tzinfo=UTC)
                        fetch_datetime64 = np.datetime64(int(start_time.timestamp() * 1000), TIME_TYPE_UNIT)
                        min_bar_start = tf.begin_of_tf(fetch_datetime64)
                        since_dt = datetime.fromtimestamp(int(min_bar_start.astype(np.int64)) / TIME_UNITS_IN_ONE_SECOND, UTC)
                    else:
                        continue
                
                tasks.append(self.fetch_bar_async(
                    param['exchange_client'],
                    param['exchange'],
                    param['symbol'],
                    tf,
                    since_dt,
                    realtime=True,
                    max_bars=MAX_FETCH_BARS,
                    retry_delay=1
                ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        fetched_bars = []
        task_params = [p for p in symbol_params if p['timeframe'] in ready_timeframes]
        
        for result, param in zip(results, task_params):
            if isinstance(result, Exception):
                logger.error(f"Exception in async fetch: {result}", exc_info=True)
            elif isinstance(result, tuple):
                exchange_name, symbol, tf, bars = result
                if bars:
                    # Update last_time to last bar time
                    last_bar_time = bars[-1][0]
                    last_bar_dt = datetime.fromtimestamp(last_bar_time / 1000.0, UTC)
                    param['last_time'] = last_bar_dt
                    
                    for bar in bars:
                        fetched_bars.append((exchange_name, symbol, tf, bar))
        
        return fetched_bars
    
    def fetch_bars_historical(self, symbol_params: List[Dict[str, any]]):
        """
        Fetch historical bars for all symbol_params
        
        Args:
            symbol_params: List of dictionaries with keys: exchange, symbol, timeframe, exchange_client, start_time
        """
        if not symbol_params:
            return
        
        self.get_last_bar_times(symbol_params)
        
        now_dt = datetime.now(UTC)
        now_ms = int(now_dt.timestamp() * 1000)
        now = np.datetime64(now_ms, TIME_TYPE_UNIT)
        
        tasks = []
        for param in symbol_params:
            last_time = param.get('last_time')
            
            if last_time is not None:
                if isinstance(last_time, datetime):
                    last_time_units = int(np.datetime64(int(last_time.timestamp() * 1000), TIME_TYPE_UNIT).astype(np.int64))
                else:
                    last_time_units = last_time
                min_bar_start_units = last_time_units + param['timeframe'].value
                min_bar_start_dt = datetime.fromtimestamp(min_bar_start_units / TIME_UNITS_IN_ONE_SECOND, UTC)
            else:
                if param.get('start_time'):
                    start_time = datetime.strptime(param['start_time'], '%Y-%m-%d').replace(tzinfo=UTC)
                    fetch_datetime64 = np.datetime64(int(start_time.timestamp() * 1000), TIME_TYPE_UNIT)
                    min_bar_start = param['timeframe'].begin_of_tf(fetch_datetime64)
                    min_bar_start_dt = datetime.fromtimestamp(int(min_bar_start.astype(np.int64)) / TIME_UNITS_IN_ONE_SECOND, UTC)
                else:
                    continue
            
            max_bar_start = param['timeframe'].begin_of_tf(now)
            max_bar_start_dt = datetime.fromtimestamp(int(max_bar_start.astype(np.int64)) / TIME_UNITS_IN_ONE_SECOND, UTC)
            
            if min_bar_start_dt >= max_bar_start_dt:
                continue
            
            tasks.append(self.fetch_bar_async(
                param['exchange_client'],
                param['exchange'],
                param['symbol'],
                param['timeframe'],
                min_bar_start_dt,
                realtime=False,
                max_bars=MAX_FETCH_BARS
            ))
        
        if not tasks:
            return
        
        async def fetch_all():
            return await asyncio.gather(*tasks, return_exceptions=True)
        
        all_fetched = asyncio.run(fetch_all())
        
        for result in all_fetched:
            if isinstance(result, Exception):
                logger.error(f"Exception in historical fetch: {result}", exc_info=True)
            elif isinstance(result, tuple):
                exchange_name, symbol, tf, bars = result
                # Bars are already saved in fetch_bar_async, no need to save again
    
    def get_last_bar_times(self, symbol_params: List[Dict[str, any]]):
        """Get last bar times from database for symbol_params"""
        if not symbol_params:
            return
        
        temp_table = 'ccxt2clickhouse_temp_symbol_params'
        
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
                SELECT t.source, t.symbol, t.timeframe, 
                       (SELECT toTimeZone(MAX(time), 'UTC') FROM quotes q 
                        WHERE q.source = t.source AND q.symbol = t.symbol AND q.timeframe = t.timeframe) as last_time
                FROM {temp_table} t
            """
            
            result = self.clickhouse_client.query(query)
            
            last_times_map = {}
            for row in result.result_rows:
                source, symbol, timeframe_str, last_time = row
                key = (source, symbol, timeframe_str)
                last_time = None if last_time is None else last_time.replace(tzinfo=UTC)
                last_times_map[key] = last_time
            
            for param in symbol_params:
                key = (param['exchange'], param['symbol'], str(param['timeframe']))
                param['last_time'] = last_times_map.get(key)
                
        except Exception as e:
            logger.warning(f"Could not get last bar times from database: {e}")
    
    def save_bars(self, exchange_name: str, symbol: str, tf: Timeframe, bars: List[list], check_data: bool = True):
        """
        Save bars to ClickHouse database
        
        Args:
            exchange_name: Exchange name (e.g., 'binance')
            symbol: Trading symbol (e.g., 'BTC/USDT')
            tf: Timeframe object
            bars: List of bars, each bar is [timestamp, open, high, low, close, volume]
            check_data: If True, check for duplicates and gaps before saving
        """
        if not bars:
            return
        
        tf_str = str(tf)
        temp_table = 'temp_save_bars'
        
        try:
            # Create temporary table
            self.clickhouse_client.command(f"""
                CREATE TABLE IF NOT EXISTS {temp_table}
                (
                    source String,
                    symbol String,
                    timeframe String,
                    time DateTime64(3, 'UTC'),
                    open Float64,
                    high Float64,
                    low Float64,
                    close Float64,
                    volume Float64
                )
                ENGINE = Memory
            """)
            
            self.clickhouse_client.command(f"TRUNCATE TABLE {temp_table}")
            
            # Prepare data for insertion
            data = [
                [exchange_name, symbol, tf_str, datetime.fromtimestamp(bar[0] / 1000.0, UTC), bar[1], bar[2], bar[3], bar[4], bar[5]]
                for bar in bars
            ]
            
            self.clickhouse_client.insert(
                temp_table,
                data,
                column_names=['source', 'symbol', 'timeframe', 'time', 'open', 'high', 'low', 'close', 'volume']
            )
            
            # Check for duplicates and gaps if requested
            if check_data:
                first_bar_time = datetime.fromtimestamp(bars[0][0] / 1000.0, UTC)
                
                # Single query to check duplicates and get last time
                query = f"""
                    SELECT 
                        (SELECT COUNT(*) FROM {temp_table} t
                         INNER JOIN quotes q ON t.source = q.source 
                             AND t.symbol = q.symbol 
                             AND t.timeframe = q.timeframe 
                             AND t.time = q.time) as duplicate_count,
                        (SELECT MAX(time) FROM quotes
                         WHERE source = '{exchange_name.replace("'", "''")}' 
                           AND symbol = '{symbol.replace("'", "''")}' 
                           AND timeframe = '{tf_str.replace("'", "''")}') as last_time
                """
                result = self.clickhouse_client.query(query)
                
                if result.result_rows:
                    duplicate_count = result.result_rows[0][0] or 0
                    last_time = result.result_rows[0][1]
                    
                    if last_time:
                        last_time = last_time.replace(tzinfo=UTC)
                    
                    if duplicate_count > 0:
                        raise ValueError(f"Found {duplicate_count} duplicate bars for {exchange_name}/{symbol}/{tf_str}")
                    
                    # Check for gaps only if we have existing data (last_time is not None and not default 1970)
                    if last_time and last_time.year > 1970:
                        expected_next_time = last_time + tf.timedelta()
                        if first_bar_time != expected_next_time:
                            raise ValueError(
                                f"Data gap detected for {exchange_name}/{symbol}/{tf_str}: "
                                f"last time in DB: {last_time}, expected next: {expected_next_time}, "
                                f"got: {first_bar_time}"
                            )
            
            # Insert from temp table to main table
            # Convert timestamp (milliseconds) to DateTime64 with UTC timezone
            self.clickhouse_client.command(f"""
                INSERT INTO quotes
                SELECT source, symbol, timeframe, time, open, high, low, close, volume
                FROM {temp_table}
            """)
            
            self.clickhouse_client.command(f"TRUNCATE TABLE {temp_table}")
            logger.info(f"Saved {len(bars)} bars to database ({exchange_name}/{symbol}/{tf_str})")
            
        except Exception as e:
            logger.error(f"Error saving bars to database: {e}", exc_info=True)
            raise
    
    def get_history_from_database(self, exchange: str, symbol: str, timeframe: str, start_time: datetime, end_time: datetime) -> tuple:
        """
        Get historical quotes from ClickHouse as numpy arrays
        
        Args:
            exchange: Exchange name (e.g., 'binance')
            symbol: Trading symbol (e.g., 'BTC/USDT')
            timeframe: Timeframe string (e.g., '1m', '1h', '1d')
            start_time: Start datetime (UTC)
            end_time: End datetime (UTC)
            
        Returns:
            Tuple of 5 numpy arrays: (open, high, low, close, volume)
        """
        try:
            # Validate timeframe
            tf = Timeframe.cast(timeframe)
            tf_str = str(tf)
            
            # Format datetimes for ClickHouse query
            start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
            
            query = f"""
                SELECT open, high, low, close, volume
                FROM quotes
                WHERE source = '{exchange.replace("'", "''")}'
                  AND symbol = '{symbol.replace("'", "''")}'
                  AND timeframe = '{tf_str.replace("'", "''")}'
                  AND time >= '{start_str}'
                  AND time <= '{end_str}'
                ORDER BY time ASC
            """
            
            result = self.clickhouse_client.query(query)
            
            # Get columns as numpy arrays
            columns = result.result_columns
            if not columns or len(columns) < 5:
                # Return empty arrays if no data
                return (np.array([], dtype=np.float64),) * 5
            
            open_arr = np.array(columns[0], dtype=np.float64)
            high_arr = np.array(columns[1], dtype=np.float64)
            low_arr = np.array(columns[2], dtype=np.float64)
            close_arr = np.array(columns[3], dtype=np.float64)
            volume_arr = np.array(columns[4], dtype=np.float64)
            
            logger.debug(f"Retrieved {len(open_arr)} bars for {exchange}/{symbol}/{tf_str} from {start_str} to {end_str}")
            return (open_arr, high_arr, low_arr, close_arr, volume_arr)
            
        except Exception as e:
            logger.error(f"Error getting history: {e}", exc_info=True)
            raise
    
    def wait_next_bar(self, timeframes: List[Timeframe], next_bar_times: Dict[Timeframe, int], bar_delay: int, running_flag, interruptible_sleep_func) -> List[Timeframe]:
        """
        Wait until the next bar time arrives for at least one timeframe.
        Updates next_bar_times dictionary with calculated bar times.
        
        Args:
            timeframes: List of Timeframe objects
            next_bar_times: Dictionary mapping timeframes to their next bar times (in milliseconds)
            bar_delay: Additional delay in seconds after bar time to wait for exchange to finalize the bar
            running_flag: Flag to check if should continue running
            interruptible_sleep_func: Function to sleep with interruption support
            
        Returns:
            List of timeframes that are ready (their bar time has arrived)
        """
        now_dt = datetime.now(UTC)
        now_ms = int(now_dt.timestamp() * 1000)
        now = np.datetime64(now_ms, TIME_TYPE_UNIT)
        
        for tf in timeframes:
            if tf not in next_bar_times:
                current_bar_start = tf.begin_of_tf(now)
                current_bar_start_units = int(current_bar_start.astype(np.int64))
                bar_delay_units = bar_delay * TIME_UNITS_IN_ONE_SECOND
                next_bar_start_units = current_bar_start_units + tf.value + bar_delay_units
                next_bar_times[tf] = next_bar_start_units
            else:
                now_units = int(now.astype(np.int64))
                if now_units >= next_bar_times[tf]:
                    current_bar_start = tf.begin_of_tf(now)
                    current_bar_start_units = int(current_bar_start.astype(np.int64))
                    bar_delay_units = bar_delay * TIME_UNITS_IN_ONE_SECOND
                    next_bar_start_units = current_bar_start_units + tf.value + bar_delay_units
                    next_bar_times[tf] = next_bar_start_units
        
        min_next_time = min(next_bar_times.values())
        now_units = int(now.astype(np.int64))
        sleep_units = max(0, min_next_time - now_units)
        sleep_ms = sleep_units * 1000 / TIME_UNITS_IN_ONE_SECOND
        
        if sleep_ms > 0:
            sleep_s = sleep_ms / 1000.0
            logger.debug(f"Sleeping {sleep_s:.2f}s until next bar (timeframes: {[str(tf) for tf in timeframes]})")
            interruptible_sleep_func(sleep_s)
        
        now_dt = datetime.now(UTC)
        now_ms = int(now_dt.timestamp() * 1000)
        now = np.datetime64(now_ms, TIME_TYPE_UNIT)
        now_units = int(now.astype(np.int64))
        ready_timeframes = [
            tf for tf in timeframes
            if now_units >= next_bar_times[tf]
        ]
        
        return ready_timeframes
    
    def fetch_bars_realtime(self, symbol_params: List[Dict[str, any]], bar_delay: int, running_flag, interruptible_sleep_func):
        """
        Main work procedure - fetch quotes and write to ClickHouse
        
        Synchronizes requests to fetch bars exactly when new bars appear.
        For each timeframe, calculates the time of the next bar and waits until that moment.
        After bar time arrives, waits additional bar_delay seconds for exchange to finalize the bar.
        
        Args:
            symbol_params: List of dictionaries with keys: exchange, symbol, timeframe
            bar_delay: Additional delay in seconds after bar time to wait for exchange to finalize the bar
            running_flag: Flag to check if should continue running
            interruptible_sleep_func: Function to sleep with interruption support
        """
        import time
        
        # Get last saved bar times from database
        self.get_last_bar_times(symbol_params)
        
        timeframes = list({param['timeframe'] for param in symbol_params})
        
        next_bar_times: Dict[Timeframe, int] = {}
        
        while running_flag:
            try:
                ready_timeframes = self.wait_next_bar(timeframes, next_bar_times, bar_delay, running_flag, interruptible_sleep_func)
                
                if not ready_timeframes:
                    continue
                
                now_dt = datetime.now(UTC)
                now_ms = int(now_dt.timestamp() * 1000)
                now = np.datetime64(now_ms, TIME_TYPE_UNIT)
                
                fetched_bars = asyncio.run(
                    self.fetch_bars_batch(symbol_params, ready_timeframes, now)
                )
                
                for exchange_name, symbol, tf, bar in fetched_bars:
                    tf_str = str(tf)
                    logger.info(
                        f"Fetched {exchange_name}/{symbol}/{tf_str}: "
                        f"time={bar[0]}, close={bar[4]}, volume={bar[5]}"
                    )
                
                for tf in ready_timeframes:
                    current_bar_start = tf.begin_of_tf(now)
                    current_bar_start_units = int(current_bar_start.astype(np.int64))
                    bar_delay_units = bar_delay * TIME_UNITS_IN_ONE_SECOND
                    next_bar_start_units = current_bar_start_units + tf.value + bar_delay_units
                    next_bar_times[tf] = next_bar_start_units
                    
            except Exception as e:
                logger.error(f"Error in work loop: {e}", exc_info=True)
                if running_flag:
                    time.sleep(1)
    
    def close(self):
        """Close database connection"""
        if self.clickhouse_client:
            try:
                self.clickhouse_client.close()
            except Exception:
                pass

