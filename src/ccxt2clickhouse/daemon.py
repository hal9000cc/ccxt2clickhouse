"""
CCXT2ClickHouse Daemon - manages the main daemon process
"""
import os
import signal
import time
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from .core import CCXT2ClickHouse
from .ccxt2clickhouse import logger, env_symbol_params
from .redis_worker import RedisWorker


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
        
        # Log config file being used
        if self.config_path:
            logger.info(f"Configuration file: {self.config_path}")
        else:
            logger.info("No configuration file specified, using environment variables and defaults")
        
        # Load configuration
        self.load_config()
        
        # Read ClickHouse configuration from environment
        clickhouse_params = {
            'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('CLICKHOUSE_PORT', '8123')),
            'database': os.getenv('CLICKHOUSE_DATABASE', 'default'),
            'username': os.getenv('CLICKHOUSE_USER', 'default'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', '')
        }
        
        # Create core instance with configuration
        self.core = CCXT2ClickHouse(clickhouse_params=clickhouse_params)
        
        # Read Redis configuration from environment
        redis_params = {
            'host': os.getenv('REDIS_HOST', 'localhost'),
            'port': int(os.getenv('REDIS_PORT', '6379')),
            'db': int(os.getenv('REDIS_DB', '0')),
            'password': os.getenv('REDIS_PASSWORD', None),
            'request_queue': os.getenv('REDIS_REQUEST_QUEUE', 'ccxt2clickhouse:requests'),
            'response_prefix': os.getenv('REDIS_RESPONSE_PREFIX', 'ccxt2clickhouse:response:')
        }
        
        # Create Redis worker with configuration
        self.redis_worker = RedisWorker(self.core, redis_params=redis_params)
        
        logger.info("CCXT2ClickHouse daemon initialized")
    
    def interruptible_sleep(self, duration: float, check_interval: float = 3.0):
        """
        Sleep with periodic checks for self.running flag to allow graceful shutdown
        
        Args:
            duration: Total sleep duration in seconds
            check_interval: Interval between checks in seconds (default: 3.0)
        """
        start_time = time.monotonic()
        while self.running:
            elapsed = time.monotonic() - start_time
            if elapsed >= duration:
                break
            remaining = duration - elapsed
            sleep_time = min(check_interval, remaining)
            time.sleep(sleep_time)
    
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
                logger.debug(f"Using config file: {path}")
                return str(path)
        
        logger.warning("Config file .env not found, using defaults")
        return None
    
    def load_config(self):
        """Load configuration from .env file"""
        if not self.config_path or not os.path.exists(self.config_path):
            logger.warning("Config file not found, using environment variables or defaults")
            return
        
        load_dotenv(self.config_path)
        logger.debug(f"Config loaded from {self.config_path}")
    
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
        
        # Start Redis worker if available
        self.redis_worker.start()
        
        try:
            self.start_loops()
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
        finally:
            self.cleanup()
    
    def start_loops(self):
        """Start main loops"""
        if not self.core.init_database():
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
        
        self.core.init_exchanges(symbol_params)

        try:
            self.core.fetch_bars_historical(symbol_params)
            self.core.fetch_bars_realtime(symbol_params, bar_delay, self.running, self.interruptible_sleep)
        except Exception as e:
            logger.error(f"Error in main loop iteration: {e}", exc_info=True)
            if self.running:
                self.interruptible_sleep(10)  # Pause before retry on error
    
    def cleanup(self):
        """Cleanup resources on shutdown"""
        logger.info("Cleaning up resources...")
        
        if self.redis_worker:
            self.redis_worker.stop()
        
        if self.core:
            self.core.close()
        
        logger.info("CCXT2ClickHouse daemon stopped")

