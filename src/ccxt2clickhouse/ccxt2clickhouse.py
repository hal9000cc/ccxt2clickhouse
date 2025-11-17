#!/usr/bin/env python3
"""
CCXT to ClickHouse daemon
Daemon for loading cryptocurrency quotes from exchanges via CCXT into ClickHouse
"""

import os
import sys
import argparse
import logging
import logging.handlers
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime, UTC
from .timeframe import Timeframe
from .daemon import CCXT2ClickHouseDaemon

MAX_FETCH_BARS = 1000

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
    root_logger.setLevel(logging.INFO)
    
    # Set ccxt library logging level to WARNING to reduce noise
    logging.getLogger('ccxt').setLevel(logging.WARNING)
    
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
    console_handler.setLevel(logging.DEBUG)
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


setup_logging()
logger = logging.getLogger(__name__)


def env_symbol_param(symbol_entry: str) -> Dict[str, any]:
    """
    Parse a single SYMBOLS entry: exchange:symbol[:start_time]
    
    Args:
        symbol_entry: Single symbol entry string (e.g., "binance:BTC/USDT:2024-01-01")
        
    Returns:
        Dictionary with keys: exchange, symbol, start_time
        
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
        'start_time': start_date
    }


def env_symbol_params() -> Optional[List[Dict[str, any]]]:
    """
    Parse SYMBOLS and TIMEFRAMES environment variables and create symbol_params array.
    
    SYMBOLS format: exchange:symbol:start_time (comma-separated)
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
        
        # Parse symbols: exchange:symbol:start_time
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
                'start_time': sp['start_time']
            }
            for sp in symbol_params_list
            for tf in timeframes
        ]
        
        logger.info(f"Parsed {len(result)} symbol/timeframe combinations from {len(symbol_params_list)} symbols and {len(timeframes)} timeframes")
        return result
        
    except Exception as e:
        logger.error(f"Error parsing SYMBOLS and TIMEFRAMES: {e}", exc_info=True)
        return None




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
        # Find project root for working directory
        package_dir = Path(__file__).parent
        work_dir = package_dir.parent.parent
        # Use full path to the command in venv
        script_path = str(work_dir / "venv" / "bin" / "ccxt2clickhouse-daemon")
        
        # Determine service user
        # If running from root, use current user or create a dedicated one
        service_user = os.getenv('SUDO_USER') or os.getenv('USER', 'root')
        
        service_content = f"""[Unit]
Description=CCXT to ClickHouse daemon - loads cryptocurrency quotes to ClickHouse
After=network.target

[Service]
Type=simple
User={service_user}
WorkingDirectory={str(work_dir)}
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

