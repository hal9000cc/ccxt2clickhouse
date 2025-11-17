"""
Redis worker for handling quote history requests
"""
import os
import time
import multiprocessing
import json
import importlib.util
from typing import Dict, Any, Optional
from datetime import datetime, UTC
import logging
import numpy as np
from .core import CCXT2ClickHouse

logger = logging.getLogger(__name__)

# Check if redis module is available
_redis_spec = importlib.util.find_spec('redis')
REDIS_AVAILABLE = _redis_spec is not None

if REDIS_AVAILABLE:
    import redis
else:
    redis = None




class RedisWorker:
    """Redis worker for handling quote history requests"""
    
    def __init__(self, core, redis_params: Optional[Dict[str, Any]] = None):
        """
        Initialize Redis worker
        
        Args:
            core: CCXT2ClickHouse instance to access ClickHouse (not used in worker process, but kept for compatibility)
            redis_params: Dictionary with Redis connection parameters:
                - host: Redis host (default: 'localhost')
                - port: Redis port (default: 6379)
                - db: Redis database number (default: 0)
                - password: Redis password (default: None)
                - request_queue: Redis queue name for requests (default: 'ccxt2clickhouse:requests')
                - response_prefix: Redis key prefix for responses (default: 'ccxt2clickhouse:response:')
        """
        if redis_params is None:
            redis_params = {}
        
        # Note: core is not used in worker process, each process creates its own instance
        # But we need to keep reference to access ClickHouse config parameters
        self.core = core
        self.running = multiprocessing.Value('b', False)  # Shared flag for process
        self.process = None
        
        # Redis configuration
        self.redis_host = redis_params.get('host', 'localhost')
        self.redis_port = redis_params.get('port', 6379)
        self.redis_db = redis_params.get('db', 0)
        self.redis_password = redis_params.get('password', None)
        self.request_queue = redis_params.get('request_queue', 'ccxt2clickhouse:requests')
        self.response_prefix = redis_params.get('response_prefix', 'ccxt2clickhouse:response:')
    
    @staticmethod
    def _worker_process(running_flag, redis_params, clickhouse_params):
        """
        Main worker process function - processes requests from Redis
        Static method to avoid serialization issues with multiprocessing
        
        Args:
            running_flag: multiprocessing.Value('b') - shared flag for process control
            redis_params: Dictionary with Redis connection parameters
            clickhouse_params: Dictionary with ClickHouse connection parameters
        """
        # Create CCXT2ClickHouse instance in this process
        try:
            core = CCXT2ClickHouse(clickhouse_params=clickhouse_params)
            if not core.init_database():
                logger.error("Failed to initialize database in worker process")
                return
        except Exception as e:
            logger.error(f"Failed to create CCXT2ClickHouse instance in worker process: {e}")
            return
        
        # Create Redis client in this process
        try:
            redis_client = redis.Redis(
                host=redis_params.get('host', 'localhost'),
                port=redis_params.get('port', 6379),
                db=redis_params.get('db', 0),
                password=redis_params.get('password', None),
                decode_responses=False,
                socket_connect_timeout=5
            )
            redis_client.ping()
        except Exception as e:
            logger.error(f"Failed to create Redis client in worker process: {e}")
            return
        
        # Import here to avoid circular imports
        from .timeframe import Timeframe
        
        request_queue = redis_params.get('request_queue', 'ccxt2clickhouse:requests')
        response_prefix = redis_params.get('response_prefix', 'ccxt2clickhouse:response:')
        
        while running_flag.value:
            try:
                # Blocking pop from queue with timeout
                result = redis_client.blpop(request_queue, timeout=1)
                if not result:
                    continue
                
                queue_name, request_data = result
                RedisWorker._process_request_worker(request_data, redis_client, core, Timeframe, response_prefix)
                
            except redis.ConnectionError:
                logger.error("Redis connection lost, retrying...")
                if running_flag.value:
                    time.sleep(5)
                    try:
                        redis_client = redis.Redis(
                            host=redis_params.get('host', 'localhost'),
                            port=redis_params.get('port', 6379),
                            db=redis_params.get('db', 0),
                            password=redis_params.get('password', None),
                            decode_responses=False,
                            socket_connect_timeout=5
                        )
                        redis_client.ping()
                        logger.info("Redis connection restored")
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Error in Redis worker process: {e}", exc_info=True)
                if running_flag.value:
                    time.sleep(1)
    
    @staticmethod
    def _process_request_worker(request_data, redis_client, core, Timeframe, response_prefix):
        """Process a single request from Redis (worker process version)"""
        try:
            # Decode request if it's bytes (binary mode)
            if isinstance(request_data, bytes):
                request = json.loads(request_data.decode('utf-8'))
            else:
                request = json.loads(request_data)
            request_id = request.get('id')
            request_type = request.get('type')
            
            if not request_id:
                logger.warning("Request without ID, skipping")
                return
            
            if request_type == 'get_history':
                RedisWorker._handle_get_history_worker(request_id, request, redis_client, core, Timeframe, response_prefix)
            else:
                logger.warning(f"Unknown request type: {request_type}")
                RedisWorker._send_error_worker(request_id, f"Unknown request type: {request_type}", redis_client, response_prefix)
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in request: {e}")
        except Exception as e:
            logger.error(f"Error processing request: {e}", exc_info=True)
            request_id = None
            try:
                if isinstance(request_data, bytes):
                    request = json.loads(request_data.decode('utf-8'))
                else:
                    request = json.loads(request_data)
                request_id = request.get('id')
            except:
                pass
            if request_id:
                RedisWorker._send_error_worker(request_id, str(e), redis_client, response_prefix)
    
    @staticmethod
    def _handle_get_history_worker(request_id: str, request: Dict[str, Any], redis_client, core, Timeframe, response_prefix):
        """Handle get_history request (worker process version)"""
        try:
            exchange = request.get('exchange')
            symbol = request.get('symbol')
            timeframe = request.get('timeframe')
            start_time_str = request.get('start_time')
            end_time_str = request.get('end_time')
            
            if not all([exchange, symbol, timeframe, start_time_str, end_time_str]):
                RedisWorker._send_error_worker(request_id, "Missing required parameters: exchange, symbol, timeframe, start_time, end_time", redis_client, response_prefix)
                return
            
            # Parse datetime strings (expecting ISO format or YYYY-MM-DD HH:MM:SS)
            try:
                if 'T' in start_time_str:
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                else:
                    start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)
                
                if 'T' in end_time_str:
                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                else:
                    end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)
            except ValueError as e:
                RedisWorker._send_error_worker(request_id, f"Invalid datetime format: {e}", redis_client, response_prefix)
                return
            
            # Get history from ClickHouse as numpy arrays
            open_arr, high_arr, low_arr, close_arr, volume_arr = core.get_history_from_database(
                exchange, symbol, timeframe, start_time, end_time
            )
            
            # Convert numpy arrays to binary format using tobytes()
            response = {
                'id': request_id,
                'status': 'success',
                'data': {
                    'open': {
                        'bytes': open_arr.tobytes(),
                        'shape': open_arr.shape,
                        'dtype': str(open_arr.dtype)
                    },
                    'high': {
                        'bytes': high_arr.tobytes(),
                        'shape': high_arr.shape,
                        'dtype': str(high_arr.dtype)
                    },
                    'low': {
                        'bytes': low_arr.tobytes(),
                        'shape': low_arr.shape,
                        'dtype': str(low_arr.dtype)
                    },
                    'close': {
                        'bytes': close_arr.tobytes(),
                        'shape': close_arr.shape,
                        'dtype': str(close_arr.dtype)
                    },
                    'volume': {
                        'bytes': volume_arr.tobytes(),
                        'shape': volume_arr.shape,
                        'dtype': str(volume_arr.dtype)
                    }
                }
            }
            RedisWorker._send_response_worker(request_id, response, redis_client, response_prefix)
            
        except Exception as e:
            logger.error(f"Error handling get_history request: {e}", exc_info=True)
            RedisWorker._send_error_worker(request_id, str(e), redis_client, response_prefix)
    
    @staticmethod
    def _send_response_worker(request_id: str, response: Dict[str, Any], redis_client, response_prefix):
        """Send response to Redis as binary data (numpy arrays as bytes) - worker process version"""
        try:
            response_key = f"{response_prefix}{request_id}"
            # Serialize response: metadata as JSON, numpy arrays as binary bytes
            # We need to encode the response structure with binary data
            # Using pickle for the entire structure since it contains bytes objects
            import pickle
            response_binary = pickle.dumps(response)
            redis_client.setex(response_key, 300, response_binary)  # 5 minutes TTL
        except Exception as e:
            logger.error(f"Error sending response to Redis: {e}", exc_info=True)
    
    @staticmethod
    def _send_error_worker(request_id: str, error_message: str, redis_client, response_prefix):
        """Send error response to Redis - worker process version"""
        response = {
            'id': request_id,
            'status': 'error',
            'error': error_message
        }
        RedisWorker._send_response_worker(request_id, response, redis_client, response_prefix)
        
    def start(self):
        """Start Redis worker in a separate process"""
        if not REDIS_AVAILABLE:
            logger.info("Redis not available (package not installed), skipping Redis worker")
            return False
        
        # Test Redis connection before starting process
        try:
            test_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=False,
                socket_connect_timeout=5
            )
            test_client.ping()
            test_client.close()
            logger.info(f"Redis connection test successful at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            logger.warning(f"Could not connect to Redis: {e}, skipping Redis worker")
            return False
        
        # Prepare parameters dictionaries for worker process
        redis_params = {
            'host': self.redis_host,
            'port': self.redis_port,
            'db': self.redis_db,
            'password': self.redis_password,
            'request_queue': self.request_queue,
            'response_prefix': self.response_prefix
        }
        
        clickhouse_params = {
            'host': self.core.clickhouse_host,
            'port': self.core.clickhouse_port,
            'database': self.core.clickhouse_database,
            'username': self.core.clickhouse_username,
            'password': self.core.clickhouse_password
        }
        
        self.running.value = True
        self.process = multiprocessing.Process(
            target=RedisWorker._worker_process,
            args=(
                self.running,
                redis_params,
                clickhouse_params
            ),
            daemon=True
        )
        self.process.start()
        logger.info(f"Redis worker started in separate process, listening on queue: {self.request_queue}")
        return True
    
    def stop(self):
        """Stop Redis worker"""
        self.running.value = False
        if self.process:
            self.process.join(timeout=5)
            if self.process.is_alive():
                self.process.terminate()
                self.process.join(timeout=2)
                if self.process.is_alive():
                    self.process.kill()
        logger.info("Redis worker stopped")
    
    def _worker_process(self):
        """Main worker process - processes requests from Redis"""
        # Create CCXT2ClickHouse instance in this process
        try:
            core = CCXT2ClickHouse()
            if not core.init_database():
                logger.error("Failed to initialize database in worker process")
                return
        except Exception as e:
            logger.error(f"Failed to create CCXT2ClickHouse instance in worker process: {e}")
            return
        
        # Create Redis client in this process
        try:
            redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=False,
                socket_connect_timeout=5
            )
            redis_client.ping()
        except Exception as e:
            logger.error(f"Failed to create Redis client in worker process: {e}")
            return
        
        # Import here to avoid circular imports
        from .timeframe import Timeframe
        
        while self.running.value:
            try:
                # Blocking pop from queue with timeout
                result = redis_client.blpop(self.request_queue, timeout=1)
                if not result:
                    continue
                
                queue_name, request_data = result
                self._process_request_worker(request_data, redis_client, core, Timeframe)
                
            except redis.ConnectionError:
                logger.error("Redis connection lost, retrying...")
                if self.running.value:
                    time.sleep(5)
                    try:
                        redis_client = redis.Redis(
                            host=self.redis_host,
                            port=self.redis_port,
                            db=self.redis_db,
                            password=self.redis_password,
                            decode_responses=False,
                            socket_connect_timeout=5
                        )
                        redis_client.ping()
                        logger.info("Redis connection restored")
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Error in Redis worker process: {e}", exc_info=True)
                if self.running.value:
                    time.sleep(1)
    
    def _process_request_worker(self, request_data, redis_client, core, Timeframe):
        """Process a single request from Redis (worker process version)"""
        try:
            # Decode request if it's bytes (binary mode)
            if isinstance(request_data, bytes):
                request = json.loads(request_data.decode('utf-8'))
            else:
                request = json.loads(request_data)
            request_id = request.get('id')
            request_type = request.get('type')
            
            if not request_id:
                logger.warning("Request without ID, skipping")
                return
            
            if request_type == 'get_history':
                self._handle_get_history_worker(request_id, request, redis_client, core, Timeframe)
            else:
                logger.warning(f"Unknown request type: {request_type}")
                self._send_error_worker(request_id, f"Unknown request type: {request_type}", redis_client)
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in request: {e}")
        except Exception as e:
            logger.error(f"Error processing request: {e}", exc_info=True)
            request_id = None
            try:
                if isinstance(request_data, bytes):
                    request = json.loads(request_data.decode('utf-8'))
                else:
                    request = json.loads(request_data)
                request_id = request.get('id')
            except:
                pass
            if request_id:
                self._send_error_worker(request_id, str(e), redis_client)
    
    def _handle_get_history_worker(self, request_id: str, request: Dict[str, Any], redis_client, core, Timeframe):
        """Handle get_history request (worker process version)"""
        try:
            exchange = request.get('exchange')
            symbol = request.get('symbol')
            timeframe = request.get('timeframe')
            start_time_str = request.get('start_time')
            end_time_str = request.get('end_time')
            
            if not all([exchange, symbol, timeframe, start_time_str, end_time_str]):
                self._send_error_worker(request_id, "Missing required parameters: exchange, symbol, timeframe, start_time, end_time", redis_client)
                return
            
            # Parse datetime strings (expecting ISO format or YYYY-MM-DD HH:MM:SS)
            try:
                if 'T' in start_time_str:
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                else:
                    start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)
                
                if 'T' in end_time_str:
                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                else:
                    end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)
            except ValueError as e:
                self._send_error_worker(request_id, f"Invalid datetime format: {e}", redis_client)
                return
            
            # Get history from ClickHouse as numpy arrays
            open_arr, high_arr, low_arr, close_arr, volume_arr = core.get_history_from_database(
                exchange, symbol, timeframe, start_time, end_time
            )
            
            # Convert numpy arrays to binary format using tobytes()
            response = {
                'id': request_id,
                'status': 'success',
                'data': {
                    'open': {
                        'bytes': open_arr.tobytes(),
                        'shape': open_arr.shape,
                        'dtype': str(open_arr.dtype)
                    },
                    'high': {
                        'bytes': high_arr.tobytes(),
                        'shape': high_arr.shape,
                        'dtype': str(high_arr.dtype)
                    },
                    'low': {
                        'bytes': low_arr.tobytes(),
                        'shape': low_arr.shape,
                        'dtype': str(low_arr.dtype)
                    },
                    'close': {
                        'bytes': close_arr.tobytes(),
                        'shape': close_arr.shape,
                        'dtype': str(close_arr.dtype)
                    },
                    'volume': {
                        'bytes': volume_arr.tobytes(),
                        'shape': volume_arr.shape,
                        'dtype': str(volume_arr.dtype)
                    }
                }
            }
            self._send_response_worker(request_id, response, redis_client)
            
        except Exception as e:
            logger.error(f"Error handling get_history request: {e}", exc_info=True)
            self._send_error_worker(request_id, str(e), redis_client)
    
    def _send_response_worker(self, request_id: str, response: Dict[str, Any], redis_client):
        """Send response to Redis as binary data (numpy arrays as bytes) - worker process version"""
        try:
            response_key = f"{self.response_prefix}{request_id}"
            # Serialize response: metadata as JSON, numpy arrays as binary bytes
            # We need to encode the response structure with binary data
            # Using pickle for the entire structure since it contains bytes objects
            import pickle
            response_binary = pickle.dumps(response)
            redis_client.setex(response_key, 300, response_binary)  # 5 minutes TTL
        except Exception as e:
            logger.error(f"Error sending response to Redis: {e}", exc_info=True)
    
    def _send_error_worker(self, request_id: str, error_message: str, redis_client):
        """Send error response to Redis - worker process version"""
        response = {
            'id': request_id,
            'status': 'error',
            'error': error_message
        }
        self._send_response_worker(request_id, response, redis_client)

