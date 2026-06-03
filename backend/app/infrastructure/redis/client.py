"""Redis client wrapper for connection management."""

import logging
from typing import Optional, Any

try:
    import redis
    from redis import Redis, ConnectionPool
except ImportError:
    redis = None
    Redis = None
    ConnectionPool = None

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis connection wrapper with connection pooling."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        max_connections: int = 50,
        decode_responses: bool = True,
    ):
        """Initialize Redis client with connection pool.
        
        Args:
            host: Redis host
            port: Redis port
            db: Database number
            password: Optional password
            max_connections: Max pooled connections
            decode_responses: Whether to decode responses as strings
        """
        if Redis is None:
            raise RuntimeError("redis package not installed")
        
        self.host = host
        self.port = port
        self.db = db
        
        try:
            pool = ConnectionPool(
                host=host,
                port=port,
                db=db,
                password=password,
                max_connections=max_connections,
                decode_responses=decode_responses,
            )
            self.client = Redis(connection_pool=pool)
            
            # Test connection
            self.client.ping()
            logger.info(f"Redis connected: {host}:{port}/{db}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from Redis.
        
        Args:
            key: Redis key
            
        Returns:
            Value or None if key doesn't exist
        """
        try:
            return self.client.get(key)
        except Exception as e:
            logger.error(f"Error getting key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """Set a value in Redis.
        
        Args:
            key: Redis key
            value: Value to set
            ex: Expiration time in seconds
            
        Returns:
            True if successful
        """
        try:
            self.client.set(key, value, ex=ex)
            return True
        except Exception as e:
            logger.error(f"Error setting key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete a key from Redis.
        
        Args:
            key: Redis key
            
        Returns:
            True if successful
        """
        try:
            self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Error deleting key {key}: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if a key exists.
        
        Args:
            key: Redis key
            
        Returns:
            True if key exists
        """
        try:
            return self.client.exists(key) > 0
        except Exception as e:
            logger.error(f"Error checking key {key}: {e}")
            return False
    
    def ttl(self, key: str) -> int:
        """Get TTL of a key in seconds.
        
        Args:
            key: Redis key
            
        Returns:
            TTL in seconds, -1 if no expiry, -2 if not exists
        """
        try:
            return self.client.ttl(key)
        except Exception as e:
            logger.error(f"Error getting TTL for key {key}: {e}")
            return -1
    
    def flushdb(self) -> bool:
        """Flush all keys from current database.
        
        Returns:
            True if successful
        """
        try:
            self.client.flushdb()
            logger.info("Database flushed")
            return True
        except Exception as e:
            logger.error(f"Error flushing database: {e}")
            return False
    
    def close(self) -> None:
        """Close the connection pool."""
        try:
            self.client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")


# Global client instance
_client: Optional[RedisClient] = None


def get_redis_client() -> RedisClient:
    """Get the global Redis client instance."""
    global _client
    if _client is None:
        raise RuntimeError("Redis client not initialized")
    return _client


def initialize_redis(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
) -> RedisClient:
    """Initialize the global Redis client."""
    global _client
    _client = RedisClient(host=host, port=port, db=db, decode_responses=True)
    return _client
