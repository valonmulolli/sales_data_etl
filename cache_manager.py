"""Cache management for ETL pipeline."""

import os
import json
import pickle
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Optional, Dict
import pandas as pd

logger = logging.getLogger(__name__)

class CacheManager:
    """Manages caching of ETL pipeline data and results."""
    
    def __init__(self, cache_dir: str = 'cache', ttl_hours: int = 24):
        """
        Initialize cache manager.
        
        Args:
            cache_dir (str): Directory to store cache files
            ttl_hours (int): Cache time-to-live in hours
        """
        self.cache_dir = cache_dir
        self.ttl = timedelta(hours=ttl_hours)
        self._ensure_cache_dir()
        
    def _ensure_cache_dir(self) -> None:
        """Create cache directory if it doesn't exist."""
        os.makedirs(self.cache_dir, exist_ok=True)
        
    def _generate_key(self, data: Any) -> str:
        """
        Generate a unique cache key based on input data.
        
        Args:
            data: Input data to generate key for
            
        Returns:
            str: Unique cache key
        """
        if isinstance(data, pd.DataFrame):
            # For DataFrames, use content hash
            data_str = str(pd.util.hash_pandas_object(data).sum())
        elif isinstance(data, (dict, list)):
            # For dictionaries and lists, use JSON representation
            data_str = json.dumps(data, sort_keys=True)
        else:
            # For other types, use string representation
            data_str = str(data)
            
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def _get_cache_path(self, key: str) -> str:
        """
        Get full path for cache file.
        
        Args:
            key (str): Cache key
            
        Returns:
            str: Full cache file path
        """
        return os.path.join(self.cache_dir, f"{key}.pickle")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve data from cache.
        
        Args:
            key (str): Cache key
            
        Returns:
            Optional[Any]: Cached data if valid, None otherwise
        """
        cache_path = self._get_cache_path(key)
        
        try:
            if not os.path.exists(cache_path):
                return None
                
            # Check if cache has expired
            mod_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
            if datetime.now() - mod_time > self.ttl:
                logger.info(f"Cache expired for key: {key}")
                os.remove(cache_path)
                return None
                
            with open(cache_path, 'rb') as f:
                return pickle.load(f)
                
        except Exception as e:
            logger.warning(f"Error reading cache for key {key}: {str(e)}")
            return None
    
    def set(self, key: str, data: Any) -> None:
        """
        Store data in cache.
        
        Args:
            key (str): Cache key
            data: Data to cache
        """
        try:
            cache_path = self._get_cache_path(key)
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
            logger.debug(f"Cached data for key: {key}")
        except Exception as e:
            logger.warning(f"Error caching data for key {key}: {str(e)}")
    
    def cache_dataframe(self, df: pd.DataFrame, operation: str) -> Optional[pd.DataFrame]:
        """
        Cache DataFrame results for specific operations.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            operation (str): Operation identifier
            
        Returns:
            Optional[pd.DataFrame]: Cached DataFrame if available
        """
        key = f"{operation}_{self._generate_key(df)}"
        cached_df = self.get(key)
        
        if cached_df is not None:
            logger.info(f"Using cached results for operation: {operation}")
            return cached_df
            
        return None
    
    def save_dataframe(self, df: pd.DataFrame, operation: str, result: pd.DataFrame) -> None:
        """
        Save DataFrame operation results to cache.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            operation (str): Operation identifier
            result (pd.DataFrame): Operation result to cache
        """
        key = f"{operation}_{self._generate_key(df)}"
        self.set(key, result)
    
    def clear_expired(self) -> None:
        """Remove all expired cache entries."""
        try:
            current_time = datetime.now()
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                if current_time - mod_time > self.ttl:
                    os.remove(file_path)
                    logger.info(f"Removed expired cache file: {filename}")
        except Exception as e:
            logger.error(f"Error clearing expired cache: {str(e)}")
    
    def clear_all(self) -> None:
        """Remove all cache entries."""
        try:
            for filename in os.listdir(self.cache_dir):
                os.remove(os.path.join(self.cache_dir, filename))
            logger.info("Cleared all cache entries")
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict[str, Any]: Cache statistics
        """
        try:
            total_files = 0
            total_size = 0
            expired_files = 0
            current_time = datetime.now()
            
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                total_files += 1
                total_size += os.path.getsize(file_path)
                
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if current_time - mod_time > self.ttl:
                    expired_files += 1
            
            return {
                'total_files': total_files,
                'total_size_mb': total_size / (1024 * 1024),
                'expired_files': expired_files,
                'cache_dir': self.cache_dir,
                'ttl_hours': self.ttl.total_seconds() / 3600
            }
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {}
