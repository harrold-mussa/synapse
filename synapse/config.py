"""
Config - Configuration management for pyql pipelines.
"""

import json
import os
from typing import Any, Dict, Optional
from pathlib import Path


class Config:
    """
    Configuration management with environment variable support and defaults.
    """
    
    def __init__(self, config_dict: Dict[str, Any] = None):
        self._config = config_dict or {}
        self._defaults = {
            'database': ':memory:',
            'log_level': 'INFO',
            'retry_count': 0,
            'timeout': None,
            'batch_size': 10000,
            'parallel_execution': False,
            'checkpoint_enabled': False,
            'checkpoint_dir': '.pyql_checkpoints'
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        # Check environment variables first (with pyql_ prefix)
        env_key = f"pyql_{key.upper()}"
        env_value = os.environ.get(env_key)
        if env_value is not None:
            return self._parse_env_value(env_value)
        
        # Check config dict
        if key in self._config:
            return self._config[key]
        
        # Check defaults
        if key in self._defaults:
            return self._defaults[key]
        
        return default
    
    def set(self, key: str, value: Any):
        """Set a configuration value."""
        self._config[key] = value
    
    def update(self, config_dict: Dict[str, Any]):
        """Update multiple configuration values."""
        self._config.update(config_dict)
    
    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable string to appropriate type."""
        # Try to parse as JSON
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            pass
        
        # Try to parse as boolean
        if value.lower() in ('true', 'yes', '1'):
            return True
        if value.lower() in ('false', 'no', '0'):
            return False
        
        # Try to parse as number
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            pass
        
        return value
    
    @classmethod
    def from_file(cls, path: str) -> 'Config':
        """Load configuration from a JSON file."""
        with open(path, 'r') as f:
            config_dict = json.load(f)
        return cls(config_dict)
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        config = cls()
        # Load any pyql_* environment variables
        for key, value in os.environ.items():
            if key.startswith('pyql_'):
                config_key = key[5:].lower()
                config.set(config_key, config._parse_env_value(value))
        return config
    
    def to_file(self, path: str):
        """Save configuration to a JSON file."""
        with open(path, 'w') as f:
            json.dump(self._config, f, indent=2)
    
    def to_dict(self) -> Dict[str, Any]:
        """Get all configuration as dictionary."""
        result = self._defaults.copy()
        result.update(self._config)
        return result
    
    def __getitem__(self, key: str) -> Any:
        return self.get(key)
    
    def __setitem__(self, key: str, value: Any):
        self.set(key, value)
    
    def __repr__(self):
        return f"Config({self._config})"


# Default global configuration
_default_config = Config()


def get_config() -> Config:
    """Get the default global configuration."""
    return _default_config


def set_config(config: Config):
    """Set the default global configuration."""
    global _default_config
    _default_config = config
