#!/usr/bin/env python3
"""
Utilities for handling environment variables and configuration.
"""

import os
import logging
from typing import Optional, Any

# Configure logging
logger = logging.getLogger(__name__)

def load_env(file_path='.env'):
    """
    Load environment variables from .env file
    
    Args:
        file_path: Path to the .env file
        
    Returns:
        Dictionary containing environment variables
    """
    if not os.path.exists(file_path):
        logger.error(f".env file not found at {file_path}")
        return {}
        
    env_vars = {}
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            key, value = line.split('=', 1)
            env_vars[key.strip()] = value.strip()
            
    return env_vars

def safe_duration_to_seconds(duration_obj: Any) -> Optional[int]:
    """
    Safely extract seconds from a duration object, handling different stravalib versions.
    
    Args:
        duration_obj: A duration object from stravalib, could be various implementations
        
    Returns:
        Total seconds as int, or None if conversion fails
    """
    if duration_obj is None:
        return None
        
    try:
        # Try the timedelta interface with total_seconds
        if hasattr(duration_obj, 'total_seconds'):
            return int(duration_obj.total_seconds())
        # Try direct seconds attribute
        elif hasattr(duration_obj, 'seconds'):
            return int(duration_obj.seconds)
        # Try converting to int directly
        else:
            return int(duration_obj)
    except (AttributeError, ValueError, TypeError) as e:
        logger.warning(f"Could not convert duration to seconds: {e}")
        return None
