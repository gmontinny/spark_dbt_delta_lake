"""
Utility module for setting up consistent logging across the application.
"""

import os
import logging
from config.config import APP_CONFIG

def setup_logger(name, log_file=None):
    """
    Set up a logger with consistent formatting and handlers.
    
    Args:
        name (str): Name of the logger, typically __name__
        log_file (str, optional): Name of the log file. If None, uses the name parameter.
        
    Returns:
        logging.Logger: Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        # Set level from config
        logger.setLevel(getattr(logging, APP_CONFIG['log_level']))
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Create file handler
        if log_file is None:
            log_file = f"{name.split('.')[-1]}.log"
        
        file_handler = logging.FileHandler(os.path.join('logs', log_file))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger