from loguru import logger
import sys
import os


class Config:
    """Configuration class for the application."""
    # General
    args = None
    LOG_DEBUG = os.environ.get('REDFORECAST_DEBUG', 'False').lower() in ('true', '1', 't')

    df = None
    mcp = None
    db = None


# Create a single instance of the Config class
mcp_config = Config()

# Configure logger
logger.remove()  # Remove default handler
log_format = "<green>{time:HH:mm:ss.SSS}</green> - <green>{time:x}</green> | <level>{level: <8}</level> | <green>{process.name}:{thread.name}</green> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

# Set log level based on configuration
log_level = "DEBUG" if mcp_config.LOG_DEBUG else "INFO"
logger.add(sys.stderr, level=log_level, format=log_format)

# Set log file if specified in environment
log_file = os.environ.get('REDFORECAST_LOG_FILE')
if log_file:
    logger.add(log_file, rotation="10 MB", retention="1 week", level=log_level, format=log_format)

logger.debug(f"Logger initialized with level {log_level}")
