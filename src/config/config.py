from decouple import config
from loguru import logger
import sys


class Config(object):
    # General
    logger = None
    args = None
    LOG_DEBUG = True  # True if logging level is DEBUG, False otherwise

    df = None
    mcp = None
    db = None


class ProductionConfig(Config):
    pass


class DebugConfig(Config):
    pass


CONFIG_DEBUG = True  # True if debug configuration is used, production otherwise
if CONFIG_DEBUG:
    mcp_config = DebugConfig
else:
    mcp_config = ProductionConfig

if mcp_config.LOG_DEBUG:
    logger.remove()
    logger.add(sys.stderr, level="DEBUG",
               format="<green>{time:HH:mm:ss.SSS}</green> - <green>{time:x}</green> | <level>{level: <8}</level> | <green>{process.name}:{thread.name}</green> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
else:
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss.SSS}</green> - <green>{time:x}</green> | <level>{level: <8}</level> | <green>{process.name}:{thread.name}</green> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
