import argparse
import configparser
import warnings
from typing import Dict, Callable, Union, List, Optional
from config.config import logger, mcp_config
from config.const import HEADER_ROWS, COLUMNS_NAMES


def parse_options():
    parser = argparse.ArgumentParser(
        prog=mcp_config, description="RED Forecast"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Verbose debugging", default=False
    )
    parser.add_argument("--logfile", action="store", help="Write log data to LOGFILE")
    parsed_args = parser.parse_args()
    return parsed_args
