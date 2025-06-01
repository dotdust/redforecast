#!/usr/bin/env python3
"""
RED Forecast - Main application module
This module contains the main application logic for the RED Forecast application.
"""

import sys
from typing import Any, NoReturn, Optional
from fastmcp import FastMCP
from signal import signal, SIGINT
from utils.misc import parse_options
from datawrangler.pandas_functions import (
    read_excel, normalize_data
)
from config.config import logger, mcp_config
from config.const import (
    app_Name, app_Version, FORECAST_FILE_PATHNAME, HEADER_ROWS, COLUMNS_NAMES
)
from mcptools.mcptools import register_tools

mcp_forecast = None


def sig_handler(signal_received: int, frame: Any) -> NoReturn:
    """
    Handle signals (e.g., SIGINT) by gracefully shutting down the application.

    Args:
        signal_received: The signal number received
        frame: The current stack frame
    """
    logger.debug(f"Signal {signal_received} received at frame {frame}")
    app_stop(0)


def app_stop(exitcode: int) -> NoReturn:
    """
    Stop the application with the given exit code.

    Args:
        exitcode: The exit code to use when terminating the application
    """
    sys.exit(exitcode)


def app_start() -> None:
    """
    Initialize and start the application.
    Parses command line options and displays startup information.
    """
    try:
        mcp_config.args = parse_options()
    except Exception as start_error:
        logger.error(f"Failed to start application: {start_error}")
        app_stop(1)


def app_run() -> None:
    """
    Run the MCP server.
    This function starts the server and blocks until the server is stopped.
    """
    global mcp_forecast

    # Loading Excel data
    try:
        mcp_forecast = FastMCP("RED Forecast - MCP Server")
        mcp_config.mcp = mcp_forecast
        mcp_config.df = read_excel(FORECAST_FILE_PATHNAME)
        mcp_config.df = normalize_data(mcp_config.df, COLUMNS_NAMES)
    except Exception as load_error:
        logger.error(f"Failed to load data: {load_error}")
        app_stop(1)

    try:
        register_tools()
        mcp_forecast.run()
    except Exception as run_error:
        logger.error(f"Failed to run MCP server: {run_error}")
        app_stop(1)


if __name__ == "__main__":
    signal(SIGINT, sig_handler)

    try:
        app_start()
        app_run()
        # Note: app_run() is blocking, so the following line will only execute
        # if the server stops normally
        app_stop(0)
    except Exception as exc:
        logger.error("Unhandled exception: {}", exc)
        app_stop(1)
