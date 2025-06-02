from config.config import mcp_config
from datawrangler.pandas_functions import (
    read_excel, normalize_data, filter_opportunities, get_forecast)
from config.const import FORECAST_FILE_PATHNAME, HEADER_ROWS, COLUMNS_NAMES
from typing import Dict, Callable, Union, List, Optional, Any
from datetime import datetime
from utils.misc import get_closest_dates

import json
import sqlite3


def compare_forecast_dates(date1: str, date2: str) -> str:
    """
        Read the historical forecast data from the database and return two forecasts to be compared

        Args:
            date1 (str): The first date as a string (YYYY-MM-DD).
            date2 (str): The second date as a string (YYYY-MM-DD).

        Returns:
            A JSON object with the forecast data for the two dates.
    """
    d1, d2 = get_closest_dates(mcp_config.db, date1, date2)
    cursor = mcp_config.db.cursor()

    result = {"forecasts": {}}

    # Get first forecast
    cursor.execute("SELECT json_data FROM forecast WHERE fdate = ?", (d1,))
    row1 = cursor.fetchone()
    if row1:
        result["forecasts"][d1] = {
            "date": d1,
            "forecast": json.loads(row1[0])
        }

    # Get the second forecast
    cursor.execute("SELECT json_data FROM forecast WHERE fdate = ?", (d2,))
    row2 = cursor.fetchone()
    if row2:
        result["forecasts"][d2] = {
            "date": d2,
            "forecast": json.loads(row2[0])
        }

    return json.dumps(result)


def get_forecast_data(months: List[str], factory: str = 'All') -> str:
    """
        Get forecast data for the given months, optionally filtered by Factory.
        If the month list is empty, use the current month.

        Args:
            months (List): A list with month to filter by in the 'Start' column.
            factory (str): The factory name to filter by, or all by default.

        Returns:
            str: Formatted string of the forecast data.
        """
    if not hasattr(mcp_config, 'df') or mcp_config.df is None:
        return "Error: Dataframe not loaded. Please ensure the application has loaded the data."

    try:
        return get_forecast(mcp_config.df, months, factory)
    except Exception as load_error:
        return f"Error: Something went wrong...\n {load_error}"


def get_opportunities_with_filters(month: str = "All", content_owner: str = "All", factory: str = "All",
                                   from_sensitivity: float = 0, to_sensitivity: float = -1,
                                   status: str = "All") -> str:
    """
    Filter opportunities from the dataframe, optionally filtered by Content Owner, Factory,
    Sensitivity, and Status.

    Args:
        month (str): The month to filter by in the 'Start' column.
        content_owner (str): The content owner name to filter by, or 'All'.
        factory (str): The factory name to filter by, or 'Design'.
        from_sensitivity (float): The minimum sensitivity to filter by.
        to_sensitivity (float): The maximum sensitivity to filter by.
        status (str): The status of the opportunity to filter by, or 'All'.

    Returns:
        str: Formatted string of filtered opportunities.
    """
    if not hasattr(mcp_config, 'df') or mcp_config.df is None:
        return "Error: Dataframe not loaded. Please ensure the application has loaded the data."

    try:
        return filter_opportunities(mcp_config.df, month, content_owner, factory,
                                    from_sensitivity, to_sensitivity, status, )
    except Exception as load_error:
        return f"Error: Something went wrong...\n {load_error}"


def register_tools():
    """Register all MCP tools from this module."""
    mcp = mcp_config.mcp
    if mcp is None:
        raise RuntimeError("MCP instance not initialized")

    # Register the tool
    mcp.tool()(get_opportunities_with_filters)
    mcp.tool()(get_forecast_data)
    mcp.tool()(compare_forecast_dates)


def reload_forecast_data() -> str:
    """
    Reload the forecast data from the Excel file.

    Args:

    Returns:
        None
    """
    result = "Data loaded successfully"

    try:
        mcp_config.df = read_excel(FORECAST_FILE_PATHNAME)
        mcp_config.df = set_column_names(mcp_config.df, COLUMNS_NAMES)
    except Exception as load_error:
        result = f"Failed to load data: {load_error}"

    return result
