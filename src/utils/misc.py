import argparse
import configparser
import warnings
from typing import Dict, Callable, Union, List, Optional, Tuple
from config.config import logger, mcp_config
from config.const import HEADER_ROWS, COLUMNS_NAMES
import sqlite3
from sqlite3 import Connection


def open_db(pathname: str) -> Connection:
    """
    Opens or creates SQLite database with required schema.

    Args:
        pathname: Path to SQLite database file

    Returns:
        sqlite3.Connection: Open database connection
    """
    try:
        conn = sqlite3.connect(pathname)
    except Exception as open_error:
        logger.error(f"Failed to open database: {open_error}")
        raise open_error

    return conn


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


def get_closest_dates(db_connection: sqlite3.Connection, date1: str, date2: str) -> Tuple[str, str]:
    """
    Finds the closest dates to date1 (backward) and date2 (forward) in the forecast database.

    Args:
        db_connection (sqlite3.connection): Database connection.
        date1 (str): The first date as a string (YYYY-MM-DD).
        date2 (str): The second date as a string (YYYY-MM-DD).

    Returns:
        Tuple[str, str]: A tuple containing the closest backward date to date1,
                         and the closest forward date to date2.
    """
    cursor = db_connection.cursor()

    # Closest backward date
    cursor.execute("""
                   SELECT fdate
                   FROM forecast
                   WHERE fdate <= ?
                   ORDER BY fdate DESC LIMIT 1
                   """, (date1,))

    result1 = cursor.fetchone()
    closest_date1 = result1[0] if result1 else None

    # Closest forward date
    cursor.execute("""
                   SELECT fdate
                   FROM forecast
                   WHERE fdate >= ?
                   ORDER BY fdate ASC LIMIT 1
                   """, (date2,))

    result2 = cursor.fetchone()
    closest_date2 = result2[0] if result2 else None
    return closest_date1, closest_date2
