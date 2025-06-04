import sqlite3
from logging import Logger
from sqlite3 import Connection
import pandas as pd
import numpy as np
import datetime
from datetime import date
import os
import json
import warnings
from typing import Dict, Callable, Union, List, Optional, Any, NoReturn
from signal import signal, SIGINT
import sys
from loguru import logger
from typing import Tuple

FORECAST_FILE_PATHNAME = ('/Users/ag/Library/CloudStorage/OneDrive-BUSINESSINTEGRATIONPARTNERSSPA/'
                          'RED Team - Market Board - Forecast/RED Forecast.xlsx')
HEADER_ROWS = 21
COLUMNS_NAMES = ['na', 'na', 'id', 'Client', 'Contact Role', 'Project Name', 'na', 'na', 'Status', 'na',
                 'PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech', 'Others', 'Total Value', 'Sensitivity',
                 'Psensitivity', 'na', 'PPCC', 'na', 'PPE', 'na', 'PCPS', 'na', 'PCBE', 'na', 'Pdesign', 'na',
                 'PTech', 'na', 'na', 'na', 'na', 'na', 'na', 'Tender', 'AdB', 'Opportunity Owner',
                 'Content Owner', 'na', 'Start', 'Duration', 'na', 'January', 'February', 'March',
                 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December',
                 'na', 'Q1', 'Q2', 'Q3', 'Q4', 'na', 'FY', 'na', 'na', 'Next years']
DEBUG = True
data_file = "/Users/ag/Documents/code/sketchin/redforecast/historical_data/forecast.db"
db_connection = None


def get_forecast(conn: Connection, date1: str, date2: str) -> str:
    """
    Read the historical forecast data from the database and return two forecasts to be compared.

    Args:
        conn: Database connection
        date1 (str): The first date as a string (YYYY-MM-DD).
        date2 (str): The second date as a string (YYYY-MM-DD).

    Returns:
        str: A JSON string with the forecast data for the two dates.
    """
    d1, d2 = get_closest_dates(conn, date1, date2)
    cursor = conn.cursor()

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


def compare_forecast_entries(data: Dict, date1: str, date2: str) -> list:
    """
    Compares Opportunities for two dates by matching on 'id',
    returning full opportunities with differences annotated.

    Args:
        data (Dict): Dictionary containing forecast data.
        date1 (str): First date to compare.
        date2 (str): Second date to compare.

    Returns:
        list: Annotated opportunities with differences.
    """

    def annotate_diff(v1, v2):
        if v1 != v2:
            return {"old": v1, "new": v2}
        return v1

    def deep_annotate(o1, o2):
        if o1 is None and o2 is None:
            return None
        elif o1 is None:
            return {"old": None, "new": o2}
        elif o2 is None:
            return {"old": o1, "new": None}
        elif isinstance(o1, dict) and isinstance(o2, dict):
            return {k: deep_annotate(o1.get(k), o2.get(k)) for k in set(o1) | set(o2)}
        elif isinstance(o1, list) and isinstance(o2, list):
            # Handle lists of different lengths
            max_len = max(len(o1), len(o2))
            result = []
            for i in range(max_len):
                i1 = o1[i] if i < len(o1) else None
                i2 = o2[i] if i < len(o2) else None
                result.append(deep_annotate(i1, i2))
            return result
        else:
            return annotate_diff(o1, o2)

    opps1 = {opp['id']: opp for opp in data.get("forecasts", {}).get(date1, {}).get("Opportunities", [])}
    opps2 = {opp['id']: opp for opp in data.get("forecasts", {}).get(date2, {}).get("Opportunities", [])}

    all_ids = set(opps1) | set(opps2)
    annotated_opps = []

    for oid in all_ids:
        opp1 = opps1.get(oid)
        opp2 = opps2.get(oid)

        if opp1 and opp2:
            annotated = deep_annotate(opp1, opp2)
            annotated_opps.append(annotated)
        elif opp1:
            annotated_opps.append({"id": oid, "status": f"Only in {date1}", **opp1})
        elif opp2:
            annotated_opps.append({"id": oid, "status": f"Only in {date2}", **opp2})

    return annotated_opps


def get_closest_dates(conn: Connection, date1: str, date2: str) -> Tuple[str, str]:
    """
    Finds the closest dates to date1 (backward) and date2 (forward) in the forecast database.

    Args:
        conn: Database connection
        date1 (str): The first date as a string (YYYY-MM-DD).
        date2 (str): The second date as a string (YYYY-MM-DD).

    Returns:
        Tuple[str, str]: A tuple containing the closest backward date to date1,
                         and the closest forward date to date2.
    """
    cursor = conn.cursor()

    # Closest backward date
    cursor.execute("""
                   SELECT fdate
                   FROM forecast
                   WHERE fdate <= ?
                   ORDER BY fdate DESC
                   LIMIT 1
                   """, (date1,))

    result1 = cursor.fetchone()
    closest_date1 = result1[0] if result1 else None

    # Closest forward date
    cursor.execute("""
                   SELECT fdate
                   FROM forecast
                   WHERE fdate >= ?
                   ORDER BY fdate ASC
                   LIMIT 1
                   """, (date2,))

    result2 = cursor.fetchone()
    closest_date2 = result2[0] if result2 else None
    return closest_date1, closest_date2


def create_json(df: pd.DataFrame) -> str:
    """
    Create a JSON string from a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing opportunity data

    Returns:
        str: JSON formatted string containing opportunity data
    """
    columns_to_display = ['id', 'Client', 'Project Name', 'Status', 'AdB', 'Opportunity Owner', 'Content Owner',
                          'Start', 'Duration', 'Psensitivity', 'Total Value', 'PCC', 'PE', 'CPIS', 'CBE', 'Design',
                          'Tech', 'Others', 'January', 'February', 'March', 'April', 'May', 'June', 'July',
                          'August', 'September', 'October', 'November', 'December', 'Q1', 'Q2', 'Q3', 'Q4', 'FY']

    float_columns = ['PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech', 'Others', 'Psensitivity', 'Total Value',
                     'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                     'October', 'November', 'December', 'Q1', 'Q2', 'Q3', 'Q4', 'FY']
    split_columns = ['PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech', 'Others']
    revenue_columns = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                       'October', 'November', 'December', 'Q1', 'Q2', 'Q3', 'Q4', 'FY']

    # Build the list of project dictionaries more efficiently
    opportunities = []

    # Convert DataFrame to dictionary for faster access
    records = df[columns_to_display].to_dict('records')

    for record in records:
        opportunity = {}
        opportunity_split = {}
        opportunity_revenues = {}

        for col, value in record.items():
            if col in float_columns and value is not None:
                try:
                    value = f"{float(value):,.0f}"  # no decimals
                except (ValueError, TypeError):
                    value = "0"

            if col in revenue_columns:
                opportunity_revenues[col] = value
            elif col in split_columns:
                opportunity_split[col] = value
            else:
                opportunity[col] = value

        opportunity["Factories split"] = opportunity_split
        opportunity["Revenues by month"] = opportunity_revenues
        opportunities.append(opportunity)

    # Combine everything into a final dictionary
    output_json = {
        "Opportunities": opportunities,
    }

    return json.dumps(output_json, indent=4)


def open_db(pathname: str) -> Connection:
    """
    Opens or creates SQLite database with required schema.

    Args:
        pathname: Path to SQLite database file

    Returns:
        sqlite3.Connection: Open database connection
    """
    conn = sqlite3.connect(pathname)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS forecast
                   (
                       id
                                 INTEGER
                           PRIMARY
                               KEY
                           AUTOINCREMENT,
                       fdate
                                 TEXT(8),
                       json_data TEXT(1000000)
                   )''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_id ON forecast(id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_fdate ON forecast(fdate)')
    conn.commit()
    return conn


def normalize_data(df: pd.DataFrame, column_names: List[str]) -> pd.DataFrame:
    """
    Set the column names of a DataFrame using a provided list.

    Args:
        df (pd.DataFrame): The input DataFrame
        column_names (List[str]): List of column names to set

    Returns:
        pd.DataFrame: DataFrame with updated column names and normalized 'Start' column
    """
    # List of columns to convert
    columns_to_convert = [
        'PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech', 'Total Value',
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December',
        'Q1', 'Q2', 'Q3', 'Q4', 'FY'
    ]

    df_copy = df.copy()
    df_copy.columns = column_names
    for col in columns_to_convert:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0.0)
    df_copy['Start'] = pd.to_datetime(df_copy['Start'], errors='coerce')
    df_copy['Start'] = df_copy['Start'].dt.strftime('%B')
    df_copy['Psensitivity'] = df_copy['Psensitivity'].astype(float)
    return df_copy


def read_excel(file_path: str) -> pd.DataFrame:
    """
    Read an Excel file from the specified path, remove the first 20 rows,
    remove the first two columns, and set row 0 as column names.

    Args:
        file_path (str): Path to the Excel file

    Returns:
        pd.DataFrame: DataFrame with the Excel content, first 20 rows and first 2 columns removed,
        and row 0 set as column names
    """
    try:
        warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
        df = pd.read_excel(file_path, sheet_name="All opportunities")
        df = df.iloc[HEADER_ROWS:]
        df = df.reset_index(drop=True)
    except Exception as load_error:
        logger.error(f"Failed to load and normalize data: {load_error}")
        app_stop(1)
    print(f"Loaded and normalized data from {file_path}")
    return df


def sig_handler(signal_received: int, frame: Any) -> NoReturn:
    """
    Handle signals (e.g., SIGINT) by gracefully shutting down the application.

    Args:
    """
    app_stop(0)


def app_stop(exitcode: int) -> NoReturn:
    """
    Stop the application with the given exit code.

    Args:
        exitcode: The exit code to use when terminating the application
    """
    global db_connection
    if db_connection is not None:
        print("Closing database connection...")
        db_connection.close()
    print(f"Stopping application with exit code {exitcode}")
    sys.exit(exitcode)


def app_start() -> None:
    """
    Initialize and start the application.
    Parses command line options and displays startup information.
    """
    try:
        print("Starting application...")
    except Exception:
        app_stop(1)


def record_exists(conn: Connection, date_str: str) -> bool:
    """
    Check if a record exists for the given date.

    Args:
        conn: Database connection
        date_str: Date string in yyyy-mm-dd format

    Returns:
        bool: True if the record exists, False otherwise
    """
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM forecast WHERE fdate = ?', (date_str,))
    count = cur.fetchone()[0]
    return count > 0


def insert_record(conn: Connection, date_str: str, json_data: str) -> None:
    """
    Insert a new record into the database.

    Args:
        conn: Database connection
        date_str: Date string in yyyy-mm-dd format
        json_data: JSON data to store
    """
    cur = conn.cursor()
    cur.execute('INSERT INTO forecast (fdate, json_data) VALUES (?, ?)',
                (date_str, json_data))
    conn.commit()


def app_run() -> None:
    """
    Run the application.
    This function processes the Excel data and updates the database.
    """
    global db_connection
    # Loading Excel data
    try:
        print("Loading Excel data...")
        df = read_excel(FORECAST_FILE_PATHNAME)
        df = normalize_data(df, COLUMNS_NAMES)
        db_connection = open_db(data_file)
        json_data = create_json(df)
        print(f"Retrieved {len(df)} opportunities from Excel file with a size of {sys.getsizeof(json_data)} bytes")

        # Check and insert record for today
        today = date.today().strftime('%Y-%m-%d')
        if not record_exists(db_connection, today):
            insert_record(db_connection, today, json_data)
            print(f"Added new record for date {today}")
        else:
            print(f"Record for date {today} already exists")
        # Compare "2025-06-01" and "2025-06-04" forecast
        forecast_json = get_forecast(db_connection, "2025-06-01", "2025-06-04")
        forecast_data = json.loads(forecast_json)
        differences = compare_forecast_entries(forecast_data, "2025-06-01", "2025-06-04")
        print(f"Found {len(differences)} opportunities with differences between 2025-06-01 and 2025-06-04")
    except Exception as load_error:
        logger.error(f"Error in app_run: {load_error}")
        app_stop(1)


if __name__ == "__main__":
    signal(SIGINT, sig_handler)
    if DEBUG:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG",
                   format="<green>{time:HH:mm:ss.SSS}</green> - <green>{time:x}</green> | <level>{level: <8}</level> | "
                          "<green>{process.name}:{thread.name}</green> | <cyan>{name}</cyan>:<cyan>{function}"
                          "</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
    else:
        logger.remove()
        logger.add(sys.stderr, level="INFO",
                   format="<green>{time:HH:mm:ss.SSS}</green> - <green>{time:x}</green> | <level>{level: <8}</level> | "
                          "<green>{process.name}:{thread.name}</green> | <cyan>{name}</cyan>:<cyan>{function}"
                          "</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

    try:
        app_start()
        app_run()
        # Note: app_run() is blocking, so the following line will only execute
        # if the server stops normally
        app_stop(0)
    except Exception as exc:
        app_stop(1)
