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


def create_json(df: pd.DataFrame) -> str:
    """
    Create a JSON string from a DataFrame.

    Returns:
        str: JSON formatted string containing opportunity data
    """
    # Define required variables
    filtered_df = df

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

    # Build the list of project dictionaries
    opportunities = []
    for _, row in filtered_df.iterrows():
        opportunity = {}
        opportunity_split = {}  # reset for each row
        opportunity_revenues = {}
        for col in columns_to_display:
            value = row.get(col, None)
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
    df_copy['Psensitivty'] = df_copy['Psensitivity'].astype(float)
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
    print(f"Closing database connection...")
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


def record_exists(date_str: str) -> bool:
    """
    Check if a record exists for the given date.

    Args:
        date_str: Date string in yyyy-mm-dd format

    Returns:
        bool: True if the record exists, False otherwise
    """
    cur = db_connection.cursor()
    cur.execute('SELECT COUNT(*) FROM forecast WHERE fdate = ?', (date_str,))
    count = cur.fetchone()[0]
    return count > 0


def insert_record(date_str: str, json_data: str) -> None:
    """
    Insert a new record into the database.

    Args:
        date_str: Date string in yyyy-mm-dd format
        json_data: JSON data to store
    """
    cur = db_connection.cursor()
    cur.execute('INSERT INTO forecast (fdate, json_data) VALUES (?, ?)',
                (date_str, json_data))
    db_connection.commit()


def app_run() -> None:
    """
    Run the MCP server.
    This function starts the server and blocks until the server is stopped.
    """
    global db_connection
    # Loading Excel data
    try:
        print("Loading Excel data...")
        df = None
        df = read_excel(FORECAST_FILE_PATHNAME)
        df = normalize_data(df, COLUMNS_NAMES)
        db_connection = open_db(data_file)
        json_data = create_json(df)
        print(f"Retrieved {len(df)} opportunities from Excel file with a size of {sys.getsizeof(json_data)} bytes")
        # Check and insert record for today
        today = date.today().strftime('%Y-%m-%d')
        if not record_exists(today):
            insert_record(today, json_data)
            print(f"Added new record for date {today}")
        else:
            print(f"Record for date {today} already exists")
    except Exception as load_error:
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
