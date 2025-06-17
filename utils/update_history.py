import sqlite3
from sqlite3 import Connection
import pandas as pd
import datetime
from datetime import date
import os
import json
import sys
from loguru import logger
from typing import Dict, Callable, Union, List, Optional, Any, NoReturn, Tuple
from signal import signal, SIGINT
# Import from the main application
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from mcpserver.config.const import FORECAST_FILE_PATHNAME, HEADER_ROWS, COLUMNS_NAMES, DB_FILE_PATHNAME
from mcpserver.utils.misc import open_db, get_closest_dates
from mcpserver.datawrangler.pandas_functions import read_excel, normalize_data

# Configuration
DEBUG = os.environ.get('REDFORECAST_DEBUG', 'False').lower() in ('true', '1', 't')
data_file = DB_FILE_PATHNAME
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

    # Get the first forecast
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
            # Calculate the difference between oldest and newest values
            # For numeric values, calculate actual difference
            if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                difference = v2 - v1
            else:
                # For non-numeric values, just use the newest value as the difference
                difference = v2
            return {"oldest": v1, "newest": v2, "difference": difference}
        return {"value": v1}

    def is_empty(value):
        """Check if a value is considered empty."""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        if isinstance(value, (list, dict)) and len(value) == 0:
            return True
        if pd.isna(value):
            return True
        return False

    def should_exclude_field(key):
        """Check if a field should be excluded from comparison and results."""
        excluded_fields = [
            "PCC", "PE", "CPIS", "Design", "Tech", "Others",
            "PPCC", "PPE", "PCPS", "PCBE", "Pdesign", "PTech",
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
            "Q1", "Q2", "Q3", "Q4", "FY", "Next years",
            "Factories split", "Revenues by month",
            "id"  # Exclude id field from comparison
        ]
        return key in excluded_fields or key.endswith("_id") or key.endswith("Id")

    def filter_excluded_fields(opp):
        """Remove excluded fields from an opportunity dictionary."""
        return {k: v for k, v in opp.items() if not should_exclude_field(k)}

    def deep_annotate(o1, o2, key=None):
        # Special handling for Start and Duration fields
        if key in ["Start", "Duration"]:
            # If both values are NaN, return no difference
            if pd.isna(o1) and pd.isna(o2):
                return {"value": None}
            # Only report difference if first set is NaN and second set is a valid Month
            elif pd.isna(o1) and not pd.isna(o2):
                return {"oldest": o1, "newest": o2, "difference": o2}
            # If values are the same or don't meet the special condition, no difference
            elif o1 == o2 or (not pd.isna(o1) and pd.isna(o2)):
                return {"value": o1}
            else:
                return annotate_diff(o1, o2)

        # If both values are None or empty, they're not different
        if is_empty(o1) and is_empty(o2):
            return {"value": None}
        # Standard None handling
        elif o1 is None and o2 is None:
            return {"value": None}
        elif o1 is None:
            return {"oldest": None, "newest": o2, "difference": o2}
        elif o2 is None:
            return {"oldest": o1, "newest": None, "difference": None}
        elif isinstance(o1, dict) and isinstance(o2, dict):
            result = {}
            has_diff = False
            for k in set(o1) | set(o2):
                # Skip excluded fields
                if should_exclude_field(k):
                    continue
                result[k] = deep_annotate(o1.get(k), o2.get(k), k)
                # Check if this key has a difference by looking for 'oldest' and 'newest' keys
                if "oldest" in result[k] and "newest" in result[k]:
                    has_diff = True
            # We'll use has_diff for internal logic but won't include it in the output
            return result
        elif isinstance(o1, list) and isinstance(o2, list):
            # Handle lists of different lengths
            max_len = max(len(o1), len(o2))
            result = []
            has_diff = False
            for i in range(max_len):
                i1 = o1[i] if i < len(o1) else None
                i2 = o2[i] if i < len(o2) else None
                item_result = deep_annotate(i1, i2)
                result.append(item_result)
                # Check if this item has a difference
                if "oldest" in item_result and "newest" in item_result:
                    has_diff = True
            # We'll use has_diff for internal logic but won't include it in the output
            return {"value": result}
        else:
            return annotate_diff(o1, o2)

    # Extract opportunities from the forecast data
    forecast1 = data.get("forecasts", {}).get(date1, {}).get("forecast", {})
    forecast2 = data.get("forecasts", {}).get(date2, {}).get("forecast", {})

    # Create a composite key using Client and Project Name
    def create_composite_key(opp):
        return f"{opp.get('Client', '')}__{opp.get('Project Name', '')}"

    # Create dictionaries with composite keys
    opps1 = {create_composite_key(opp): opp for opp in forecast1.get("Opportunities", [])}
    opps2 = {create_composite_key(opp): opp for opp in forecast2.get("Opportunities", [])}

    all_keys = set(opps1) | set(opps2)
    annotated_opps = []

    for key in all_keys:
        opp1 = opps1.get(key)
        opp2 = opps2.get(key)

        # Skip opportunities in the first set where Total Value is 0
        if opp1 and opp1.get("Total Value") == "0":
            continue

        if opp1 and opp2:
            # Skip opportunity if both Duration and Start are NaN in both sets
            if (pd.isna(opp1.get('Start')) and pd.isna(opp2.get('Start')) and
                    pd.isna(opp1.get('Duration')) and pd.isna(opp2.get('Duration'))):
                continue

            annotated = deep_annotate(opp1, opp2)
            # Add id and status to the annotated opportunity
            # Use the id from opp2 (newer version) if available, otherwise from opp1
            annotated["id"] = opp2.get('id', opp1.get('id', ''))

            # Check if there are any differences by looking for keys with 'oldest' and 'newest'
            has_differences = False

            def check_for_differences(obj):
                if isinstance(obj, dict):
                    if "oldest" in obj and "newest" in obj:
                        return True
                    return any(check_for_differences(v) for v in obj.values())
                elif isinstance(obj, list):
                    return any(check_for_differences(item) for item in obj)
                return False

            def generate_difference_explanation(obj, path=None):
                """
                Generate a human-readable explanation of differences between old and new versions.

                Args:
                    obj: The annotated object containing differences
                    path: Current path in the object (for recursive calls)

                Returns:
                    str: Human-readable explanation of differences
                """
                if path is None:
                    path = []

                explanations = []

                if isinstance(obj, dict):
                    if "oldest" in obj and "newest" in obj:
                        # This is a leaf node with a difference
                        field_name = ".".join(path) if path else "Field"
                        old_val = obj["oldest"]
                        new_val = obj["newest"]

                        # Format the explanation based on value type
                        if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
                            diff = new_val - old_val
                            if diff > 0:
                                explanations.append(f"{field_name} increased from {old_val} to {new_val} (+{diff})")
                            else:
                                explanations.append(f"{field_name} decreased from {old_val} to {new_val} ({diff})")
                        else:
                            explanations.append(f"{field_name} changed from '{old_val}' to '{new_val}'")
                    else:
                        # Recursively check all keys in the dictionary
                        for key, value in obj.items():
                            # Skip special keys
                            if key in ["id", "status", "difference_explanation"]:
                                continue

                            new_path = path + [key]
                            sub_explanations = generate_difference_explanation(value, new_path)
                            if sub_explanations:
                                explanations.extend(sub_explanations)
                elif isinstance(obj, list):
                    # For lists, check each item
                    for i, item in enumerate(obj):
                        new_path = path + [f"[{i}]"]
                        sub_explanations = generate_difference_explanation(item, new_path)
                        if sub_explanations:
                            explanations.extend(sub_explanations)

                return explanations

            has_differences = check_for_differences(annotated)

            if has_differences:
                annotated["status"] = "Modified"
                # Generate explanation of differences
                explanations = generate_difference_explanation(annotated)
                if explanations:
                    annotated["difference_explanation"] = "\n".join(explanations)
                else:
                    annotated["difference_explanation"] = "No specific differences found"
            else:
                annotated["status"] = "Unchanged"
            annotated_opps.append(annotated)
        elif opp1:
            filtered_opp1 = filter_excluded_fields(opp1)
            annotated_opps.append({
                "id": opp1.get('id', ''),
                "status": "Deleted",
                "difference_explanation": f"This opportunity was present in {date1} but removed in {date2}",
                **filtered_opp1
            })
        elif opp2:
            filtered_opp2 = filter_excluded_fields(opp2)
            annotated_opps.append({
                "id": opp2.get('id', ''),
                "status": "New",
                "difference_explanation": f"This is a new opportunity added in {date2}",
                **filtered_opp2
            })

    # Filter out opportunities that haven't changed if there are any differences
    has_any_differences = any(opp.get("status") in ["Modified", "New", "Deleted"] for opp in annotated_opps)
    if has_any_differences:
        return [opp for opp in annotated_opps if opp.get("status") != "Unchanged"]
    else:
        return annotated_opps


# Using get_closest_dates from src.utils.docs


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


# Using open_db from src.utils.docs


# Using normalize_data and read_excel from src.datawrangler.pandas_functions


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
        forecast_json = get_forecast(db_connection, "2025-06-01", "2025-06-07")
        forecast_data = json.loads(forecast_json)
        differences = compare_forecast_entries(forecast_data, "2025-06-01", "2025-06-07")
        print(f"Found {len(differences)} opportunities with differences between 2025-06-01 and 2025-06-04")
        print(json.dumps(differences, indent=4))
    except Exception as load_error:
        logger.error(f"Error in app_run: {load_error}")
        app_stop(1)


if __name__ == "__main__":
    signal(SIGINT, sig_handler)

    # Configure logger
    logger.remove()  # Remove default handler
    log_format = ("<green>{time:HH:mm:ss.SSS}</green> - <green>{time:x}</green> | <level>{level: <8}</level> | "
                  "<green>{process.name}:{thread.name}</green> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>"
                  "{line}</cyan> - <level>{message}</level>")

    # Set log level based on configuration
    log_level = "DEBUG" if DEBUG else "INFO"
    logger.add(sys.stderr, level=log_level, format=log_format)

    # Set log file if specified in environment
    log_file = os.environ.get('REDFORECAST_LOG_FILE')
    if log_file:
        logger.add(log_file, rotation="10 MB", retention="1 week", level=log_level, format=log_format)

    logger.debug(f"Logger initialized with level {log_level}")

    try:
        app_start()
        app_run()
        # Note: app_run() is blocking, so the following line will only execute
        # if the server stops normally
        app_stop(0)
    except Exception as exc:
        app_stop(1)
