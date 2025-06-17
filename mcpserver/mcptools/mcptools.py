try:
    # Try relative imports (when running from mcpserver directory)
    from config.config import mcp_config
    from datawrangler.pandas_functions import (
        read_excel, normalize_data, filter_opportunities, get_forecast)
    from config.const import FORECAST_FILE_PATHNAME, HEADER_ROWS, COLUMNS_NAMES
    from utils.misc import get_closest_dates
except ImportError:
    # Try absolute imports (when running from project root)
    from mcpserver.config.config import mcp_config
    from mcpserver.datawrangler.pandas_functions import (
        read_excel, normalize_data, filter_opportunities, get_forecast)
    from mcpserver.config.const import FORECAST_FILE_PATHNAME, HEADER_ROWS, COLUMNS_NAMES
    from mcpserver.utils.misc import get_closest_dates

from typing import Dict, Callable, Union, List, Optional, Any
from datetime import datetime
import json
import sqlite3


def compare_forecast_dates(date1: str, date2: str) -> str:
    """
        Read the historical forecast data from the database, compare the forecasts,
        and return the differences as JSON data.

        Args:
            date1 (str): The first date as a string (YYYY-MM-DD).
            date2 (str): The second date as a string (YYYY-MM-DD).

        Returns:
            A JSON object with the differences between the forecasts for the two dates.
    """
    d1, d2 = get_closest_dates(mcp_config.db, date1, date2)
    cursor = mcp_config.db.cursor()

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

    # Compare the forecasts using the same logic as compare_forecast_entries in update_history.py
    differences = compare_forecast_entries(result, d1, d2)

    # Return the differences as JSON data
    return json.dumps({"differences": differences})


# noinspection PyTypeChecker
def compare_forecast_entries(data: dict, date1: str, date2: str) -> list:
    """
    Compares Opportunities for two dates by matching on 'Client' and 'Project Name',
    returning full opportunities with differences annotated.

    Args:
        data (dict): Dictionary containing forecast data.
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
                # For non-numeric values, use the newest value as the difference
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
        try:
            import pandas as pd
            if pd.isna(value):
                return True
        except (ImportError, AttributeError):
            pass
        return False

    def should_exclude_field(field_key):
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
        return field_key in excluded_fields or field_key.endswith("_id") or field_key.endswith("Id")

    def filter_excluded_fields(opp):
        """Remove excluded fields from an opportunity dictionary."""
        return {k: v for k, v in opp.items() if not should_exclude_field(k)}

    def deep_annotate(o1, o2, field_key=None):
        # Special handling for Start and Duration fields
        if field_key in ["Start", "Duration"]:
            # If both values are empty, return no difference
            if is_empty(o1) and is_empty(o2):
                return {"value": None}
            # Only report difference if the first set is empty and the second set is not
            elif is_empty(o1) and not is_empty(o2):
                return {"oldest": o1, "newest": o2, "difference": o2}
            # If values are the same or don't meet the special condition, no difference
            elif o1 == o2 or (not is_empty(o1) and is_empty(o2)):
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

        # Skip opportunities in the first set where the Total Value is 0
        if opp1 and opp1.get("Total Value") == "0":
            continue

        if opp1 and opp2:
            # Skip opportunity if both Duration and Start are empty in both sets
            if (is_empty(opp1.get('Start')) and is_empty(opp2.get('Start')) and
                    is_empty(opp1.get('Duration')) and is_empty(opp2.get('Duration'))):
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

    # Register the tools
    mcp.tool()(get_opportunities_with_filters)
    mcp.tool()(get_forecast_data)
    mcp.tool()(compare_forecast_dates)
    mcp.tool()(reload_forecast_data)


def reload_forecast_data() -> str:
    """
    Reload the forecast data from the Excel file.

    Args:

    Returns:
        str: Status message indicating success or failure
    """
    result = "Data loaded successfully"

    try:
        mcp_config.df = read_excel(FORECAST_FILE_PATHNAME)
        mcp_config.df = normalize_data(mcp_config.df, COLUMNS_NAMES)
    except Exception as load_error:
        result = f"Failed to load data: {load_error}"

    return result
