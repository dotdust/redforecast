import pandas as pd
import json
from config.const import HEADER_ROWS, COLUMNS_NAMES
from typing import Dict, Callable, Union, List, Optional, Any
import warnings


def filter_opportunities(df: pd.DataFrame, month: str = "All",
                         content_owner: str = "All",
                         factory: str = "All",
                         from_sensitivity: float = 0,
                         to_sensitivity: float = -1,
                         status: str = "All") -> str:
    """
    Filter opportunities from the dataframe, optionally filtered by Content Owner, Factory,
    Minimum Sensitivity, Maximum Sensitivity, and Status.

    Args:
        df (pd.DataFrame): The input dataframe.
        month (str): The month to filter by in the 'Start' column.
        content_owner (str): The content owner name to filter by, or 'All'.
        factory (str): The factory name to filter by, or 'Design'.
        from_sensitivity (int): The minimum sensitivity to filter by.
        to_sensitivity (int): The maximum sensitivity to filter by.
        status (str): The status of the opportunity to filter by, or 'All'.

    Returns:
        str: Formatted string of filtered opportunities.
    """
    required_columns = [
        'id', 'Client', 'Project Name', 'Duration', 'PCC', 'PE', 'CPIS',
        'CBE', 'Design', 'Tech', 'Others', 'Start', 'Content Owner', 'Status', 'Total Value', 'Psensitivity'
    ]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    filtered_df = df

    if month != "All":
        filtered_df = df[(df['Start'] == month)]

    if content_owner != "All":
        filtered_df = filtered_df[filtered_df['Content Owner'] == content_owner]

    if status != "All":
        filtered_df = filtered_df[filtered_df['Status'] == status]

    if factory != "All":
        filtered_df = filtered_df[pd.to_numeric(filtered_df[factory], errors='coerce') > 0]

    if from_sensitivity != 0:
        filtered_df = filtered_df[pd.to_numeric(filtered_df['Psensitivity'] >= from_sensitivity,
                                                errors='coerce') >= from_sensitivity]

    if to_sensitivity != -1:
        filtered_df = filtered_df[pd.to_numeric(filtered_df['Psensitivity'] <= to_sensitivity,
                                                errors='coerce') <= to_sensitivity]

    if filtered_df.empty:
        return "No matching opportunities found."

    columns_to_display = ['id', 'Client', 'Project Name', 'Status', 'AdB', 'Opportunity Owner', 'Content Owner',
                          'Start', 'Duration', 'Psensitivity', 'Total Value', 'PCC', 'PE', 'CPIS', 'CBE', 'Design',
                          'Tech', 'Others']

    float_columns = ['PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech', 'Others', 'Psensitivity', 'Total Value']
    split_columns = ['PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech', 'Others']

    # Build the list of project dictionaries
    opportunities = []
    opportunity_split = []
    for _, row in filtered_df.iterrows():
        opportunity = {}
        opportunity_split = {}  # reset for each row

        for col in columns_to_display:
            value = row.get(col, None)
            if col in float_columns and value is not None:
                try:
                    value = f"{float(value):,.0f}"  # no decimals
                except (ValueError, TypeError):
                    value = "0"
            if col in split_columns:
                opportunity_split[col] = value
            else:
                opportunity[col] = value
        opportunity["Factories Split"] = opportunity_split
        opportunities.append(opportunity)
    summary = {
        "Total projects": len(filtered_df),
        "Total value": filtered_df['Total Value'].sum()
    }

    # Combine everything into a final dictionary
    output_json = {
        "Opportunities": opportunities,
        "Summary": summary
    }
    return json.dumps(output_json, indent=4)


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
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    df = pd.read_excel(file_path, sheet_name="All opportunities")
    df = df.iloc[HEADER_ROWS:]
    df = df.reset_index(drop=True)
    return df


def get_forecast(df: pd.DataFrame, months: List[str] = None, factory: str = 'All') -> str:
    """
    Calculate forecast metrics for the given months.

    Args:
        df (pd.DataFrame): The input dataframe containing forecast data.
        months (List[str]): List of months to calculate forecast for.
        factory (str, optional): The factory name to filter by, or 'All'. Defaults to 'All'.

    Returns:
        str: Structured JSON data containing forecast metrics with numeric values formatted with
        the thousand separators and no decimals.
    """

    def format_number(value: float) -> str:
        """Format a number with the thousand separators and no decimals."""
        return f"{int(value):,}"

    def format_values(obj):
        """Recursively format all numeric values in a dictionary or list."""
        if isinstance(obj, dict):
            return {k: format_values(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [format_values(item) for item in obj]
        elif isinstance(obj, (int, float)):
            return format_number(obj)
        else:
            return obj

    # Handle an empty dataframe or month list
    if df.empty or not months:
        empty_result = {
            "months": {},
            "total": {
                "forecast": 0,
                "weighted_forecast": 0,
                "split": {},
                "weighted_split": {}
            }
        }
        # Format all numeric values in the empty result
        formatted_empty_result = format_values(empty_result)
        return json.dumps(formatted_empty_result, indent=4)

    # Validate required columns
    required_columns = [
        'id', 'Client', 'Project Name', 'Status', 'AdB', 'Opportunity Owner', 'Content Owner', 'Start',
        'Duration', 'Psensitivity', 'Total Value', 'PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech',
        'Others'
    ]
    required_columns.extend(months)

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Define split columns
    split_columns = ['PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech', 'Others']

    # Ensure numeric columns are properly converted
    numeric_columns = ['Psensitivity', 'Total Value'] + split_columns + months
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # Filter by factory if specified
    if factory != 'All':
        df = df[pd.to_numeric(df[factory], errors='coerce') > 0]

    # Initialize result dictionary
    result = {
        "months": {},
        "total": {
            "forecast": 0,
            "weighted_forecast": 0,
            "split": {col: 0 for col in split_columns},
            "weighted_split": {col: 0 for col in split_columns}
        }
    }

    # Create a copy of the dataframe to avoid modifying the original
    df_copy = df.copy()

    # Process each month
    for month in months:
        # Initialize month data
        result["months"][month] = {
            "forecast": 0,
            "weighted_forecast": 0,
            "split": {col: 0 for col in split_columns},
            "weighted_split": {col: 0 for col in split_columns}
        }

        # Calculate total forecast for the month
        month_total = df_copy[month].sum()
        result["months"][month]["forecast"] = float(month_total)
        result["total"]["forecast"] += float(month_total)

        # Calculate split of total for the month
        for col in split_columns:
            # Calculate the proportion of each split column relative to Total Value
            # and apply it to the month's forecast
            # Handle division by zero by replacing zeros with NaN
            df_copy['temp_ratio'] = df_copy[col] / df_copy['Total Value'].replace(0, float('nan'))
            df_copy['temp_ratio'] = df_copy['temp_ratio'].fillna(0)

            # Calculate split value for the month
            split_value = (df_copy['temp_ratio'] * df_copy[month]).sum()
            result["months"][month]["split"][col] = float(split_value)
            result["total"]["split"][col] += float(split_value)

        # Calculate weighted forecast (Total Value * sensitivity)
        df_copy['weighted_value'] = df_copy[month] * df_copy['Psensitivity']
        weighted_total = df_copy['weighted_value'].sum()
        result["months"][month]["weighted_forecast"] = float(weighted_total)
        result["total"]["weighted_forecast"] += float(weighted_total)

        # Calculate split of weighted forecast
        for col in split_columns:
            # Use the same ratio but apply to weighted values
            weighted_split_value = (df_copy['temp_ratio'] * df_copy['weighted_value']).sum()
            result["months"][month]["weighted_split"][col] = float(weighted_split_value)
            result["total"]["weighted_split"][col] += float(weighted_split_value)

        # Clean up temporary columns
        df_copy = df_copy.drop(['temp_ratio', 'weighted_value'], axis=1, errors='ignore')

    # Format all numeric values in the result dictionary
    formatted_result = format_values(result)
    return json.dumps(formatted_result, indent=4)
