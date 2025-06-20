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
        filtered_df = filtered_df[filtered_df['Psensitivity'] >= from_sensitivity]

    if to_sensitivity != -1:
        filtered_df = filtered_df[filtered_df['Psensitivity'] <= to_sensitivity]

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
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    df = pd.read_excel(file_path, sheet_name="All opportunities")
    df = df.iloc[HEADER_ROWS:]
    df = df.reset_index(drop=True)
    return df


def get_forecast(
        df: pd.DataFrame,
        months: Optional[List[str]] = None,
        factory: str = 'All'
) -> str:
    """
    Calculate weighted and unweighted forecast metrics with splits for given months.

    Args:
        df (pd.DataFrame): Input dataframe containing forecast data.
        months (List[str]): List of months for calculations.
        factory (str, optional): Factory filter, defaults to 'All'.

    Returns:
        str: JSON-formatted forecast data with the thousand separators.
    """

    def format_number(value: float) -> str:
        return f"{int(round(value)):,.0f}"

    def format_values(obj):
        if isinstance(obj, dict):
            return {k: format_values(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [format_values(item) for item in obj]
        elif isinstance(obj, (int, float)):
            return format_number(obj)
        return obj

    # Initialize months to an empty list if None
    if months is None:
        months = []

    if df.empty or not months:
        return json.dumps({"months": {}, "total": {}}, indent=4)

    required_cols = ['Psensitivity', 'PPCC', 'PPE', 'PCPS', 'PCBE', 'Pdesign', 'PTech'] + months
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    df = df[required_cols].copy()
    df.fillna(0, inplace=True)

    numeric_cols = ['Psensitivity'] + months + required_cols[1:7]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

    if factory != 'All' and factory in df.columns:
        df = df[df[factory] > 0]

    split_columns = {col: col[1:].capitalize() for col in required_cols[1:7]}
    result = {"months": {}, "total": {"unweighted_forecast": 0, "weighted_forecast": 0, "split": {}}}

    for col in list(split_columns.values()) + ['Others']:
        result["total"]["split"][col] = {"unweighted": 0, "weighted": 0}

    for month in months:
        unweighted = (df[month] / df['Psensitivity'].replace(0, pd.NA)).fillna(0)
        weighted = df[month]

        splits = df[list(split_columns.keys())]
        # Ensure POthers is not negative by clamping to 0
        splits['POthers'] = (1 - splits.sum(axis=1)).clip(lower=0)

        month_data = {
            "unweighted_forecast": unweighted.sum(),
            "weighted_forecast": weighted.sum(),
            "split": {}
        }

        for original_col, new_col in split_columns.items():
            uw_value = (splits[original_col] * unweighted).sum()
            w_value = (splits[original_col] * weighted).sum()
            month_data["split"][new_col] = {"unweighted": uw_value, "weighted": w_value}
            result["total"]["split"][new_col]["unweighted"] += uw_value
            result["total"]["split"][new_col]["weighted"] += w_value

        others_uw = (splits['POthers'] * unweighted).sum()
        others_w = (splits['POthers'] * weighted).sum()
        month_data["split"]["Others"] = {"unweighted": others_uw, "weighted": others_w}

        result["total"]["split"]["Others"]["unweighted"] += others_uw
        result["total"]["split"]["Others"]["weighted"] += others_w

        result["total"]["unweighted_forecast"] += month_data["unweighted_forecast"]
        result["total"]["weighted_forecast"] += month_data["weighted_forecast"]

        result["months"][month] = month_data

    return json.dumps(format_values(result), indent=4)
