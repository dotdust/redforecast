import pandas as pd
from config.const import HEADER_ROWS, COLUMNS_NAMES
from typing import Dict, Callable, Union, List, Optional
import warnings


def projects_by_month(df: pd.DataFrame, month: str, content_owner: str = "All", factory: str = "Design") -> str:
    """
    Extracts opportunities for a given month from the dataframe,
    optionally filtered by Content Owner. If 'All' is specified, only
    rows with Design > 0 are returned. Returns a formatted string for readability.

    Args:
        df (pd.DataFrame): The input dataframe.
        month (str): The month to filter by in the 'Start' column.
        content_owner (str): The content owner name to filter by, or 'All'.
        factory (str): The factory name to filter by, or 'Design'.

    Returns:
        str: Formatted string of filtered opportunities. Teh returned data must not be processed further.
    """
    required_columns = [
        'id', 'Client', 'Project Name', 'Duration', 'PCC', 'PE', 'CPIS',
        'CBE', 'Design', 'Tech', 'Others', 'Start', 'Content Owner'
    ]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    filtered_df = df[
        (df['Start'] == month) &
        (pd.to_numeric(df[factory], errors='coerce').notna())
        ]

    if content_owner != "All":
        filtered_df = filtered_df[filtered_df['Content Owner'] == content_owner]

    if filtered_df.empty:
        return "No matching opportunities found."

    output = f"\nOpportunities for month: {month}"
    if content_owner != "All":
        output += f" | Content Owner: {content_owner}"
    output += "\n\n"

    for idx, row in filtered_df.iterrows():
        output += f"Opportunity ID: {row['id']}\n"
        output += f"  Client       : {row['Client']}\n"
        output += f"  Project Name : {row['Project Name']}\n"
        output += f"  Duration     : {row['Duration']}\n"
        output += f"  PCC          : {row['PCC']}\n"
        output += f"  PE           : {row['PE']}\n"
        output += f"  CPIS         : {row['CPIS']}\n"
        output += f"  CBE          : {row['CBE']}\n"
        output += f"  Design       : {row['Design']}\n"
        output += f"  Tech         : {row['Tech']}\n"
        output += f"  Others       : {row['Others']}\n\n"

    return output.strip()


def projects_from_content_owner(df: pd.DataFrame, sd: str) -> str:
    """
    Extract rows from the DataFrame where 'Content Owner' matches the provided value,
    collect specific columns for each row, and include summary statistics.

    Args:
        df (pd.DataFrame): The input DataFrame
        sd (str, optional): The content owner to filter by.

    Returns:
        str: A text string containing the project details and summary statistics
    """
    # Filter rows where 'Content Owner' matches the provided value
    filtered_df = df[df['Content Owner'] == sd]

    # Get the columns to display
    columns_to_display = ['Client', 'Project Name', 'Sensitivity', 'Start', 'Duration', 'Total Value']

    # Initialize output string
    output = f"\nProjects owned by {sd}:\n\n"

    # Add each row with the specified columns to the output
    for index, row in filtered_df.iterrows():
        output += f"Project {index + 1}:\n"
        for col in columns_to_display:
            output += f"  {col}: {row[col]}\n"
        output += "\n"

    # Calculate and add summary statistics to the output
    row_count = len(filtered_df)
    total_value_sum = filtered_df['Total Value'].sum()

    output += f"Summary:\n"
    output += f"  Total projects: {row_count}\n"
    output += f"  Total value: {total_value_sum}"

    # Return the output string
    return output


def normalize_data(df: pd.DataFrame, column_names: List[str]) -> pd.DataFrame:
    """
    Set the column names of a DataFrame using a provided list.

    Args:
        df (pd.DataFrame): The input DataFrame
        column_names (List[str]): List of column names to set

    Returns:
        pd.DataFrame: DataFrame with updated column names and normalized 'Start' column
    """
    df_copy = df.copy()
    df_copy.columns = column_names
    df_copy['Start'] = pd.to_datetime(df_copy['Start'], errors='coerce')
    df_copy['Start'] = df_copy['Start'].dt.strftime('%B')
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
