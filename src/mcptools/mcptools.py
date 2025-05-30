from config.config import mcp_config
from datawrangler.pandas_functions import (
    read_excel, normalize_data, projects_from_content_owner, projects_by_month)
from config.const import FORECAST_FILE_PATHNAME, HEADER_ROWS, COLUMNS_NAMES


def register_tools():
    """Register all MCP tools from this module."""
    mcp = mcp_config.mcp
    if mcp is None:
        raise RuntimeError("MCP instance not initialized")

    # Register the tool
    mcp.tool()(get_projects_by_content_owner)
    mcp.tool()(reload_forecast_data)
    mcp.tool()(get_projects_by_month)


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


def get_projects_by_month(month: str, content_owner: str = "All") -> str:
    """
    Extracts opportunities for a given month from the dataframe,
    optionally filtered by Content Owner. If 'All' is specified, only
    rows with Design > 0 are returned. Returns a formatted string for readability.

    Args:
        month (str): The month to filter by in the 'Start' column.
        content_owner (str): The content owner name to filter by, or 'All'.

    Returns:
        str: Formatted string of filtered opportunities.
    """
    if not hasattr(mcp_config, 'df') or mcp_config.df is None:
        return "Error: Dataframe not loaded. Please ensure the application has loaded the data."

    return projects_by_month(mcp_config.df, month, content_owner)


def get_projects_by_content_owner(content_owner: str) -> str:
    """
    Extract rows from the dataframe where 'Content Owner' matches the provided value.

    Args:
        content_owner (str): The name of the Content Owner to filter by

    Returns:
        str: A text string containing the project details and summary statistics
    """
    if not hasattr(mcp_config, 'df') or mcp_config.df is None:
        return "Error: Dataframe not loaded. Please ensure the application has loaded the data."

    return projects_from_content_owner(mcp_config.df, content_owner)
