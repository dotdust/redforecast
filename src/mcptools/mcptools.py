from config.config import mcp_config
from datawrangler.pandas_functions import (
    read_excel, normalize_data, filter_opportunities)
from config.const import FORECAST_FILE_PATHNAME, HEADER_ROWS, COLUMNS_NAMES


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

    return filter_opportunities(mcp_config.df, month, content_owner, factory,
                                from_sensitivity, to_sensitivity, status, )


def register_tools():
    """Register all MCP tools from this module."""
    mcp = mcp_config.mcp
    if mcp is None:
        raise RuntimeError("MCP instance not initialized")

    # Register the tool
    mcp.tool()(get_opportunities_with_filters)


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
