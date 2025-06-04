import os
from pathlib import Path

app_Name = "redforecast"
app_Version = "0.1.0"

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()

# Default paths that can be overridden by environment variables
DEFAULT_FORECAST_FILE = ('/Users/ag/Library/CloudStorage/OneDrive-BUSINESSINTEGRATIONPARTNERSSPA/'
                         'RED Team - Market Board - Forecast/RED Forecast.xlsx')
DEFAULT_DB_FILE = os.path.join(PROJECT_ROOT, 'historical_data', 'forecast.db')

# Use environment variables if set, otherwise use defaults
FORECAST_FILE_PATHNAME = os.environ.get('REDFORECAST_EXCEL_PATH', DEFAULT_FORECAST_FILE)
DB_FILE_PATHNAME = os.environ.get('REDFORECAST_DB_PATH', DEFAULT_DB_FILE)

HEADER_ROWS = 21

# Column names mapping for the Excel file
# 'na' indicates columns that should be skipped/ignored
COLUMNS_NAMES = [
    'na', 'na',  # Skip first two columns
    'id', 'Client', 'Contact Role', 'Project Name',
    'na', 'na',  # Skip columns 7-8
    'Status',
    'na',  # Skip column 10
    'PCC', 'PE', 'CPIS', 'CBE', 'Design', 'Tech', 'Others', 'Total Value', 'Sensitivity', 'Psensitivity',
    'na',  # Skip column 21
    'PPCC',
    'na',  # Skip column 23
    'PPE',
    'na',  # Skip column 25
    'PCPS',
    'na',  # Skip column 27
    'PCBE',
    'na',  # Skip column 29
    'Pdesign',
    'na',  # Skip column 31
    'PTech',
    'na', 'na', 'na', 'na', 'na', 'na',  # Skip columns 33-38
    'Tender', 'AdB', 'Opportunity Owner', 'Content Owner',
    'na',  # Skip column 43
    'Start', 'Duration',
    'na',  # Skip column 46
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December',
    'na',  # Skip column 59
    'Q1', 'Q2', 'Q3', 'Q4',
    'na',  # Skip column 64
    'FY',
    'na', 'na',  # Skip columns 66-67
    'Next years'
]
