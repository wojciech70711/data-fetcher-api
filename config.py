# API Configuration File

# Server Settings
HOST = "0.0.0.0"
PORT = 8000
RELOAD = True  # Auto-reload on code changes

# Data Settings
DATA_DIRECTORY = "data"  # Directory to store CSV files
CSV_FILENAME_PATTERN = "{symbol}_ohlcv.csv"  # Pattern for CSV filenames

# OHLCV Validation
MIN_VOLUME = 0
MAX_PRICE = 1_000_000  # Maximum price allowed
MIN_PRICE = 0.00001    # Minimum price allowed

# CSV Settings
CSV_ENCODING = "utf-8"
CSV_QUOTE_CHAR = '"'
CSV_DELIMITER = ","

# API Settings
API_TITLE = "Cryptocurrency OHLCV Data Storage API"
API_VERSION = "1.0.0"
API_DESCRIPTION = "REST API for storing and retrieving OHLCV data from cryptocurrency markets"
