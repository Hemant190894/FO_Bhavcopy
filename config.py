import os

# Config file for file paths and constants

# Base paths
BASE_PATH = r"/Users/hemantdayma/Documents/FO_OI"
DATA_PATH = os.path.join(BASE_PATH, "data")

# Input/Output paths
INPUT_PATH = os.path.join(DATA_PATH, "Input")
OUTPUT_PATH = os.path.join(DATA_PATH, "Output")
Parquet_OUTPUT_PATH = os.path.join(DATA_PATH, "Output_Parquet")
HOLIDAYS_PATH = os.path.join(DATA_PATH, "holidays")

# Holiday file
NSE_HOLIDAYS_FILE = os.path.join(HOLIDAYS_PATH, "nse_holidays_2025.csv")

# Log file path
LOG_FILE_PATH = os.path.join(BASE_PATH, "logs", "bhavcopy.log")

# Date formats for different URLs
DATE_FORMATS = {
    'bhavcopy': '%Y%m%d',  # YYYYMMDD
    'volatility': '%d%m%Y',  # DDMMYYYY
    'secban': '%d%m%Y'  # DDMMYYYY
}

# NSE URLs with proper date formats
NSE_URLS = {
    'bhavcopy': "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip",
    'volatility': "https://nsearchives.nseindia.com/archives/nsccl/volt/FOVOLT_{date}.csv",
    'secban': "https://nsearchives.nseindia.com/archives/fo/sec_ban/fo_secban_{date}.csv"
}

# Headers for NSE requests
URL_HEADERS = {
    'authority': 'www.nseindia.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'dnt': '1',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}

# Headers for API requests after getting cookies
API_HEADERS = {
    'authority': 'www.nseindia.com',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'referer': 'https://www.nseindia.com/',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}

# Create required directories
os.makedirs(INPUT_PATH, exist_ok=True)
os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(Parquet_OUTPUT_PATH, exist_ok=True)
os.makedirs(HOLIDAYS_PATH, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

# Database configuration
DB_PARAMS = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'hemant_nse',
    'password': 'Hemant1908',
    'database': 'fo_market_data'
}
