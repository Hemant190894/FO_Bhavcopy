import os
import io
import zipfile
import requests
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import calendar
import pyarrow
import config
from src.db_utils import insert_fo_data

# Configure logging
logging.basicConfig(
    filename=config.LOG_FILE_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_last_thursday(year, month):
    # Get last day of the month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)

    last_day = next_month - timedelta(days=1)

    # Go backwards to find the last Thursday
    while last_day.weekday() != 3:  # 3 = Thursday
        last_day -= timedelta(days=1)
    
    return last_day

def get_next_expiry_thursday(today=None):
    if today is None:
        today = datetime.today()
        
    current_year = today.year
    current_month = today.month
    current_day = today.day

    # Find last Thursday of current month
    last_thursday_this_month = get_last_thursday(current_year, current_month)

    if today.date() > last_thursday_this_month.date():
        # Move to next month
        if current_month == 12:
            next_year = current_year + 1
            next_month = 1
        else:
            next_year = current_year
            next_month = current_month + 1

        return get_last_thursday(next_year, next_month)
    else:
        return last_thursday_this_month


def read_csv_file(filename):
    try:
        df = pd.read_csv(filename)
        logging.info(f"Successfully read file: {filename}")
        return df
    except FileNotFoundError:
        logging.error(f"Error: {filename} not found.")
        return None

def calculate_percentiles(final_data):
    final_data = final_data.copy()

    # Calculate percentiles
    final_data['Percentile_Volume'] = np.ceil(final_data['Trade_volume'].rank(pct=True) * 100).astype(int)
    final_data['Percentile_Volatility'] = np.ceil(final_data['Daily_Volatility'].rank(pct=True) * 100).astype(int)

    final_data['Average_Percentile'] = np.ceil(
        (final_data['Percentile_Volume'] + final_data['Percentile_Volatility']) / 2
    ).astype(int)

    # Define labels
    conditions = [
        final_data['Average_Percentile'] >= 80,
        (final_data['Average_Percentile'] >= 60) & (final_data['Average_Percentile'] < 80),
        (final_data['Average_Percentile'] >= 40) & (final_data['Average_Percentile'] < 60),
        (final_data['Average_Percentile'] >= 20) & (final_data['Average_Percentile'] < 40),
        final_data['Average_Percentile'] < 20
    ]
    labels = ['Very High', 'High', 'Moderate', 'Low', 'Very Low']

    # Apply labels safely
    final_data['Average_Percentile_Desc'] = np.select(conditions, labels, default='Unknown')

    logging.info("Percentiles calculated successfully with rounded values.")
    return final_data

def download_files(target_date=None):
    """Download files from NSE for a specific date or today"""
    from src.date_utils import get_valid_dates
    import io
    
    if target_date is None:
        target_date = datetime.now()
    
    # Get valid dates for each file type considering holidays and weekends
    valid_dates = get_valid_dates(target_date)
    downloaded_files = {}
    
    # Create a session to maintain cookies
    session = requests.Session()
    
    for file_type, url_template in config.NSE_URLS.items():
        # Get the appropriate date for this file type
        file_date = valid_dates[file_type]
        
        # Format date according to the specific format required for this file type
        date_str = file_date.strftime(config.DATE_FORMATS[file_type])
        url = url_template.format(date=date_str)
        
        logging.info(f"Downloading {file_type} file for date {file_date.strftime('%Y-%m-%d')} from: {url}")
        try:
            response = session.get(url, headers=config.URL_HEADERS)
            response.raise_for_status()
            
            if file_type == 'bhavcopy':
                # For bhavcopy, we need to handle zip file
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    # Get the first file in the zip (should be the CSV)
                    csv_filename = zip_file.namelist()[0]
                    with zip_file.open(csv_filename) as csv_file:
                        df = pd.read_csv(csv_file)
            else:
                # For volatility and secban, direct CSV download
                df = pd.read_csv(io.StringIO(response.text))
            
            # Save to file
            output_path = os.path.join(config.INPUT_PATH, f"{file_type}_{date_str}.csv")
            df.to_csv(output_path, index=False)
            
            downloaded_files[file_type] = df
            logging.info(f"Successfully downloaded and read {file_type} file")
            
        except Exception as e:
            logging.error(f"Error downloading {file_type} file: {e}")
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
                logging.info(f"File not found for {file_type} on {file_date.strftime('%Y-%m-%d')}, might be a holiday")
            downloaded_files[file_type] = None
    
    return downloaded_files

def transform_data(data, data_name):
    try:
        data.columns = data.columns.str.strip()

        if data is not None:
            if data_name == "Bhavcopy":
                data['XpryDt'] = pd.to_datetime(data['XpryDt'], errors='coerce')
                data.dropna(subset=['XpryDt'], inplace=True)

                # Extract year & month
                data['Year'] = data['XpryDt'].dt.year
                data['Month'] = data['XpryDt'].dt.month

                # Get last Thursday for each year-month
                data['Last_Thursday'] = data.apply(lambda row: get_last_thursday(row['Year'], row['Month']), axis=1)

                # Filter only where XpryDt == Last Thursday
                bhavcopy_data = data.loc[
                    (data['FinInstrmTp'].str.strip() == 'STF') &
                    (data['XpryDt'] == data['Last_Thursday']) &
                    (data['TtlTradgVol'] >= 3000),
                    ['TckrSymb', 'TtlTradgVol']
                ].rename(columns={
                    'TckrSymb': 'Symbol',
                    'TtlTradgVol': 'Trade_volume'
                })

                logging.info(f"Bhavcopy Data Count (Last Thursday): {len(bhavcopy_data)}")
                return bhavcopy_data


            elif data_name == "Volatility":
                data = data.rename(columns={
                    'Applicable Daily Volatility (M) = Max (E or K)': 'Daily_Volatility'
                })
                avg_volatility = data['Daily_Volatility'].mean()
                filtered_data = data.loc[data['Daily_Volatility'] > avg_volatility, ['Symbol', 'Daily_Volatility']]
                logging.info(f"Volatility Data Count: {len(filtered_data)}")
                return filtered_data

            elif data_name == "Secban":
                secban_data = data.iloc[1:].reset_index(drop=True)
                secban_data['Symbol'] = secban_data.iloc[:, 0].str.strip().str.split().str[-1]
                logging.info(f"Secban Data Count: {len(secban_data)}")
                return secban_data[['Symbol']]

    except Exception as e:
        logging.error(f"Error occurred while transforming data: {e}")
        return None

def join_and_save_data(bhavcopy_df, volatility_df, secban_df, target_date):
    """Join the dataframes and save the results"""
    try:
        if bhavcopy_df is not None and volatility_df is not None:
            # Merge bhavcopy and volatility data
            merged_data = pd.merge(bhavcopy_df, volatility_df, on='Symbol', how='inner')

            # Filter out banned securities if secban data exists
            if secban_df is not None:
                logging.info("Merging data for secban")
                final_data = merged_data[~merged_data['Symbol'].isin(secban_df['Symbol'].values)]
            else:
                final_data = merged_data

            # Add Percentile Calculations
            final_data = calculate_percentiles(final_data)

            # Add expiry, processed date, and request date columns
            expiry_date = get_next_expiry_thursday().strftime("%Y-%m-%d")
            processed_datetime = datetime.now().strftime('%Y-%m-%d')
            final_data["Expiry_Date"] = expiry_date
            final_data["Processed_Timestamp"] = processed_datetime
            final_data["Request_Date"] = target_date.strftime('%Y-%m-%d')
            
            # Remove duplicates based on Symbol and Request_Date
            final_data = final_data.drop_duplicates(subset=['Symbol', 'Request_Date'], keep='last')

            # Create year-month based directory structure
            year_month = target_date.strftime('%Y%m')
            csv_output_dir = os.path.join(config.OUTPUT_PATH, year_month)
            parquet_output_dir = os.path.join(config.Parquet_OUTPUT_PATH, year_month)
            
            # Ensure output paths exist
            os.makedirs(csv_output_dir, exist_ok=True)
            os.makedirs(parquet_output_dir, exist_ok=True)

            # === CSV Handling ===
            csv_path = os.path.join(csv_output_dir, "filtered_data_with_percentiles.csv")
            
            if os.path.exists(csv_path):
                # Read existing CSV and append new data
                existing_df = pd.read_csv(csv_path)
                # Remove any existing entries for the same symbols and date
                existing_df = existing_df[~((existing_df['Symbol'].isin(final_data['Symbol'])) & 
                                          (existing_df['Request_Date'] == final_data['Request_Date'].iloc[0]))]
                # Append new data
                combined_df = pd.concat([existing_df, final_data], ignore_index=True)
                # Sort by request date and symbol
                combined_df.sort_values(['Request_Date', 'Symbol'], inplace=True)
                # Save the combined data
                combined_df.to_csv(csv_path, index=False)
            else:
                # Create new file
                final_data.to_csv(csv_path, index=False)
            
            logging.info(f"CSV file updated: {csv_path}")

            # === Parquet Handling ===
            parquet_path = os.path.join(parquet_output_dir, "filtered_data_with_percentiles.parquet")
            
            if os.path.exists(parquet_path):
                # Read existing parquet and append new data
                existing_df = pd.read_parquet(parquet_path)
                # Remove any existing entries for the same symbols and date
                existing_df = existing_df[~((existing_df['Symbol'].isin(final_data['Symbol'])) & 
                                          (existing_df['Request_Date'] == final_data['Request_Date'].iloc[0]))]
                # Append new data
                combined_df = pd.concat([existing_df, final_data], ignore_index=True)
                # Sort by request date and symbol
                combined_df.sort_values(['Request_Date', 'Symbol'], inplace=True)
                # Save the combined data
                combined_df.to_parquet(parquet_path, index=False)
            else:
                # Create new file
                final_data.to_parquet(parquet_path, index=False)
            
            logging.info(f"Parquet file updated: {parquet_path}")

            # Insert into database if configured
            try:
                if hasattr(config, 'DB_PARAMS'):
                    insert_fo_data(final_data, config.DB_PARAMS)
            except Exception as e:
                logging.error(f"Error inserting data into database: {e}")
                # Continue execution even if database insert fails

            return final_data
        else:
            logging.warning("Error: One or more DataFrames are empty.")
            return None

    except Exception as e:
        logging.error(f"Error occurred while joining data: {e}")
        return None

def process_for_date(date_str=None):
    """Process data for a specific date"""
    if date_str:
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
    else:
        target_date = datetime.now()
    
    logging.info(f"Processing data for date: {target_date.strftime('%Y-%m-%d')}")
    downloaded_data = download_files(target_date)
    
    FO_bhavcopy = downloaded_data.get('bhavcopy')
    FO_Volatility = downloaded_data.get('volatility')
    FO_secban = downloaded_data.get('secban')
    
    if all(v is None for v in downloaded_data.values()):
        logging.error("All downloads failed. Exiting.")
        return
    
    # Transform the data
    FO_bhavcopy_filtered = transform_data(FO_bhavcopy, 'Bhavcopy')
    FO_Volatility_filtered = transform_data(FO_Volatility, 'Volatility')
    FO_secban_filtered = transform_data(FO_secban, 'Secban')
    
    # Join and save the data
    final_data = join_and_save_data(FO_bhavcopy_filtered, FO_Volatility_filtered, FO_secban_filtered, target_date)
    return final_data

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process F&O data for a given date')
    parser.add_argument('--date', type=str, required=False,
                        help='Date in YYYY-MM-DD format. If not provided, uses current date')
    
    args = parser.parse_args()
    
    # Use current date if no date is provided
    target_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
    
    # Process the data
    process_for_date(target_date)
