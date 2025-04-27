import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import logging

def insert_to_db(df, table_name, db_params):
    """
    Insert DataFrame to MySQL database using mysql.connector
    
    Args:
        df (pandas.DataFrame): DataFrame to insert
        table_name (str): Name of the table to insert into
        db_params (dict): Database connection parameters
    """
    conn = None
    cursor = None
    try:
        # Connect to database
        logging.info(f"Connecting to database {db_params['database']}")
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()
        
        # Convert DataFrame to list of tuples
        values = [tuple(x) for x in df.values]
        
        # Create the INSERT query
        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"""INSERT INTO {table_name} ({', '.join(columns)}) 
                   VALUES ({placeholders})
                   ON DUPLICATE KEY UPDATE
                   {', '.join(f'{col}=VALUES({col})' for col in columns)}"""
        
        logging.info(f"Executing query with {len(values)} rows")
        logging.info(f"Sample value: {values[0] if values else None}")
        
        cursor.executemany(query, values)
        conn.commit()
        logging.info(f"Successfully inserted/updated {cursor.rowcount} rows")
        
    except mysql.connector.Error as e:
        logging.error(f"MySQL Error: {e}")
        if 'values' in locals():
            logging.error(f"Sample value: {values[0] if values else None}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def insert_fo_data(df, db_params):
    """Insert data into the fo_market_analysis table"""
    try:
        # Create a copy of the dataframe
        insert_df = df.copy()
        
        # Rename columns to match database schema
        column_mapping = {
            'Symbol': 'symbol',
            'Request_Date': 'request_date',
            'Expiry_Date': 'expiry_date',
            'Processed_Timestamp': 'processed_timestamp',
            'Daily_Volatility': 'daily_volatility',
            'Trade_volume': 'trade_volume',
            'Percentile_Volume': 'percentile_volume',
            'Percentile_Volatility': 'percentile_volatility',
            'Average_Percentile': 'average_percentile',
            'Average_Percentile_Desc': 'average_percentile_desc'
        }
        
        # Rename columns
        insert_df = insert_df.rename(columns=column_mapping)
        
        # Convert date columns to proper format
        insert_df['request_date'] = pd.to_datetime(insert_df['request_date']).dt.strftime('%Y-%m-%d')
        insert_df['expiry_date'] = pd.to_datetime(insert_df['expiry_date']).dt.strftime('%Y-%m-%d')
        insert_df['processed_timestamp'] = pd.to_datetime(insert_df['processed_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Ensure numeric columns are of correct type
        insert_df['daily_volatility'] = insert_df['daily_volatility'].astype(float)
        insert_df['trade_volume'] = insert_df['trade_volume'].astype(int)
        insert_df['percentile_volume'] = insert_df['percentile_volume'].astype(int)
        insert_df['percentile_volatility'] = insert_df['percentile_volatility'].astype(int)
        insert_df['average_percentile'] = insert_df['average_percentile'].astype(float)
        
        # Select only the columns we want to insert
        columns_to_insert = list(column_mapping.values())
        insert_df = insert_df[columns_to_insert]
        
        logging.info(f"Preparing to insert {len(insert_df)} rows into database")
        logging.info(f"Columns: {list(insert_df.columns)}")
        
        # Insert into database
        insert_to_db(insert_df, 'fo_market_analysis', db_params)
        
    except Exception as e:
        logging.error(f"Error inserting data into database: {e}")
        if 'insert_df' in locals():
            logging.error(f"DataFrame head:\n{insert_df.head()}")
        raise
