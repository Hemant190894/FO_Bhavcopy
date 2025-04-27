import mysql.connector
from mysql.connector import Error
import sys
import os
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_PARAMS

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database_if_not_exists():
    """Create the database and table if they don't exist"""
    try:
        # First connect without database to check/create it
        conn = mysql.connector.connect(
            host=DB_PARAMS['host'],
            port=DB_PARAMS['port'],
            user=DB_PARAMS['user'],
            password=DB_PARAMS['password']
        )
        
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_PARAMS['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        logger.info(f"Database {DB_PARAMS['database']} is ready")
        
        # Switch to the database
        cursor.execute(f"USE {DB_PARAMS['database']}")
        
        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fo_market_analysis (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            request_date DATE NOT NULL,
            expiry_date DATE NOT NULL,
            processed_timestamp TIMESTAMP NOT NULL,
            daily_volatility DECIMAL(10,4) NOT NULL,
            trade_volume BIGINT NOT NULL,
            percentile_volume INT NOT NULL,
            percentile_volatility INT NOT NULL,
            average_percentile DECIMAL(5,2) NOT NULL,
            average_percentile_desc VARCHAR(20) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            
            -- Add unique constraint to prevent duplicate entries
            UNIQUE KEY uk_symbol_request_date (symbol, request_date),
            
            -- Add indexes for common queries
            INDEX idx_request_date (request_date),
            INDEX idx_expiry_date (expiry_date),
            INDEX idx_symbol (symbol),
            INDEX idx_average_percentile (average_percentile),
            
            -- Add composite indexes for common query patterns
            INDEX idx_symbol_dates (symbol, request_date, expiry_date),
            INDEX idx_percentiles (average_percentile, percentile_volume, percentile_volatility),
            
            -- Add constraints
            CONSTRAINT chk_percentiles CHECK (
                percentile_volume BETWEEN 0 AND 100
                AND percentile_volatility BETWEEN 0 AND 100
                AND average_percentile BETWEEN 0 AND 100
            ),
            CONSTRAINT chk_volatility CHECK (daily_volatility >= 0),
            CONSTRAINT chk_volume CHECK (trade_volume >= 0)
        ) 
        ENGINE = InnoDB
        PARTITION BY RANGE (YEAR(request_date)) (
            PARTITION p_2024 VALUES LESS THAN (2025),
            PARTITION p_2025 VALUES LESS THAN (2026),
            PARTITION p_2026 VALUES LESS THAN (2027),
            PARTITION p_max VALUES LESS THAN MAXVALUE
        )
        """
        cursor.execute(create_table_sql)
        logger.info("Table fo_market_analysis is ready")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Error as e:
        logger.error(f"Error: {e}")
        return False

if __name__ == "__main__":
    if create_database_if_not_exists():
        logger.info("Database setup completed successfully")
    else:
        logger.error("Database setup failed")
        sys.exit(1)
