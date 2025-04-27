-- Create database for F&O data
CREATE DATABASE IF NOT EXISTS fo_market_data
    CHARACTER SET = 'utf8mb4'
    COLLATE = 'utf8mb4_unicode_ci';

USE fo_market_data;

-- Create table for F&O market data
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
);

-- Create a view for easy querying of latest data
CREATE OR REPLACE VIEW v_latest_market_data AS
SELECT 
    symbol,
    request_date,
    expiry_date,
    daily_volatility,
    trade_volume,
    percentile_volume,
    percentile_volatility,
    average_percentile,
    average_percentile_desc
FROM fo_market_analysis
WHERE (symbol, request_date) IN (
    SELECT symbol, MAX(request_date)
    FROM fo_market_analysis
    GROUP BY symbol
);

-- Add comments for documentation
ALTER TABLE fo_market_analysis
    COMMENT 'Stores F&O market analysis data including volatility and volume metrics';

-- Create user and grant permissions (customize username/password)
CREATE USER IF NOT EXISTS 'fo_app_user'@'localhost' IDENTIFIED BY 'your_strong_password_here';
GRANT SELECT, INSERT, UPDATE ON fo_market_data.* TO 'fo_app_user'@'localhost';
FLUSH PRIVILEGES;
