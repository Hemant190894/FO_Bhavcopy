# F&O Market Analysis Tool

## Technical Architecture & Data Flow

```mermaid
flowchart LR
    %% Define styles
    classDef api fill:#f9f9f9,stroke:#333,stroke-width:1px
    classDef python fill:#306998,stroke:#333,stroke-width:1px,color:#fff
    classDef storage fill:#e6ffe6,stroke:#333,stroke-width:1px
    classDef transform fill:#FFD43B,stroke:#333,stroke-width:1px
    classDef db fill:#00758F,stroke:#333,stroke-width:1px,color:#fff

    %% NSE API Layer
    subgraph NSE [🏢 NSE Data APIs]
        direction TB
        API1["🔄 /api/bhav-copy
{date, type: 'FO'}"]:::api
        API2["📊 /api/volatility
{symbol, expiry}"]:::api
        API3["⚠️ /api/security-ban
{date}"]:::api
    end

    %% API Communication
    subgraph FETCH [API Communication]
        direction TB
        R1["🔐 Authentication
(API Key + Headers)"]:::api
        R2["🌐 HTTPS Requests
(aiohttp/requests)"]:::api
        R3["📡 Rate Limiting
(Max 5 req/sec)"]:::api
        
        R1 --> R2
        R2 --> R3
    end

    %% Python Processing
    subgraph PYTHON [🐍 Python Data Pipeline]
        direction TB
        P1["📥 Data Fetcher
Async Downloader"]:::python
        P2["🔄 Transformer
pandas + numpy"]:::python
        P3["📊 Analytics Engine
scipy + ta-lib"]:::python
        P4["💾 Data Persistence
SQLAlchemy ORM"]:::python

        P1 --> |"DataFrame"| P2
        P2 --> |"Processed Data"| P3
        P3 --> |"Analysis Results"| P4
    end

    %% Storage Solutions
    subgraph STORE [Data Storage]
        direction TB
        S1[("MySQL DB
(Partitioned Tables)")]:::db
        S2["Parquet Files
(Columnar Storage)"]:::storage
        S3["CSV Backups
(Daily Snapshots)"]:::storage
    end

    %% Connections with dotted lines and labels
    API1 -..->|"GET Request"| R1
    API2 -..->|"GET Request"| R1
    API3 -..->|"GET Request"| R1
    
    R3 ==>|"JSON Response"| P1
    
    P4 --> S1
    P4 --> S2
    P4 --> S3

    %% Styles for subgraphs
    style NSE fill:#f8f8f8,stroke:#333,stroke-width:1px
    style FETCH fill:#f5f5f5,stroke:#333,stroke-width:1px
    style PYTHON fill:#FFDE57,stroke:#333,stroke-width:1px
    style STORE fill:#f5fff5,stroke:#333,stroke-width:1px
```

### Technical Implementation Details

1. **NSE API Integration**
   - RESTful API endpoints
   - Authentication using API keys
   - Rate limiting compliance
   - Error handling & retries

2. **Python Data Pipeline**
   - Asynchronous data fetching
   - Pandas for data transformation
   - NumPy for numerical computations
   - SQLAlchemy for database operations

3. **Data Transformations**
   - Clean missing values
   - Calculate technical indicators
   - Compute volatility metrics
   - Generate trading signals

4. **Storage Strategy**
   - Partitioned MySQL tables
   - Parquet for analytics
   - CSV for data backups
   - Data versioning

    %% Processing Pipeline
    subgraph Process [Data Processing]
        direction TB
        P1["⬇️ Download & Validate"]:::process
        P2["🔍 Data Cleaning"]:::process
        P3["📊 Feature Engineering"]:::process
        P4["🔄 Data Integration"]:::process

        P1 --> P2
        P2 --> P3
        P3 --> P4
    end

    %% Storage Layer
    subgraph Storage [Data Storage]
        direction LR
        S1[("MySQL DB")]:::storage
        S2["CSV Files"]:::storage
        S3["Parquet Files"]:::storage
    end

    %% Analysis Layer
    subgraph Analysis [Market Analysis]
        direction LR
        AN1["Volume Analysis"]:::analysis
        AN2["Volatility Trends"]:::analysis
        AN3["Trading Signals"]:::analysis
    end

    %% Connections
    A1 --> |"Daily Data"| P1
    A2 --> |"Real-time"| P1
    A3 --> |"Trading"| P1

    P4 --> |"Structured"| S1
    P4 --> |"Backup"| S2
    P4 --> |"Analytics"| S3

    S1 --> |"Query"| AN1
    S1 --> |"Analysis"| AN2
    S1 --> |"Signals"| AN3

    %% Subgraph styles
    style Sources fill:#fff5ff,stroke:#333,stroke-width:1px
    style Process fill:#f5f5ff,stroke:#333,stroke-width:1px
    style Storage fill:#f5fff5,stroke:#333,stroke-width:1px
    style Analysis fill:#fff5f5,stroke:#333,stroke-width:1px
```

### Process Flow Description

1. **Data Collection**
   - Download Bhavcopy (F&O trading data)
   - Download Volatility data
   - Download Security Ban information
   - Handle market holidays and weekends

2. **Data Processing**
   - Clean and validate data
   - Calculate daily volatility metrics
   - Compute volume percentiles
   - Generate trading signals

3. **Data Storage**
   - Store in MySQL database with partitioning
   - Save CSV files for backup
   - Save Parquet files for efficient querying

4. **Data Analysis**
   - Volume analysis
   - Volatility trends
   - Trading signal generation
   - Performance metrics

### Database Schema

```mermaid
erDiagram
    fo_market_analysis {
        bigint id PK
        varchar(50) symbol
        date request_date
        date expiry_date
        timestamp processed_timestamp
        decimal(10,4) daily_volatility
        bigint trade_volume
        int percentile_volume
        int percentile_volatility
        decimal(5,2) average_percentile
        varchar(20) average_percentile_desc
    }
```

The database is optimized with:
- Partitioning by request_date
- Indexes on symbol and dates
- Constraints for data integrity

This tool downloads and processes F&O (Futures & Options) market data from the National Stock Exchange of India (NSE) and provides analysis based on volatility and trading volume.

## Features

- Downloads daily bhavcopy, volatility, and security ban data from NSE
- Processes and combines data to generate meaningful insights
- Calculates percentiles for volume and volatility
- Stores processed data in MySQL database
- Handles market holidays and weekends automatically
- Supports both historical and current date processing

## Prerequisites

- Python 3.8 or higher
- MySQL Server
- Required Python packages (install via `pip install -r requirements.txt`):
  - pandas
  - numpy
  - pyarrow
  - mysql-connector-python
  - requests
  - python-dateutil
  - pytz
  - python-dotenv

## Database Setup

1. Create a MySQL database named `fo_market_data`
2. The tool will automatically create the required table `fo_market_analysis`
3. Update database credentials in `config.py`

## Usage

1. Process data for a specific date:
```bash
python combine_code.py --date YYYY-MM-DD
```

2. Process data for current date:
```bash
python combine_code.py
```

## Output

The tool generates three types of output:
1. CSV files in `data/Output/YYYYMM/`
2. Parquet files in `data/Output_Parquet/YYYYMM/`
3. Database entries in the `fo_market_analysis` table

### Database Schema

- `id`: Auto-incrementing primary key
- `symbol`: Stock symbol
- `request_date`: Date of the data
- `expiry_date`: F&O contract expiry date
- `processed_timestamp`: When the data was processed
- `daily_volatility`: Daily volatility value
- `trade_volume`: Trading volume
- `percentile_volume`: Volume percentile (1-100)
- `percentile_volatility`: Volatility percentile (1-100)
- `average_percentile`: Average of volume and volatility percentiles
- `average_percentile_desc`: Descriptive category (Very Low/Low/Moderate/High/Very High)

## Data Sources

The tool fetches data from the following NSE sources:
1. Bhavcopy: Daily F&O trading data
2. Volatility: Daily volatility data
3. Security Ban: List of securities under F&O ban

## Logging

Logs are stored in `logs/bhavcopy.log` with detailed information about:
- Data download status
- Processing steps
- Database operations
- Any errors or warnings

## Version Control

The repository includes a `.gitignore` file that excludes:
- Generated data files (`data/Output/`, `data/Output_Parquet/`)
- Log files (`logs/*.log`)
- Python cache and virtual environment files
- IDE-specific files
- System files (e.g., `.DS_Store`)
- Local configuration files

This ensures that only source code and essential configuration files are tracked in version control.
