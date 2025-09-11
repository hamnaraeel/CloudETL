# Load Service - Data Warehouse

## Overview
Professional data warehouse service implementing **Star Schema** with **SCD Type 2** for historical stock data. Built with FastAPI, SQLAlchemy, and SQLite for reliable data storage and analytics.

## Star Schema Architecture

### Fact Table
**`fact_stock_prices`** - Central fact table containing stock price measurements
```sql
- fact_id (PK) - Unique identifier
- ticker_key (FK) - Reference to dim_ticker
- date_key (FK) - Reference to dim_date  
- time_key (FK) - Reference to dim_time
- open_price, high_price, low_price, close_price
- volume, ma_7, ma_30, rsi, daily_return, volatility
```

### Dimension Tables

**`dim_ticker`** - Ticker dimension with SCD Type 2
```sql
- ticker_key (PK) - Surrogate key
- ticker_symbol - Stock symbol (AAPL, MSFT, etc.)
- company_name, sector, industry, market_cap
- effective_date, end_date, is_current, version
```

**`dim_date`** - Date dimension for time-based analysis
```sql
- date_key (PK) - YYYYMMDD format
- full_date, year, quarter, month, day
- day_of_week, day_name, month_name
- is_weekend, is_holiday
```

**`dim_time`** - Time dimension for trading sessions
```sql
- time_key (PK) - Surrogate key
- hour, minute, period (1D, 1W, 1M, etc.)
- trading_session (pre_market, regular, after_hours)
```

## SCD Type 2 Implementation

### Slowly Changing Dimensions
Tracks historical changes in ticker metadata:

- **Current Record**: `is_current = True`, `end_date = NULL`
- **Historical Record**: `is_current = False`, `end_date = actual_date`
- **Versioning**: Sequential version numbers for each change
- **Effective Dating**: `effective_date` and `end_date` for time validity

### Example SCD Flow
```sql
-- Original record
ticker_key=1, ticker_symbol='AAPL', company_name='Apple Inc', 
effective_date='2024-01-01', end_date=NULL, is_current=TRUE, version=1

-- After company name change
ticker_key=1, ticker_symbol='AAPL', company_name='Apple Inc', 
effective_date='2024-01-01', end_date='2024-06-01', is_current=FALSE, version=1

ticker_key=2, ticker_symbol='AAPL', company_name='Apple Corporation', 
effective_date='2024-06-01', end_date=NULL, is_current=TRUE, version=2
```

## API Endpoints

### Load Data
```bash
# Load single ticker
POST /load/{ticker}?period=1mo

# Load multiple tickers
POST /load/batch
{
  "tickers": "AAPL,MSFT,GOOGL",
  "period": "1mo"
}
```

### Query Data
```bash
# Get ticker data
GET /data/{ticker}?limit=100

# Get warehouse statistics
GET /stats

# Health check
GET /health
```

## Quick Start

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Start service (ensure Transform Service is running)
python app.py

# Service available at http://localhost:8003
```

### Docker Deployment
```bash
# Build and run
docker build -t load-service .
docker run -p 8003:8003 -v $(pwd)/data:/app/data load-service
```

### Docker Compose Integration
```yaml
load-service:
  build: ./services/load
  ports:
    - "8003:8003"
  depends_on:
    - transform-service
  volumes:
    - ./data:/app/data
  environment:
    - TRANSFORM_SERVICE_URL=http://transform-service:8001
```

## Data Flow

### ETL Pipeline Integration
```
Extract Service → Transform Service → Load Service → SQLite Warehouse
     ↓                    ↓                ↓              ↓
  Raw Data         Processed Data    Star Schema    Analytics Ready
```

### Loading Process
1. **Receives** processed data from Transform Service
2. **Creates/Updates** dimension records (SCD Type 2)
3. **Generates** surrogate keys for relationships
4. **Inserts** fact records with proper foreign keys
5. **Prevents** duplicate records with composite key checking

## Data Warehouse Features

### Performance Optimization
- **Surrogate Keys**: Integer keys for fast joins
- **Composite Indexes**: On ticker_key + date_key + time_key
- **Batch Loading**: Multiple records per transaction
- **Duplicate Prevention**: Automatic deduplication

### Data Quality
- **Type Safety**: Decimal precision for financial data
- **Null Handling**: Graceful handling of missing indicators
- **Data Validation**: Input validation via Pydantic models
- **Transaction Safety**: Rollback on errors

### Analytics Ready
- **Historical Analysis**: Full historical ticker changes
- **Time Series**: Complete date and time dimensions
- **Aggregation Friendly**: Star schema optimized for OLAP
- **Flexible Querying**: Multiple access patterns supported

## Usage Examples

### Load Stock Data
```bash
# Load Apple stock for 1 month
curl -X POST "http://localhost:8003/load/AAPL?period=1mo"

# Load multiple stocks
curl -X POST "http://localhost:8003/load/batch" \
  -H "Content-Type: application/json" \
  -d '{"tickers": "AAPL,MSFT,GOOGL", "period": "3mo"}'
```

### Query Warehouse
```bash
# Get Apple data (latest 100 records)
curl "http://localhost:8003/data/AAPL?limit=100"

# Get warehouse statistics
curl "http://localhost:8003/stats"
```

### Sample Response
```json
{
  "ticker": "AAPL",
  "total_records": 23,
  "data": [
    {
      "date_key": 20241201,
      "open": 228.50,
      "high": 241.32,
      "low": 223.78,
      "close": 228.93,
      "volume": 48661973,
      "ma_7": 230.15,
      "ma_30": 225.80,
      "rsi": 55.23,
      "daily_return": 0.0044,
      "volatility": 0.009
    }
  ]
}
```

## Database Schema DDL

### Create Tables
```sql
-- Fact table
CREATE TABLE fact_stock_prices (
    fact_id INTEGER PRIMARY KEY,
    ticker_key INTEGER,
    date_key INTEGER,
    time_key INTEGER,
    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    volume BIGINT,
    ma_7 DECIMAL(12,4),
    ma_30 DECIMAL(12,4),
    rsi DECIMAL(5,2),
    daily_return DECIMAL(8,4),
    volatility DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ticker dimension (SCD Type 2)
CREATE TABLE dim_ticker (
    ticker_key INTEGER PRIMARY KEY,
    ticker_symbol VARCHAR(10),
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap BIGINT,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    version INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date dimension
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- Time dimension  
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    hour INTEGER,
    minute INTEGER,
    period VARCHAR(10),
    trading_session VARCHAR(20)
);
```

### Indexes
```sql
CREATE INDEX idx_fact_ticker_date ON fact_stock_prices(ticker_key, date_key);
CREATE INDEX idx_ticker_symbol_current ON dim_ticker(ticker_symbol, is_current);
CREATE INDEX idx_date_full ON dim_date(full_date);
CREATE INDEX idx_time_period ON dim_time(period);
```

## Troubleshooting

### Common Issues

**"Transform service unavailable"**
- Ensure Transform Service is running on port 8001
- Check network connectivity between services
- Verify Transform Service health: `curl http://localhost:8001/health`

**"No data returned from transform service"**
- Verify ticker symbol is valid (AAPL, MSFT, etc.)
- Check Transform Service logs for errors
- Test Transform Service directly: `POST /transform_batch`

**Database connection errors**
- Check SQLite file permissions in `/app/data/`
- Ensure volume mount is configured in Docker
- Verify database initialization completed

### Debug Mode
```bash
# Run with detailed logging
python app.py

# Check database contents
sqlite3 ./stock_warehouse.db
.tables
.schema fact_stock_prices
SELECT COUNT(*) FROM fact_stock_prices;
```

### Performance Monitoring
```bash
# Check warehouse statistics
curl http://localhost:8003/stats

# Monitor loading performance
time curl -X POST "http://localhost:8003/load/batch" \
  -H "Content-Type: application/json" \
  -d '{"tickers": "AAPL,MSFT,GOOGL", "period": "1y"}'
```

## Production Considerations

### Scaling
- **Read Replicas**: Create read-only database copies
- **Partitioning**: Partition fact table by date ranges
- **Caching**: Add Redis for frequently accessed data
- **Load Balancing**: Multiple Load Service instances

### Security
- **Database Encryption**: Encrypt SQLite database file
- **API Authentication**: Add JWT token validation
- **Input Validation**: Comprehensive data validation
- **Audit Logging**: Track all data modifications

### Monitoring
- **Health Checks**: Automated service monitoring
- **Data Quality Metrics**: Monitor data completeness
- **Performance Metrics**: Track loading and query times
- **Alert System**: Notify on failures or anomalies

## License
Part of the PSX ETL Pipeline project.