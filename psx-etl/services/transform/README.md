# Transform Service

## Overview
The Transform Service is a production-ready FastAPI microservice that processes raw stock market data and applies comprehensive financial analysis transformations. It converts basic OHLCV (Open, High, Low, Close, Volume) data into enriched datasets with technical indicators, sector analysis, and risk metrics.

## What This Service Does

### Multi-Phase Data Transformation Pipeline

#### **Phase 1: Data Cleaning & Validation**
- **Input Validation**: Validates required fields (Open, High, Low, Close, Volume)
- **Price Validation**: Ensures prices are positive and follow OHLC logic (Low ≤ Open/Close ≤ High)
- **Volume Validation**: Ensures volume is non-negative
- **Ticker Standardization**: Converts ticker symbols to uppercase and validates format (1-5 letters)
- **Date Standardization**: Converts dates to UTC ISO format (YYYY-MM-DDTHH:MM:SSZ)
- **Data Quality Filtering**: Removes invalid or corrupted records

#### **Phase 2: Basic Financial Metrics**
- **Daily Return**: Percentage change from open to close price
- **Price Range**: High - Low price for the day
- **Typical Price**: Average of High, Low, and Close prices
- **Relative Volume**: Current volume / average volume ratio
- **Volume Weighted Price**: Price weighted by trading volume
- **PE Growth**: Difference between trailing and forward P/E ratios
- **Market Cap Categorization**: Small (<$2B), Mid ($2B-$10B), Large (>$10B)

#### **Phase 3: Technical Indicators**
- **Moving Averages**: 
  - MA_7: 7-day simple moving average
  - MA_30: 30-day simple moving average
- **Price Position Indicators**:
  - Price vs MA7: Percentage above/below 7-day MA
  - Price vs MA30: Percentage above/below 30-day MA
- **Volatility Metrics**:
  - Volatility_7: 7-day rolling standard deviation of returns
  - Volatility_30: 30-day rolling standard deviation of returns
- **Volume Analysis**:
  - Volume_MA_7: 7-day moving average of volume
  - Volume_Trend: Current volume vs 7-day average volume
- **RSI (Relative Strength Index)**: 14-day momentum oscillator (0-100 scale)
- **Price Change**: Day-over-day price change percentage

#### **Phase 4: Sector & Industry Analysis**
- **Sector Performance**: Comparison against sector average returns
- **Industry Performance**: Comparison against industry average returns
- **Relative Performance Metrics**:
  - Sector_Relative_Performance: Stock return minus sector average
  - Industry_Relative_Performance: Stock return minus industry average
- **Valuation Analysis**:
  - PE_vs_Sector_Avg: P/E ratio compared to sector average

#### **Phase 5: Risk & Advanced Metrics**
- **Maximum Drawdown**: Largest peak-to-trough decline
- **Sharpe Ratio**: Risk-adjusted return (annualized)
- **Value at Risk (VaR)**: 5th percentile of return distribution
- **Return Skewness**: Asymmetry of return distribution
- **Return Kurtosis**: Tail heaviness of return distribution

## Configuration

The service uses feature flags to enable/disable transformation phases:

```python
# Feature Flags
enable_technical_indicators = True    # Enable Phase 3
enable_sector_analysis = True        # Enable Phase 4  
enable_risk_metrics = True           # Enable Phase 5

# Calculation Parameters
ma_short_period = 7                  # Short-term moving average window
ma_long_period = 30                  # Long-term moving average window
volatility_window = 30               # Volatility calculation window
rsi_period = 14                      # RSI calculation period
```

## API Endpoints

### Core Endpoints

#### `GET /` - Service Information
Returns basic service metadata and status.

#### `GET /health` - Health Check
Health monitoring endpoint for load balancers and orchestrators.

#### `GET /config` - Configuration Details
Returns current feature flags and transformation parameters.

### Data Processing Endpoints

#### `POST /transform` - Transform Raw Data
**Purpose**: Transform raw stock data with comprehensive analysis

**Request Body**:
```json
{
  "raw_data": [
    {
      "Ticker": "AAPL",
      "Date": "2024-01-01",
      "Open": 150.0,
      "High": 155.0,
      "Low": 149.0,
      "Close": 154.0,
      "Volume": 1000000,
      "Dividend": 0.0,
      "industry": "Technology",
      "sector": "Technology",
      "marketCap": 3000000000000,
      "trailingPE": 25.5,
      "forwardPE": 22.0,
      "averageVolume": 50000000
    }
  ]
}
```

**Response**: Enhanced data with all calculated metrics

#### `POST /transform_batch` - Batch Processing
**Purpose**: Fetch data from extract service and transform it

**Request Body**:
```json
{
  "tickers": "AAPL,MSFT,GOOGL",
  "period": "1mo",
  "extract_service_url": "http://extract-service:8000"
}
```

## Running the Service

### Prerequisites
- Python 3.9+
- Required packages: `fastapi`, `uvicorn`, `pandas`, `numpy`, `httpx`

### Local Development

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Service**:
   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8001 --reload
   ```

3. **Access API Documentation**:
   - Swagger UI: http://localhost:8001/docs
   - ReDoc: http://localhost:8001/redoc

### Docker Deployment

1. **Build Docker Image**:
   ```bash
   docker build -t transform-service .
   ```

2. **Run Container**:
   ```bash
   docker run -p 8001:8001 transform-service
   ```

3. **Health Check**:
   ```bash
   curl http://localhost:8001/health
   ```

### Docker Compose (Multi-Service)

```yaml
version: '3.8'
services:
  transform-service:
    build: ./services/transform
    ports:
      - "8001:8001"
    environment:
      - EXTRACT_SERVICE_URL=http://extract-service:8000
    depends_on:
      - extract-service
    networks:
      - psx-etl-network
```

## Testing the Service

### Using cURL

**Health Check**:
```bash
curl -X GET http://localhost:8001/health
```

**Transform Data**:
```bash
curl -X POST http://localhost:8001/transform \
  -H "Content-Type: application/json" \
  -d '{
    "raw_data": [
      {
        "Ticker": "AAPL",
        "Date": "2024-01-01",
        "Open": 150.0,
        "High": 155.0,
        "Low": 149.0,
        "Close": 154.0,
        "Volume": 1000000,
        "industry": "Technology",
        "sector": "Technology"
      }
    ]
  }'
```

### Using Postman

1. **Set Base URL**: `http://localhost:8001`
2. **Import Collection**: Use the API documentation at `/docs` to generate requests
3. **Test Endpoints**: Start with `/health`, then try `/transform` with sample data

## Output Data Structure

### Enhanced Stock Record Fields

**Original Fields** (preserved):
- `Ticker`, `Date`, `Open`, `High`, `Low`, `Close`, `Volume`
- `industry`, `sector`, `marketCap`, `trailingPE`, `forwardPE`

**Calculated Fields** (added):

**Basic Metrics**:
- `Daily_Return`: Daily percentage return
- `Price_Range`: High-Low spread
- `Typical_Price`: (H+L+C)/3
- `Relative_Volume`: Volume vs average
- `Market_Cap_Category`: Size classification

**Technical Indicators**:
- `MA_7`, `MA_30`: Moving averages
- `Volatility_7`, `Volatility_30`: Price volatility
- `RSI_14`: Relative Strength Index
- `Price_vs_MA7`, `Price_vs_MA30`: Position vs moving averages
- `Price_Change_Pct`: Day-over-day change

**Sector Analysis**:
- `Sector_Relative_Performance`: vs sector average
- `Industry_Relative_Performance`: vs industry average
- `PE_vs_Sector_Avg`: P/E vs sector P/E

**Risk Metrics**:
- `Max_Drawdown`: Maximum decline
- `Sharpe_Ratio`: Risk-adjusted return
- `Value_at_Risk_5`: 5% VaR
- `Return_Skewness`, `Return_Kurtosis`: Distribution metrics

## Error Handling

The service includes comprehensive error handling:

- **Validation Errors**: Invalid input data format
- **Data Quality Issues**: Insufficient or corrupted data
- **Calculation Errors**: Mathematical computation failures
- **JSON Serialization**: Handles NaN, infinity, and numpy types
- **External Service Failures**: Extract service communication issues

## Performance Considerations

### Data Processing Limits
- **Batch Size**: Configurable maximum batch size (default: 100 records)
- **Memory Optimization**: Efficient pandas operations
- **JSON Safety**: All float values are validated for JSON compliance

### Scalability Features
- **Stateless Design**: Each request is independent
- **Horizontal Scaling**: Multiple instances can run concurrently
- **Resource Monitoring**: Built-in health checks and metrics endpoints

## Monitoring & Observability

### Health Endpoints
- `GET /health`: Basic health status
- `GET /ready`: Kubernetes readiness probe
- `GET /metrics`: Prometheus metrics (placeholder)

### Logging
- Structured error logging
- Request/response tracking
- Performance metrics

## Architecture Integration

### Service Dependencies
- **Extract Service**: Provides raw market data
- **Load Service**: Consumes transformed data
- **Scheduler Service**: Orchestrates batch processing

### Data Flow
```
Raw Data → Transform Service → Enhanced Data
    ↑                              ↓
Extract Service              Load Service
```

## Development Guidelines

### Code Structure
- Single-file architecture for simplicity
- Modular functions for each transformation phase
- Comprehensive docstrings and comments

### Testing
- Unit tests for individual transformation functions
- Integration tests for API endpoints
- Performance tests for large datasets

### Contributing
1. Follow existing code style and patterns
2. Add comprehensive error handling
3. Update documentation for new features
4. Ensure JSON serialization compatibility

## Troubleshooting

### Common Issues

**Import Errors**: Ensure all dependencies are installed
```bash
pip install -r requirements.txt
```

**JSON Serialization Errors**: The service handles NaN/infinity values automatically

**Memory Issues**: For large datasets, consider batch processing

**Performance Issues**: Check configuration parameters and enable only needed features

### Debug Mode
Run with debug logging:
```bash
uvicorn app:app --host 0.0.0.0 --port 8001 --log-level debug
```

## Production Deployment

### Environment Variables
```bash
ENABLE_TECHNICAL_INDICATORS=true
ENABLE_SECTOR_ANALYSIS=true
ENABLE_RISK_METRICS=true
DEFAULT_EXTRACT_SERVICE_URL=http://extract-service:8000
```

### Resource Requirements
- **CPU**: 1-2 cores for typical workloads
- **Memory**: 512MB-2GB depending on dataset size
- **Storage**: Minimal (stateless service)

### Security Considerations
- Input validation and sanitization
- Rate limiting (implement with reverse proxy)
- HTTPS termination (handle at load balancer level)
- No sensitive data storage

## License & Support

This is part of the PSX ETL Pipeline project. For issues and feature requests, please refer to the project documentation or contact the development team.