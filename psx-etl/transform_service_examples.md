# Transform Service Testing Examples

## Available Endpoints

### 1. GET `/` - Root endpoint
**Description**: Basic service information

**Example Request**:
```bash
curl -X GET http://localhost:8001/
```

**Expected Response**:
```json
{
  "service": "transform-service",
  "version": "2.0.0",
  "status": "running"
}
```

---

### 2. GET `/health` - Health check
**Description**: Service health status

**Example Request**:
```bash
curl -X GET http://localhost:8001/health
```

**Expected Response**:
```json
{
  "service": "transform-service",
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:45.123456"
}
```

---

### 3. GET `/config` - Configuration info
**Description**: Current transformation configuration and features

**Example Request**:
```bash
curl -X GET http://localhost:8001/config
```

**Expected Response**:
```json
{
  "config": {
    "enable_technical_indicators": true,
    "enable_sector_analysis": true,
    "enable_risk_metrics": true,
    "ma_short_period": 7,
    "ma_long_period": 30,
    "volatility_window": 30,
    "rsi_period": 14,
    "max_batch_size": 100
  },
  "features": {
    "phase_1": "Data cleaning & basic price/volume metrics",
    "phase_2": "Moving averages & volatility calculations",
    "phase_3": "Technical indicators & sector analysis",
    "phase_4": "Advanced risk metrics"
  }
}
```

---

### 4. POST `/transform` - Transform raw stock data
**Description**: Transform raw stock data with comprehensive analysis

**Example Request**:
```bash
curl -X POST http://localhost:8001/transform \
  -H "Content-Type: application/json" \
  -d '{
    "raw_data": [
      {
        "Ticker": "AAPL",
        "Date": "2024-01-01T00:00:00Z",
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
        "dividendYield": 0.5,
        "averageVolume": 50000000
      },
      {
        "Ticker": "AAPL",
        "Date": "2024-01-02T00:00:00Z",
        "Open": 154.0,
        "High": 158.0,
        "Low": 153.0,
        "Close": 157.0,
        "Volume": 1200000,
        "Dividend": 0.0,
        "industry": "Technology",
        "sector": "Technology",
        "marketCap": 3000000000000,
        "trailingPE": 25.5,
        "forwardPE": 22.0,
        "dividendYield": 0.5,
        "averageVolume": 50000000
      }
    ]
  }'
```

**Expected Response**:
```json
{
  "success": true,
  "records_processed": 2,
  "records_cleaned": 0,
  "data": [
    {
      "Ticker": "AAPL",
      "Date": "2024-01-01T00:00:00Z",
      "Open": 150.0,
      "High": 155.0,
      "Low": 149.0,
      "Close": 154.0,
      "Volume": 1000000,
      "Dividend": 0.0,
      "Daily_Return": 2.6667,
      "Price_Range": 6.0,
      "Typical_Price": 152.6667,
      "Relative_Volume": 0.02,
      "Volume_Weighted_Price": 152666700.0,
      "Market_Cap_Category": "Large",
      "MA_7": null,
      "MA_30": null,
      "Volatility_7": null,
      "Volatility_30": null,
      "Price_Change_Pct": null,
      "transformation_timestamp": "2024-01-15T10:30:45.123456+00:00",
      "transformation_version": "2.0.0"
    }
  ],
  "timestamp": "2024-01-15T10:30:45.123456+00:00"
}
```

---

### 5. POST `/transform_batch` - Batch transform from extract service
**Description**: Fetch data from extract service and transform it

**Example Request**:
```bash
curl -X POST http://localhost:8001/transform_batch \
  -H "Content-Type: application/json" \
  -d '{
    "tickers": "AAPL,MSFT,GOOGL",
    "period": "1mo",
    "extract_service_url": "http://localhost:8000"
  }'
```

**Expected Response**:
```json
{
  "success": true,
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "tickers_requested": "AAPL,MSFT,GOOGL",
  "records_transformed": 90,
  "data": [
    {
      "Ticker": "AAPL",
      "Date": "2024-01-01T00:00:00Z",
      "Open": 150.0,
      "High": 155.0,
      "Low": 149.0,
      "Close": 154.0,
      "Volume": 1000000,
      "Daily_Return": 2.6667,
      "MA_7": 152.5,
      "MA_30": null,
      "Volatility_7": 0.0234,
      "RSI_14": null,
      "Sector_Relative_Performance": 1.2345,
      "PE_vs_Sector_Avg": 1.05,
      "transformation_timestamp": "2024-01-15T10:30:45.123456+00:00",
      "transformation_version": "2.0.0"
    }
  ],
  "timestamp": "2024-01-15T10:30:45.123456"
}
```

---

## Testing with Multiple Tickers and Sectors

**Example Multi-Sector Data**:
```json
{
  "raw_data": [
    {
      "Ticker": "AAPL",
      "Date": "2024-01-01T00:00:00Z",
      "Open": 150.0,
      "High": 155.0,
      "Low": 149.0,
      "Close": 154.0,
      "Volume": 1000000,
      "Dividend": 0.0,
      "industry": "Consumer Electronics",
      "sector": "Technology",
      "marketCap": 3000000000000,
      "trailingPE": 25.5,
      "forwardPE": 22.0
    },
    {
      "Ticker": "JPM",
      "Date": "2024-01-01T00:00:00Z",
      "Open": 140.0,
      "High": 142.0,
      "Low": 138.0,
      "Close": 141.5,
      "Volume": 800000,
      "Dividend": 1.0,
      "industry": "Investment Banking",
      "sector": "Financial Services",
      "marketCap": 450000000000,
      "trailingPE": 12.5,
      "forwardPE": 11.0
    },
    {
      "Ticker": "MSFT",
      "Date": "2024-01-01T00:00:00Z",
      "Open": 320.0,
      "High": 325.0,
      "Low": 318.0,
      "Close": 323.0,
      "Volume": 1500000,
      "Dividend": 0.75,
      "industry": "Software",
      "sector": "Technology",
      "marketCap": 2500000000000,
      "trailingPE": 28.0,
      "forwardPE": 24.5
    }
  ]
}
```

## Error Testing Examples

### 1. Invalid Data Format
```bash
curl -X POST http://localhost:8001/transform \
  -H "Content-Type: application/json" \
  -d '{
    "invalid_field": "test"
  }'
```

**Expected Response**:
```json
{
  "detail": "raw_data field required"
}
```

### 2. Empty Data
```bash
curl -X POST http://localhost:8001/transform \
  -H "Content-Type: application/json" \
  -d '{
    "raw_data": []
  }'
```

**Expected Response**:
```json
{
  "detail": "No valid records after cleaning"
}
```

### 3. Invalid Price Data
```bash
curl -X POST http://localhost:8001/transform \
  -H "Content-Type: application/json" \
  -d '{
    "raw_data": [
      {
        "Ticker": "INVALID",
        "Date": "2024-01-01",
        "Open": -50.0,
        "High": -45.0,
        "Low": -55.0,
        "Close": -48.0,
        "Volume": -1000
      }
    ]
  }'
```

**Expected Response**:
```json
{
  "detail": "No valid records after cleaning"
}
```

---

## Running the Transform Service

1. **Start the service**:
   ```bash
   cd services/transform
   uvicorn app:app --host 0.0.0.0 --port 8001 --reload
   ```

2. **Test with curl** (use examples above)

3. **View API documentation**:
   ```bash
   # Visit in browser
   http://localhost:8001/docs
   ```

4. **Check service logs** for debugging any JSON serialization issues

## Key Features Tested

- ✅ **JSON Safety**: All NaN, inf, and invalid floats are handled
- ✅ **Data Validation**: Invalid records are filtered out
- ✅ **Technical Indicators**: Moving averages, volatility, RSI
- ✅ **Sector Analysis**: Relative performance vs sector/industry averages
- ✅ **Risk Metrics**: Sharpe ratio, drawdown, VaR calculations
- ✅ **Error Handling**: Comprehensive error responses
- ✅ **Pandas/NumPy Compatibility**: Proper conversion of numpy types to JSON-safe types