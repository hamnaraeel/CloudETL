# Visualization Service

## Overview
Advanced Stock Analytics Dashboard built with Dash and Plotly. Provides comprehensive financial data visualization with historical analysis capabilities.

## Features

### 📊 9 Professional Chart Types
1. **Candlestick Chart** - OHLC with moving averages overlay
2. **Volume Chart** - Trading volume with color-coded bars
3. **Technical Indicators** - RSI and price vs moving averages
4. **Price Trend Analysis** - Trend line with fill area
5. **Sector Heatmap** - Performance comparison matrix
6. **Risk Metrics** - Volatility and returns distribution
7. **Daily Returns** - Bar chart of daily percentage changes
8. **Volume Analysis** - Volume vs moving average comparison
9. **Live Stats Panel** - Key metrics and statistics

### 📈 Historical Data Support
- **7 Time Periods**: 1D, 1W, 1M, 3M, 6M, 1Y, 5Y
- **Real-time Updates**: Refresh button for latest data
- **Data Caching**: 5-minute cache to optimize performance
- **Period-Aware Charts**: Adaptive visualizations based on timeframe

### 🎨 Professional Design
- **Dark Theme**: Modern financial dashboard appearance
- **Responsive Layout**: Works on desktop, tablet, and mobile
- **Color Coded**: Green/Red for gains/losses, consistent theming
- **Interactive Charts**: Zoom, pan, hover tooltips
- **Loading States**: Professional loading indicators

## Quick Start

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Start the service (ensure Transform Service is running on port 8001)
python app.py

# Access dashboard
http://localhost:8002
```

### Docker Deployment
```bash
# Build image
docker build -t visualization-service .

# Run container
docker run -p 8002:8002 visualization-service

# Access dashboard
http://localhost:8002
```

### Docker Compose Integration
```yaml
version: '3.8'
services:
  visualization-service:
    build: ./services/visualization
    ports:
      - "8002:8002"
    depends_on:
      - transform-service
    environment:
      - TRANSFORM_SERVICE_URL=http://transform-service:8001
```

## Architecture

### Data Flow
```
User Interface → Historical Controls → Transform Service → Data Processing → 9 Chart Types
```

### Service Dependencies
- **Transform Service** (port 8001): Provides processed stock data
- **Extract Service** (indirect): Data flows through Transform Service

## Usage Guide

### 1. Select Stock
Choose from dropdown: AAPL, MSFT, GOOGL, TSLA, AMZN

### 2. Choose Time Period
Click time buttons: 1D, 1W, 1M, 3M, 6M, 1Y, 5Y

### 3. View Analytics
- **Top Row**: Main candlestick chart, volume, technical indicators
- **Middle Row**: Trend analysis, sector heatmap, risk metrics  
- **Bottom Row**: Daily returns, volume analysis, key statistics

### 4. Interactive Features
- **Zoom**: Click and drag on charts
- **Pan**: Hold shift and drag
- **Hover**: View detailed data points
- **Legend**: Click to show/hide data series

## API Integration

### Transform Service Calls
```python
# Historical data request
POST http://transform-service:8001/transform_batch
{
  "tickers": "AAPL",
  "period": "1mo"
}
```

### Supported Periods
- `1d` - 1 day (intraday)
- `5d` - 1 week
- `1mo` - 1 month
- `3mo` - 3 months  
- `6mo` - 6 months
- `1y` - 1 year
- `5y` - 5 years

## Chart Details

### Candlestick Chart
- **OHLC Data**: Open, High, Low, Close prices
- **Volume Integration**: Combined with volume bars
- **Moving Averages**: 7-day and 30-day overlays
- **Color Coding**: Green for up days, red for down days

### Technical Indicators
- **RSI**: Relative Strength Index (14-period)
- **Overbought/Oversold Lines**: 70/30 levels
- **Moving Average Comparison**: Price vs MA7 and MA30
- **Dual Chart Layout**: RSI on top, price comparison below

### Risk Analytics
- **Volatility Tracking**: 7-day rolling standard deviation
- **Returns Distribution**: Histogram of daily returns
- **Drawdown Analysis**: Peak-to-trough declines
- **Statistical Measures**: Sharpe ratio, VaR calculations

## Performance Optimization

### Caching Strategy
- **5-minute cache**: Avoids repeated API calls
- **LRU Cache**: Automatic memory management
- **Cache Key**: Ticker + Period + Time bucket

### Loading Optimization
- **Lazy Loading**: Charts render on-demand
- **Batch Processing**: Single API call for all charts
- **Error Handling**: Graceful degradation on failures

## Troubleshooting

### Common Issues

**Dashboard shows "No data available"**
- Ensure Transform Service is running on port 8001
- Check if Extract Service is accessible
- Verify ticker symbol is valid

**Charts not updating**
- Click Refresh button to clear cache
- Check browser console for errors
- Verify API connectivity

**Performance Issues**
- Cache is automatically managed
- Reduce time period for faster loading
- Close unused browser tabs

### Debug Mode
```bash
# Run with debug logging
python app.py
```

### Health Check
```bash
# Test service availability
curl http://localhost:8002
```

## Production Deployment

### Environment Variables
```bash
TRANSFORM_SERVICE_URL=http://transform-service:8001
DASH_DEBUG=False
DASH_HOST=0.0.0.0
DASH_PORT=8002
```

### Resource Requirements
- **CPU**: 1-2 cores
- **Memory**: 512MB-1GB
- **Storage**: 100MB (minimal)
- **Network**: HTTP access to Transform Service

### Scaling Considerations
- **Stateless**: Can run multiple instances
- **Load Balancer**: Use sticky sessions for caching
- **CDN**: Serve static assets externally
- **Monitoring**: Health check endpoint available

## Development

### Adding New Charts
1. Create chart function in app.py
2. Add to layout structure
3. Update master callback
4. Test with sample data

### Customizing Appearance
```python
# Update color scheme
colors = {
    'background': '#your-color',
    'paper': '#your-color', 
    'text': '#your-color',
    # ...
}
```

### Adding New Time Periods
1. Update PERIOD_OPTIONS mapping
2. Add button to layout
3. Include in callback inputs
4. Test with Transform Service

## License
Part of the PSX ETL Pipeline project.