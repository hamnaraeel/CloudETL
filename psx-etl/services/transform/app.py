from fastapi import FastAPI, HTTPException
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from dataclasses import dataclass
import httpx
import pandas as pd
import numpy as np
from collections import defaultdict
import re
import math
import json

@dataclass
class TransformConfig:
    # Feature flags
    enable_technical_indicators: bool = True
    enable_sector_analysis: bool = True
    enable_risk_metrics: bool = True
    
    # Calculation parameters
    ma_short_period: int = 7
    ma_long_period: int = 30
    volatility_window: int = 30
    rsi_period: int = 14
    
    # Performance settings
    max_batch_size: int = 100
    enable_parallel_processing: bool = True
    cache_results: bool = True

config = TransformConfig()

def safe_float(value, default=None):
    """Convert value to JSON-safe float, handling NaN, inf, and None"""
    if value is None:
        return default
    try:
        # Handle pandas/numpy types
        if hasattr(value, 'item'):
            value = value.item()  # Convert numpy scalars to Python types
        
        # Convert to float
        float_val = float(value)
        
        # Check for NaN or infinity
        if math.isnan(float_val) or math.isinf(float_val):
            return default
        
        return round(float_val, 4)
    except (ValueError, TypeError, OverflowError):
        return default

def ensure_json_serializable(data):
    """Recursively ensure all data is JSON serializable"""
    if isinstance(data, dict):
        return {key: ensure_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [ensure_json_serializable(item) for item in data]
    elif isinstance(data, (np.integer, int)):
        return int(data)
    elif isinstance(data, (np.floating, float)):
        return safe_float(data, 0.0)
    elif hasattr(data, 'item'):  # numpy scalars
        return ensure_json_serializable(data.item())
    elif pd.isna(data):
        return None
    else:
        return data

app = FastAPI(title="Transform Service", version="2.0.0")

@app.get("/")
def root():
    return {
        "service": "transform-service",
        "version": "2.0.0",
        "status": "running"
    }

@app.get("/health")
def health_check():
    """Comprehensive health check for transform service"""
    import time
    import psutil
    import httpx
    
    start_time = time.time()
    
    checks = {
        "service": "transform-service",
        "version": "2.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": {}
    }
    
    # Test 1: Memory usage check
    try:
        memory_percent = psutil.virtual_memory().percent
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        if memory_percent > 90:
            checks["checks"]["memory"] = "unhealthy - high usage"
        elif memory_percent > 80:
            checks["checks"]["memory"] = "degraded - moderate usage"  
        else:
            checks["checks"]["memory"] = "healthy"
        checks["memory_usage_percent"] = round(memory_percent, 1)
        
        if cpu_percent > 90:
            checks["checks"]["cpu"] = "unhealthy - high usage"
        elif cpu_percent > 80:
            checks["checks"]["cpu"] = "degraded - moderate usage"
        else:
            checks["checks"]["cpu"] = "healthy"
        checks["cpu_usage_percent"] = round(cpu_percent, 1)
        
    except Exception as e:
        checks["checks"]["system"] = f"degraded - {str(e)[:50]}"
    
    # Test 2: Data processing capability
    try:
        # Test basic transformation with small dataset
        test_data = [{
            "Ticker": "TEST",
            "Date": "2024-01-01T00:00:00Z",
            "Open": 100.0,
            "High": 105.0,
            "Low": 95.0,
            "Close": 102.0,
            "Volume": 1000000
        }]
        
        cleaned_data = clean_and_standardize(test_data)
        if len(cleaned_data) > 0:
            checks["checks"]["data_processing"] = "healthy"
        else:
            checks["checks"]["data_processing"] = "unhealthy - processing failed"
            
    except Exception as e:
        checks["checks"]["data_processing"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 3: Dependencies check (pandas, numpy)
    try:
        import pandas as pd
        import numpy as np
        
        # Test basic operations
        test_df = pd.DataFrame({"test": [1, 2, 3]})
        test_array = np.array([1, 2, 3])
        
        if len(test_df) == 3 and len(test_array) == 3:
            checks["checks"]["dependencies"] = "healthy"
        else:
            checks["checks"]["dependencies"] = "degraded - unexpected behavior"
            
    except Exception as e:
        checks["checks"]["dependencies"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 4: Response time check
    response_time = (time.time() - start_time) * 1000
    checks["response_time_ms"] = round(response_time, 2)
    
    if response_time > 5000:
        checks["checks"]["response_time"] = "unhealthy - too slow"
    elif response_time > 2000:
        checks["checks"]["response_time"] = "degraded - slow"
    else:
        checks["checks"]["response_time"] = "healthy"
    
    # Test 5: Extract service connectivity (optional)
    try:
        async def test_extract_connectivity():
            async with httpx.AsyncClient(timeout=2) as client:
                response = await client.get("http://extract-service:8000/health")
                return response.status_code == 200
        
        # This is a sync endpoint, so we'll skip the async test for now
        checks["checks"]["extract_service"] = "unknown - async test skipped"
        
    except Exception:
        checks["checks"]["extract_service"] = "unknown - connectivity test failed"
    
    # Overall status calculation
    unhealthy_count = sum(1 for check in checks["checks"].values() if "unhealthy" in str(check))
    degraded_count = sum(1 for check in checks["checks"].values() if "degraded" in str(check))
    
    if unhealthy_count > 0:
        checks["status"] = "unhealthy"
    elif degraded_count > 0:
        checks["status"] = "degraded"
    else:
        checks["status"] = "healthy"
    
    return checks

@app.get("/config")
def get_config():
    """Get current transformation configuration"""
    return {
        "config": {
            "enable_technical_indicators": config.enable_technical_indicators,
            "enable_sector_analysis": config.enable_sector_analysis,
            "enable_risk_metrics": config.enable_risk_metrics,
            "ma_short_period": config.ma_short_period,
            "ma_long_period": config.ma_long_period,
            "volatility_window": config.volatility_window,
            "rsi_period": config.rsi_period,
            "max_batch_size": config.max_batch_size
        },
        "features": {
            "phase_1": "Data cleaning & basic price/volume metrics",
            "phase_2": "Moving averages & volatility calculations",
            "phase_3": "Technical indicators & sector analysis", 
            "phase_4": "Advanced risk metrics"
        }
    }

def clean_and_standardize(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Phase 1: Data cleaning and standardization"""
    cleaned_records = []
    
    for record in raw_records:
        # Skip invalid records
        if not all(key in record for key in ["Open", "High", "Low", "Close", "Volume"]):
            continue
            
        try:
            # Standardize ticker
            ticker = str(record.get("Ticker", "")).strip().upper()
            if not ticker or not re.match(r'^[A-Z]{1,5}$', ticker):
                continue
            
            # Validate and convert prices
            open_price = float(record["Open"])
            high_price = float(record["High"])
            low_price = float(record["Low"])
            close_price = float(record["Close"])
            volume = int(record["Volume"])
            
            # Data validation
            if any(price <= 0 for price in [open_price, high_price, low_price, close_price]):
                continue
            if volume < 0:
                continue
            if not (low_price <= open_price <= high_price and low_price <= close_price <= high_price):
                continue
            
            # Standardize date to UTC ISO format
            date_str = record.get("Date", "")
            if date_str:
                try:
                    if "T" not in date_str:
                        date_str += "T00:00:00Z"
                    elif not date_str.endswith("Z"):
                        date_str += "Z"
                except:
                    continue
            
            cleaned_record = {
                "Ticker": ticker,
                "Date": date_str,
                "Open": round(open_price, 4),
                "High": round(high_price, 4),
                "Low": round(low_price, 4),
                "Close": round(close_price, 4),
                "Volume": volume,
                "Dividend": float(record.get("Dividend", 0.0)),
                "industry": record.get("industry", ""),
                "sector": record.get("sector", ""),
                "marketCap": record.get("marketCap"),
                "trailingPE": record.get("trailingPE"),
                "forwardPE": record.get("forwardPE"),
                "dividendYield": record.get("dividendYield"),
                "dividendRate": record.get("dividendRate"),
                "averageVolume": record.get("averageVolume"),
                "previousClose": record.get("previousClose")
            }
            
            cleaned_records.append(cleaned_record)
            
        except (ValueError, TypeError, KeyError):
            continue
    
    return cleaned_records

def calculate_basic_metrics(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Calculate basic price and volume metrics"""
    for record in records:
        open_price = record["Open"]
        high_price = record["High"]
        low_price = record["Low"]
        close_price = record["Close"]
        volume = record["Volume"]
        average_volume = record.get("averageVolume", volume)
        
        # Basic price metrics
        record["Daily_Return"] = safe_float(((close_price - open_price) / open_price) * 100, 0.0) if open_price > 0 else 0.0
        record["Price_Range"] = safe_float(high_price - low_price)
        record["Typical_Price"] = safe_float((high_price + low_price + close_price) / 3)
        
        # Volume metrics
        record["Relative_Volume"] = safe_float(volume / average_volume, 1.0) if average_volume and average_volume > 0 else 1.0
        record["Volume_Weighted_Price"] = safe_float(record["Typical_Price"] * volume) if record["Typical_Price"] else None
        
        # PE Analysis (if available)
        if record.get("trailingPE") and record.get("forwardPE"):
            record["PE_Growth"] = safe_float(record["trailingPE"] - record["forwardPE"])
        
        # Market Cap Category
        market_cap = record.get("marketCap")
        if market_cap:
            if market_cap < 2e9:
                record["Market_Cap_Category"] = "Small"
            elif market_cap < 10e9:
                record["Market_Cap_Category"] = "Mid"
            else:
                record["Market_Cap_Category"] = "Large"
    
    return records

def calculate_time_series_metrics(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Phase 2: Calculate moving averages and volatility metrics"""
    # Group by ticker for time-series calculations
    ticker_groups = defaultdict(list)
    for record in records:
        ticker_groups[record["Ticker"]].append(record)
    
    # Sort each group by date
    for ticker in ticker_groups:
        ticker_groups[ticker].sort(key=lambda x: x["Date"])
    
    enhanced_records = []
    
    for ticker, ticker_records in ticker_groups.items():
        # Convert to DataFrame for easier calculation
        df = pd.DataFrame(ticker_records)
        
        if len(df) < config.ma_short_period:
            # Not enough data for calculations, add null values
            for record in ticker_records:
                record.update({
                    "MA_7": None,
                    "MA_30": None,
                    "Volatility_7": None,
                    "Volatility_30": None,
                    "Price_Change_Pct": None,
                    "Price_vs_MA7": None,
                    "Price_vs_MA30": None,
                    "Volume_MA_7": None,
                    "Volume_Trend": None,
                    "RSI_14": None
                })
            enhanced_records.extend(ticker_records)
            continue
        
        # Calculate moving averages
        df["MA_7"] = df["Close"].rolling(window=config.ma_short_period, min_periods=1).mean()
        if len(df) >= config.ma_long_period:
            df["MA_30"] = df["Close"].rolling(window=config.ma_long_period, min_periods=config.ma_long_period).mean()
        else:
            df["MA_30"] = None
        
        # Calculate volatility (rolling standard deviation of daily returns)
        df["Daily_Return_Decimal"] = df["Daily_Return"] / 100  # Convert to decimal for std calculation
        df["Volatility_7"] = df["Daily_Return_Decimal"].rolling(window=config.ma_short_period, min_periods=2).std()
        if len(df) >= config.volatility_window:
            df["Volatility_30"] = df["Daily_Return_Decimal"].rolling(window=config.volatility_window, min_periods=config.volatility_window).std()
        else:
            df["Volatility_30"] = None
        
        # Calculate price change percentage (day-over-day)
        df["Previous_Close"] = df["Close"].shift(1)
        df["Price_Change_Pct"] = ((df["Close"] - df["Previous_Close"]) / df["Previous_Close"] * 100)
        
        # Price position indicators
        df["Price_vs_MA7"] = ((df["Close"] - df["MA_7"]) / df["MA_7"] * 100)
        df["Price_vs_MA30"] = ((df["Close"] - df["MA_30"]) / df["MA_30"] * 100) if df["MA_30"].notna().any() else None
        
        # Volume analysis
        df["Volume_MA_7"] = df["Volume"].rolling(window=config.ma_short_period, min_periods=1).mean()
        df["Volume_Trend"] = ((df["Volume"] - df["Volume_MA_7"]) / df["Volume_MA_7"] * 100)
        
        # Simple RSI calculation
        if config.enable_technical_indicators and len(df) >= config.rsi_period:
            delta = df["Close"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=config.rsi_period, min_periods=config.rsi_period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=config.rsi_period, min_periods=config.rsi_period).mean()
            rs = gain / loss
            df["RSI_14"] = (100 - (100 / (1 + rs)))
        else:
            df["RSI_14"] = None
        
        # Convert back to dict records with safe float conversion
        for _, row in df.iterrows():
            record = {}
            for key, value in row.to_dict().items():
                if key in ["Daily_Return_Decimal", "Previous_Close"]:
                    continue  # Skip helper columns
                
                # Apply safe_float to numerical columns that might contain NaN/inf
                if key in ["MA_7", "MA_30", "Volatility_7", "Volatility_30", "Price_Change_Pct", 
                          "Price_vs_MA7", "Price_vs_MA30", "Volume_MA_7", "Volume_Trend", "RSI_14"]:
                    record[key] = safe_float(value)
                else:
                    # Keep original values for non-calculated fields
                    record[key] = value
            
            enhanced_records.append(record)
    
    return enhanced_records

def calculate_sector_analysis(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Phase 3: Sector and industry analysis"""
    if not config.enable_sector_analysis:
        return records
    
    # Group by sector and industry for comparative analysis
    sector_data = defaultdict(list)
    industry_data = defaultdict(list)
    
    for record in records:
        sector = record.get("sector", "Unknown")
        industry = record.get("industry", "Unknown")
        
        if sector != "Unknown" and record.get("Daily_Return") is not None:
            sector_data[sector].append(record["Daily_Return"])
        if industry != "Unknown" and record.get("Daily_Return") is not None:
            industry_data[industry].append(record["Daily_Return"])
    
    # Calculate sector averages
    sector_avg_returns = {}
    for sector, returns in sector_data.items():
        if returns:
            sector_avg_returns[sector] = np.mean(returns)
    
    # Calculate industry averages
    industry_avg_returns = {}
    for industry, returns in industry_data.items():
        if returns:
            industry_avg_returns[industry] = np.mean(returns)
    
    # Add sector analysis to each record
    for record in records:
        sector = record.get("sector", "Unknown")
        industry = record.get("industry", "Unknown")
        
        # Sector relative performance
        if sector in sector_avg_returns and record.get("Daily_Return") is not None:
            record["Sector_Relative_Performance"] = safe_float(record["Daily_Return"] - sector_avg_returns[sector])
            record["Sector_Avg_Return"] = safe_float(sector_avg_returns[sector])
        else:
            record["Sector_Relative_Performance"] = None
            record["Sector_Avg_Return"] = None
        
        # Industry relative performance
        if industry in industry_avg_returns and record.get("Daily_Return") is not None:
            record["Industry_Relative_Performance"] = safe_float(record["Daily_Return"] - industry_avg_returns[industry])
            record["Industry_Avg_Return"] = safe_float(industry_avg_returns[industry])
        else:
            record["Industry_Relative_Performance"] = None
            record["Industry_Avg_Return"] = None
        
        # PE analysis vs sector
        if record.get("trailingPE") and sector in sector_data:
            sector_pe_values = [r.get("trailingPE") for r in records if r.get("sector") == sector and r.get("trailingPE")]
            if sector_pe_values:
                sector_avg_pe = np.mean(sector_pe_values)
                record["PE_vs_Sector_Avg"] = safe_float(record["trailingPE"] / sector_avg_pe) if sector_avg_pe > 0 else None
            else:
                record["PE_vs_Sector_Avg"] = None
        else:
            record["PE_vs_Sector_Avg"] = None
    
    return records

def calculate_risk_metrics(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Phase 4: Risk and advanced metrics"""
    if not config.enable_risk_metrics:
        return records
    
    # Group by ticker for risk calculations
    ticker_groups = defaultdict(list)
    for record in records:
        ticker_groups[record["Ticker"]].append(record)
    
    enhanced_records = []
    
    for ticker, ticker_records in ticker_groups.items():
        # Sort by date
        ticker_records.sort(key=lambda x: x["Date"])
        
        if len(ticker_records) < 5:  # Need minimum data for risk metrics
            for record in ticker_records:
                record.update({
                    "Max_Drawdown": None,
                    "Sharpe_Ratio": None,
                    "Value_at_Risk_5": None,
                    "Return_Skewness": None,
                    "Return_Kurtosis": None
                })
            enhanced_records.extend(ticker_records)
            continue
        
        # Convert to DataFrame for calculations
        df = pd.DataFrame(ticker_records)
        
        # Calculate cumulative returns and drawdown
        returns = df["Daily_Return"].dropna() / 100  # Convert to decimal
        if len(returns) > 0:
            cumulative_returns = (1 + returns).cumprod()
            rolling_max = cumulative_returns.expanding().max()
            drawdown = (cumulative_returns - rolling_max) / rolling_max
            max_drawdown = drawdown.min()
            
            # Sharpe Ratio (assuming 0% risk-free rate for simplicity)
            if returns.std() > 0:
                sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252)  # Annualized
            else:
                sharpe_ratio = 0
            
            # Value at Risk (5th percentile)
            var_5 = np.percentile(returns, 5)
            
            # Skewness and Kurtosis
            skewness = returns.skew() if len(returns) >= 3 else None
            kurtosis = returns.kurtosis() if len(returns) >= 4 else None
        else:
            max_drawdown = sharpe_ratio = var_5 = skewness = kurtosis = None
        
        # Add risk metrics to each record
        for record in ticker_records:
            record.update({
                "Max_Drawdown": safe_float(max_drawdown),
                "Sharpe_Ratio": safe_float(sharpe_ratio),
                "Value_at_Risk_5": safe_float(var_5),
                "Return_Skewness": safe_float(skewness),
                "Return_Kurtosis": safe_float(kurtosis)
            })
        
        enhanced_records.extend(ticker_records)
    
    return enhanced_records

@app.post("/transform")
def transform_data(data: Dict[str, Any]):
    """Transform raw stock data with comprehensive analysis"""
    try:
        if "raw_data" not in data:
            raise HTTPException(status_code=400, detail="raw_data field required")
        
        raw_records = data["raw_data"]
        if not isinstance(raw_records, list):
            raise HTTPException(status_code=400, detail="raw_data must be a list")
        
        # Phase 1: Clean and standardize data
        cleaned_data = clean_and_standardize(raw_records)
        
        if not cleaned_data:
            raise HTTPException(status_code=400, detail="No valid records after cleaning")
        
        # Phase 1: Calculate basic metrics
        transformed_records = calculate_basic_metrics(cleaned_data)
        
        # Phase 2: Calculate time-series metrics (if enabled and sufficient data)
        if config.enable_technical_indicators and len(transformed_records) > config.ma_short_period:
            transformed_records = calculate_time_series_metrics(transformed_records)
        
        # Phase 3: Sector and industry analysis
        if config.enable_sector_analysis:
            transformed_records = calculate_sector_analysis(transformed_records)
        
        # Phase 4: Risk metrics
        if config.enable_risk_metrics:
            transformed_records = calculate_risk_metrics(transformed_records)
        
        # Add transformation metadata
        for record in transformed_records:
            record["transformation_timestamp"] = datetime.now(timezone.utc).isoformat()
            record["transformation_version"] = "2.0.0"
        
        # Ensure all data is JSON serializable before returning
        safe_data = ensure_json_serializable(transformed_records)
        
        return {
            "success": True,
            "records_processed": len(safe_data),
            "records_cleaned": len(raw_records) - len(safe_data),
            "data": safe_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transformation failed: {str(e)}")

@app.post("/transform_batch")
async def transform_batch(request: Dict[str, Any]):
    """Fetch from extract service and transform"""
    try:
        tickers = request.get("tickers", "")
        period = request.get("period", "1mo")
        extract_url = request.get("extract_service_url", "http://extract-service:8000")
        
        if not tickers:
            raise HTTPException(status_code=400, detail="tickers required")
        
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{extract_url}/extract_many",
                params={"ticker": tickers, "period": period}
            )
            
            if response.status_code != 200:
                raise HTTPException(status_code=400, detail=f"Failed to fetch data from extract service. Status: {response.status_code}, Response: {response.text[:200]}")
            
            extract_data = response.json()
            
            if "data" not in extract_data:
                raise HTTPException(status_code=400, detail=f"No data key in extract service response: {extract_data}")
        
        all_records = []
        for ticker_data in extract_data["data"].values():
            if isinstance(ticker_data, list):
                all_records.extend(ticker_data)
        
        if not all_records:
            raise HTTPException(status_code=404, detail="No data to transform")
        
        transform_result = transform_data({"raw_data": all_records})
        
        # Ensure JSON serializable data
        safe_transform_data = ensure_json_serializable(transform_result["data"])
        
        return {
            "success": True,
            "batch_id": extract_data.get("batch_id", "unknown"),
            "tickers_requested": tickers,
            "records_transformed": transform_result["records_processed"],
            "data": safe_transform_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch transformation failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)