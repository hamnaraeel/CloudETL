from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
# Retry decorator for yfinance calls
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception)
)
def get_ticker_info(ticker):
    t = yf.Ticker(ticker)
    return t.info

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception)
)
def get_ticker_history(ticker, period):
    t = yf.Ticker(ticker)
    return t.history(period=period).reset_index(), t.info
from fastapi import FastAPI, Query
from typing import List, Dict, Any


import yfinance as yf
import uuid

app = FastAPI()

SELECTED_INFO_FIELDS = [
    "industry", "sector", "fullTimeEmployees", "marketCap", "previousClose",
    "averageVolume", "currency", "dividendRate", "dividendYield", "trailingPE", "forwardPE"
]

@app.get("/health")
def health_check():
    """Health check endpoint - actually tests service health"""
    import time
    start_time = time.time()
    
    checks = {
        "service": "extract-service",
        "version": "1.0.0",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "checks": {}
    }
    
    # Test 1: Check if yfinance is accessible
    try:
        test_ticker = yf.Ticker("AAPL")
        info = test_ticker.info
        if info and len(info) > 0:
            checks["checks"]["yfinance"] = "healthy"
        else:
            checks["checks"]["yfinance"] = "unhealthy - no data"
    except Exception as e:
        checks["checks"]["yfinance"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 2: Check response time
    response_time = (time.time() - start_time) * 1000  # in milliseconds
    checks["response_time_ms"] = round(response_time, 2)
    
    if response_time > 5000:  # 5 seconds
        checks["checks"]["response_time"] = "unhealthy - too slow"
    elif response_time > 2000:  # 2 seconds
        checks["checks"]["response_time"] = "degraded - slow"
    else:
        checks["checks"]["response_time"] = "healthy"
    
    # Test 3: Memory check (basic)
    try:
        import psutil
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 90:
            checks["checks"]["memory"] = "unhealthy - high usage"
        elif memory_percent > 80:
            checks["checks"]["memory"] = "degraded - moderate usage"
        else:
            checks["checks"]["memory"] = "healthy"
        checks["memory_usage_percent"] = memory_percent
    except ImportError:
        checks["checks"]["memory"] = "unknown - psutil not available"
    
    # Overall status
    unhealthy_count = sum(1 for check in checks["checks"].values() if "unhealthy" in str(check))
    degraded_count = sum(1 for check in checks["checks"].values() if "degraded" in str(check))
    
    if unhealthy_count > 0:
        checks["status"] = "unhealthy"
    elif degraded_count > 0:
        checks["status"] = "degraded"
    else:
        checks["status"] = "healthy"
    
    return checks

@app.get("/extract/{ticker}")
def extract_ticker(
    ticker: str,
    period: str = Query("1mo", description="Period for historical data, e.g. '1mo', '1y', etc.")
) -> Dict[str, Any]:
    try:
        hist, info = get_ticker_history(ticker, period)
    except Exception as e:
        return {"error": f"Failed to fetch data for {ticker}: {str(e)}"}
    if hist.empty:
        return {"error": f"No data found for ticker '{ticker}'. It may be invalid or delisted."}
    filtered_info = {k: info.get(k) for k in SELECTED_INFO_FIELDS}
    filtered_info["Ticker"] = ticker
    records = []
    for _, row in hist.iterrows():
        record = {
            "Date": row["Date"].strftime("%Y-%m-%d") if hasattr(row["Date"], "strftime") else str(row["Date"]),
            "Open": row.get("Open"),
            "High": row.get("High"),
            "Low": row.get("Low"),
            "Close": row.get("Close"),
            "Volume": row.get("Volume"),
            "Dividend": row.get("Dividends", 0.0),
        }
        record.update(filtered_info)
        records.append(record)
    return {"data": records}

# New endpoint: Only company info fields
@app.get("/info/{ticker}")
def get_info(ticker: str) -> Dict[str, Any]:
    try:
        info = get_ticker_info(ticker)
    except Exception as e:
        return {"error": f"Failed to fetch info for {ticker}: {str(e)}"}
    filtered_info = {k: info.get(k) for k in SELECTED_INFO_FIELDS}
    filtered_info["Ticker"] = ticker
    return filtered_info

# New endpoint: Only historical OHLCV and dividends
@app.get("/history/{ticker}")
def get_history(
    ticker: str,
    period: str = Query("1mo", description="Period for historical data, e.g. '1mo', '1y', etc.")
) -> Dict[str, Any]:
    try:
        hist, _ = get_ticker_history(ticker, period)
    except Exception as e:
        return {"error": f"Failed to fetch history for {ticker}: {str(e)}"}
    records = []
    for _, row in hist.iterrows():
        record = {
            "Date": row["Date"].strftime("%Y-%m-%d") if hasattr(row["Date"], "strftime") else str(row["Date"]),
            "Open": row.get("Open"),
            "High": row.get("High"),
            "Low": row.get("Low"),
            "Close": row.get("Close"),
            "Volume": row.get("Volume"),
            "Dividend": row.get("Dividends", 0.0),
        }
        records.append(record)
    return {"data": records}

# New endpoint: Multiple tickers at once
@app.get("/extract_many")
def extract_many(
    ticker: str = Query(..., description="Comma-separated tickers, e.g. 'AAPL,MSFT'"),
    period: str = Query("1mo", description="Period for historical data, e.g. '1mo', '1y', etc."),
    batch_id: str = Query(None, description="Idempotency batch identifier (optional)")
) -> Dict[str, Any]:
    if not batch_id:
        batch_id = str(uuid.uuid4())
    tickers = [t.strip() for t in ticker.split(",") if t.strip()]
    all_data = {}
    for tkr in tickers:
        try:
            hist, info = get_ticker_history(tkr, period)
        except Exception as e:
            all_data[tkr] = {"error": f"Failed to fetch data: {str(e)}"}
            continue
        if hist.empty:
            all_data[tkr] = {"error": f"No data found for ticker '{tkr}'. It may be invalid or delisted."}
            continue
        filtered_info = {k: info.get(k) for k in SELECTED_INFO_FIELDS}
        filtered_info["Ticker"] = tkr
        records = []
        for _, row in hist.iterrows():
            record = {
                "Date": row["Date"].strftime("%Y-%m-%d") if hasattr(row["Date"], "strftime") else str(row["Date"]),
                "Open": row.get("Open"),
                "High": row.get("High"),
                "Low": row.get("Low"),
                "Close": row.get("Close"),
                "Volume": row.get("Volume"),
                "Dividend": row.get("Dividends", 0.0),
            }
            record.update(filtered_info)
            records.append(record)
        all_data[tkr] = records
    return {"batch_id": batch_id, "data": all_data}
