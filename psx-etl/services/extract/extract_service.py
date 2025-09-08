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
