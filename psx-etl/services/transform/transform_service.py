from fastapi import FastAPI
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field
from datetime import datetime


app = FastAPI()


class Record(BaseModel):
    Date: Optional[str] = Field(None, description="Date in ISO format")
    Open: Optional[float]
    High: Optional[float]
    Low: Optional[float]
    Close: Optional[float]
    Volume: Optional[int]
    Dividend: Optional[float]
    industry: Optional[str]
    sector: Optional[str]
    fullTimeEmployees: Optional[int]
    marketCap: Optional[float]
    previousClose: Optional[float]
    averageVolume: Optional[float]
    currency: Optional[str]
    dividendRate: Optional[float]
    dividendYield: Optional[float]
    trailingPE: Optional[float]
    forwardPE: Optional[float]
    Ticker: Optional[str]


class TransformRequest(BaseModel):
    batch_id: Optional[str] = Field(None, description="Idempotency key for the batch")
    data: Any  # Accepts either List[Record] or Dict[str, List[Record]]
    # Optionally, add more fields for config/rules


class TransformResponse(BaseModel):
    batch_id: Optional[str] = Field(None, description="Idempotency key for the batch")
    data: List[Dict[str, Any]]
    errors: List[str]


@app.post("/transform", response_model=TransformResponse)
def transform_data(request: TransformRequest) -> TransformResponse:

    # Accept both list and dict for 'data'. If dict, flatten to list.
    records = request.data
    if isinstance(records, dict):
        # Flatten all lists in the dict (for multi-ticker support)
        flat = []
        for v in records.values():
            if isinstance(v, list):
                flat.extend(v)
        records = flat
    # If already a list, use as is
    df = pd.DataFrame(records)

    # --- CLEAN ---
    df = df.drop_duplicates()
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    string_cols = df.select_dtypes(include=[object]).columns.tolist()
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df[string_cols] = df[string_cols].fillna("")
    critical_cols = [col for col in ['Date', 'Close', 'Ticker'] if col in df.columns]
    df = df.dropna(subset=critical_cols)
    # Standardize date format and normalize to UTC
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        if df['Date'].dt.tz is None:
            df['Date'] = df['Date'].dt.tz_localize('UTC')
        else:
            df['Date'] = df['Date'].dt.tz_convert('UTC')
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    if 'currency' in df.columns:
        df['currency'] = df['currency'].str.replace('$', '', regex=False)
    if 'Ticker' in df.columns:
        df['Ticker'] = df['Ticker'].str.upper()

    # --- FEATURE ENGINEERING ---
    # Sort by Ticker and Date for rolling calculations
    if 'Ticker' in df.columns and 'Date' in df.columns:
        df = df.sort_values(['Ticker', 'Date'])

    # Price & Trading Data
    if all(col in df.columns for col in ['Open', 'Close']):
        df['Daily_Return'] = (df['Close'] - df['Open']) / df['Open']
    if 'Close' in df.columns:
        df['Close_shifted'] = df.groupby('Ticker')['Close'].shift(1)
        df['Price_Change_Pct'] = (df['Close'] - df['Close_shifted']) / df['Close_shifted']
        df.drop(columns=['Close_shifted'], inplace=True)
        # Moving averages
        df['MA_7'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(7, min_periods=1).mean())
        df['MA_30'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(30, min_periods=1).mean())
        # Volatility (std dev of returns)
        if 'Daily_Return' in df.columns:
            df['Volatility_7'] = df.groupby('Ticker')['Daily_Return'].transform(lambda x: x.rolling(7, min_periods=1).std())
            df['Volatility_30'] = df.groupby('Ticker')['Daily_Return'].transform(lambda x: x.rolling(30, min_periods=1).std())
    # Relative Volume
    if 'Volume' in df.columns and 'averageVolume' in df.columns:
        df['Relative_Volume'] = df['Volume'] / df['averageVolume']

    # Valuation Metrics
    if 'trailingPE' in df.columns and 'forwardPE' in df.columns:
        df['PE_Growth'] = df['trailingPE'] - df['forwardPE']

    # Dividend Data
    if 'Dividend' in df.columns:
        # Total Dividend Paid in Period (per Ticker)
        df['Total_Dividend_Paid'] = df.groupby('Ticker')['Dividend'].transform('sum')
        # Dividend Growth Rate (period over period)
        df['Dividend_shifted'] = df.groupby('Ticker')['Dividend'].shift(1)
        df['Dividend_Growth_Rate'] = (df['Dividend'] - df['Dividend_shifted']) / df['Dividend_shifted'].replace(0, np.nan)
        df.drop(columns=['Dividend_shifted'], inplace=True)

    # Company Profile: sector, industry already present for grouping

    # --- VALIDATE ---
    errors = []
    # Row-level validation: collect errors for each row
    for idx, row in df.iterrows():
        row_errors = []
        if 'Close' in row and (not isinstance(row['Close'], (int, float)) or pd.isnull(row['Close'])):
            row_errors.append(f"Row {idx}: Close price must be numeric and not null")
        if 'Volume' in row and (row['Volume'] < 0 or pd.isnull(row['Volume'])):
            row_errors.append(f"Row {idx}: Volume must be >= 0 and not null")
        if 'Date' in row and (not isinstance(row['Date'], str) or row['Date'] == ""):
            row_errors.append(f"Row {idx}: Date must be present and a string")
        if 'Ticker' in row and (not isinstance(row['Ticker'], str) or row['Ticker'] == ""):
            row_errors.append(f"Row {idx}: Ticker must be present and a string")
        if row_errors:
            errors.extend(row_errors)


    # (No enrichment or reshaping applied)

    # Return transformed data and errors, with batch_id passthrough
    return TransformResponse(batch_id=request.batch_id, data=df.to_dict(orient="records"), errors=errors)
