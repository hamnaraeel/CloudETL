from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String, Date, DateTime, Boolean, DECIMAL, BigInteger, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from datetime import datetime, date
from typing import List, Optional
import requests
import pandas as pd
from dateutil.parser import parse

app = FastAPI(title="Load Service", description="Data Warehouse Load Service with Star Schema")

# Database Configuration
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://psx_user:psx_password123@postgres-db:5432/psx_warehouse")
TRANSFORM_SERVICE_URL = os.getenv("TRANSFORM_SERVICE_URL", "http://transform-service:8001")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Star Schema Models
class FactStockPrices(Base):
    __tablename__ = "fact_stock_prices"
    
    fact_id = Column(Integer, primary_key=True, index=True)
    ticker_key = Column(Integer, index=True)
    date_key = Column(Integer, index=True)
    time_key = Column(Integer, index=True)
    open_price = Column(DECIMAL(12, 4))
    high_price = Column(DECIMAL(12, 4))
    low_price = Column(DECIMAL(12, 4))
    close_price = Column(DECIMAL(12, 4))
    volume = Column(BigInteger)
    ma_7 = Column(DECIMAL(12, 4))
    ma_30 = Column(DECIMAL(12, 4))
    rsi = Column(DECIMAL(5, 2))
    daily_return = Column(DECIMAL(8, 4))
    volatility = Column(DECIMAL(8, 4))
    created_at = Column(DateTime, default=datetime.utcnow)

class DimTicker(Base):
    __tablename__ = "dim_ticker"
    
    ticker_key = Column(Integer, primary_key=True, index=True)
    ticker_symbol = Column(String(10), index=True)
    company_name = Column(String(255))
    sector = Column(String(100))
    industry = Column(String(100))
    market_cap = Column(BigInteger)
    effective_date = Column(Date)
    end_date = Column(Date)
    is_current = Column(Boolean, default=True)
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)

class DimDate(Base):
    __tablename__ = "dim_date"
    
    date_key = Column(Integer, primary_key=True)
    full_date = Column(Date, unique=True, index=True)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    day_of_week = Column(Integer)
    day_name = Column(String(10))
    month_name = Column(String(10))
    is_weekend = Column(Boolean)
    is_holiday = Column(Boolean, default=False)

class DimTime(Base):
    __tablename__ = "dim_time"
    
    time_key = Column(Integer, primary_key=True, index=True)
    hour = Column(Integer)
    minute = Column(Integer)
    period = Column(String(10))
    trading_session = Column(String(20))

# Pydantic Models
class StockDataLoad(BaseModel):
    ticker: str
    period: str = "1mo"
    
class BatchLoad(BaseModel):
    tickers: str
    period: str = "1mo"

# Database Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Create tables
Base.metadata.create_all(bind=engine)

@app.get("/health")
async def health_check():
    """Comprehensive health check for load service"""
    import time
    import psutil
    
    start_time = time.time()
    
    checks = {
        "service": "load-service",
        "version": "1.0.0", 
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    # Test 1: Database connectivity
    try:
        db = SessionLocal()
        # Simple query to test database connection
        result = db.execute("SELECT 1").fetchone()
        db.close()
        
        if result and result[0] == 1:
            checks["checks"]["database"] = "healthy"
        else:
            checks["checks"]["database"] = "unhealthy - unexpected result"
            
    except Exception as e:
        checks["checks"]["database"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 2: Database table existence
    try:
        db = SessionLocal()
        # Check if main tables exist
        tables_to_check = ["fact_stock_prices", "dim_ticker", "dim_date", "dim_time"]
        missing_tables = []
        
        for table in tables_to_check:
            result = db.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}')").fetchone()
            if not result or not result[0]:
                missing_tables.append(table)
        
        db.close()
        
        if len(missing_tables) == 0:
            checks["checks"]["database_schema"] = "healthy"
        else:
            checks["checks"]["database_schema"] = f"unhealthy - missing tables: {missing_tables}"
            
    except Exception as e:
        checks["checks"]["database_schema"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 3: System resource check
    try:
        memory_percent = psutil.virtual_memory().percent
        cpu_percent = psutil.cpu_percent(interval=0.1)
        disk_percent = psutil.disk_usage('/').percent
        
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
        
        if disk_percent > 95:
            checks["checks"]["disk"] = "unhealthy - critically low space"
        elif disk_percent > 85:
            checks["checks"]["disk"] = "degraded - low space"
        else:
            checks["checks"]["disk"] = "healthy"
        checks["disk_usage_percent"] = round(disk_percent, 1)
        
    except Exception as e:
        checks["checks"]["system"] = f"degraded - {str(e)[:50]}"
    
    # Test 4: Transform service connectivity
    try:
        import httpx
        async with httpx.AsyncClient(timeout=3) as client:
            response = await client.get(f"{TRANSFORM_SERVICE_URL}/health")
            if response.status_code == 200:
                checks["checks"]["transform_service"] = "healthy"
            else:
                checks["checks"]["transform_service"] = f"degraded - status {response.status_code}"
                
    except Exception as e:
        checks["checks"]["transform_service"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 5: Response time check
    response_time = (time.time() - start_time) * 1000
    checks["response_time_ms"] = round(response_time, 2)
    
    if response_time > 5000:
        checks["checks"]["response_time"] = "unhealthy - too slow"
    elif response_time > 2000:
        checks["checks"]["response_time"] = "degraded - slow"
    else:
        checks["checks"]["response_time"] = "healthy"
    
    # Test 6: Basic data loading capability
    try:
        # Test database session creation
        db = SessionLocal()
        # Test query execution
        test_query = db.query(DimTicker).limit(1).first()
        db.close()
        checks["checks"]["data_loading"] = "healthy"
        
    except Exception as e:
        checks["checks"]["data_loading"] = f"degraded - {str(e)[:50]}"
    
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

@app.get("/")
async def root():
    return {"message": "Stock Data Warehouse Load Service", "version": "1.0.0"}

# SCD Type 2 Helper Functions
def get_or_create_ticker_key(db: Session, ticker_symbol: str, company_name: str = None, sector: str = None, industry: str = None) -> int:
    current_ticker = db.query(DimTicker).filter(
        DimTicker.ticker_symbol == ticker_symbol,
        DimTicker.is_current == True
    ).first()
    
    if current_ticker:
        return current_ticker.ticker_key
    
    # Create new ticker record
    max_key = db.query(DimTicker.ticker_key).order_by(DimTicker.ticker_key.desc()).first()
    new_key = (max_key[0] + 1) if max_key else 1
    
    new_ticker = DimTicker(
        ticker_key=new_key,
        ticker_symbol=ticker_symbol,
        company_name=company_name or f"{ticker_symbol} Corporation",
        sector=sector or "Technology",
        industry=industry or "Software",
        market_cap=1000000000,
        effective_date=date.today(),
        end_date=None,
        is_current=True,
        version=1
    )
    db.add(new_ticker)
    db.commit()
    return new_key

def get_or_create_date_key(db: Session, target_date: date) -> int:
    date_record = db.query(DimDate).filter(DimDate.full_date == target_date).first()
    
    if date_record:
        return date_record.date_key
    
    # Create date key as YYYYMMDD
    date_key = int(target_date.strftime("%Y%m%d"))
    
    new_date = DimDate(
        date_key=date_key,
        full_date=target_date,
        year=target_date.year,
        quarter=(target_date.month - 1) // 3 + 1,
        month=target_date.month,
        day=target_date.day,
        day_of_week=target_date.weekday(),
        day_name=target_date.strftime("%A"),
        month_name=target_date.strftime("%B"),
        is_weekend=target_date.weekday() >= 5
    )
    db.add(new_date)
    db.commit()
    return date_key

def get_or_create_time_key(db: Session, period: str, hour: int = 16, minute: int = 0) -> int:
    time_record = db.query(DimTime).filter(
        DimTime.period == period,
        DimTime.hour == hour,
        DimTime.minute == minute
    ).first()
    
    if time_record:
        return time_record.time_key
    
    max_key = db.query(DimTime.time_key).order_by(DimTime.time_key.desc()).first()
    new_key = (max_key[0] + 1) if max_key else 1
    
    trading_session = "regular"
    if hour < 9 or (hour == 9 and minute < 30):
        trading_session = "pre_market"
    elif hour > 16:
        trading_session = "after_hours"
    
    new_time = DimTime(
        time_key=new_key,
        hour=hour,
        minute=minute,
        period=period,
        trading_session=trading_session
    )
    db.add(new_time)
    db.commit()
    return new_key

# Data Loading Functions
def load_stock_data_to_warehouse(db: Session, ticker: str, period: str):
    import time
    
    try:
        # Call transform service with retry logic
        transform_url = f"{TRANSFORM_SERVICE_URL}/transform_batch"
        payload = {"tickers": ticker, "period": period}
        
        # Retry logic for service connectivity
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(transform_url, json=payload, timeout=30)
                response.raise_for_status()
                break
            except requests.exceptions.ConnectionError as e:
                if attempt < max_retries - 1:
                    print(f"Connection attempt {attempt + 1} failed, retrying in 2 seconds...")
                    time.sleep(2)
                    continue
                else:
                    raise HTTPException(status_code=503, detail=f"Transform service connection failed after {max_retries} attempts: {str(e)}")
            except requests.exceptions.HTTPError as e:
                if response.status_code == 404:
                    raise HTTPException(status_code=404, detail=f"Transform service endpoint not found. Check if /transform_batch exists. Response: {response.text}")
                else:
                    raise HTTPException(status_code=response.status_code, detail=f"Transform service error: {response.text}")
        
        data = response.json()
        if not data or 'data' not in data:
            raise HTTPException(status_code=404, detail="No data returned from transform service")
        
        stock_data = data['data']
        
        # Get dimension keys
        ticker_key = get_or_create_ticker_key(db, ticker)
        time_key = get_or_create_time_key(db, period)
        
        loaded_count = 0
        
        for record in stock_data:
            try:
                # Parse date
                record_date = parse(record['Date']).date()
                date_key = get_or_create_date_key(db, record_date)
                
                # Check if record already exists
                existing = db.query(FactStockPrices).filter(
                    FactStockPrices.ticker_key == ticker_key,
                    FactStockPrices.date_key == date_key,
                    FactStockPrices.time_key == time_key
                ).first()
                
                if existing:
                    continue
                
                # Create fact record
                fact_record = FactStockPrices(
                    ticker_key=ticker_key,
                    date_key=date_key,
                    time_key=time_key,
                    open_price=float(record.get('Open', 0)),
                    high_price=float(record.get('High', 0)),
                    low_price=float(record.get('Low', 0)),
                    close_price=float(record.get('Close', 0)),
                    volume=int(record.get('Volume', 0)),
                    ma_7=float(record.get('MA_7', 0)) if record.get('MA_7') else None,
                    ma_30=float(record.get('MA_30', 0)) if record.get('MA_30') else None,
                    rsi=float(record.get('RSI', 0)) if record.get('RSI') else None,
                    daily_return=float(record.get('Daily_Return', 0)) if record.get('Daily_Return') else None,
                    volatility=float(record.get('Volatility', 0)) if record.get('Volatility') else None
                )
                
                db.add(fact_record)
                loaded_count += 1
                
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
        
        db.commit()
        return {"loaded_records": loaded_count, "ticker": ticker, "period": period}
        
    except requests.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Transform service unavailable: {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Loading failed: {str(e)}")

# API Endpoints
@app.post("/load/batch")
async def load_batch(batch_data: BatchLoad, db: Session = Depends(get_db)):
    tickers = [t.strip().upper() for t in batch_data.tickers.split(",")]
    results = []
    
    for ticker in tickers:
        try:
            result = load_stock_data_to_warehouse(db, ticker, batch_data.period)
            results.append({"ticker": ticker, "status": "success", "details": result})
        except Exception as e:
            results.append({"ticker": ticker, "status": "failed", "error": str(e)})
    
    return {"message": "Batch load completed", "results": results}

@app.post("/load/{ticker}")
async def load_single_ticker(ticker: str, period: str = "1mo", db: Session = Depends(get_db)):
    result = load_stock_data_to_warehouse(db, ticker.upper(), period)
    return {"message": "Data loaded successfully", "details": result}

@app.get("/data/{ticker}")
async def get_ticker_data(ticker: str, limit: int = 100, db: Session = Depends(get_db)):
    ticker_record = db.query(DimTicker).filter(
        DimTicker.ticker_symbol == ticker.upper(),
        DimTicker.is_current == True
    ).first()
    
    if not ticker_record:
        raise HTTPException(status_code=404, detail="Ticker not found")
    
    facts = db.query(FactStockPrices).filter(
        FactStockPrices.ticker_key == ticker_record.ticker_key
    ).order_by(FactStockPrices.date_key.desc()).limit(limit).all()
    
    return {
        "ticker": ticker.upper(),
        "total_records": len(facts),
        "data": [
            {
                "date_key": fact.date_key,
                "open": float(fact.open_price) if fact.open_price else None,
                "high": float(fact.high_price) if fact.high_price else None,
                "low": float(fact.low_price) if fact.low_price else None,
                "close": float(fact.close_price) if fact.close_price else None,
                "volume": fact.volume,
                "ma_7": float(fact.ma_7) if fact.ma_7 else None,
                "ma_30": float(fact.ma_30) if fact.ma_30 else None,
                "rsi": float(fact.rsi) if fact.rsi else None,
                "daily_return": float(fact.daily_return) if fact.daily_return else None,
                "volatility": float(fact.volatility) if fact.volatility else None
            } for fact in facts
        ]
    }

@app.get("/stats")
async def get_warehouse_stats(db: Session = Depends(get_db)):
    total_facts = db.query(FactStockPrices).count()
    total_tickers = db.query(DimTicker).filter(DimTicker.is_current == True).count()
    total_dates = db.query(DimDate).count()
    
    return {
        "warehouse_stats": {
            "total_price_records": total_facts,
            "active_tickers": total_tickers,
            "date_range": total_dates,
            "last_updated": datetime.utcnow().isoformat()
        }
    }

@app.get("/debug/transform-connection")
async def test_transform_connection():
    try:
        # Test basic connectivity
        health_url = f"{TRANSFORM_SERVICE_URL}/health"
        response = requests.get(health_url, timeout=5)
        health_status = response.status_code
        
        # Test transform_batch endpoint
        batch_url = f"{TRANSFORM_SERVICE_URL}/transform_batch"
        test_payload = {"tickers": "AAPL", "period": "5d"}
        batch_response = requests.post(batch_url, json=test_payload, timeout=10)
        
        return {
            "transform_service_health": {
                "status_code": health_status,
                "reachable": health_status == 200
            },
            "transform_batch_endpoint": {
                "status_code": batch_response.status_code,
                "available": batch_response.status_code == 200,
                "response_preview": batch_response.text[:200] if batch_response.status_code != 200 else "Success"
            }
        }
    except Exception as e:
        return {
            "error": f"Connection test failed: {str(e)}",
            "transform_service_reachable": False
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)