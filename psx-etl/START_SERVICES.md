# 🚀 PSX ETL Pipeline - Quick Start Guide

## Start All Services with Docker Compose

### **One-Command Startup**
```bash
# Build and start all services
docker-compose up --build

# Or run in background
docker-compose up --build -d
```

### **Service URLs**
- **🔄 Extract Service**: http://localhost:8000
  - API Docs: http://localhost:8000/docs
  - Health Check: http://localhost:8000/health

- **⚡ Transform Service**: http://localhost:8001
  - API Docs: http://localhost:8001/docs  
  - Health Check: http://localhost:8001/health

- **📊 Visualization Dashboard**: http://localhost:8002
  - **MAIN DASHBOARD** - Full stock analytics interface

- **🏛️ Load Service (Data Warehouse)**: http://localhost:8003
  - API Docs: http://localhost:8003/docs
  - Health Check: http://localhost:8003/health
  - Warehouse Stats: http://localhost:8003/stats

### **Stop All Services**
```bash
# Stop services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

### **View Logs**
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f extract-service
docker-compose logs -f transform-service  
docker-compose logs -f visualization-service
docker-compose logs -f load-service
```

### **Individual Service Management**
```bash
# Start only specific services
docker-compose up extract-service transform-service

# Restart a service
docker-compose restart visualization-service

# Rebuild a specific service
docker-compose up --build visualization-service
```

## 🎯 Quick Test Workflow

### **1. Start Services**
```bash
docker-compose up --build -d
```

### **2. Wait for Services (30 seconds)**
```bash
# Check all services are healthy
docker-compose ps
```

### **3. Open Dashboard**
Visit: **http://localhost:8002**

### **4. Test the Pipeline**
1. Select stock (AAPL, MSFT, GOOGL, etc.)
2. Choose time period (1D, 1W, 1M, etc.)  
3. Click Refresh to load data
4. View 9 different chart types!

## 🔧 Development Mode

### **Local Development (without Docker)**
```bash
# Terminal 1 - Extract Service
cd services/extract
pip install -r requirements.txt
uvicorn extract_service:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 - Transform Service  
cd services/transform
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8001 --reload

# Terminal 3 - Visualization Service
cd services/visualization
pip install -r requirements.txt
python app.py
```

## 🐛 Troubleshooting

### **Services Not Starting**
```bash
# Check Docker is running
docker --version

# View detailed logs
docker-compose logs -f
```

### **Dashboard Shows "No Data"**
1. Check Transform Service: http://localhost:8001/health
2. Check Extract Service: http://localhost:8000/health
3. Check Load Service: http://localhost:8003/health
4. View logs: `docker-compose logs -f transform-service`

### **Port Conflicts**
If ports are in use, modify docker-compose.yml:
```yaml
ports:
  - "8003:8000"  # Use 8003 instead of 8000
```

### **Rebuild After Changes**
```bash
# Rebuild specific service
docker-compose up --build visualization-service

# Rebuild all services  
docker-compose up --build
```

## 📈 Usage Examples

### **API Testing**
```bash
# Test Extract Service
curl http://localhost:8000/extract/AAPL

# Test Transform Service
curl -X POST http://localhost:8001/transform_batch \
  -H "Content-Type: application/json" \
  -d '{"tickers": "AAPL", "period": "1mo"}'

# Test Load Service (Data Warehouse)
curl -X POST http://localhost:8003/load/AAPL?period=1mo

# Query warehouse data
curl http://localhost:8003/data/AAPL
```

### **Dashboard Features**
- **Multi-Stock Support**: AAPL, MSFT, GOOGL, TSLA, AMZN
- **Historical Analysis**: 1D to 5Y timeframes
- **9 Chart Types**: Candlestick, Volume, Technical Indicators, etc.
- **Real-time Refresh**: Latest market data
- **Mobile Responsive**: Works on all devices

## 🎉 Success!
When all services are running, you'll have:
- Professional stock data extraction
- Advanced financial analytics  
- Beautiful interactive dashboard
- **Star Schema Data Warehouse** with SCD Type 2
- Complete ETL pipeline ready for production!

**Main Dashboard URL: http://localhost:8002**