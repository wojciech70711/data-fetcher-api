from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime
import csv
import os
from pathlib import Path

app = FastAPI(title="Cryptocurrency OHLCV Data API")

# Data model for OHLCV
class OHLCVData(BaseModel):
    timestamp: str  # ISO format: 2024-12-13T10:30:00
    symbol: str    # e.g., "BTC/USD", "ETH/USD"
    open: float
    high: float
    low: float
    close: float
    volume: float

class OHLCVBatch(BaseModel):
    data: List[OHLCVData]

# CSV file path
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

def get_csv_path(symbol: str) -> Path:
    """Get CSV file path for a symbol"""
    filename = f"{symbol.replace('/', '_')}_ohlcv.csv"
    return DATA_DIR / filename

def write_to_csv(ohlcv: OHLCVData):
    """Write OHLCV data to CSV file"""
    csv_path = get_csv_path(ohlcv.symbol)
    file_exists = csv_path.exists()
    
    try:
        with open(csv_path, 'a', newline='') as csvfile:
            fieldnames = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            writer.writerow({
                'timestamp': ohlcv.timestamp,
                'symbol': ohlcv.symbol,
                'open': ohlcv.open,
                'high': ohlcv.high,
                'low': ohlcv.low,
                'close': ohlcv.close,
                'volume': ohlcv.volume
            })
    except IOError as e:
        raise HTTPException(status_code=500, detail=f"Error writing to CSV: {str(e)}")

@app.get("/")
async def root():
    """API health check"""
    return {
        "status": "healthy",
        "api": "Cryptocurrency OHLCV Data Storage API",
        "version": "1.0.0"
    }

@app.post("/ohlcv/add")
async def add_ohlcv(data: OHLCVData):
    """
    Add single OHLCV data point
    
    Example:
    {
        "timestamp": "2024-12-13T10:30:00",
        "symbol": "BTC/USD",
        "open": 42000.50,
        "high": 42500.00,
        "low": 41500.00,
        "close": 42100.25,
        "volume": 1500.5
    }
    """
    try:
        # Validate that close price is between high and low
        if not (data.low <= data.close <= data.high):
            raise HTTPException(
                status_code=400,
                detail="Close price must be between low and high prices"
            )
        
        write_to_csv(data)
        return {
            "status": "success",
            "message": f"OHLCV data for {data.symbol} stored successfully",
            "file": str(get_csv_path(data.symbol))
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ohlcv/batch")
async def add_ohlcv_batch(batch: OHLCVBatch):
    """
    Add multiple OHLCV data points at once
    
    Example:
    {
        "data": [
            {
                "timestamp": "2024-12-13T10:30:00",
                "symbol": "BTC/USD",
                "open": 42000.50,
                "high": 42500.00,
                "low": 41500.00,
                "close": 42100.25,
                "volume": 1500.5
            },
            {
                "timestamp": "2024-12-13T10:31:00",
                "symbol": "ETH/USD",
                "open": 2300.00,
                "high": 2350.00,
                "low": 2280.00,
                "close": 2330.50,
                "volume": 5000.0
            }
        ]
    }
    """
    success_count = 0
    errors = []
    
    for idx, ohlcv in enumerate(batch.data):
        try:
            # Validate prices
            if not (ohlcv.low <= ohlcv.close <= ohlcv.high):
                errors.append({
                    "index": idx,
                    "symbol": ohlcv.symbol,
                    "error": "Close price must be between low and high prices"
                })
                continue
            
            write_to_csv(ohlcv)
            success_count += 1
        except Exception as e:
            errors.append({
                "index": idx,
                "symbol": ohlcv.symbol,
                "error": str(e)
            })
    
    return {
        "status": "completed",
        "total_records": len(batch.data),
        "successful": success_count,
        "failed": len(errors),
        "errors": errors if errors else None
    }

@app.get("/ohlcv/{symbol}")
async def get_ohlcv_data(symbol: str):
    """
    Get all OHLCV data for a symbol
    
    Example: /ohlcv/BTC_USD or /ohlcv/ETH_USD
    """
    csv_path = get_csv_path(symbol)
    
    if not csv_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"No data found for symbol {symbol}"
        )
    
    try:
        data = []
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append({
                    "timestamp": row['timestamp'],
                    "symbol": row['symbol'],
                    "open": float(row['open']),
                    "high": float(row['high']),
                    "low": float(row['low']),
                    "close": float(row['close']),
                    "volume": float(row['volume'])
                })
        
        return {
            "symbol": symbol,
            "count": len(data),
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/symbols")
async def get_symbols():
    """Get all symbols with stored data"""
    symbols = []
    try:
        for csv_file in DATA_DIR.glob("*_ohlcv.csv"):
            symbol = csv_file.stem.replace('_ohlcv', '').replace('_', '/')
            symbols.append(symbol)
        
        return {
            "count": len(symbols),
            "symbols": sorted(symbols)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/{symbol}")
async def get_symbol_stats(symbol: str):
    """Get statistics for a symbol"""
    csv_path = get_csv_path(symbol)
    
    if not csv_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"No data found for symbol {symbol}"
        )
    
    try:
        prices = []
        volumes = []
        
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                prices.append(float(row['close']))
                volumes.append(float(row['volume']))
        
        if not prices:
            raise HTTPException(status_code=404, detail="No valid data found")
        
        return {
            "symbol": symbol,
            "records": len(prices),
            "price": {
                "highest": max(prices),
                "lowest": min(prices),
                "average": sum(prices) / len(prices)
            },
            "volume": {
                "total": sum(volumes),
                "average": sum(volumes) / len(volumes)
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
