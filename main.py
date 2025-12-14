from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime
import csv
from pathlib import Path
import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

app = FastAPI(title="Cryptocurrency OHLCV Data API")

# Configure logging
logger = logging.getLogger("data-api")
logging.basicConfig(level=logging.INFO)

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

# Kafka configuration
KAFKA_BOOTSTRAP = ["192.168.100.8:9094"]
KAFKA_TOPIC = "ohlcv"
_producer = None

def get_kafka_producer():
    """Return a KafkaProducer instance (lazy init)."""
    global _producer
    if _producer is not None:
        return _producer

    try:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
            acks=1
        )
        logger.info("Kafka producer created, bootstrap=%s", KAFKA_BOOTSTRAP)
    except Exception as e:
        logger.error("Failed to create Kafka producer: %s", e)
        _producer = None

    return _producer

def send_to_kafka(topic: str, message: dict):
    """Send a message (dict) to Kafka topic. Non-blocking; logs errors."""
    producer = get_kafka_producer()
    if not producer:
        logger.warning("Kafka producer not available; skipping send")
        return False

    try:
        future = producer.send(topic, message)

        # add callbacks to catch errors asynchronously
        def on_send_success(record_metadata):
            logger.debug("Message sent to %s partition=%s offset=%s",
                         record_metadata.topic, record_metadata.partition, record_metadata.offset)

        def on_send_error(exc):
            logger.error("Failed to send message to Kafka: %s", exc)

        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
        return True
    except KafkaError as ke:
        logger.error("Kafka error while sending: %s", ke)
        return False
    except Exception as e:
        logger.error("Unexpected error while sending to Kafka: %s", e)
        return False

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
        
        # Send to Kafka asynchronously (non-blocking)
        try:
            sent = send_to_kafka(KAFKA_TOPIC, data.dict())
            if sent:
                logger.info("OHLCV message queued for Kafka: %s", data.symbol)
        except Exception:
            logger.exception("Error while attempting to send single OHLCV to Kafka")
        
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
            
            # Send to Kafka asynchronously (non-blocking)
            try:
                sent = send_to_kafka(KAFKA_TOPIC, ohlcv.dict())
                if sent:
                    logger.debug("Queued OHLCV for Kafka: %s", ohlcv.symbol)
            except Exception:
                logger.exception("Error while attempting to send batch OHLCV to Kafka")
            
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
