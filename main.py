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
import uvicorn
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, API_TITLE, API_VERSION, API_DESCRIPTION, DATA_DIRECTORY, CSV_FILENAME_PATTERN

app = FastAPI(title=API_TITLE, version=API_VERSION, description=API_DESCRIPTION)

# Configure logging
logger = logging.getLogger("data-api")
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# File handler - save logs to file
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / f"api_{datetime.now().strftime('%Y%m%d')}.log"
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(console_handler)
logger.addHandler(file_handler)

logger.info(f"Logging configured. Log file: {log_file}")

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
KAFKA_BOOTSTRAP = [KAFKA_BOOTSTRAP_SERVERS]
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
DATA_DIR = Path(DATA_DIRECTORY)
DATA_DIR.mkdir(exist_ok=True)

def get_csv_path(symbol: str) -> Path:
    """Get CSV file path for a symbol"""
    filename = CSV_FILENAME_PATTERN.format(symbol=symbol.replace('/', '_'))
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
        "api": API_TITLE,
        "version": API_VERSION
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
            sent = send_to_kafka(KAFKA_TOPIC, data.model_dump())
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
                sent = send_to_kafka(KAFKA_TOPIC, ohlcv.model_dump())
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

if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)
