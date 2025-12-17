from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
from pathlib import Path
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import uvicorn
import pickle
import numpy as np
import os
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, API_TITLE, API_VERSION, API_DESCRIPTION

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

# ML Model configuration
MODEL_PATH = "models/gbt_model.pkl"
SCALER_PATH = "models/scaler.pkl"
_model = None
_scaler = None

def load_ml_model():
    """Load the trained ML model and scaler"""
    global _model, _scaler
    if _model is not None and _scaler is not None:
        return _model, _scaler
    
    try:
        if os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH):
            with open(MODEL_PATH, 'rb') as f:
                _model = pickle.load(f)
            with open(SCALER_PATH, 'rb') as f:
                _scaler = pickle.load(f)
            logger.info("ML model loaded successfully")
            return _model, _scaler
        else:
            logger.warning("ML model not found. Train the model first using ml_predictor.py")
            return None, None
    except Exception as e:
        logger.error(f"Error loading ML model: {e}")
        return None, None

def get_trading_signal(current_price: float, predicted_price: float) -> tuple:
    """Generate trading signal based on prediction"""
    change_pct = ((predicted_price - current_price) / current_price) * 100
    
    if change_pct > 1.0:
        return "STRONG BUY", change_pct
    elif change_pct > 0.3:
        return "BUY", change_pct
    elif change_pct < -1.0:
        return "STRONG SELL", change_pct
    elif change_pct < -0.3:
        return "SELL", change_pct
    else:
        return "HOLD", change_pct

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

class PredictionRequest(BaseModel):
    ohlcv_data: List[OHLCVData]

class PredictionResponse(BaseModel):
    status: str
    predicted_price: Optional[float] = None
    current_price: Optional[float] = None
    expected_change_pct: Optional[float] = None
    trading_signal: Optional[str] = None
    batch_stats: Optional[Dict] = None
    message: Optional[str] = None

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

def check_kafka_broker():
    """Check if Kafka broker is reachable."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        # Get cluster metadata to verify connection
        producer.bootstrap_connected()
        producer.close()
        return True
    except Exception as e:
        logger.warning("Kafka broker not reachable: %s", e)
        return False

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

@app.get("/")
async def root():
    """API health check"""
    kafka_status = "connected" if check_kafka_broker() else "disconnected"
    model, scaler = load_ml_model()
    model_status = "loaded" if model is not None else "not_loaded"
    
    return {
        "status": "healthy",
        "api": API_TITLE,
        "version": API_VERSION,
        "kafka_broker": kafka_status,
        "ml_model": model_status
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

@app.post("/predict", response_model=PredictionResponse)
async def predict_price(request: PredictionRequest):
    """
    Predict cryptocurrency price from OHLCV batch data
    
    Example:
    {
        "ohlcv_data": [
            {
                "timestamp": "2024-12-17T10:00:00",
                "symbol": "BTC/USD",
                "open": 42000.50,
                "high": 42500.00,
                "low": 41500.00,
                "close": 42100.25,
                "volume": 1500.5
            },
            {
                "timestamp": "2024-12-17T10:01:00",
                "symbol": "BTC/USD",
                "open": 42100.25,
                "high": 42600.00,
                "low": 41800.00,
                "close": 42300.00,
                "volume": 1600.0
            }
        ]
    }
    """
    try:
        # Load model
        model, scaler = load_ml_model()
        if model is None or scaler is None:
            raise HTTPException(
                status_code=503,
                detail="ML model not available. Please train the model first using ml_predictor.py"
            )
        
        # Validate input
        if not request.ohlcv_data or len(request.ohlcv_data) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one OHLCV data point is required"
            )
        
        # Validate prices
        for idx, data in enumerate(request.ohlcv_data):
            if not (data.low <= data.close <= data.high):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid data at index {idx}: close price must be between low and high"
                )
        
        # Calculate batch statistics
        batch_stats = {
            "num_messages": len(request.ohlcv_data),
            "avg_close": np.mean([d.close for d in request.ohlcv_data]),
            "avg_volume": np.mean([d.volume for d in request.ohlcv_data]),
            "price_range": max(d.high for d in request.ohlcv_data) - min(d.low for d in request.ohlcv_data),
            "total_volume": sum(d.volume for d in request.ohlcv_data)
        }
        
        # Get latest data point for prediction
        latest = request.ohlcv_data[-1]
        
        # Create feature vector (simplified - using latest values)
        features = np.array([[
            latest.open,
            latest.high,
            latest.low,
            latest.volume,
            0.0,  # price_change
            0.0,  # price_change_pct
            latest.close,  # ma_5
            latest.close,  # ma_10
            latest.close,  # ma_20
            0.0,  # volatility
            latest.high - latest.low,  # hl_spread
            abs(latest.close - latest.open),  # oc_spread
            latest.volume,  # volume_ma
            0.0   # volume_change
        ]])
        
        # Scale and predict
        features_scaled = scaler.transform(features)
        predicted_price = float(model.predict(features_scaled)[0])
        
        # Generate trading signal
        current_price = latest.close
        signal, change_pct = get_trading_signal(current_price, predicted_price)
        
        logger.info(f"Prediction for {latest.symbol}: ${predicted_price:.2f} ({signal})")
        
        return PredictionResponse(
            status="success",
            predicted_price=round(predicted_price, 2),
            current_price=round(current_price, 2),
            expected_change_pct=round(change_pct, 2),
            trading_signal=signal,
            batch_stats=batch_stats,
            message=f"Prediction based on {len(request.ohlcv_data)} OHLCV data points"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error making prediction: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/predict/single")
async def predict_single(data: OHLCVData):
    """
    Predict price from a single OHLCV data point
    
    Example:
    {
        "timestamp": "2024-12-17T10:00:00",
        "symbol": "BTC/USD",
        "open": 42000.50,
        "high": 42500.00,
        "low": 41500.00,
        "close": 42100.25,
        "volume": 1500.5
    }
    """
    return await predict_price(PredictionRequest(ohlcv_data=[data]))

if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)
