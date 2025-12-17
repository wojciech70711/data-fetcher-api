"""
Cryptocurrency OHLCV Data API - Binance Data Fetcher
Fetches real OHLCV data from Binance API and stores it in localhost API
"""

import requests
from datetime import datetime, timedelta
import json
import time
import argparse
import sys
import logging
import os

BASE_URL = "http://localhost:8000"
BINANCE_API = "https://api4.binance.com/api/v3"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, f"data_fetch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Create logs directory if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
BINANCE_API = "https://api4.binance.com/api/v3"

# Default cryptocurrency pairs
DEFAULT_PAIRS = [
    ("BTCUSDT", "BTC/USDT"),  # Bitcoin
    ("ETHUSDT", "ETH/USDT"),  # Ethereum
    ("XRPUSDT", "XRP/USDT"),  # Ripple
    ("BNBUSDT", "BNB/USDT"),  # Binance Coin
    ("ADAUSDT", "ADA/USDT"),  # Cardano
]

# Default timeframe for historical data
DEFAULT_INTERVAL = "1h"
DEFAULT_LIMIT = 10

# Runtime variables (set by command-line arguments)
CRYPTO_PAIRS = DEFAULT_PAIRS
INTERVAL = DEFAULT_INTERVAL
LIMIT = DEFAULT_LIMIT

def fetch_binance_data(symbol, interval=DEFAULT_INTERVAL, limit=DEFAULT_LIMIT):
    """
    Fetch OHLCV data from Binance API
    Returns list of OHLCV data points
    """
    try:
        url = f"{BINANCE_API}/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        response = requests.get(url, params=params, timeout=5)
        
        if response.status_code != 200:
            logger.error(f"Error fetching {symbol}: {response.status_code}")
            return []
        
        klines = response.json()
        ohlcv_data = []
        
        for kline in klines:
            # Binance kline format: [open_time, open, high, low, close, volume, ...]
            timestamp = datetime.fromtimestamp(kline[0] / 1000).isoformat()
            
            ohlcv_data.append({
                "timestamp": timestamp,
                "symbol": symbol.replace("USDT", "/USDT"),
                "open": float(kline[1]),
                "high": float(kline[2]),
                "low": float(kline[3]),
                "close": float(kline[4]),
                "volume": float(kline[7])  # Quote asset volume
            })
        
        return ohlcv_data
    
    except Exception as e:
        logger.error(f"Exception fetching {symbol}: {str(e)}")
        return []

def send_batch_to_api(ohlcv_list):
    """
    Send a batch of OHLCV records to the local API
    """
    if not ohlcv_list:
        return False
    
    try:
        payload = {"data": ohlcv_list}
        response = requests.post(f"{BASE_URL}/ohlcv/batch", json=payload, timeout=5)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("successful", 0) > 0:
                logger.info(f"Sent {result['successful']}/{result['total_records']} records")
                if result.get("failed", 0) > 0:
                    logger.warning(f"{result['failed']} records had errors")
                return True
        else:
            logger.error(f"API Error: {response.status_code}")
            return False
    
    except Exception as e:
        logger.error(f"Exception sending to API: {str(e)}")
        return False

def get_price_prediction(ohlcv_list):
    """
    Get price prediction from the ML model API
    """
    if not ohlcv_list:
        return None
    
    try:
        # Use the last 5-10 data points for better prediction
        prediction_data = ohlcv_list[-min(10, len(ohlcv_list)):]
        
        payload = {"ohlcv_data": prediction_data}
        response = requests.post(f"{BASE_URL}/predict", json=payload, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                return result
            else:
                logger.warning(f"Prediction status: {result.get('status')} - {result.get('message')}")
                return None
        else:
            logger.warning(f"Prediction API Error: {response.status_code}")
            return None
    
    except Exception as e:
        logger.warning(f"Exception getting prediction: {str(e)}")
        return None

def parse_arguments():
    """Parse command-line arguments for pairs and time range"""
    parser = argparse.ArgumentParser(
        description="Fetch cryptocurrency OHLCV data from Binance and store in local API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python get_data_from_market.py
    (Uses default pairs: BTC, ETH, XRP, BNB, ADA)
  
  python get_data_from_market.py --pairs BTC ETH LTC
    (Fetch Bitcoin, Ethereum, Litecoin)
  
  python get_data_from_market.py --pairs BTC --interval 4h --limit 24
    (Fetch Bitcoin, 4-hour candles, last 24 intervals)
  
  python get_data_from_market.py --pairs SOL DOGE --interval 1d --limit 30
    (Fetch Solana and Dogecoin, daily candles, last 30 days)

Valid intervals: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M
        """
    )
    
    parser.add_argument(
        '--pairs',
        nargs='+',
        help='Cryptocurrency symbols to fetch (without USDT). Example: BTC ETH XRP',
        default=None
    )
    
    parser.add_argument(
        '--interval',
        type=str,
        default=DEFAULT_INTERVAL,
        help=f'Candlestick interval (default: {DEFAULT_INTERVAL}). Options: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=DEFAULT_LIMIT,
        help=f'Number of candles to fetch (default: {DEFAULT_LIMIT}, max: 1000)'
    )
    
    parser.add_argument(
        '--list-pairs',
        action='store_true',
        help='Show available cryptocurrency pairs'
    )
    
    args = parser.parse_args()
    return args

def get_crypto_pairs(symbol_list):
    """Convert symbol list to trading pairs"""
    if not symbol_list:
        return DEFAULT_PAIRS
    
    # Map of common symbols to trading pairs
    symbol_map = {
        'BTC': ('BTCUSDT', 'BTC/USDT'),
        'ETH': ('ETHUSDT', 'ETH/USDT'),
        'XRP': ('XRPUSDT', 'XRP/USDT'),
        'BNB': ('BNBUSDT', 'BNB/USDT'),
        'ADA': ('ADAUSDT', 'ADA/USDT'),
        'SOL': ('SOLUSDT', 'SOL/USDT'),
        'DOGE': ('DOGEUSDT', 'DOGE/USDT'),
        'LTC': ('LTCUSDT', 'LTC/USDT'),
        'DOT': ('DOTUSDT', 'DOT/USDT'),
        'LINK': ('LINKUSDT', 'LINK/USDT'),
        'UNI': ('UNIUSDT', 'UNI/USDT'),
        'MATIC': ('MATICUSDT', 'MATIC/USDT'),
    }
    
    pairs = []
    for symbol in symbol_list:
        symbol_upper = symbol.upper()
        if symbol_upper in symbol_map:
            pairs.append(symbol_map[symbol_upper])
        else:
            # Try to use it directly (e.g., if user provides BTCUSDT)
            pairs.append((symbol_upper, symbol_upper.replace('USDT', '/USDT')))
    
    return pairs if pairs else DEFAULT_PAIRS

def list_available_pairs():
    """Display available cryptocurrency pairs"""
    symbol_map = {
        'BTC': 'Bitcoin',
        'ETH': 'Ethereum',
        'XRP': 'Ripple',
        'BNB': 'Binance Coin',
        'ADA': 'Cardano',
        'SOL': 'Solana',
        'DOGE': 'Dogecoin',
        'LTC': 'Litecoin',
        'DOT': 'Polkadot',
        'LINK': 'Chainlink',
        'UNI': 'Uniswap',
        'MATIC': 'Polygon',
    }
    
    logger.info("Available cryptocurrency pairs:")
    for symbol, name in symbol_map.items():
        logger.info(f"  {symbol:8} - {name}")
    logger.info("Usage: python get_data_from_market.py --pairs BTC ETH XRP")

def test_api(crypto_pairs, interval, limit):
    """Fetch data from Binance and send to local API"""
    global CRYPTO_PAIRS, INTERVAL, LIMIT
    CRYPTO_PAIRS = crypto_pairs
    INTERVAL = interval
    LIMIT = limit
    
    logger.info("=" * 70)
    logger.info("Cryptocurrency OHLCV Data API - Binance Data Fetcher")
    logger.info("=" * 70)
    logger.info(f"Configuration: Pairs={', '.join([pair[1] for pair in CRYPTO_PAIRS])}, Interval={INTERVAL}, Limit={LIMIT} candles")
    
    logger.info("Fetching data from Binance API...")
    all_data = []
    for binance_symbol, display_symbol in CRYPTO_PAIRS:
        logger.info(f"Fetching {display_symbol}...")
        data = fetch_binance_data(binance_symbol, INTERVAL, LIMIT)
        if data:
            logger.info(f"OK ({len(data)} candles)")
            all_data.extend(data)
            
            # Get price prediction for this symbol
            prediction = get_price_prediction(data)
            if prediction:
                logger.info("-" * 70)
                logger.info(f"ML PREDICTION for {display_symbol}:")
                logger.info(f"  Current Price:      ${prediction['current_price']:,.2f}")
                logger.info(f"  Predicted Price:    ${prediction['predicted_price']:,.2f}")
                logger.info(f"  Expected Change:    {prediction['expected_change_pct']:+.2f}%")
                logger.info(f"  Trading Signal:     {prediction['trading_signal']}")
                logger.info(f"  Based on:           {prediction['batch_stats']['num_messages']} data points")
                logger.info("-" * 70)
        else:
            logger.error("FAILED")
        time.sleep(0.5)  # Rate limiting
    
    logger.info(f"Total data retrieved: {len(all_data)} OHLCV records")
    
    logger.info(f"Sending {len(all_data)} records to {BASE_URL}/ohlcv/batch...")
    if all_data:
        send_batch_to_api(all_data)
        logger.info("OK: Data fetching and upload completed!")
    else:
        logger.error("No data to send. Check your Binance connection.")
    
    logger.info("=" * 70)

if __name__ == "__main__":
    logger.info(f"Log file: {LOG_FILE}")
    args = parse_arguments()
    
    # Show available pairs if requested
    if args.list_pairs:
        list_available_pairs()
        sys.exit(0)
    
    # Get cryptocurrency pairs from arguments
    crypto_pairs = get_crypto_pairs(args.pairs)
    
    # Validate interval
    valid_intervals = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']
    if args.interval not in valid_intervals:
        logger.error(f"Invalid interval '{args.interval}'")
        logger.error(f"Valid intervals: {', '.join(valid_intervals)}")
        sys.exit(1)
    
    # Validate limit
    if args.limit < 1 or args.limit > 1000:
        logger.error("Limit must be between 1 and 1000")
        sys.exit(1)
    
    try:
        test_api(crypto_pairs, args.interval, args.limit)
    except KeyboardInterrupt:
        logger.warning("Demo interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
