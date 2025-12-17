"""Load configuration from environment with sensible defaults.

Drop a `.env` file in the project root to override values locally. This
module uses `python-dotenv` to load environment variables when available.
"""
from dotenv import load_dotenv
import os

load_dotenv()


def _bool_env(value, default=False):
	if value is None:
		return default
	return str(value).lower() in ("1", "true", "yes", "on")


# Server Settings
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
RELOAD = _bool_env(os.getenv("RELOAD"), True)


# Data Settings
DATA_DIRECTORY = os.getenv("DATA_DIRECTORY", "data")
CSV_FILENAME_PATTERN = os.getenv("CSV_FILENAME_PATTERN", "{symbol}_ohlcv.csv")


# OHLCV Validation
MIN_VOLUME = float(os.getenv("MIN_VOLUME", "0"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "1000000"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.00001"))


# CSV Settings
CSV_ENCODING = os.getenv("CSV_ENCODING", "utf-8")
CSV_QUOTE_CHAR = os.getenv("CSV_QUOTE_CHAR", '"')
CSV_DELIMITER = os.getenv("CSV_DELIMITER", ",")


# API Settings
API_TITLE = os.getenv("API_TITLE", "Cryptocurrency OHLCV Data API")
API_VERSION = os.getenv("API_VERSION", "1.0.0")
API_DESCRIPTION = os.getenv(
	"API_DESCRIPTION",
	"REST API for storing and retrieving OHLCV data from cryptocurrency markets",
)


# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ohlcv")


# Spark Configuration
SPARK_APP_NAME = "KafkaSparkConsumer"
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# Processing Configuration
STARTING_OFFSETS = "latest"  # Options: "earliest", "latest"
OUTPUT_MODE = "append"  # Options: "append", "complete", "update"
