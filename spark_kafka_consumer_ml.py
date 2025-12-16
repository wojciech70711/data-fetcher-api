from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, stddev, lag, when, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel
import os
import sys

# Increase recursion limit significantly to prevent stack overflow when loading models
sys.setrecursionlimit(100000)

# Fix for Windows - set Hadoop home
hadoop_home = os.path.join(os.getcwd(), "hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
os.makedirs(hadoop_bin, exist_ok=True)

# Create checkpoint directory
checkpoint_dir = os.path.join(os.getcwd(), "checkpoint_ml")
os.makedirs(checkpoint_dir, exist_ok=True)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkMLConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.kryoserializer.buffer.max", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load trained model
model_path = os.path.join(os.getcwd(), "crypto_price_model")
# Convert Windows path to forward slashes for Spark compatibility
model_path = model_path.replace("\\", "/")
print(f"Loading model from {model_path}...")

# Verify model directory exists and contains necessary files
if not os.path.exists(model_path):
    print(f"ERROR: Model directory not found at {model_path}")
    print("Please train the model first by running: python train_model.py sample")
    spark.stop()
    exit(1)

# Check for metadata directory
metadata_path = os.path.join(model_path.replace("/", "\\"), "metadata")
if not os.path.exists(metadata_path):
    print(f"ERROR: Model metadata not found. Model may be corrupted.")
    print("Please retrain the model by running: python train_model.py sample")
    spark.stop()
    exit(1)

try:
    # Load model with explicit path handling
    print("Loading model stages...")
    model = PipelineModel.load(model_path)
    print("Model loaded successfully!")
    print(f"  - Model has {len(model.stages)} stages")
except RecursionError as e:
    print(f"ERROR: RecursionError during model loading: {e}")
    print("Try increasing recursion limit or retrain with simpler model parameters")
    print("In train_model.py, reduce maxDepth or maxIter in GBTRegressor")
    spark.stop()
    exit(1)
except Exception as e:
    print(f"ERROR: Error loading model: {type(e).__name__}")
    print(f"   Details: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure the model was trained successfully")
    print("2. Check if model files are not corrupted")
    print("3. Try retraining: python train_model.py sample")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "ohlcv"

# Define schema for OHLCV data
ohlcv_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from value column
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), ohlcv_schema)) \
    .select("data.*")

# Define function to create features and make predictions
def process_batch(batch_df, batch_id):
    """
    Process each micro-batch: create features and make predictions
    """
    if batch_df.isEmpty():
        return
    
    print(f"\n{'='*80}")
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    print(f"{'='*80}")
    
    # Define windows for feature engineering
    window_5 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-5, -1)
    window_10 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-10, -1)
    window_20 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-20, -1)
    window_1 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-1, -1)
    
    # Create features
    df_features = batch_df
    
    # Price change features
    df_features = df_features.withColumn("price_change", 
                                         col("close") - lag("close", 1).over(window_1))
    df_features = df_features.withColumn("price_change_pct", 
                                         (col("close") - lag("close", 1).over(window_1)) / 
                                         lag("close", 1).over(window_1) * 100)
    
    # Moving averages
    df_features = df_features.withColumn("ma_5", avg("close").over(window_5))
    df_features = df_features.withColumn("ma_10", avg("close").over(window_10))
    df_features = df_features.withColumn("ma_20", avg("close").over(window_20))
    
    # Volatility
    df_features = df_features.withColumn("volatility_5", stddev("close").over(window_5))
    df_features = df_features.withColumn("volatility_10", stddev("close").over(window_10))
    
    # High-Low spread
    df_features = df_features.withColumn("hl_spread", col("high") - col("low"))
    df_features = df_features.withColumn("hl_spread_pct", 
                                         (col("high") - col("low")) / col("close") * 100)
    
    # Volume features
    df_features = df_features.withColumn("volume_ma_5", avg("volume").over(window_5))
    df_features = df_features.withColumn("volume_ratio", 
                                         col("volume") / avg("volume").over(window_10))
    
    # Price position in range
    df_features = df_features.withColumn("close_position", 
                                         (col("close") - col("low")) / 
                                         when(col("high") != col("low"), 
                                              col("high") - col("low")).otherwise(1))
    
    # Fill nulls with 0 for streaming (incomplete windows)
    df_features = df_features.fillna(0)
    
    # Make predictions
    try:
        predictions = model.transform(df_features)
        
        # Calculate prediction metrics
        predictions = predictions.withColumn("predicted_change", 
                                            predictions["prediction"] - col("close"))
        predictions = predictions.withColumn("predicted_change_pct", 
                                            (predictions["prediction"] - col("close")) / col("close") * 100)
        
        # Show predictions
        print("\nPRICE PREDICTIONS:")
        predictions.select(
            "timestamp",
            "symbol",
            col("close").alias("current_price"),
            col("prediction").alias("predicted_price"),
            col("predicted_change").alias("predicted_change_$"),
            col("predicted_change_pct").alias("predicted_change_%")
        ).show(truncate=False)
        
        # Show trading signals
        print("\nTRADING SIGNALS:")
        predictions.withColumn("signal", 
                              when(col("predicted_change_pct") > 1, "STRONG BUY")
                              .when(col("predicted_change_pct") > 0.3, "BUY")
                              .when(col("predicted_change_pct") < -1, "STRONG SELL")
                              .when(col("predicted_change_pct") < -0.3, "SELL")
                              .otherwise("HOLD")) \
            .select("timestamp", "symbol", "current_price", "predicted_change_%", "signal") \
            .withColumnRenamed("close", "current_price") \
            .show(truncate=False)
        
    except Exception as e:
        print(f"Error making predictions: {e}")
        batch_df.show(truncate=False)

# Write stream with foreachBatch
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

print(f"\nML-powered Kafka consumer started!")
print(f"Listening to topic: {kafka_topic}")
print(f"Model: {model_path}")
print(f"Waiting for data...\n")

# Wait for termination
query.awaitTermination()
