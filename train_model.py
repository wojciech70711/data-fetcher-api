from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, stddev, when, from_json
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import os
import sys

# Increase recursion limit significantly to prevent stack overflow when saving/loading models
sys.setrecursionlimit(100000)

# Fix for Windows - set Hadoop home
hadoop_home = os.path.join(os.getcwd(), "hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
os.makedirs(hadoop_bin, exist_ok=True)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CryptoPricePredictor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.kryoserializer.buffer.max", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Suppress Kafka AdminClientConfig warnings
import logging
logging.getLogger("org.apache.kafka.clients.admin.AdminClientConfig").setLevel(logging.ERROR)

def create_features(df):
    """
    Create technical indicators and features from OHLCV data
    """
    # Define window specifications
    window_5 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-5, -1)
    window_10 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-10, -1)
    window_20 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-20, -1)
    window_1 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-1, -1)
    
    # Price change features
    df = df.withColumn("price_change", col("close") - lag("close", 1).over(window_1))
    df = df.withColumn("price_change_pct", 
                       (col("close") - lag("close", 1).over(window_1)) / lag("close", 1).over(window_1) * 100)
    
    # Moving averages
    df = df.withColumn("ma_5", avg("close").over(window_5))
    df = df.withColumn("ma_10", avg("close").over(window_10))
    df = df.withColumn("ma_20", avg("close").over(window_20))
    
    # Volatility
    df = df.withColumn("volatility_5", stddev("close").over(window_5))
    df = df.withColumn("volatility_10", stddev("close").over(window_10))
    
    # High-Low spread
    df = df.withColumn("hl_spread", col("high") - col("low"))
    df = df.withColumn("hl_spread_pct", (col("high") - col("low")) / col("close") * 100)
    
    # Volume features
    df = df.withColumn("volume_ma_5", avg("volume").over(window_5))
    df = df.withColumn("volume_ratio", col("volume") / avg("volume").over(window_10))
    
    # Price position in range
    df = df.withColumn("close_position", 
                       (col("close") - col("low")) / when(col("high") != col("low"), 
                                                           col("high") - col("low")).otherwise(1))
    
    # Target: Next period's close price
    df = df.withColumn("target_price", lag("close", -1).over(Window.partitionBy("symbol").orderBy("timestamp")))
    df = df.withColumn("target_change_pct", 
                       (lag("close", -1).over(Window.partitionBy("symbol").orderBy("timestamp")) - col("close")) 
                       / col("close") * 100)
    
    # Drop nulls created by window functions
    df = df.na.drop()
    
    return df

def load_data_from_kafka(kafka_servers="localhost:9092", topic="ohlcv", duration_seconds=60):
    """
    Load historical data from Kafka for training
    """
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
    print(f"Reading data from Kafka topic '{topic}' for {duration_seconds} seconds...")
    print("Make sure your Kafka producer is sending data!\n")
    
    # Define OHLCV schema
    ohlcv_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])
    
    # Read from Kafka in batch mode (read all available data)
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    df = df_kafka.selectExpr("CAST(value AS STRING)") \
        .withColumn("data", from_json(col("value"), ohlcv_schema)) \
        .select("data.*")
    
    return df

def train_model(data_source="kafka", data_path=None, kafka_servers="localhost:9092", topic="ohlcv"):
    """
    Train a machine learning model to predict cryptocurrency prices
    
    Parameters:
    - data_source: "kafka", "file", or "sample"
    - data_path: path to parquet/csv file (if data_source="file")
    - kafka_servers: Kafka broker address
    - topic: Kafka topic name
    """
    if data_source == "kafka":
        # Load data from Kafka
        df = load_data_from_kafka(kafka_servers, topic)
    elif data_source == "file" and data_path:
        # Load from file
        print(f"Loading data from {data_path}...")
        if data_path.endswith('.parquet'):
            df = spark.read.parquet(data_path)
        else:
            df = spark.read.csv(data_path, header=True, inferSchema=True)
    else:
        # Create sample data for demonstration
        print("Creating sample training data...")
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
        from datetime import datetime, timedelta
        import random
        
        schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("symbol", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True)
        ])
        
        # Generate synthetic data
        base_price = 50000
        data = []
        for i in range(1000):
            timestamp = datetime.now() - timedelta(hours=1000-i)
            price = base_price + random.uniform(-5000, 5000) + i * 10
            high = price + random.uniform(0, 500)
            low = price - random.uniform(0, 500)
            open_price = price + random.uniform(-200, 200)
            close = price + random.uniform(-200, 200)
            volume = random.uniform(1000000, 10000000)
            
            data.append((timestamp, "BTC", open_price, high, low, close, volume))
        
        df = spark.createDataFrame(data, schema)
    
    print(f"Training data rows: {df.count()}")
    
    # Create features
    print("Creating features...")
    df_features = create_features(df)
    
    # Select features for training
    feature_columns = [
        "open", "high", "low", "close", "volume",
        "price_change", "price_change_pct",
        "ma_5", "ma_10", "ma_20",
        "volatility_5", "volatility_10",
        "hl_spread", "hl_spread_pct",
        "volume_ma_5", "volume_ratio",
        "close_position"
    ]
    
    # Prepare training data
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    
    # Use Gradient Boosted Trees for regression (reduced complexity to avoid stack overflow)
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="target_price",
        maxIter=20,
        maxDepth=4,
        stepSize=0.1
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    # Split data
    train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)
    
    print(f"Training set: {train_data.count()} rows")
    print(f"Test set: {test_data.count()} rows")
    
    # Train model
    print("Training model...")
    model = pipeline.fit(train_data)
    
    # Make predictions
    print("Evaluating model...")
    predictions = model.transform(test_data)
    
    # Evaluate
    evaluator_rmse = RegressionEvaluator(labelCol="target_price", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="target_price", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="target_price", predictionCol="prediction", metricName="r2")
    
    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    
    print(f"\nModel Performance:")
    print(f"RMSE: ${rmse:.2f}")
    print(f"MAE: ${mae:.2f}")
    print(f"R²: {r2:.4f}")
    
    # Show sample predictions
    print("\nSample Predictions:")
    predictions.select("timestamp", "symbol", "close", "target_price", "prediction").show(10, truncate=False)
    
    # Save model
    model_path = os.path.join(os.getcwd(), "crypto_price_model")
    # Convert to forward slashes for Spark compatibility
    model_path_spark = model_path.replace("\\", "/")
    print(f"\nSaving model to {model_path}...")
    
    try:
        model.write().overwrite().save(model_path_spark)
        print("Model saved successfully!")
        
        # Verify model was saved correctly
        if os.path.exists(os.path.join(model_path, "metadata")):
            print("Model metadata verified")
        else:
            print("Warning: Model metadata not found after save")
            
    except Exception as e:
        print(f"Error saving model: {e}")
        raise
    
    print("\nModel training complete!")
    return model

if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "sample":
            print("Using sample data...")
            train_model(data_source="sample")
        elif sys.argv[1] == "file" and len(sys.argv) > 2:
            print(f"Loading from file: {sys.argv[2]}")
            train_model(data_source="file", data_path=sys.argv[2])
        else:
            print("Usage: python train_model.py [sample|file <path>|kafka]")
            print("Using Kafka by default...")
            train_model(data_source="kafka")
    else:
        # Default: use Kafka
        print("Training model on data from Kafka broker...")
        print("Make sure Kafka is running and data is available in 'ohlcv' topic\n")
        train_model(data_source="kafka")
    
    spark.stop()
