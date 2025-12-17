"""
Simple ML Price Predictor for Cryptocurrency using PySpark Streaming
Trains on historical CSV data and consumes OHLCV batches from Kafka
"""

import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, sum as spark_sum, max as spark_max, min as spark_min, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pickle
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
import os
import sys
import json
from datetime import datetime
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set HADOOP_HOME for Windows
if sys.platform.startswith('win'):
    hadoop_home = os.path.join(os.getcwd(), 'hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] = f"{os.path.join(hadoop_home, 'bin')};{os.environ.get('PATH', '')}"

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "ohlcv"
TRAINING_DATA_FILE = "data/train_data.csv"
MODEL_PATH = "models/gbt_model.pkl"
SCALER_PATH = "models/scaler.pkl"
BATCH_DURATION = "10 seconds"
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

class CryptoPredictor:
    def __init__(self):
        """Initialize predictor with sklearn model and Spark session"""
        self.model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42
        )
        self.scaler = StandardScaler()
        self.is_trained = False
        
        # Initialize Spark for streaming
        self.spark = SparkSession.builder \
            .appName("CryptoPricePredictor") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.memory", "1g") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
    def create_features(self, df):
        """Create technical indicators from OHLCV data"""
        # Price changes
        df['price_change'] = df['close'].diff()
        df['price_change_pct'] = df['close'].pct_change() * 100
        
        # Moving averages
        df['ma_5'] = df['close'].rolling(window=5, min_periods=1).mean()
        df['ma_10'] = df['close'].rolling(window=10, min_periods=1).mean()
        df['ma_20'] = df['close'].rolling(window=20, min_periods=1).mean()
        
        # Volatility
        df['volatility'] = df['close'].rolling(window=5, min_periods=1).std()
        
        # Price ranges
        df['hl_spread'] = df['high'] - df['low']
        df['oc_spread'] = abs(df['close'] - df['open'])
        
        # Volume indicators
        df['volume_ma'] = df['volume'].rolling(window=5, min_periods=1).mean()
        df['volume_change'] = df['volume'].pct_change() * 100
        
        # Fill NaN values
        df = df.fillna(0)
        
        return df
    
    def train_from_csv(self, csv_file):
        """Train Gradient Boosting model using historical data"""
        print(f"\n{'='*80}")
        print(f"Training ML Model on Historical Data")
        print(f"{'='*80}")
        print(f"Data source: {csv_file}")
        
        try:
            # Load CSV data
            df = pd.read_csv(csv_file)
            print(f"Loaded {len(df)} records")
            
            # Create features
            df = self.create_features(df)
            
            # Feature columns
            feature_cols = [
                'open', 'high', 'low', 'volume',
                'price_change', 'price_change_pct',
                'ma_5', 'ma_10', 'ma_20',
                'volatility', 'hl_spread', 'oc_spread',
                'volume_ma', 'volume_change'
            ]
            
            # Prepare data: predict next close price
            X = df[feature_cols].values[:-1]
            y = df['close'].values[1:]
            
            # Split: 80% train, 20% test
            split_idx = int(len(X) * 0.8)
            X_train, X_test = X[:split_idx], X[split_idx:]
            y_train, y_test = y[:split_idx], y[split_idx:]
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            print(f"\nTraining Gradient Boosting model...")
            print(f"Training samples: {len(X_train)}")
            print(f"Test samples:     {len(X_test)}")
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            self.is_trained = True
            
            # Evaluate
            train_score = self.model.score(X_train_scaled, y_train)
            test_score = self.model.score(X_test_scaled, y_test)
            
            # Calculate MAE
            train_pred = self.model.predict(X_train_scaled)
            test_pred = self.model.predict(X_test_scaled)
            train_mae = np.mean(np.abs(train_pred - y_train))
            test_mae = np.mean(np.abs(test_pred - y_test))
            
            print(f"\n{'='*80}")
            print(f"Model Training Complete!")
            print(f"{'='*80}")
            print(f"Train R² score:      {train_score:.4f}")
            print(f"Test R² score:       {test_score:.4f}")
            print(f"Train MAE:           ${train_mae:,.2f}")
            print(f"Test MAE:            ${test_mae:,.2f}")
            print(f"{'='*80}\n")
            
            # Save model
            os.makedirs("models", exist_ok=True)
            with open(MODEL_PATH, 'wb') as f:
                pickle.dump(self.model, f)
            with open(SCALER_PATH, 'wb') as f:
                pickle.dump(self.scaler, f)
            print(f"Model saved to {MODEL_PATH}\n")
            
            return True
            
        except Exception as e:
            print(f" Error training model: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def load_model(self):
        """Load pre-trained model"""
        try:
            with open(MODEL_PATH, 'rb') as f:
                self.model = pickle.load(f)
            with open(SCALER_PATH, 'rb') as f:
                self.scaler = pickle.load(f)
            self.is_trained = True
            print(f"Model loaded from {MODEL_PATH}")
            return True
        except:
            return False
    
    def consume_kafka_stream(self):
        """Consume OHLCV data from Kafka using Spark Structured Streaming"""
        if not self.is_trained:
            print("Model not trained. Please train the model first.")
            return
        
        print(f"\nConnecting to Kafka broker: {KAFKA_BROKER}")
        print(f"Topic: {KAFKA_TOPIC}")
        print(f"Batch duration: {BATCH_DURATION}\n")
        
        # Define schema for incoming JSON data
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True)
        ])
        
        try:
            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .load()
            
            # Parse JSON
            parsed_df = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), schema).alias("data")) \
                .select("data.*")
            
            # Process batches
            query = parsed_df.writeStream \
                .foreachBatch(self.process_batch) \
                .outputMode("update") \
                .trigger(processingTime=BATCH_DURATION) \
                .start()
            
            print("Connected to Kafka stream")
            print("Waiting for OHLCV batches...\n")
            
            query.awaitTermination()
            
        except KeyboardInterrupt:
            print("\n\nShutting down...")
            query.stop()
            self.spark.stop()
            print("Spark session closed. Goodbye!")
        except Exception as e:
            print(f"\n Error: {e}")
            import traceback
            traceback.print_exc()
    
    def process_batch(self, batch_df, batch_id):
        """Process each batch of streaming data"""
        if batch_df.isEmpty():
            return
        
 
        
        # Calculate batch statistics
        stats = batch_df.agg(
            count("*").alias("num_messages"),
            avg("close").alias("avg_close"),
            avg("volume").alias("avg_volume"),
            (spark_max("high") - spark_min("low")).alias("price_range"),
            spark_sum("volume").alias("total_volume")
        ).collect()[0]
        
        print(f"Batch Statistics:")
        print(f"   Messages:          {stats['num_messages']}")
        print(f"   Avg Close:         ${stats['avg_close']:,.2f}")
        print(f"   Price Range:       ${stats['price_range']:,.2f}")
        print(f"   Total Volume:      {stats['total_volume']:,.2f}")
        
        # Get latest record for prediction
        latest = batch_df.orderBy(col("timestamp").desc()).first()
        
        # Create simple feature vector (using latest values)
        features = np.array([[
            latest['open'],
            latest['high'],
            latest['low'],
            latest['volume'],
            0.0,  # price_change
            0.0,  # price_change_pct
            latest['close'],  # ma_5 (use close as proxy)
            latest['close'],  # ma_10
            latest['close'],  # ma_20
            0.0,  # volatility
            latest['high'] - latest['low'],  # hl_spread
            abs(latest['close'] - latest['open']),  # oc_spread
            latest['volume'],  # volume_ma
            0.0   # volume_change
        ]])
        
        # Scale and predict
        features_scaled = self.scaler.transform(features)
        prediction = self.model.predict(features_scaled)[0]
        
        current_price = latest['close']
        change_pct = ((prediction - current_price) / current_price) * 100
        
        # Generate signal
        if change_pct > 1.0:
            signal = "STRONG BUY"
        elif change_pct > 0.3:
            signal = "BUY"
        elif change_pct < -1.0:
            signal = "STRONG SELL"
        elif change_pct < -0.3:
            signal = "SELL"
        else:
            signal = "HOLD"
        
        print(f"\nPrediction:")
        print(f"   Symbol:            {latest['symbol']}")
        print(f"   Current Price:     ${current_price:,.2f}")
        print(f"   Predicted Price:   ${prediction:,.2f}")
        print(f"   Expected Change:   {change_pct:+.2f}%")
        print(f"   Signal:            {signal}")
        print(f"{'='*80}\n")
        
        # Save results to file
        batch_id = uuid.uuid4().hex[:8]

        result_data = {
            "batch_id": batch_id,
            "timestamp": datetime.now().isoformat(),
            "batch_statistics": {
                "num_messages": int(stats['num_messages']),
                "avg_close": round(float(stats['avg_close']), 2),
                "avg_volume": round(float(stats['avg_volume']), 2),
                "price_range": round(float(stats['price_range']), 2),
                "total_volume": round(float(stats['total_volume']), 2)
            },
            "prediction": {
                "symbol": latest['symbol'],
                "current_price": round(float(current_price), 2),
                "predicted_price": round(float(prediction), 2),
                "expected_change_pct": round(float(change_pct), 2),
                "trading_signal": signal
            }
        }
        
        result_file = RESULTS_DIR / f"batch_{batch_id}.json"
        try:
            with open(result_file, 'w') as f:
                json.dump(result_data, f, indent=2)
            print(f"Results saved to: {result_file}")
        except Exception as e:
            print(f"Error saving results: {e}")


def main():
    """Main function"""
    print("="*80)
    print("Crypto Price Predicting ML model with PySpark Streaming")
    print("="*80)
    
    predictor = CryptoPredictor()
    
    # Try to load existing model
    if os.path.exists(MODEL_PATH):
        print(f"\nFound existing model at {MODEL_PATH}")
        print("Loading existing model...")
        if predictor.load_model():
            predictor.consume_kafka_stream()
            return
    
    # Train new model
    if predictor.train_from_csv(TRAINING_DATA_FILE):
        predictor.consume_kafka_stream()
    else:
        print("Failed to train model. Exiting...")


if __name__ == "__main__":
    main()
