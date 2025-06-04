#!/bin/bash

echo "Starting model training pipeline..."

# Set environment variables
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Run feature engineering
python airflow/jobs/feature_engineering.py

# Run model training
python -c "
from src.model.churn_predictor import ChurnPredictor
from src.storage.delta_lake_manager import DeltaLakeManager
from config.spark_config import SparkConfig
import pandas as pd

# Load training data
spark = SparkConfig.create_spark_session()
delta_manager = DeltaLakeManager(spark)

features_df = delta_manager.read_table('/data/customer_features').toPandas()
# Simulate churn labels for demo
features_df['is_churned'] = (features_df['days_since_last_activity'] > 30).astype(int)

# Train model
predictor = ChurnPredictor()
X, feature_names = predictor.prepare_features(features_df)
y = features_df['is_churned'].values

predictor.feature_names = feature_names
metrics = predictor.train(X, y)

print('Training metrics:', metrics)

# Save model
predictor.save_model('models/production/churn_model_latest.pkl')
print('Model saved successfully!')
"

echo "Model training completed!"
