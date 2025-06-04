#!/bin/bash

echo "Setting up Churn Prediction Platform environment..."

# Create necessary directories
mkdir -p data/{raw_events,customer_features,churn_predictions}
mkdir -p models/{training,production}
mkdir -p logs
mkdir -p checkpoints/{events,predictions,features}

# Set environment variables
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$SPARK_HOME/bin

# Create MLflow tracking directory
mkdir -p mlruns

# Initialize Great Expectations
great_expectations init

echo "Environment setup completed!"
