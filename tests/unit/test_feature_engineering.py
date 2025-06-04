import pytest
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.feature_engineering.feature_transformer import FeatureTransformer

@pytest.fixture
def spark_session():
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

@pytest.fixture
def sample_events_data():
    return [
        ("customer_1", datetime.now() - timedelta(days=5), "purchase", {"amount": "100"}),
        ("customer_1", datetime.now() - timedelta(days=3), "page_view", {}),
        ("customer_1", datetime.now() - timedelta(days=1), "support_ticket", {}),
        ("customer_2", datetime.now() - timedelta(days=10), "purchase", {"amount": "50"}),
        ("customer_2", datetime.now() - timedelta(days=2), "page_view", {})
    ]

def test_create_customer_features(spark_session, sample_events_data):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_data", MapType(StringType(), StringType()), True)
    ])
    
    events_df = spark_session.createDataFrame(sample_events_data, schema)
    
    transformer = FeatureTransformer(spark_session)
    features_df = transformer.create_customer_features(events_df)
    
    features_pd = features_df.toPandas()
    
    assert len(features_pd) == 2
    assert "total_events" in features_pd.columns
    assert "purchase_count" in features_pd.columns
    assert features_pd[features_pd["customer_id"] == "customer_1"]["total_events"].iloc[0] == 3
    assert features_pd[features_pd["customer_id"] == "customer_1"]["purchase_count"].iloc[0] == 1

def test_time_based_features(spark_session, sample_events_data):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_data", MapType(StringType(), StringType()), True)
    ])
    
    events_df = spark_session.createDataFrame(sample_events_data, schema)
    
    transformer = FeatureTransformer(spark_session)
    time_features_df = transformer.create_time_based_features(events_df, window_days=30)
    
    time_features_pd = time_features_df.toPandas()
    
    assert len(time_features_pd) == 2
    assert "events_last_30d" in time_features_pd.columns
    assert "purchases_last_30d" in time_features_pd.columns
