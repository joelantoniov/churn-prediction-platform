import pandas as pd
import numpy as np
from typing import Dict, List, Any
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import structlog

logger = structlog.get_logger()

class FeatureTransformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def create_customer_features(self, events_df: DataFrame) -> DataFrame:
        customer_features = events_df.groupBy("customer_id").agg(
            count("*").alias("total_events"),
            countDistinct("event_type").alias("unique_event_types"),
            min("timestamp").alias("first_event_date"),
            max("timestamp").alias("last_event_date"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
            sum(when(col("event_type") == "support_ticket", 1).otherwise(0)).alias("support_ticket_count"),
            avg(when(col("event_type") == "purchase", col("event_data.amount")).otherwise(0)).alias("avg_purchase_amount"),
            sum(when(col("event_type") == "purchase", col("event_data.amount")).otherwise(0)).alias("total_purchase_amount")
        )
        
        current_time = datetime.now()
        customer_features = customer_features.withColumn(
            "days_since_last_activity",
            datediff(lit(current_time), col("last_event_date"))
        ).withColumn(
            "customer_lifetime_days",
            datediff(col("last_event_date"), col("first_event_date")) + 1
        ).withColumn(
            "events_per_day",
            col("total_events") / col("customer_lifetime_days")
        ).withColumn(
            "purchase_frequency",
            col("purchase_count") / col("customer_lifetime_days")
        )
        
        return customer_features
    
    def create_time_based_features(self, events_df: DataFrame, window_days: int = 30) -> DataFrame:
        window_start = datetime.now() - timedelta(days=window_days)
        
        recent_events = events_df.filter(col("timestamp") >= lit(window_start))
        
        time_features = recent_events.groupBy("customer_id").agg(
            count("*").alias(f"events_last_{window_days}d"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias(f"purchases_last_{window_days}d"),
            sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias(f"page_views_last_{window_days}d"),
            countDistinct(date_format(col("timestamp"), "yyyy-MM-dd")).alias(f"active_days_last_{window_days}d"),
            avg(when(col("event_type") == "purchase", col("event_data.amount")).otherwise(0)).alias(f"avg_purchase_amount_last_{window_days}d")
        )
        
        return time_features
    
    def create_behavioral_features(self, events_df: DataFrame) -> DataFrame:
        events_with_hour = events_df.withColumn("hour_of_day", hour(col("timestamp")))
        events_with_dow = events_with_hour.withColumn("day_of_week", dayofweek(col("timestamp")))
        
        behavioral_features = events_with_dow.groupBy("customer_id").agg(
            mode(col("hour_of_day")).alias("preferred_hour"),
            mode(col("day_of_week")).alias("preferred_day"),
            stddev(col("hour_of_day")).alias("activity_time_variance"),
            count(when(col("hour_of_day").between(9, 17), 1)).alias("business_hours_activity"),
            count(when(col("hour_of_day").between(18, 23), 1)).alias("evening_activity"),
            count(when(col("day_of_week").isin([1, 7]), 1)).alias("weekend_activity")
        )
        
        return behavioral_features
