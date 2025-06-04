from typing import Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import structlog

logger = structlog.get_logger()

class AggregationEngine:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def create_rolling_aggregations(self, df: DataFrame, window_sizes: List[int] = [7, 30, 90]) -> DataFrame:
        windowSpec = Window.partitionBy("customer_id").orderBy("timestamp").rangeBetween(-86400 * max(window_sizes), 0)
        
        result_df = df
        
        for window_size in window_sizes:
            window_spec_current = Window.partitionBy("customer_id").orderBy("timestamp").rangeBetween(-86400 * window_size, 0)
            
            result_df = result_df.withColumn(
                f"rolling_events_{window_size}d",
                count("*").over(window_spec_current)
            ).withColumn(
                f"rolling_purchases_{window_size}d",
                sum(when(col("event_type") == "purchase", 1).otherwise(0)).over(window_spec_current)
            ).withColumn(
                f"rolling_revenue_{window_size}d",
                sum(when(col("event_type") == "purchase", col("event_data.amount")).otherwise(0)).over(window_spec_current)
            )
            
        return result_df
    
    def create_cohort_features(self, df: DataFrame) -> DataFrame:
        first_purchase_date = df.filter(col("event_type") == "purchase") \
            .groupBy("customer_id") \
            .agg(min("timestamp").alias("first_purchase_date"))
        
        df_with_cohort = df.join(first_purchase_date, "customer_id", "left")
        
        cohort_features = df_with_cohort.withColumn(
            "days_since_first_purchase",
            datediff(col("timestamp"), col("first_purchase_date"))
        ).withColumn(
            "cohort_month",
            date_format(col("first_purchase_date"), "yyyy-MM")
        )
        
        return cohort_features
