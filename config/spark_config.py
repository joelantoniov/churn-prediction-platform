from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

class SparkConfig:
    APP_NAME = "churn-prediction-platform"
    
    @staticmethod
    def create_spark_session():
        builder = SparkSession.builder \
            .appName(SparkConfig.APP_NAME) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        
        return configure_spark_with_delta_pip(builder).getOrCreate()
