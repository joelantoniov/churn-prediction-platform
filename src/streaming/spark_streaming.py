from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable
import json
from typing import Dict, Any
import structlog
from config.spark_config import SparkConfig
from config.kafka_config import KafkaConfig

logger = structlog.get_logger()

class ChurnStreamingProcessor:
    def __init__(self):
        self.spark = SparkConfig.create_spark_session()
        self.model_broadcast = None
        
    def create_kafka_stream(self) -> StreamingQuery:
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS) \
            .option("subscribe", KafkaConfig.CUSTOMER_EVENTS_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        event_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_data", MapType(StringType(), StringType()), True),
            StructField("schema_version", StringType(), True)
        ])
        
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), event_schema).alias("data")
        ).select("data.*")
        
        return parsed_df
    
    def process_churn_predictions(self, events_df):
        watermarked_df = events_df.withWatermark("timestamp", "10 minutes")
        
        features_df = watermarked_df.groupBy(
            window(col("timestamp"), "5 minutes"),
            col("customer_id")
        ).agg(
            count("*").alias("event_count"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
            avg(when(col("event_type") == "purchase", col("event_data.amount").cast("double")).otherwise(0)).alias("avg_purchase_amount")
        )
        
        predictions_df = features_df.withColumn(
            "churn_score",
            when(col("event_count") < 2, 0.8)
            .when(col("purchase_count") == 0, 0.6)
            .otherwise(0.2)
        ).withColumn(
            "prediction_timestamp",
            current_timestamp()
        )
        
        return predictions_df
    
    def write_to_delta(self, df, path: str, checkpoint_path: str):
        return df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", path) \
            .trigger(processingTime='30 seconds') \
            .start()
    
    def write_to_kafka(self, df, topic: str, checkpoint_path: str):
        kafka_output = df.select(
            col("customer_id").alias("key"),
            to_json(struct(
                col("customer_id"),
                col("churn_score"),
                col("prediction_timestamp")
            )).alias("value")
        )
        
        return kafka_output.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append") \
            .start()
