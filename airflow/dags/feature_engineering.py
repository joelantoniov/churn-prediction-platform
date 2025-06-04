from pyspark.sql import SparkSession
from src.feature_engineering.feature_transformer import FeatureTransformer
from src.feature_engineering.aggregation_engine import AggregationEngine
from src.storage.delta_lake_manager import DeltaLakeManager
from config.spark_config import SparkConfig
import structlog

logger = structlog.get_logger()

def main():
    spark = SparkConfig.create_spark_session()
    
    try:
        # Initialize components
        transformer = FeatureTransformer(spark)
        aggregation_engine = AggregationEngine(spark)
        delta_manager = DeltaLakeManager(spark)
        
        # Read raw events
        events_df = delta_manager.read_table("/data/raw_events")
        
        # Create features
        customer_features = transformer.create_customer_features(events_df)
        time_features = transformer.create_time_based_features(events_df)
        behavioral_features = transformer.create_behavioral_features(events_df)
        
        # Create rolling aggregations
        rolling_features = aggregation_engine.create_rolling_aggregations(events_df)
        cohort_features = aggregation_engine.create_cohort_features(events_df)
        
        # Combine all features
        final_features = customer_features \
            .join(time_features, "customer_id", "outer") \
            .join(behavioral_features, "customer_id", "outer") \
            .fillna(0)
        
        # Write to Delta Lake
        delta_manager.upsert_data(
            final_features,
            "/data/customer_features",
            "target.customer_id = source.customer_id",
            {col: f"source.{col}" for col in final_features.columns if col != "customer_id"}
        )
        
        logger.info("Feature engineering completed successfully")
        
    except Exception as e:
        logger.error("Feature engineering failed", error=str(e))
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
