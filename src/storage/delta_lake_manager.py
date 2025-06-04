from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from typing import Optional, Dict, Any
import structlog

logger = structlog.get_logger()

class DeltaLakeManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def create_table_if_not_exists(self, df: DataFrame, table_path: str, partition_cols: Optional[list] = None):
        try:
            DeltaTable.forPath(self.spark, table_path)
            logger.info("Delta table already exists", path=table_path)
        except:
            writer = df.write.format("delta")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(table_path)
            logger.info("Created new Delta table", path=table_path)
    
    def upsert_data(self, df: DataFrame, table_path: str, merge_condition: str, update_cols: Dict[str, str]):
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(set=update_cols) \
         .whenNotMatchedInsertAll() \
         .execute()
        
        logger.info("Upserted data to Delta table", path=table_path, rows=df.count())
    
    def read_table(self, table_path: str, version: Optional[int] = None) -> DataFrame:
        reader = self.spark.read.format("delta")
        if version:
            reader = reader.option("versionAsOf", version)
        return reader.load(table_path)
    
    def optimize_table(self, table_path: str):
        delta_table = DeltaTable.forPath(self.spark, table_path)
        delta_table.optimize().executeCompaction()
        logger.info("Optimized Delta table", path=table_path)
    
    def vacuum_table(self, table_path: str, retention_hours: int = 168):
        delta_table = DeltaTable.forPath(self.spark, table_path)
        delta_table.vacuum(retention_hours)
        logger.info("Vacuumed Delta table", path=table_path, retention_hours=retention_hours)
