import asyncio
from typing import Dict, Any, List
from datetime import datetime
import structlog
from config.kafka_config import KafkaConfig
from config.database_config import DatabaseConfig
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import sqlalchemy

logger = structlog.get_logger()

class HealthChecker:
    def __init__(self):
        self.checks = {
            'kafka': self._check_kafka,
            'database': self._check_database,
            'spark': self._check_spark,
            'storage': self._check_storage
        }
    
    async def check_all_systems(self) -> Dict[str, Any]:
        results = {}
        overall_healthy = True
        
        for system_name, check_func in self.checks.items():
            try:
                result = await check_func()
                results[system_name] = result
                if not result['healthy']:
                    overall_healthy = False
            except Exception as e:
                results[system_name] = {
                    'healthy': False,
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                }
                overall_healthy = False
        
        return {
            'healthy': overall_healthy,
            'timestamp': datetime.utcnow().isoformat(),
            'systems': results
        }
    
    async def _check_kafka(self) -> Dict[str, Any]:
        try:
            producer = KafkaProducer(**KafkaConfig.PRODUCER_CONFIG)
            producer.close()
            return {
                'healthy': True,
                'timestamp': datetime.utcnow().isoformat(),
                'message': 'Kafka connection successful'
            }
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def _check_database(self) -> Dict[str, Any]:
        try:
            engine = DatabaseConfig.get_engine()
            with engine.connect() as conn:
                result = conn.execute(sqlalchemy.text("SELECT 1"))
                result.fetchone()
            return {
                'healthy': True,
                'timestamp': datetime.utcnow().isoformat(),
                'message': 'Database connection successful'
            }
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def _check_spark(self) -> Dict[str, Any]:
        try:
            from config.spark_config import SparkConfig
            spark = SparkConfig.create_spark_session()
            spark.sql("SELECT 1").collect()
            spark.stop()
            return {
                'healthy': True,
                'timestamp': datetime.utcnow().isoformat(),
                'message': 'Spark session successful'
            }
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def _check_storage(self) -> Dict[str, Any]:
        try:
            from config.azure_config import AzureConfig
            blob_client = AzureConfig.get_blob_client()
            list(blob_client.list_containers(max_results=1))
            return {
                'healthy': True,
                'timestamp': datetime.utcnow().isoformat(),
                'message': 'Azure Storage connection successful'
            }
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
