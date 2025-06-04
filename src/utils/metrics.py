import time
from typing import Dict, Any
from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram, push_to_gateway
import psutil
import structlog
from config.kafka_config import KafkaConfig
from config.database_config import DatabaseConfig

logger = structlog.get_logger()

class MetricsCollector:
    def __init__(self):
        self.registry = CollectorRegistry()
        self._setup_metrics()
    
    def _setup_metrics(self):
        # System metrics
        self.cpu_usage = Gauge('system_cpu_usage_percent', 'CPU usage percentage', registry=self.registry)
        self.memory_usage = Gauge('system_memory_usage_percent', 'Memory usage percentage', registry=self.registry)
        self.disk_usage = Gauge('system_disk_usage_percent', 'Disk usage percentage', registry=self.registry)
        
        # Application metrics
        self.events_processed = Counter('events_processed_total', 'Total events processed', registry=self.registry)
        self.predictions_made = Counter('predictions_made_total', 'Total predictions made', registry=self.registry)
        self.model_accuracy = Gauge('model_accuracy_score', 'Current model accuracy', registry=self.registry)
        self.processing_time = Histogram('event_processing_seconds', 'Time spent processing events', registry=self.registry)
        
        # Business metrics
        self.churn_rate = Gauge('customer_churn_rate', 'Current customer churn rate', registry=self.registry)
        self.high_risk_customers = Gauge('high_risk_customers_count', 'Number of high-risk customers', registry=self.registry)
    
    async def collect_and_export_metrics(self):
        try:
            # Collect system metrics
            self.cpu_usage.set(psutil.cpu_percent())
            self.memory_usage.set(psutil.virtual_memory().percent)
            self.disk_usage.set(psutil.disk_usage('/').percent)
            
            # Collect application metrics
            await self._collect_application_metrics()
            
            # Push to Prometheus gateway (if configured)
            # push_to_gateway('localhost:9091', job='churn-prediction', registry=self.registry)
            
            logger.info("Metrics collected and exported successfully")
            
        except Exception as e:
            logger.error("Failed to collect metrics", error=str(e))
    
    async def _collect_application_metrics(self):
        try:
            from src.storage.sql_server_manager import SQLServerManager
            sql_manager = SQLServerManager()
            
            # Get recent predictions
            recent_predictions = sql_manager.execute_query("""
                SELECT 
                    COUNT(*) as total_predictions,
                    AVG(churn_probability) as avg_churn_score,
                    SUM(CASE WHEN churn_probability > 0.7 THEN 1 ELSE 0 END) as high_risk_count
                FROM churn_predictions 
                WHERE prediction_date >= DATEADD(hour, -1, GETUTCDATE())
            """)
            
            if not recent_predictions.empty:
                self.predictions_made.inc(recent_predictions['total_predictions'].iloc[0])
                self.churn_rate.set(recent_predictions['avg_churn_score'].iloc[0])
                self.high_risk_customers.set(recent_predictions['high_risk_count'].iloc[0])
                
        except Exception as e:
            logger.warning("Could not collect application metrics", error=str(e))
