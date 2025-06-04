import json
import time
from datetime import datetime
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import structlog
from config.kafka_config import KafkaConfig

logger = structlog.get_logger()

class CustomerEventProducer:
    def __init__(self):
        self.producer = KafkaProducer(**KafkaConfig.PRODUCER_CONFIG)
        self.events_sent = 0
        
    def send_event(self, customer_id: str, event_data: Dict[Any, Any]) -> bool:
        try:
            event_payload = {
                'customer_id': customer_id,
                'timestamp': datetime.utcnow().isoformat(),
                'event_data': event_data,
                'schema_version': '1.0'
            }
            
            future = self.producer.send(
                KafkaConfig.CUSTOMER_EVENTS_TOPIC,
                key=customer_id,
                value=event_payload
            )
            
            future.add_callback(self._success_callback)
            future.add_errback(self._error_callback)
            
            self.events_sent += 1
            return True
            
        except Exception as e:
            logger.error("Failed to send event", customer_id=customer_id, error=str(e))
            return False
    
    def _success_callback(self, metadata):
        logger.debug("Event sent successfully", 
                    topic=metadata.topic, 
                    partition=metadata.partition, 
                    offset=metadata.offset)
    
    def _error_callback(self, exception):
        logger.error("Failed to send event", error=str(exception))
    
    def close(self):
        self.producer.flush()
        self.producer.close()
