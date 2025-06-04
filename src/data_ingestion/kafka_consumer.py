import json
from typing import Callable, Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import structlog
from config.kafka_config import KafkaConfig

logger = structlog.get_logger()

class CustomerEventConsumer:
    def __init__(self, message_handler: Callable[[Dict[str, Any]], None]):
        self.consumer = KafkaConsumer(
            KafkaConfig.CUSTOMER_EVENTS_TOPIC,
            **KafkaConfig.CONSUMER_CONFIG
        )
        self.message_handler = message_handler
        self.processed_count = 0
        
    def start_consuming(self):
        logger.info("Starting consumer", topic=KafkaConfig.CUSTOMER_EVENTS_TOPIC)
        
        try:
            for message in self.consumer:
                try:
                    self.message_handler(message.value)
                    self.consumer.commit()
                    self.processed_count += 1
                    
                    if self.processed_count % 1000 == 0:
                        logger.info("Processed messages", count=self.processed_count)
                        
                except Exception as e:
                    logger.error("Error processing message", error=str(e), 
                               message=message.value)
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            self.consumer.close()
