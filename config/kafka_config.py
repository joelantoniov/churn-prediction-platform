import os
from typing import Dict, Any

class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    CUSTOMER_EVENTS_TOPIC = 'customer-events'
    CHURN_PREDICTIONS_TOPIC = 'churn-predictions'
    CONSUMER_GROUP = 'churn-prediction-consumer'
    
    PRODUCER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
        'key_serializer': lambda x: x.encode('utf-8') if x else None,
        'acks': 'all',
        'retries': 3,
        'batch_size': 16384,
        'linger_ms': 100,
        'compression_type': 'snappy'
    }
    
    CONSUMER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'group_id': CONSUMER_GROUP,
        'auto_offset_reset': 'latest',
        'enable_auto_commit': False,
        'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
        'key_deserializer': lambda x: x.decode('utf-8') if x else None,
        'max_poll_records': 1000,
        'session_timeout_ms': 30000
    }
