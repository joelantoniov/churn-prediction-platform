from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
import structlog

logger = structlog.get_logger()

class CustomerEventSchema(BaseModel):
    customer_id: str = Field(..., min_length=1, max_length=50)
    timestamp: datetime
    event_type: str = Field(..., regex=r'^(page_view|purchase|support_ticket|login|logout)$')
    event_data: Dict[str, Any]
    schema_version: str = Field(default="1.0")
    
    class Config:
        extra = "forbid"

class EventValidator:
    def __init__(self):
        self.validation_errors = 0
        self.valid_events = 0
        
    def validate_event(self, event_data: Dict[str, Any]) -> Optional[CustomerEventSchema]:
        try:
            validated_event = CustomerEventSchema(**event_data)
            self.valid_events += 1
            return validated_event
        except ValidationError as e:
            self.validation_errors += 1
            logger.warning("Event validation failed", 
                         error=str(e), 
                         event_data=event_data)
            return None
    
    def get_validation_stats(self) -> Dict[str, int]:
        return {
            'valid_events': self.valid_events,
            'validation_errors': self.validation_errors,
            'total_processed': self.valid_events + self.validation_errors
        }
