import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any, List, Optional
import structlog
from config.database_config import DatabaseConfig

logger = structlog.get_logger()

class SQLServerManager:
    def __init__(self):
        self.engine = DatabaseConfig.get_engine()
        
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        try:
            return pd.read_sql(text(query), self.engine, params=params)
        except SQLAlchemyError as e:
            logger.error("Query execution failed", query=query, error=str(e))
            raise
    
    def insert_predictions(self, predictions_df: pd.DataFrame):
        try:
            predictions_df.to_sql(
                'churn_predictions',
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info("Inserted predictions", rows=len(predictions_df))
        except SQLAlchemyError as e:
            logger.error("Failed to insert predictions", error=str(e))
            raise
    
    def get_customer_features(self, customer_ids: List[str]) -> pd.DataFrame:
        query = """
        SELECT 
            customer_id,
            total_events,
            unique_event_types,
            purchase_count,
            page_view_count,
            support_ticket_count,
            avg_purchase_amount,
            total_purchase_amount,
            days_since_last_activity,
            customer_lifetime_days,
            events_per_day,
            purchase_frequency
        FROM customer_features 
        WHERE customer_id IN :customer_ids
        """
        return self.execute_query(query, {'customer_ids': tuple(customer_ids)})
    
    def create_tables(self):
        create_statements = [
            """
            CREATE TABLE IF NOT EXISTS churn_predictions (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                customer_id NVARCHAR(50) NOT NULL,
                churn_probability FLOAT NOT NULL,
                prediction_date DATETIME2 DEFAULT GETUTCDATE(),
                model_version NVARCHAR(20) DEFAULT '1.0',
                features NVARCHAR(MAX),
                INDEX IX_customer_date (customer_id, prediction_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS customer_features (
                customer_id NVARCHAR(50) PRIMARY KEY,
                total_events INT DEFAULT 0,
                unique_event_types INT DEFAULT 0,
                purchase_count INT DEFAULT 0,
                page_view_count INT DEFAULT 0,
                support_ticket_count INT DEFAULT 0,
                avg_purchase_amount FLOAT DEFAULT 0,
                total_purchase_amount FLOAT DEFAULT 0,
                days_since_last_activity INT DEFAULT 0,
                customer_lifetime_days INT DEFAULT 0,
                events_per_day FLOAT DEFAULT 0,
                purchase_frequency FLOAT DEFAULT 0,
                last_updated DATETIME2 DEFAULT GETUTCDATE()
            )
            """
        ]
        
        for statement in create_statements:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(statement))
                    conn.commit()
                logger.info("Created table successfully")
            except SQLAlchemyError as e:
                logger.error("Failed to create table", error=str(e))
