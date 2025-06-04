import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus

class DatabaseConfig:
    SERVER = os.getenv('SQL_SERVER_HOST', 'localhost')
    DATABASE = os.getenv('SQL_SERVER_DATABASE', 'churn_predictions')
    USERNAME = os.getenv('SQL_SERVER_USERNAME')
    PASSWORD = os.getenv('SQL_SERVER_PASSWORD')
    DRIVER = 'ODBC Driver 17 for SQL Server'
    
    @staticmethod
    def get_connection_string():
        return f"mssql+pyodbc://{DatabaseConfig.USERNAME}:{quote_plus(DatabaseConfig.PASSWORD)}@{DatabaseConfig.SERVER}:1433/{DatabaseConfig.DATABASE}?driver={quote_plus(DatabaseConfig.DRIVER)}"
    
    @staticmethod
    def get_engine():
        return create_engine(
            DatabaseConfig.get_connection_string(),
            pool_size=20,
            max_overflow=30,
            pool_pre_ping=True,
            pool_recycle=3600
        )
