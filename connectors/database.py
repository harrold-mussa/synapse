"""
Database connectors for Synapse
Supports PostgreSQL, MySQL, SQLite, Snowflake, BigQuery, Redshift
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
from urllib.parse import urlparse, parse_qs
import os

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class DatabaseConnector(ABC):
    """Abstract base class for database connectors."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.parsed_url = urlparse(connection_string)
        self.connection = None
        
    @abstractmethod
    def connect(self):
        """Establish database connection."""
        pass
    
    @abstractmethod
    def load(self, query: str) -> pd.DataFrame:
        """Load data from database."""
        pass
    
    @abstractmethod
    def save(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        """Save data to database."""
        pass
    
    @abstractmethod
    def close(self):
        """Close database connection."""
        pass
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector."""
    
    def connect(self):
        try:
            import psycopg2
            from sqlalchemy import create_engine
            
            self.engine = create_engine(self.connection_string)
            logging.info(f"Connected to PostgreSQL: {self.parsed_url.hostname}")
        except ImportError:
            raise ImportError("psycopg2 and sqlalchemy required for PostgreSQL. Install with: pip install psycopg2-binary sqlalchemy")
    
    def load(self, query: str) -> pd.DataFrame:
        """Load data using SQL query."""
        try:
            df = pd.read_sql(query, self.engine)
            logging.info(f"Loaded {len(df)} rows from PostgreSQL")
            return df
        except Exception as e:
            logging.error(f"Failed to load from PostgreSQL: {e}")
            raise
    
    def save(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        """Save DataFrame to PostgreSQL table."""
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logging.info(f"Saved {len(df)} rows to PostgreSQL table '{table_name}'")
        except Exception as e:
            logging.error(f"Failed to save to PostgreSQL: {e}")
            raise
    
    def close(self):
        if self.engine:
            self.engine.dispose()
            logging.info("PostgreSQL connection closed")


class MySQLConnector(DatabaseConnector):
    """MySQL database connector."""
    
    def connect(self):
        try:
            import pymysql
            from sqlalchemy import create_engine
            
            # Replace mysql:// with mysql+pymysql://
            connection_str = self.connection_string.replace('mysql://', 'mysql+pymysql://')
            self.engine = create_engine(connection_str)
            logging.info(f"Connected to MySQL: {self.parsed_url.hostname}")
        except ImportError:
            raise ImportError("pymysql and sqlalchemy required. Install with: pip install pymysql sqlalchemy")
    
    def load(self, query: str) -> pd.DataFrame:
        df = pd.read_sql(query, self.engine)
        logging.info(f"Loaded {len(df)} rows from MySQL")
        return df
    
    def save(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        logging.info(f"Saved {len(df)} rows to MySQL table '{table_name}'")
    
    def close(self):
        if self.engine:
            self.engine.dispose()


class SQLiteConnector(DatabaseConnector):
    """SQLite database connector."""
    
    def connect(self):
        import sqlite3
        from sqlalchemy import create_engine
        
        # Extract database path from URL
        db_path = self.parsed_url.path.lstrip('/')
        self.engine = create_engine(f'sqlite:///{db_path}')
        logging.info(f"Connected to SQLite: {db_path}")
    
    def load(self, query: str) -> pd.DataFrame:
        df = pd.read_sql(query, self.engine)
        logging.info(f"Loaded {len(df)} rows from SQLite")
        return df
    
    def save(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        logging.info(f"Saved {len(df)} rows to SQLite table '{table_name}'")
    
    def close(self):
        if self.engine:
            self.engine.dispose()


class SnowflakeConnector(DatabaseConnector):
    """Snowflake data warehouse connector."""
    
    def connect(self):
        try:
            from snowflake.sqlalchemy import URL
            from sqlalchemy import create_engine
            
            # Parse Snowflake connection string
            # Format: snowflake://user:password@account/database/schema?warehouse=wh&role=role
            query_params = parse_qs(self.parsed_url.query)
            
            self.engine = create_engine(URL(
                account=self.parsed_url.hostname,
                user=self.parsed_url.username,
                password=self.parsed_url.password,
                database=self.parsed_url.path.split('/')[1] if len(self.parsed_url.path.split('/')) > 1 else None,
                schema=self.parsed_url.path.split('/')[2] if len(self.parsed_url.path.split('/')) > 2 else None,
                warehouse=query_params.get('warehouse', [None])[0],
                role=query_params.get('role', [None])[0]
            ))
            logging.info(f"Connected to Snowflake: {self.parsed_url.hostname}")
        except ImportError:
            raise ImportError("snowflake-connector-python and snowflake-sqlalchemy required. Install with: pip install snowflake-connector-python snowflake-sqlalchemy")
    
    def load(self, query: str) -> pd.DataFrame:
        df = pd.read_sql(query, self.engine)
        logging.info(f"Loaded {len(df)} rows from Snowflake")
        return df
    
    def save(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, method='multi', chunksize=16000)
        logging.info(f"Saved {len(df)} rows to Snowflake table '{table_name}'")
    
    def close(self):
        if self.engine:
            self.engine.dispose()


class BigQueryConnector(DatabaseConnector):
    """Google BigQuery connector."""
    
    def connect(self):
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            
            # Parse project_id from URL
            # Format: bigquery://project_id/dataset_id or with credentials
            self.project_id = self.parsed_url.hostname or os.getenv('GCP_PROJECT_ID')
            
            # Check for credentials in query params or environment
            query_params = parse_qs(self.parsed_url.query)
            credentials_path = query_params.get('credentials', [None])[0] or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                self.client = bigquery.Client(project=self.project_id, credentials=credentials)
            else:
                self.client = bigquery.Client(project=self.project_id)
            
            logging.info(f"Connected to BigQuery project: {self.project_id}")
        except ImportError:
            raise ImportError("google-cloud-bigquery required. Install with: pip install google-cloud-bigquery")
    
    def load(self, query: str) -> pd.DataFrame:
        df = self.client.query(query).to_dataframe()
        logging.info(f"Loaded {len(df)} rows from BigQuery")
        return df
    
    def save(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        # Parse dataset and table from table_name (format: dataset.table)
        if '.' in table_name:
            dataset_id, table_id = table_name.split('.', 1)
        else:
            # Use dataset from URL path or default
            dataset_id = self.parsed_url.path.lstrip('/').split('/')[0] if self.parsed_url.path else 'default_dataset'
            table_id = table_name
        
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition='WRITE_TRUNCATE' if if_exists == 'replace' else 'WRITE_APPEND'
        )
        
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for completion
        
        logging.info(f"Saved {len(df)} rows to BigQuery table '{table_ref}'")
    
    def close(self):
        if self.client:
            self.client.close()


class RedshiftConnector(PostgreSQLConnector):
    """
    Amazon Redshift connector.
    Inherits from PostgreSQL connector since Redshift is PostgreSQL-compatible.
    """
    
    def connect(self):
        try:
            import psycopg2
            from sqlalchemy import create_engine
            
            # Redshift uses PostgreSQL protocol
            connection_str = self.connection_string.replace('redshift://', 'postgresql://')
            self.engine = create_engine(connection_str)
            logging.info(f"Connected to Redshift: {self.parsed_url.hostname}")
        except ImportError:
            raise ImportError("psycopg2 required for Redshift. Install with: pip install psycopg2-binary sqlalchemy")


# --- Connector Factory ---

def get_connector(connection_string: str) -> DatabaseConnector:
    """
    Factory function to get the appropriate connector based on connection string.
    
    Args:
        connection_string: Database connection URL
        
    Returns:
        Appropriate DatabaseConnector instance
        
    Examples:
        postgresql://user:pass@localhost/dbname
        mysql://user:pass@localhost/dbname
        sqlite:///path/to/database.db
        snowflake://user:pass@account/database/schema?warehouse=wh
        bigquery://project_id/dataset_id
        redshift://user:pass@cluster.region.redshift.amazonaws.com:5439/dbname
    """
    scheme = connection_string.split('://')[0].lower()
    
    connectors = {
        'postgresql': PostgreSQLConnector,
        'postgres': PostgreSQLConnector,
        'mysql': MySQLConnector,
        'sqlite': SQLiteConnector,
        'snowflake': SnowflakeConnector,
        'bigquery': BigQueryConnector,
        'redshift': RedshiftConnector
    }
    
    connector_class = connectors.get(scheme)
    if not connector_class:
        raise ValueError(f"Unsupported database type: {scheme}")
    
    return connector_class(connection_string)


# --- Enhanced Load/Save Functions ---

def load_from_database(connection_string: str, query: str = None, table: str = None) -> pd.DataFrame:
    """
    Load data from any supported database.
    
    Args:
        connection_string: Database URL
        query: SQL query to execute (optional)
        table: Table name to load (optional, used if query not provided)
        
    Returns:
        DataFrame with loaded data
    """
    if not query and not table:
        raise ValueError("Either 'query' or 'table' must be provided")
    
    if table and not query:
        query = f"SELECT * FROM {table}"
    
    with get_connector(connection_string) as conn:
        return conn.load(query)


def save_to_database(df: pd.DataFrame, connection_string: str, table: str, if_exists: str = 'replace'):
    """
    Save DataFrame to any supported database.
    
    Args:
        df: DataFrame to save
        connection_string: Database URL
        table: Target table name
        if_exists: 'replace', 'append', or 'fail'
    """
    with get_connector(connection_string) as conn:
        conn.save(df, table, if_exists=if_exists)


# --- Example Usage ---
if __name__ == '__main__':
    # Example: PostgreSQL
    conn_str = "postgresql://user:password@localhost:5432/mydb"
    
    # Load data
    df = load_from_database(conn_str, query="SELECT * FROM customers LIMIT 100")
    print(f"Loaded {len(df)} rows")
    
    # Save data
    save_to_database(df, conn_str, table="customers_backup", if_exists='replace')
    
    # Example with context manager
    with get_connector(conn_str) as conn:
        df = conn.load("SELECT * FROM orders WHERE status = 'active'")
        # Process df
        conn.save(df, "processed_orders")