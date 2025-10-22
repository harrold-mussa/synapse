import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def load_data(source_path: str) -> pd.DataFrame:
    """
    Dynamically loads data from various sources (currently supports CSV and Parquet).
    In a production system, this would handle S3, databases, etc.
    """
    source_path_lower = source_path.lower()
    
    if source_path_lower.endswith('.csv'):
        # Read CSV with common data engineering settings
        return pd.read_csv(source_path)
    elif source_path_lower.endswith('.parquet'):
        return pd.read_parquet(source_path)
    # elif source_path_lower.startswith('s3://'):
    #     # Placeholder for S3 integration (requires s3fs)
    #     pass
    else:
        raise ValueError(f"Synapse Error: Unsupported data source type for path: {source_path}")


def save_data(df: pd.DataFrame, destination: str):
    """
    Dynamically saves data to various destinations (currently supports CSV and Parquet).
    """
    destination_lower = destination.lower()
    
    if destination_lower.endswith('.csv'):
        # Save CSV without the Pandas index column
        df.to_csv(destination, index=False)
    elif destination_lower.endswith('.parquet'):
        df.to_parquet(destination, index=False)
    # elif destination_lower.startswith('postgres://'):
    #     # Placeholder for Database integration (requires sqlalchemy)
    #     pass
    else:
        raise ValueError(f"Synapse Error: Unsupported destination type for path: {destination}")