import pandas as pd
import logging

def calculate_metrics(df: pd.DataFrame, revenue_col: str = 'revenue') -> pd.DataFrame:
    """
    Calculates and logs the total and average revenue for the given DataFrame.
    
    This function demonstrates the RUN ... WITH PYTHON capability.
    
    Args:
        df: The input DataFrame passed from the Synapse Executor.
        revenue_col: The name of the column containing revenue data.
        
    Returns:
        The input DataFrame (as is, after performing side effects like logging).
    """
    if revenue_col in df.columns and pd.api.types.is_numeric_dtype(df[revenue_col]):
        total_rev = df[revenue_col].sum()
        avg_rev = df[revenue_col].mean()
        
        logging.info(f"-> Custom Metric: Total {revenue_col}: ${total_rev:,.2f}")
        logging.info(f"-> Custom Metric: Average {revenue_col}: ${avg_rev:,.2f}")
    else:
        logging.warning(f"-> Custom Metric Warning: Column '{revenue_col}' not found or is not numeric.")
        
    return df