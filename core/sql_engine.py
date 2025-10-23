"""
Enhanced SQL Engine for Synapse
Provides full SQL support including JOINs, GROUP BY, window functions, etc.
"""

import pandas as pd
import logging
from pandasql import sqldf
from typing import Dict, Any, Optional
import re

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class SQLEngine:
    """
    Advanced SQL execution engine with support for:
    - JOINs (all types)
    - GROUP BY with HAVING
    - Window functions
    - CTEs (WITH clauses)
    - Subqueries
    """
    
    def __init__(self, data_store: Dict[str, pd.DataFrame]):
        self.data_store = data_store
        
    def execute_sql(self, sql_query: str, output_var: str = None) -> pd.DataFrame:
        """
        Execute SQL query against the data store.
        
        Args:
            sql_query: SQL query string
            output_var: Optional variable name for result
            
        Returns:
            DataFrame result
        """
        try:
            # Clean and normalize the SQL
            sql_query = self._normalize_sql(sql_query)
            
            # Validate that referenced tables exist
            self._validate_table_references(sql_query)
            
            # Execute using pandasql
            result = sqldf(sql_query, self.data_store)
            
            if result is None or result.empty:
                logging.warning(f"SQL query returned no results")
                return pd.DataFrame()
                
            logging.info(f"SQL executed successfully: {len(result)} rows returned")
            return result
            
        except Exception as e:
            logging.error(f"SQL execution failed: {str(e)}")
            logging.error(f"Query: {sql_query}")
            raise SQLExecutionError(f"Failed to execute SQL: {str(e)}")
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL query for consistent execution."""
        # Remove extra whitespace
        sql = re.sub(r'\s+', ' ', sql.strip())
        return sql
    
    def _validate_table_references(self, sql: str):
        """Validate that all table references in SQL exist in data store."""
        # Extract table names from FROM and JOIN clauses
        pattern = r'(?:FROM|JOIN)\s+(\w+)'
        tables = re.findall(pattern, sql, re.IGNORECASE)
        
        missing_tables = [t for t in tables if t not in self.data_store]
        if missing_tables:
            raise SQLValidationError(
                f"Referenced tables not found in data store: {', '.join(missing_tables)}"
            )
    
    def execute_with_params(self, sql: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Execute parameterized SQL query.
        
        Args:
            sql: SQL with placeholders like {param_name}
            params: Dictionary of parameter values
            
        Returns:
            DataFrame result
        """
        # Simple parameter substitution (in production, use proper SQL parameterization)
        for key, value in params.items():
            if isinstance(value, str):
                sql = sql.replace(f"{{{key}}}", f"'{value}'")
            else:
                sql = sql.replace(f"{{{key}}}", str(value))
        
        return self.execute_sql(sql)
    
    def explain_query(self, sql: str) -> Dict[str, Any]:
        """
        Provide query explanation and statistics.
        
        Returns:
            Dictionary with query metadata
        """
        tables = re.findall(r'(?:FROM|JOIN)\s+(\w+)', sql, re.IGNORECASE)
        
        explanation = {
            'tables_accessed': list(set(tables)),
            'query_type': self._get_query_type(sql),
            'has_joins': 'JOIN' in sql.upper(),
            'has_groupby': 'GROUP BY' in sql.upper(),
            'has_window': any(fn in sql.upper() for fn in ['ROW_NUMBER', 'RANK', 'LAG', 'LEAD']),
        }
        
        return explanation
    
    def _get_query_type(self, sql: str) -> str:
        """Determine the type of SQL query."""
        sql_upper = sql.upper().strip()
        if sql_upper.startswith('SELECT'):
            return 'SELECT'
        elif sql_upper.startswith('WITH'):
            return 'CTE'
        else:
            return 'UNKNOWN'


class SQLValidationError(Exception):
    """Raised when SQL validation fails."""
    pass


class SQLExecutionError(Exception):
    """Raised when SQL execution fails."""
    pass


# --- Advanced SQL Helper Functions ---

def optimize_join(left: pd.DataFrame, right: pd.DataFrame, 
                  left_on: str, right_on: str, how: str = 'inner') -> pd.DataFrame:
    """
    Optimized join operation with automatic type handling.
    """
    # Ensure join columns have compatible types
    if left[left_on].dtype != right[right_on].dtype:
        logging.warning(f"Join column types differ: {left[left_on].dtype} vs {right[right_on].dtype}")
        # Try to convert to common type
        try:
            common_type = 'object' if any(d == 'object' for d in [left[left_on].dtype, right[right_on].dtype]) else 'int64'
            left[left_on] = left[left_on].astype(common_type)
            right[right_on] = right[right_on].astype(common_type)
        except:
            pass
    
    return pd.merge(left, right, left_on=left_on, right_on=right_on, how=how)


def apply_window_function(df: pd.DataFrame, partition_by: list, 
                          order_by: str, func_name: str, **kwargs) -> pd.Series:
    """
    Apply window functions like ROW_NUMBER, RANK, etc.
    
    Args:
        df: Input DataFrame
        partition_by: Columns to partition by
        order_by: Column to order by
        func_name: Window function name ('row_number', 'rank', 'dense_rank')
        
    Returns:
        Series with window function results
    """
    if func_name == 'row_number':
        return df.groupby(partition_by)[order_by].rank(method='first', ascending=True)
    elif func_name == 'rank':
        return df.groupby(partition_by)[order_by].rank(method='min', ascending=True)
    elif func_name == 'dense_rank':
        return df.groupby(partition_by)[order_by].rank(method='dense', ascending=True)
    else:
        raise ValueError(f"Unsupported window function: {func_name}")


# --- Example Usage ---
if __name__ == '__main__':
    # Test the SQL engine
    customers = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'segment': ['Premium', 'Standard', 'Premium']
    })
    
    orders = pd.DataFrame({
        'order_id': [101, 102, 103, 104],
        'customer_id': [1, 1, 2, 3],
        'amount': [100, 150, 200, 75]
    })
    
    data_store = {'customers': customers, 'orders': orders}
    engine = SQLEngine(data_store)
    
    # Test JOIN
    sql = """
        SELECT c.name, SUM(o.amount) as total_amount
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY c.name
        ORDER BY total_amount DESC
    """
    
    result = engine.execute_sql(sql)
    print(result)
    
    # Test query explanation
    explanation = engine.explain_query(sql)
    print(f"Query explanation: {explanation}")