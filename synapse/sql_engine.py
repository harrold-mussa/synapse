"""
SQL Engine - Handles SQL templating with Python embedding and DuckDB integration.
"""

import re
import duckdb
import pandas as pd
from typing import Any, Dict, Optional, Union
from datetime import datetime, date
import json


class SQLTemplate:
    """
    SQL template engine that allows embedding Python expressions within SQL.
    
    Syntax:
        {variable} - Simple variable substitution (safely parameterized)
        {python: expression} - Execute Python expression and embed result
        {df: dataframe_name} - Reference a DataFrame as a virtual table
    """
    
    def __init__(self, template: str):
        self.template = template
        self.pattern_var = re.compile(r'\{(\w+)\}')
        self.pattern_python = re.compile(r'\{python:\s*([^}]+)\}')
        self.pattern_df = re.compile(r'\{df:\s*(\w+)\}')
    
    def render(self, context: Dict[str, Any] = None, dataframes: Dict[str, pd.DataFrame] = None) -> str:
        """Render the SQL template with given context and dataframes."""
        context = context or {}
        dataframes = dataframes or {}
        
        sql = self.template
        
        # Replace Python expressions first
        def replace_python(match):
            expr = match.group(1).strip()
            try:
                result = eval(expr, {"__builtins__": {}}, context)
                return self._format_value(result)
            except Exception as e:
                raise ValueError(f"Error evaluating Python expression '{expr}': {e}")
        
        sql = self.pattern_python.sub(replace_python, sql)
        
        # Replace DataFrame references
        def replace_df(match):
            df_name = match.group(1).strip()
            if df_name not in dataframes:
                raise ValueError(f"DataFrame '{df_name}' not found in context")
            return f"__df_{df_name}__"
        
        sql = self.pattern_df.sub(replace_df, sql)
        
        # Replace simple variables (safely parameterized)
        def replace_var(match):
            var_name = match.group(1)
            if var_name not in context:
                raise ValueError(f"Variable '{var_name}' not found in context")
            return self._format_value(context[var_name])
        
        sql = self.pattern_var.sub(replace_var, sql)
        
        return sql
    
    def _format_value(self, value: Any) -> str:
        """Format a Python value for SQL insertion."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Escape single quotes for SQL
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, (datetime, date)):
            return f"'{value.isoformat()}'"
        elif isinstance(value, (list, tuple)):
            formatted = ", ".join(self._format_value(v) for v in value)
            return f"({formatted})"
        elif isinstance(value, dict):
            return f"'{json.dumps(value)}'"
        else:
            return f"'{str(value)}'"


def sql(template: str) -> SQLTemplate:
    """
    Create a SQL template with Python embedding support.
    
    Example:
        query = sql('''
            SELECT * FROM users 
            WHERE created_at > {cutoff_date}
            AND status IN {python: get_active_statuses()}
        ''')
        result = query.render({'cutoff_date': '2024-01-01'})
    """
    return SQLTemplate(template)


class DuckDBEngine:
    """
    DuckDB-based SQL execution engine with DataFrame support.
    """
    
    def __init__(self, database: str = ":memory:"):
        self.conn = duckdb.connect(database)
        self._registered_dfs = {}
    
    def execute(self, 
                query: Union[str, SQLTemplate], 
                context: Dict[str, Any] = None,
                dataframes: Dict[str, pd.DataFrame] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.
        
        Args:
            query: SQL string or SQLTemplate
            context: Variables to substitute in the template
            dataframes: DataFrames to make available as tables
        """
        context = context or {}
        dataframes = dataframes or {}
        
        # Render template if needed
        if isinstance(query, SQLTemplate):
            sql_string = query.render(context, dataframes)
        else:
            sql_string = query
        
        # Register DataFrames as virtual tables
        for name, df in dataframes.items():
            table_name = f"__df_{name}__"
            self.conn.register(table_name, df)
            self._registered_dfs[table_name] = df
        
        # Execute query
        try:
            result = self.conn.execute(sql_string).fetchdf()
            return result
        finally:
            # Clean up registered DataFrames
            for table_name in list(self._registered_dfs.keys()):
                try:
                    self.conn.unregister(table_name)
                except:
                    pass
                del self._registered_dfs[table_name]
    
    def execute_many(self, queries: list) -> list:
        """Execute multiple queries and return all results."""
        results = []
        for query in queries:
            if isinstance(query, tuple):
                q, ctx, dfs = query
                results.append(self.execute(q, ctx, dfs))
            else:
                results.append(self.execute(query))
        return results
    
    def create_table(self, name: str, df: pd.DataFrame, if_exists: str = "replace"):
        """Create a permanent table from a DataFrame."""
        if if_exists == "replace":
            self.conn.execute(f"DROP TABLE IF EXISTS {name}")
        self.conn.register("__temp_df__", df)
        self.conn.execute(f"CREATE TABLE {name} AS SELECT * FROM __temp_df__")
        self.conn.unregister("__temp_df__")
    
    def read_table(self, name: str) -> pd.DataFrame:
        """Read a table as a DataFrame."""
        return self.conn.execute(f"SELECT * FROM {name}").fetchdf()
    
    def list_tables(self) -> list:
        """List all tables in the database."""
        result = self.conn.execute("SHOW TABLES").fetchdf()
        return result['name'].tolist() if not result.empty else []
    
    def close(self):
        """Close the database connection."""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Global engine instance for convenience
_default_engine = None

def get_engine(database: str = ":memory:") -> DuckDBEngine:
    """Get or create the default DuckDB engine."""
    global _default_engine
    if _default_engine is None:
        _default_engine = DuckDBEngine(database)
    return _default_engine


def execute_sql(query: Union[str, SQLTemplate], 
                context: Dict[str, Any] = None,
                dataframes: Dict[str, pd.DataFrame] = None) -> pd.DataFrame:
    """Execute SQL using the default engine."""
    return get_engine().execute(query, context, dataframes)
