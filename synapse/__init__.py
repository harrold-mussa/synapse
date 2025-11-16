"""
synapse - A SQL-Python Hybrid Framework for Data Engineering
Combines the power of SQL for data manipulation with Python for orchestration.
"""

from .pipeline import Pipeline
from .sql_engine import sql, SQLTemplate
from .transforms import Transform, Extract, Load
from .schema import Schema, Column
from .config import Config

__version__ = "0.1.0"
__all__ = ["Pipeline", "sql", "SQLTemplate", "Transform", "Extract", "Load", "Schema", "Column", "Config"]
